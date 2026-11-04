"""Partie 5 — Pipeline de production Kafka -> Silver (Parquet) -> Gold (Delta Lake).

Requêtes (chacune avec son checkpoint sous lake/checkpoints/) :
  silver          : dédoublonnage (watermark 10 min) + enrichissement merchants + amount_band
                    -> lake/silver/transactions_clean/ (Parquet, partitions city/date)
  gold_tumbling   : fenêtres tumbling 10 min par ville -> Delta lake/gold/kpi_city_10min/ (MERGE idempotent)
  gold_sliding    : fenêtres glissantes 10 min / 5 min, taux de rejet par ville -> Delta (MERGE idempotent)
  gold_sessions   : session_window (5 min d'inactivité) par device_id -> Delta (append)

docker compose exec jupyter spark-submit /opt/project/src/part5_pipeline.py
(Ctrl-C pour arrêter ; relancer la même commande reprend aux offsets du checkpoint.)
"""
import argparse
import json
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common import (GOLD_KPI_SLIDING, GOLD_KPI_TUMBLING, GOLD_SESSIONS, KAFKA_BOOTSTRAP, SILVER_CLEAN, checkpoint,
                    enrich, get_spark, parse_kafka, read_kafka_stream, read_merchants, section)

ALL_QUERIES = ["silver", "gold_tumbling", "gold_sliding", "gold_sessions"]
TERMINAL = ["COMPLETED", "REJECTED"]
MONITOR_GROUP = "sahlpay-pipeline-silver"


class KafkaOffsetPublisher(StreamingQueryListener):
    """Recopie, après chaque micro-batch de la requête silver, ses offsets de fin dans le consumer group
    MONITOR_GROUP pour que `kafka-consumer-groups.sh --describe` affiche position et LAG.

    Spark ne commite jamais ses offsets dans Kafka (le checkpoint fait foi) : ce groupe sert uniquement
    à la supervision et n'est jamais relu par le pipeline."""

    def __init__(self, query_name):
        from kafka import KafkaConsumer
        self.query_name = query_name
        self.consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=MONITOR_GROUP,
                                      enable_auto_commit=False)

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        progress = event.progress
        if progress.name != self.query_name or not progress.sources:
            return
        end = json.loads(progress.sources[0].endOffset or "{}")     # {"transactions": {"0": 1234, ...}}
        if not isinstance(end, dict):
            return
        offsets = {TopicPartition(topic, int(p)): OffsetAndMetadata(o, "")
                   for topic, parts in end.items() for p, o in parts.items()}
        if offsets:
            self.consumer.commit(offsets)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def is_status(*statuses):
    return F.when(F.col("status").isin(*statuses), 1).otherwise(0)


def delta_upsert(spark, path, keys):
    """foreachBatch : MERGE du micro-batch dans la table Delta sur `keys`.

    Rejouer un batch (reprise après panne) réécrit les mêmes valeurs -> sink idempotent."""
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    def upsert(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df = batch_df.withColumn("batch_id", F.lit(batch_id)).withColumn("updated_at", F.current_timestamp())
        if not DeltaTable.isDeltaTable(spark, path):
            batch_df.write.format("delta").mode("append").save(path)
            return
        (DeltaTable.forPath(spark, path).alias("t")
            .merge(batch_df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    return upsert


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=",".join(ALL_QUERIES))
    ap.add_argument("--starting-offsets", default="earliest", help="utilisé seulement au premier démarrage")
    ap.add_argument("--max-offsets", type=int, default=5000, help="maxOffsetsPerTrigger")
    ap.add_argument("--trigger", default="20 seconds")
    ap.add_argument("--timeout", type=int, default=0, help="arrêt automatique après N secondes (0 = Ctrl-C)")
    args = ap.parse_args()
    wanted = args.queries.split(",")

    spark = get_spark("SahlPay-Partie5-Pipeline")
    merchants = read_merchants(spark).cache()

    def clean_stream():
        """Kafka -> colonnes typées -> watermark -> dédoublonnage (tx_id, status).

        dropDuplicatesWithinWatermark (Spark 3.5) : la clé reste (tx_id, status) et l'état est purgé
        une fois le watermark dépassé ; un dropDuplicates(["tx_id","status"]) simple garderait toutes
        les clés indéfiniment."""
        raw = read_kafka_stream(spark, args.starting_offsets, args.max_offsets, group_prefix="sahlpay-pipeline")
        return (parse_kafka(raw)
                .withWatermark("event_time", "10 minutes")
                .dropDuplicatesWithinWatermark(["tx_id", "status"]))

    queries = {}

    if "silver" in wanted:
        spark.streams.addListener(KafkaOffsetPublisher("silver"))
        silver = enrich(clean_stream(), merchants).withColumn("ingested_at", F.current_timestamp())
        queries["silver"] = (silver.writeStream.queryName("silver").format("parquet")
            .option("path", SILVER_CLEAN)
            .option("checkpointLocation", checkpoint("silver_transactions_clean"))
            .partitionBy("city", "date").outputMode("append")
            .trigger(processingTime=args.trigger).start())

    if "gold_tumbling" in wanted:
        tumbling = (clean_stream()
            .groupBy(F.window("event_time", "10 minutes"), "city")
            .agg(F.sum(is_status(*TERMINAL)).alias("nb_transactions"),
                 F.sum(is_status("COMPLETED")).alias("nb_completed"),
                 F.sum(is_status("REJECTED")).alias("nb_rejected"),
                 F.round(F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount_mad")).otherwise(0.0)), 2)
                  .alias("montant_total_mad"),
                 F.count("*").alias("nb_evenements"))
            .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                    "city", "nb_transactions", "nb_completed", "nb_rejected", "montant_total_mad", "nb_evenements"))
        queries["gold_tumbling"] = (tumbling.writeStream.queryName("gold_tumbling")
            .outputMode("update")
            .foreachBatch(delta_upsert(spark, GOLD_KPI_TUMBLING, ["city", "window_start"]))
            .option("checkpointLocation", checkpoint("gold_kpi_city_10min"))
            .trigger(processingTime=args.trigger).start())

    if "gold_sliding" in wanted:
        sliding = (clean_stream()
            .groupBy(F.window("event_time", "10 minutes", "5 minutes"), "city")
            .agg(F.sum(is_status(*TERMINAL)).alias("nb_transactions"),
                 F.sum(is_status("REJECTED")).alias("nb_rejected"))
            .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                    "city", "nb_transactions", "nb_rejected",
                    F.round(F.col("nb_rejected") / F.greatest(F.col("nb_transactions"), F.lit(1)), 4)
                     .alias("taux_rejet")))
        queries["gold_sliding"] = (sliding.writeStream.queryName("gold_sliding")
            .outputMode("update")
            .foreachBatch(delta_upsert(spark, GOLD_KPI_SLIDING, ["city", "window_start"]))
            .option("checkpointLocation", checkpoint("gold_kpi_city_reject_rate_sliding"))
            .trigger(processingTime=args.trigger).start())

    if "gold_sessions" in wanted:
        sessions = (clean_stream()
            .groupBy(F.session_window("event_time", "5 minutes"), "device_id")
            .agg(F.sum(is_status("INITIATED")).alias("nb_transactions"),
                 F.count("*").alias("nb_evenements"),
                 F.approx_count_distinct("payer_wallet").alias("nb_comptes"),
                 F.min("event_time").alias("premier_evenement"),
                 F.max("event_time").alias("dernier_evenement"))
            .select(F.col("session_window.start").alias("session_start"),
                    F.col("session_window.end").alias("session_end"),
                    "device_id", "nb_transactions", "nb_evenements", "nb_comptes",
                    (F.col("dernier_evenement").cast("long") - F.col("premier_evenement").cast("long"))
                     .alias("duree_activite_s"),
                    (F.col("session_window.end").cast("long") - F.col("session_window.start").cast("long"))
                     .alias("duree_session_s")))
        queries["gold_sessions"] = (sessions.writeStream.queryName("gold_sessions").format("delta")
            .outputMode("append")                   # une session n'est émise qu'une fois close par le watermark
            .option("checkpointLocation", checkpoint("gold_device_sessions"))
            .trigger(processingTime=args.trigger).start(GOLD_SESSIONS))

    section(f"Pipeline démarré : {', '.join(queries)} (Ctrl-C pour arrêter)")
    start = time.time()
    try:
        while not args.timeout or time.time() - start < args.timeout:
            time.sleep(30)
            for name, q in queries.items():
                if q.exception():
                    raise q.exception()
                p = q.lastProgress
                if p:
                    offsets = p["sources"][0].get("endOffset") if p.get("sources") else None
                    print(f"[{name:<14}] batch={p['batchId']:<5} lignes={p['numInputRows']:<6} "
                          f"debit={p.get('processedRowsPerSecond', 0):.1f} l/s "
                          f"duree={p['durationMs'].get('triggerExecution', 0)} ms "
                          f"watermark={p.get('eventTime', {}).get('watermark')} offsets={offsets}")
    except KeyboardInterrupt:
        pass
    finally:
        print("Arret propre des requetes (le prochain demarrage reprend aux offsets du checkpoint)")
        for q in queries.values():
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
