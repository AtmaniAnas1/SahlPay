"""Partie 4 — Structured Streaming depuis Kafka (topic transactions).

Requêtes lancées dans la même application :
  raw_console   : flux typé -> console (append)
  bronze_raw    : flux brut -> lake/bronze/transactions_raw/ (JSON, checkpoint dédié)
  city_status   : nombre d'événements par ville et statut -> console (complete)
  watermark     : fenêtres de 5 min par ville avec watermark de 10 min -> console (update)
  late_detector : affiche les événements arrivés avec plus de 10 min de retard et le watermark courant

Lancer en parallèle : python producer/producer.py --mode kafka --rate 20
docker compose exec jupyter spark-submit /opt/project/src/part4_streaming.py [--queries raw_console,bronze_raw]
"""
import argparse
import time
from datetime import datetime

from pyspark.sql import functions as F

from common import BRONZE_RAW, checkpoint, get_spark, parse_kafka, read_kafka_stream, save_text, section

ALL_QUERIES = ["raw_console", "bronze_raw", "city_status", "watermark", "late_detector"]
WATERMARK = {"value": None}          # dernier watermark publié par la requête "watermark"
LATE_LOG = []


def late_detector(batch_df, batch_id):
    """foreachBatch : événements dont event_time est antérieur au watermark courant de la requête
    fenêtrée -> ils seront ignorés par l'agrégation (comptés dans numRowsDroppedByWatermark)."""
    late = (batch_df.withColumn("retard_min",
                                F.round((F.col("kafka_timestamp").cast("long") - F.col("event_time").cast("long")) / 60, 1))
                    .filter(F.col("retard_min") > 10)
                    .select("tx_id", "status", "city", "event_time", "kafka_timestamp", "retard_min")
                    .collect())
    wm = WATERMARK["value"]
    for r in late:
        verdict = ("< watermark -> IGNORE par l'agregation fenetree" if wm and r.event_time < wm
                   else ">= watermark -> encore accepte")
        line = (f"[batch {batch_id}] {r.tx_id} {r.status:<10} {r.city:<10} event_time={r.event_time} "
                f"arrivee={r.kafka_timestamp} retard={r.retard_min} min | watermark={wm} {verdict}")
        print(line)
        LATE_LOG.append(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=",".join(ALL_QUERIES))
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--timeout", type=int, default=0, help="arrêt automatique après N secondes (0 = Ctrl-C)")
    args = ap.parse_args()
    wanted = args.queries.split(",")

    spark = get_spark("SahlPay-Partie4-Streaming")
    raw = read_kafka_stream(spark, args.starting_offsets, group_prefix="sahlpay-part4")
    events = parse_kafka(raw)
    queries = {}

    if "raw_console" in wanted:
        queries["raw_console"] = (events.drop("kafka_offset")
            .writeStream.queryName("raw_console").format("console")
            .outputMode("append").option("truncate", False).option("numRows", 15)
            .trigger(processingTime=args.trigger).start())

    if "bronze_raw" in wanted:
        bronze = raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.to_date("timestamp").alias("ingest_date"))
        queries["bronze_raw"] = (bronze.writeStream.queryName("bronze_raw").format("json")
            .option("path", BRONZE_RAW)
            .option("checkpointLocation", checkpoint("bronze_transactions_raw"))
            .partitionBy("ingest_date").outputMode("append")
            .trigger(processingTime="30 seconds").start())

    if "city_status" in wanted:
        counts = events.groupBy("city", "status").count().orderBy("city", "status")
        queries["city_status"] = (counts.writeStream.queryName("city_status").format("console")
            .outputMode("complete").option("truncate", False).option("numRows", 60)
            .trigger(processingTime=args.trigger).start())

    if "watermark" in wanted:
        windowed = (events.withWatermark("event_time", "10 minutes")
                          .groupBy(F.window("event_time", "5 minutes"), "city")
                          .count()
                          .select(F.col("window.start").alias("debut"), F.col("window.end").alias("fin"),
                                  "city", "count"))
        queries["watermark"] = (windowed.writeStream.queryName("watermark").format("console")
            .outputMode("update").option("truncate", False).option("numRows", 30)
            .trigger(processingTime=args.trigger).start())

    if "late_detector" in wanted:
        queries["late_detector"] = (events.writeStream.queryName("late_detector")
            .foreachBatch(late_detector).trigger(processingTime=args.trigger).start())

    section(f"Requêtes actives : {', '.join(queries)} (Ctrl-C pour arrêter)")
    start = time.time()
    try:
        while not args.timeout or time.time() - start < args.timeout:
            time.sleep(15)
            wq = queries.get("watermark")
            if wq is not None and wq.lastProgress:
                p = wq.lastProgress
                wm = p.get("eventTime", {}).get("watermark")
                if wm:
                    WATERMARK["value"] = datetime.strptime(wm, "%Y-%m-%dT%H:%M:%S.%fZ")
                dropped = sum(op.get("numRowsDroppedByWatermark", 0) for op in p.get("stateOperators", []))
                print(f"[PROGRESS watermark] batch={p['batchId']} lignes={p['numInputRows']} "
                      f"max_event_time={p.get('eventTime', {}).get('max')} watermark={wm} "
                      f"lignes ignorees (retard > watermark)={dropped}")
            for name, q in queries.items():
                if q.exception():
                    raise q.exception()
    except KeyboardInterrupt:
        pass
    finally:
        for q in queries.values():
            q.stop()
        if LATE_LOG:
            save_text("part4_late_events.log", "\n".join(LATE_LOG) + "\n")
        spark.stop()


if __name__ == "__main__":
    main()
