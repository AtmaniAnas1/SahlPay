"""Partie 5 — Lecture batch de la couche Gold (Delta + DESCRIBE HISTORY), offsets du checkpoint
et preuve de reprise sans perte ni doublon.

docker compose exec jupyter spark-submit /opt/project/src/part5_verify.py --label avant_arret
... arrêter le pipeline, attendre 2 min (producteur actif), relancer, laisser rattraper ...
docker compose exec jupyter spark-submit /opt/project/src/part5_verify.py --label apres_reprise

Lancer la vérification pipeline arrêté : Kafka est relu jusqu'aux offsets du dernier batch du checkpoint,
ce qui suppose que ce batch soit aussi validé (commits/) dans Silver.
"""
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common import (GOLD_KPI_SLIDING, GOLD_KPI_TUMBLING, GOLD_SESSIONS, KAFKA_BOOTSTRAP, SILVER_CLEAN, TOPIC,
                    TX_SCHEMA, checkpoint, get_spark, load_json, save_json, section)


def hadoop_fs(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def latest_checkpoint_offsets(spark, ckpt_dir):
    """Lit le dernier fichier checkpoints/<requête>/offsets/<batchId> :
    ligne 1 = version, ligne 2 = métadonnées (watermark...), ligne 3 = offsets par partition Kafka."""
    fs, p = hadoop_fs(spark, ckpt_dir + "/offsets")
    if not fs.exists(p):
        return None, None
    batches = sorted(int(s.getPath().getName()) for s in fs.listStatus(p) if s.getPath().getName().isdigit())
    if not batches:
        return None, None
    lines = [r.value for r in spark.read.text(f"{ckpt_dir}/offsets/{batches[-1]}").collect()]
    return batches[-1], {"version": lines[0], "metadata": json.loads(lines[1]), "offsets": json.loads(lines[2])}


def committed_batch(spark, ckpt_dir):
    fs, p = hadoop_fs(spark, ckpt_dir + "/commits")
    if not fs.exists(p):
        return None
    names = [int(s.getPath().getName()) for s in fs.listStatus(p) if s.getPath().getName().isdigit()]
    return max(names) if names else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    args = ap.parse_args()
    spark = get_spark("SahlPay-Partie5-Verification")

    section("1. Table Delta Gold kpi_city_10min relue en batch")
    gold = spark.read.format("delta").load(GOLD_KPI_TUMBLING)
    gold.orderBy(F.desc("window_start"), "city").show(20, truncate=False)
    print(f"{gold.count()} lignes (ville x fenetre de 10 min)")

    section("2. DESCRIBE HISTORY (journal de transactions Delta)")
    (spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_KPI_TUMBLING}`")
          .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
          .show(15, truncate=80))

    for name, path in (("taux de rejet (fenetres glissantes)", GOLD_KPI_SLIDING), ("sessions par appareil", GOLD_SESSIONS)):
        try:
            df = spark.read.format("delta").load(path)
            print(f"--- {name} : {df.count()} lignes")
            df.orderBy(F.desc(df.columns[0])).show(10, truncate=False)
        except Exception as exc:   # table pas encore créée (aucune fenêtre close)
            print(f"--- {name} : indisponible ({type(exc).__name__})")

    section("3. Offsets enregistrés dans le checkpoint de la requête silver")
    ckpt = checkpoint("silver_transactions_clean")
    batch_id, offsets = latest_checkpoint_offsets(spark, ckpt)
    commit = committed_batch(spark, ckpt)
    print(f"dernier batch planifie (offsets/) = {batch_id}, dernier batch valide (commits/) = {commit}")
    if offsets:
        print(f"offsets Kafka de fin du batch {batch_id} : {offsets['offsets']}")
        print(f"watermark du batch : {offsets['metadata'].get('batchWatermarkMs')} ms")

    section("4. Comptages Silver vs Kafka (preuve : ni perte, ni doublon)")
    silver = spark.read.parquet(SILVER_CLEAN)          # lit uniquement les fichiers validés (_spark_metadata)
    n_silver = silver.count()
    n_silver_keys = silver.select("tx_id", "status").distinct().count()
    print(f"silver : {n_silver} lignes, {n_silver_keys} couples (tx_id, status) distincts "
          f"-> doublons = {n_silver - n_silver_keys}")

    result = {"label": args.label, "at": datetime.now(timezone.utc).isoformat(), "checkpoint_batch": batch_id,
              "committed_batch": commit, "silver_rows": n_silver, "silver_duplicates": n_silver - n_silver_keys}
    if offsets:
        ending = json.dumps({TOPIC: offsets["offsets"][TOPIC]})
        kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                      .option("subscribe", TOPIC).option("startingOffsets", "earliest")
                      .option("endingOffsets", ending).load()
                      .select(F.from_json(F.col("value").cast("string"), TX_SCHEMA).alias("e"),
                              F.col("timestamp").alias("kafka_ts"))
                      .select("e.tx_id", "e.status", "e.event_time", "kafka_ts"))
        n_kafka = kafka.count()
        keys = (kafka.groupBy("tx_id", "status")
                     .agg(F.max((F.col("kafka_ts").cast("long") - F.col("event_time").cast("long")) / 60)
                           .alias("retard_min")))
        n_keys = keys.count()
        missing = keys.join(silver.select("tx_id", "status"), ["tx_id", "status"], "left_anti").cache()
        n_missing = missing.count()
        n_missing_late = missing.filter(F.col("retard_min") > 10).count()
        n_extra = silver.select("tx_id", "status").join(keys, ["tx_id", "status"], "left_anti").count()
        print(f"kafka (jusqu'aux offsets du checkpoint) : {n_kafka} messages, {n_keys} couples distincts "
              f"-> {n_kafka - n_keys} doublons produits volontairement, retires par le pipeline")
        print(f"absents de silver : {n_missing} dont {n_missing_late} arrives avec > 10 min de retard "
              f"(au-dela du watermark, ecartes par conception) -> pertes non expliquees = {n_missing - n_missing_late}")
        print(f"lignes silver absentes de Kafka : {n_extra}")
        result.update(kafka_messages=n_kafka, kafka_distinct=n_keys, missing=n_missing,
                      missing_late_beyond_watermark=n_missing_late, unexplained_loss=n_missing - n_missing_late,
                      silver_not_in_kafka=n_extra, checkpoint_offsets=offsets["offsets"])

    history = load_json("part5_counts.json", [])
    history.append(result)
    save_json("part5_counts.json", history)
    if len(history) > 1:
        print("\nHistorique des verifications :")
        for h in history:
            print(f"  {h['label']:<16} batch={h.get('checkpoint_batch')} silver={h['silver_rows']} "
                  f"doublons={h['silver_duplicates']} pertes_non_expliquees={h.get('unexplained_loss')}")
    spark.stop()


if __name__ == "__main__":
    main()
