"""Suit un paiement dans chaque couche de la plateforme : Kafka, Bronze, Silver et KPI Gold.

docker compose exec jupyter spark-submit src/track_payment.py TX-TEST-153012
"""
import argparse

from pyspark.sql import functions as F

from common import (BRONZE_RAW, GOLD_KPI_TUMBLING, KAFKA_BOOTSTRAP, SILVER_CLEAN, TOPIC, TX_SCHEMA, get_spark,
                    section)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tx_id")
    args = ap.parse_args()
    spark = get_spark("SahlPay-Suivi-Paiement")
    found = {}

    section(f"1. Kafka (topic {TOPIC})")
    kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                  .option("subscribe", TOPIC).load()
                  .select("partition", "offset", F.from_json(F.col("value").cast("string"), TX_SCHEMA).alias("e"))
                  .filter(F.col("e.tx_id") == args.tx_id)
                  .select("partition", "offset", "e.status", "e.amount_mad", "e.event_time"))
    found["kafka"] = kafka.count()
    kafka.orderBy("offset").show(truncate=False)

    section("2. Bronze (JSON brut écrit par le streaming de la Partie 4)")
    try:
        bronze = spark.read.json(BRONZE_RAW).filter(F.col("raw_value").contains(f'"{args.tx_id}"'))
        found["bronze"] = bronze.count()
        bronze.select("partition", "offset", "raw_value").show(truncate=110)
    except Exception as exc:
        found["bronze"] = 0
        print(f"indisponible ({type(exc).__name__}) : la Partie 4 n'a pas tourné pendant l'envoi")

    section("3. Silver (Parquet nettoyé et enrichi par le pipeline de la Partie 5)")
    silver = spark.read.parquet(SILVER_CLEAN).filter(F.col("tx_id") == args.tx_id)
    found["silver"] = silver.count()
    silver.select("tx_id", "status", "city", "amount_mad", "amount_band", "merchant_name", "merchant_category",
                  "event_time", "ingested_at").orderBy("event_time").show(truncate=False)

    section("4. Gold (KPI de la fenêtre de 10 minutes qui contient le paiement)")
    row = silver.filter(F.col("status") == "COMPLETED").select("city", "event_time").first() \
        or silver.select("city", "event_time").first()
    if row:
        gold = (spark.read.format("delta").load(GOLD_KPI_TUMBLING)
                     .filter((F.col("city") == row.city) & (F.col("window_start") <= row.event_time)
                             & (F.col("window_end") > row.event_time)))
        found["gold"] = gold.count()
        gold.select("window_start", "window_end", "city", "nb_transactions", "nb_completed", "nb_rejected",
                    "montant_total_mad", "batch_id", "updated_at").show(truncate=False)
    else:
        found["gold"] = 0
        print("paiement absent de Silver : pas de fenêtre Gold à afficher")

    section("Résumé")
    for layer in ("kafka", "bronze", "silver", "gold"):
        print(f"{layer:<8} {'OK' if found.get(layer) else 'absent'} ({found.get(layer, 0)} ligne(s))")
    spark.stop()


if __name__ == "__main__":
    main()
