"""Partie 3 — Spark SQL et DataFrames : schéma explicite, SQL, jointure, UDF vs when/otherwise,
écriture Parquet partitionnée et plans Catalyst.

docker compose exec jupyter spark-submit /opt/project/src/part3_sql.py --hold 300
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import (HISTORY_RAW, SILVER_HISTORY, TIMINGS, TX_SCHEMA, amount_band_col, enrich, get_spark,
                    hold, load_json, read_merchants, save_json, save_text, section, timed)


def amount_band(amount_mad):
    if amount_mad is None:
        return None
    if amount_mad < 50:
        return "micro"
    if amount_mad <= 2000:
        return "standard"
    return "eleve"


amount_band_udf = F.udf(amount_band, StringType())


def explain_string(df, mode="extended"):
    """Texte de df.explain(mode) (pour l'enregistrer dans outputs/)."""
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=HISTORY_RAW)
    ap.add_argument("--output", default=SILVER_HISTORY)
    ap.add_argument("--udf-scale", type=int, default=40, help="facteur de réplication pour le test UDF")
    ap.add_argument("--hold", type=int, default=0)
    args = ap.parse_args()

    spark = get_spark("SahlPay-Partie3-SQL")

    section("1. Lecture avec schéma explicite (StructType, sans inférence)")
    tx = spark.read.schema(TX_SCHEMA).option("mode", "DROPMALFORMED").json(args.input)
    merchants = read_merchants(spark)
    tx.printSchema()
    with timed("df_count_events"):
        print(f"{tx.count()} evenements, {merchants.count()} commercants")

    completed = tx.filter(F.col("status") == "COMPLETED").dropDuplicates(["tx_id"]).cache()
    with timed("df_count_completed"):
        print(f"{completed.count()} transactions COMPLETED distinctes")

    section("2a. Montant par ville — API DataFrame")
    with timed("df_total_by_city"):
        by_city = (completed.groupBy("city")
                            .agg(F.round(F.sum("amount_mad"), 2).alias("montant_total_mad"),
                                 F.count("*").alias("nb_tx"),
                                 F.round(F.avg("amount_mad"), 2).alias("montant_moyen_mad"))
                            .orderBy(F.desc("montant_total_mad")))
        by_city.show(truncate=False)

    section("2b. Montant par ville — SQL")
    completed.createOrReplaceTempView("completed_tx")
    with timed("sql_total_by_city"):
        spark.sql("""
            SELECT city,
                   ROUND(SUM(amount_mad), 2) AS montant_total_mad,
                   COUNT(*)                  AS nb_tx,
                   ROUND(AVG(amount_mad), 2) AS montant_moyen_mad
            FROM completed_tx
            GROUP BY city
            ORDER BY montant_total_mad DESC
        """).show(truncate=False)

    section("3. Jointure P2M x merchants : montant encaissé par catégorie et par ville")
    p2m = completed.filter(F.col("type") == "P2M")
    joined = p2m.join(merchants.withColumnRenamed("city", "merchant_city"),
                      p2m.payee_id == merchants.merchant_id, "inner")
    by_cat_city = (joined.groupBy("category", "merchant_city")
                         .agg(F.round(F.sum("amount_mad"), 2).alias("montant_total_mad"),
                              F.count("*").alias("nb_paiements"))
                         .orderBy("category", F.desc("montant_total_mad")))
    with timed("df_join_category_city"):
        by_cat_city.show(40, truncate=False)
    orphans = p2m.join(merchants, p2m.payee_id == merchants.merchant_id, "left_anti").count()
    print(f"Paiements P2M vers un commercant absent du referentiel : {orphans}")

    section("4. UDF Python amount_band vs expression native when/otherwise")
    # montants répliqués (x udf_scale) pour que la différence ne soit pas noyée dans la latence des jobs
    sample = tx.select("amount_mad").crossJoin(spark.range(args.udf_scale)).select("amount_mad").cache()
    print(f"{sample.count()} montants pour la comparaison")
    results = {}
    for label, column in (("udf_python", lambda: amount_band_udf(F.col("amount_mad"))),
                          ("when_otherwise", lambda: amount_band_col(F.col("amount_mad")))):
        def query():                                           # nouveau DataFrame à chaque fois : pas de réutilisation du shuffle
            return sample.withColumn("amount_band", column()).groupBy("amount_band").count()
        query().collect()                                      # échauffement (JIT, démarrage des workers Python)
        with timed(f"amount_band_{label}"):
            results[label] = sorted(query().collect())
        print(f"--- plan physique ({label}) ---")
        query().explain()
    assert results["udf_python"] == results["when_otherwise"]
    for band, n in results["when_otherwise"]:
        print(f"{band:<10}{n}")
    print("La version UDF passe par BatchEvalPython (sérialisation JVM <-> Python ligne par ligne, boîte noire\n"
          "pour Catalyst) ; when/otherwise est une expression native générée en bytecode (whole-stage codegen).")

    section("5. Écriture Parquet nettoyée et enrichie, partitionnée par city")
    clean = enrich(tx.dropDuplicates(["tx_id", "status"])
                     .filter(F.col("tx_id").isNotNull() & F.col("event_time").isNotNull()
                             & F.col("city").isNotNull() & (F.col("amount_mad") > 0)),
                   merchants)
    with timed("df_write_silver_parquet"):
        clean.write.mode("overwrite").partitionBy("city").parquet(args.output)
    print(f"Ecrit : {args.output} ({spark.read.parquet(args.output).count()} lignes)")

    section("6. explain(True) sur la jointure P2M x merchants")
    completed.unpersist(blocking=True)        # plans lisibles : pas de substitution par InMemoryRelation

    def join_p2m():
        return p2m.join(merchants.withColumnRenamed("city", "merchant_city"),
                        p2m.payee_id == merchants.merchant_id, "inner")

    bhj = join_p2m()
    bhj.explain(True)
    plans = ["### Jointure avec choix automatique de Catalyst (autoBroadcastJoinThreshold = 10 Mo)",
             explain_string(bhj)]
    with timed("df_join_broadcast"):
        bhj.groupBy("category").count().collect()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    smj = join_p2m()
    print("--- même jointure avec le broadcast désactivé ---")
    smj.explain()
    with timed("df_join_sortmerge"):
        smj.groupBy("category").count().collect()
    plans += ["### Même jointure avec spark.sql.autoBroadcastJoinThreshold = -1", explain_string(smj)]
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    save_text("part3_explain_join.txt", "\n\n".join(plans))

    section("7. Comparaison des temps RDD (Partie 2) / DataFrame / SQL (Partie 3)")
    rdd = load_json("part2_timings.json", {})
    rows = [("comptage des evenements", rdd.get("rdd_count_events"), TIMINGS["df_count_events"], None),
            ("filtre COMPLETED + dedup", rdd.get("rdd_count_completed"), TIMINGS["df_count_completed"], None),
            ("montant total par ville", rdd.get("rdd_total_by_city"), TIMINGS["df_total_by_city"],
             TIMINGS["sql_total_by_city"])]
    print(f"{'operation':<28}{'RDD (s)':>10}{'DataFrame (s)':>16}{'SQL (s)':>10}")
    for name, r, d, s in rows:
        print(f"{name:<28}{str(r):>10}{d:>16}{str(s or '-'):>10}")
    save_json("part3_timings.json", TIMINGS)
    save_json("part3_results.json", {
        "by_city": [r.asDict() for r in by_city.collect()],
        "by_category_city": [r.asDict() for r in by_cat_city.collect()],
        "amount_band": dict(results["when_otherwise"]),
        "p2m_orphans": orphans,
    })
    hold(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
