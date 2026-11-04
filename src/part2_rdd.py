"""Partie 2 — Traitement batch avec SparkCore (RDD) sur l'historique des transactions.

Prérequis : lake/bronze/history/transactions_history.jsonl (voir README).
docker compose exec jupyter spark-submit /opt/project/src/part2_rdd.py --hold 300
"""
import argparse
import json
from operator import add

from common import HISTORY_RAW, TIMINGS, get_spark, hold, save_json, section, timed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=HISTORY_RAW)
    ap.add_argument("--hold", type=int, default=0)
    args = ap.parse_args()

    spark = get_spark("SahlPay-Partie2-RDD")
    sc = spark.sparkContext

    section("1. Chargement et parsing (transformations : textFile, map)")
    raw = sc.textFile(args.input)                                    # transformation (lazy)
    events = raw.map(json.loads)                                      # transformation (lazy)
    with timed("rdd_count_events"):
        n_events = events.count()                                     # ACTION -> job 0
    print(f"{n_events} evenements, {raw.getNumPartitions()} partitions")

    section("2. filter : transactions COMPLETED (dédoublonnées par tx_id)")
    completed = (events.filter(lambda e: e["status"] == "COMPLETED")  # transformation (narrow)
                       .map(lambda e: (e["tx_id"], e))                # transformation (narrow)
                       .reduceByKey(lambda a, _b: a)                  # transformation (wide : shuffle) -> doublons retirés
                       .values()
                       .cache())
    with timed("rdd_count_completed"):
        n_completed = completed.count()                               # ACTION
    print(f"{n_completed} transactions COMPLETED distinctes")

    section("3. map + reduceByKey : montant total encaissé par ville")
    with timed("rdd_total_by_city"):
        total_by_city = (completed.map(lambda e: (e["city"], e["amount_mad"]))
                                  .reduceByKey(add)                   # shuffle
                                  .collect())                         # ACTION
    section("4. aggregateByKey : (somme, nombre) en un seul passage -> montant moyen par ville")
    with timed("rdd_avg_by_city"):
        sum_count = (completed.map(lambda e: (e["city"], e["amount_mad"]))
                              .aggregateByKey((0.0, 0),
                                              lambda acc, v: (acc[0] + v, acc[1] + 1),       # dans la partition
                                              lambda a, b: (a[0] + b[0], a[1] + b[1])))      # entre partitions
        avg_by_city = sum_count.mapValues(lambda sc_: (sc_[0], sc_[1], sc_[0] / sc_[1])).collect()

    rows = sorted(((city, round(s, 2), n, round(avg, 2)) for city, (s, n, avg) in avg_by_city),
                  key=lambda r: -r[1])
    print(f"{'ville':<12}{'montant total (MAD)':>22}{'nb tx':>9}{'montant moyen':>16}")
    for city, s, n, avg in rows:
        print(f"{city:<12}{s:>22,.2f}{n:>9}{avg:>16,.2f}")
    assert {c: round(v, 2) for c, v in total_by_city} == {r[0]: r[1] for r in rows}

    section("5. Top 5 des commerçants par nombre de paiements validés (takeOrdered)")
    with timed("rdd_top5_merchants"):
        top5 = (completed.filter(lambda e: e["type"] == "P2M")
                         .map(lambda e: (e["payee_id"], 1))
                         .reduceByKey(add)
                         .takeOrdered(5, key=lambda kv: (-kv[1], kv[0])))     # ACTION
    for rank, (merchant, n) in enumerate(top5, 1):
        print(f"{rank}. {merchant} : {n} paiements")

    section("6. Lineage (toDebugString) du RDD sum_count")
    print(sum_count.toDebugString().decode())
    print("""
Transformations (lazy, construisent le DAG) : textFile, map, filter, reduceByKey, values, cache,
    aggregateByKey, mapValues
Actions (déclenchent un job) : count, collect, takeOrdered
Chaque reduceByKey / aggregateByKey introduit une frontière de stage (ShuffleDependency).""")

    save_json("part2_results.json", {
        "n_events": n_events, "n_completed": n_completed,
        "by_city": [dict(city=c, total_mad=s, nb_tx=n, avg_mad=a) for c, s, n, a in rows],
        "top5_merchants": [dict(merchant_id=m, nb_payments=n) for m, n in top5],
    })
    save_json("part2_timings.json", TIMINGS)
    hold(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
