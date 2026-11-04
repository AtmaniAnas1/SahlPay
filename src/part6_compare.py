"""Partie 6 — Comparaison GraphFrames / GraphX sur le même sous-graphe (transferts entre comptes).

Prérequis : part6_graphframes.py puis graphx/GraphXAnalysis.scala.
docker compose exec jupyter spark-submit /opt/project/src/part6_compare.py
"""
from pyspark.sql import functions as F

from common import GRAPH_ANALYSIS, GRAPH_COMPARE, get_spark, lake, load_json, save_json, section


def main():
    spark = get_spark("SahlPay-Partie6-Comparaison")

    section("1. Degrés : GraphFrames vs GraphX (sous-graphe TRANSFERS_TO)")
    gf = spark.read.parquet(f"{GRAPH_COMPARE}/graphframes_degrees").select(
        "id", F.col("degree").alias("gf_degree"), F.col("inDegree").alias("gf_in"), F.col("outDegree").alias("gf_out"))
    gx = spark.read.parquet(f"{GRAPH_COMPARE}/graphx_degrees").select(
        "id", F.col("degree").alias("gx_degree"), F.col("inDegree").alias("gx_in"), F.col("outDegree").alias("gx_out"))
    both = gf.join(gx, "id", "full_outer").cache()
    only_gf = both.filter(F.col("gx_degree").isNull()).count()
    only_gx = both.filter(F.col("gf_degree").isNull()).count()
    diff = both.filter((F.col("gf_degree") != F.col("gx_degree")) | (F.col("gf_in") != F.col("gx_in"))
                       | (F.col("gf_out") != F.col("gx_out"))).count()
    total = both.count()
    print(f"{total} comptes compares ; seulement GraphFrames={only_gf}, seulement GraphX={only_gx}, "
          f"degres differents={diff}")
    both.orderBy(F.desc("gf_in"), "id").show(10)

    section("2. Centralité PageRank (GraphX) rapprochée des anomalies GraphFrames")
    pagerank = spark.read.parquet(lake("gold", "graphx", "pagerank"))
    anomalies = (spark.read.parquet(GRAPH_ANALYSIS)
                 .select(F.explode("entities").alias("id"), "anomaly_type")
                 .groupBy("id").agg(F.concat_ws(",", F.collect_set("anomaly_type")).alias("anomalies")))
    top = pagerank.join(anomalies, "id", "left").orderBy(F.desc("pagerank")).limit(15)
    top.show(truncate=False)
    flagged_in_top = top.filter(F.col("anomalies").isNotNull()).count()
    print(f"{flagged_in_top}/15 comptes du top PageRank sont aussi signales par une regle explicite : la centralite\n"
          "indique une position structurelle (beaucoup de flux entrants, ou un flux venant d'un compte central),\n"
          "pas une preuve de fraude ; elle sert a prioriser une revue humaine.")

    section("3. Temps d'exécution")
    gf_t = load_json("part6_graphframes_timings.json", {})
    gx_t = load_json("part6_graphx_timings.json", {})
    rows = [("construction du graphe", gf_t.get("gf_build_graph"), gx_t.get("gx_build_graph")),
            ("degres", gf_t.get("gf_degrees"), gx_t.get("gx_degrees")),
            ("composantes connexes", gf_t.get("gf_connected_components"), gx_t.get("gx_connected_components")),
            ("motifs (appareils partages)", gf_t.get("gf_motif_shared_device"), None),
            ("motifs (cycles)", gf_t.get("gf_motif_cycles"), None),
            ("pageRank", None, gx_t.get("gx_pagerank"))]
    print(f"{'operation':<30}{'GraphFrames (s)':>17}{'GraphX (s)':>12}")
    for name, a, b in rows:
        print(f"{name:<30}{str(a if a is not None else '-'):>17}{str(b if b is not None else '-'):>12}")

    save_json("part6_comparison.json", {
        "degres": {"comptes": total, "seulement_graphframes": only_gf, "seulement_graphx": only_gx, "differents": diff},
        "top_pagerank": [r.asDict() for r in top.collect()],
        "temps": {name: {"graphframes": a, "graphx": b} for name, a, b in rows},
    })
    spark.stop()


if __name__ == "__main__":
    main()
