"""Partie 6 — Graphe comptes–appareils–bénéficiaires avec GraphFrames (PySpark).

1. construit sommets (wallet, device, merchant) et arêtes (USES_DEVICE, PAYS, TRANSFERS_TO) depuis Silver ;
2. contrôle les comptages et l'absence d'arêtes orphelines ;
3. motifs find() : appareils partagés, cycles de transferts, chaînes compte -> compte -> commerçant ;
4. degrés, comptes collecteurs, composantes connexes ;
5. anomalies motivées (seuils explicites) -> lake/gold/graph_analysis/ (partitionné par anomaly_type) ;
6. confrontation avec la vérité terrain du générateur (scénarios retrouvés, faux positifs) ;
7. exports pour GraphX (lake/gold/graph_model/) et visualisation d'un sous-graphe suspect.

docker compose exec jupyter spark-submit /opt/project/src/part6_graphframes.py
(--source s3a://lake/silver/transactions_history pour travailler sur l'historique batch de la Partie 3)
"""
import argparse
import json

from graphframes import GraphFrame
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField, StructType)

from common import (GRAPH_ANALYSIS, GRAPH_COMPARE, GRAPH_MODEL, OUTPUTS, ROOT, SILVER_CLEAN, TIMINGS, checkpoint,
                    get_spark, lake, read_merchants, save_json, section, timed)

ANOMALY_SCHEMA = StructType([
    StructField("anomaly_type", StringType()),
    StructField("entity_id", StringType()),
    StructField("entities", ArrayType(StringType())),
    StructField("metric_name", StringType()),
    StructField("metric_value", DoubleType()),
    StructField("threshold", DoubleType()),
    StructField("reason", StringType()),
])


def build_graph(silver, merchants):
    tx = silver.select("tx_id", "type", "payer_wallet", "payee_id", "device_id", "status", "amount_mad")
    one_per_tx = tx.dropDuplicates(["tx_id"])                 # attributs identiques pour tous les états d'une tx
    completed = tx.filter(F.col("status") == "COMPLETED").dropDuplicates(["tx_id"])

    def vertex(df, type_, label=None, city=None, category=None):
        return df.select(
            "id", F.lit(type_).alias("type"),
            (F.col(label) if label else F.col("id")).alias("label"),
            (F.col(city) if city else F.lit(None).cast("string")).alias("city"),
            (F.col(category) if category else F.lit(None).cast("string")).alias("category"))

    wallets = (one_per_tx.select(F.col("payer_wallet").alias("id"))
               .union(one_per_tx.filter(F.col("type") == "P2P").select(F.col("payee_id").alias("id")))
               .filter(F.col("id").isNotNull()).distinct())
    devices = one_per_tx.select(F.col("device_id").alias("id")).filter(F.col("id").isNotNull()).distinct()
    merchant_v = merchants.select(F.col("merchant_id").alias("id"), "name", "city", "category")
    vertices = (vertex(wallets, "wallet")
                .union(vertex(devices, "device"))
                .union(vertex(merchant_v, "merchant", "name", "city", "category")))

    def edge(df, relationship):
        return (df.groupBy(F.col("payer_wallet").alias("src"), F.col("dst"))
                  .agg(F.count("*").alias("tx_count"), F.round(F.sum("amount_mad"), 2).alias("total_amount"))
                  .withColumn("relationship", F.lit(relationship)))

    edges_all = (edge(one_per_tx.withColumnRenamed("device_id", "dst"), "USES_DEVICE")
                 .unionByName(edge(completed.filter(F.col("type") == "P2M").withColumnRenamed("payee_id", "dst"), "PAYS"))
                 .unionByName(edge(completed.filter(F.col("type") == "P2P").withColumnRenamed("payee_id", "dst"),
                                   "TRANSFERS_TO")))
    ids = vertices.select("id")
    edges = (edges_all.join(ids.withColumnRenamed("id", "src"), "src", "left_semi")
                      .join(ids.withColumnRenamed("id", "dst"), "dst", "left_semi")
                      .select("src", "dst", "relationship", "tx_count", "total_amount"))
    orphans = (edges_all.join(edges.select("src", "dst", "relationship"), ["src", "dst", "relationship"], "left_anti"))
    return vertices, edges, orphans


def anomalies_df(spark, rows):
    return spark.createDataFrame(rows, ANOMALY_SCHEMA).withColumn("detected_at", F.current_timestamp())


def draw_subgraph(nodes, edges, path, highlight=()):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import networkx as nx

    g = nx.DiGraph()
    for n in nodes:
        g.add_node(n["id"], type=n["type"])
    for e in edges:
        g.add_edge(e["src"], e["dst"], relationship=e["relationship"], amount=e["total_amount"])
    colors = {"wallet": "#4C78A8", "device": "#F58518", "merchant": "#54A24B"}
    edge_colors = {"USES_DEVICE": "#F58518", "PAYS": "#54A24B", "TRANSFERS_TO": "#4C78A8"}
    pos = nx.spring_layout(g, seed=7, k=0.35)
    plt.figure(figsize=(14, 10))
    nx.draw_networkx_nodes(g, pos, node_color=[colors[g.nodes[n]["type"]] for n in g.nodes],
                           node_size=[420 if g.nodes[n]["type"] != "wallet" else 160 for n in g.nodes])
    nx.draw_networkx_edges(g, pos, edge_color=[edge_colors[g.edges[e]["relationship"]] for e in g.edges],
                           arrowsize=9, width=0.8, alpha=0.8)
    labels = {n: n for n in g.nodes if g.nodes[n]["type"] != "wallet" or g.degree(n) > 2 or n in highlight}
    nx.draw_networkx_labels(g, pos, labels=labels, font_size=7)
    for t, c in colors.items():
        plt.scatter([], [], color=c, label=t)
    plt.legend(loc="upper left")
    plt.title("SahlPay — sous-graphe suspect (appareil partagé, cycle de transferts, compte collecteur)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(path, dpi=130)
    print(f"-> {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=SILVER_CLEAN, help="couche Silver (Parquet)")
    ap.add_argument("--truth", default=str(ROOT / "data" / "fraud_ground_truth.json"))
    ap.add_argument("--device-min-accounts", type=int, default=5)
    ap.add_argument("--collector-min-senders", type=int, default=20)
    ap.add_argument("--collector-max-avg-amount", type=float, default=100.0)
    ap.add_argument("--cycle-min-amount", type=float, default=1000.0)
    ap.add_argument("--cash-out-min-amount", type=float, default=500.0)
    ap.add_argument("--component-min-size", type=int, default=100)
    args = ap.parse_args()

    spark = get_spark("SahlPay-Partie6-GraphFrames")
    spark.sparkContext.setCheckpointDir(checkpoint("graphframes"))

    section("1. Préparation des sommets et arêtes depuis la couche Silver")
    silver = spark.read.parquet(args.source)
    merchants = read_merchants(spark)
    with timed("gf_build_graph"):
        vertices, edges, orphans = build_graph(silver, merchants)
        # export Parquet (réutilisé par GraphX) puis relecture : lineage court pour les algorithmes itératifs
        vertices.write.mode("overwrite").parquet(f"{GRAPH_MODEL}/vertices")
        edges.write.mode("overwrite").parquet(f"{GRAPH_MODEL}/edges")
        vertices = spark.read.parquet(f"{GRAPH_MODEL}/vertices").cache()
        edges = spark.read.parquet(f"{GRAPH_MODEL}/edges").cache()
        n_v, n_e = vertices.count(), edges.count()
    print(f"{n_v} sommets, {n_e} aretes -> {GRAPH_MODEL}/vertices, {GRAPH_MODEL}/edges")
    print("Aretes orphelines exclues (extremite absente des sommets) :")
    orphans = orphans.cache()
    orphans.groupBy("relationship").agg(F.count("*").alias("nb"), F.collect_set("dst").alias("dst")).show(truncate=80)

    section("2. GraphFrame : contrôles")
    g = GraphFrame(vertices, edges)
    g.vertices.groupBy("type").count().orderBy("type").show()
    g.edges.groupBy("relationship").agg(F.count("*").alias("aretes"), F.sum("tx_count").alias("transactions"),
                                        F.round(F.sum("total_amount"), 2).alias("montant_total")).show()
    ids = g.vertices.select("id")
    remaining = (g.edges.join(ids.withColumnRenamed("id", "src"), "src", "left_anti").count()
                 + g.edges.join(ids.withColumnRenamed("id", "dst"), "dst", "left_anti").count())
    print(f"Aretes orphelines dans le GraphFrame : {remaining}")
    assert remaining == 0

    wallets_v = g.vertices.filter(F.col("type") == "wallet")
    devices_g = GraphFrame(g.vertices.filter(F.col("type").isin("wallet", "device")),
                           g.edges.filter(F.col("relationship") == "USES_DEVICE"))
    transfers_g = GraphFrame(wallets_v, g.edges.filter(F.col("relationship") == "TRANSFERS_TO")).cache()
    anomalies = []

    section("3a. Motif (a)-[]->(d); (b)-[]->(d) : appareils partagés par plusieurs comptes")
    with timed("gf_motif_shared_device"):
        shared = (devices_g.find("(a)-[]->(d); (b)-[]->(d)")
                  .filter("a.id < b.id")
                  .groupBy(F.col("d.id").alias("device_id"))
                  .agg(F.count("*").alias("paires_de_comptes"),
                       F.array_sort(F.array_union(F.collect_set("a.id"), F.collect_set("b.id"))).alias("comptes"))
                  .withColumn("nb_comptes", F.size("comptes"))
                  .orderBy(F.desc("nb_comptes")))
        shared_rows = shared.collect()
    shared.select("device_id", "nb_comptes", "paires_de_comptes").show(10)
    dist = shared.groupBy("nb_comptes").count().orderBy("nb_comptes").collect()
    print("Distribution du nombre de comptes par appareil partage : " + ", ".join(f"{r[0]}->{r[1]}" for r in dist))
    for r in shared_rows:
        if r.nb_comptes >= args.device_min_accounts:
            anomalies.append(("shared_device", r.device_id, r.comptes, "nb_comptes", float(r.nb_comptes),
                              float(args.device_min_accounts),
                              f"appareil utilise par {r.nb_comptes} comptes distincts (seuil {args.device_min_accounts})"))

    section("3b. Motif (a)->(b)->(c)->(a) : transferts circulaires")
    with timed("gf_motif_cycles"):
        cycles = (transfers_g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
                  .filter("a.id < b.id AND a.id < c.id")          # une seule rotation par cycle
                  .select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c"),
                          F.least("e1.total_amount", "e2.total_amount", "e3.total_amount").alias("montant_min"),
                          F.greatest("e1.total_amount", "e2.total_amount", "e3.total_amount").alias("montant_max"),
                          (F.col("e1.tx_count") + F.col("e2.tx_count") + F.col("e3.tx_count")).alias("nb_virements")))
        cycle_rows = cycles.orderBy(F.desc("montant_min")).collect()
    cycles.orderBy(F.desc("montant_min")).show(20, truncate=False)
    print(f"{len(cycle_rows)} cycles de longueur 3 au total")
    for r in cycle_rows:
        if r.montant_min >= args.cycle_min_amount:
            anomalies.append(("transfer_cycle", f"{r.a}->{r.b}->{r.c}->{r.a}", [r.a, r.b, r.c], "montant_min_mad",
                              float(r.montant_min), args.cycle_min_amount,
                              f"cycle A->B->C->A, {r.nb_virements} virements, chaque arete >= {r.montant_min:.0f} MAD "
                              f"(seuil {args.cycle_min_amount:.0f})"))

    section("3c. Degrés et comptes collecteurs (inDegrees / outDegrees sur le graphe des transferts)")
    with timed("gf_degrees"):
        degrees = (transfers_g.degrees
                   .join(transfers_g.inDegrees, "id", "left")
                   .join(transfers_g.outDegrees, "id", "left")
                   .fillna(0, ["inDegree", "outDegree"]))
        degrees.write.mode("overwrite").parquet(f"{GRAPH_COMPARE}/graphframes_degrees")
    received = (transfers_g.edges.groupBy(F.col("dst").alias("id"))
                .agg(F.count("*").alias("nb_emetteurs"), F.sum("tx_count").alias("nb_virements_recus"),
                     F.round(F.sum("total_amount") / F.sum("tx_count"), 2).alias("montant_moyen_recu")))
    collectors = (received.join(degrees, "id")
                  .orderBy(F.desc("inDegree")))
    collectors.show(10)
    print("Distribution des in-degres : " + ", ".join(
        f"{r[0]}->{r[1]}" for r in degrees.groupBy("inDegree").count().orderBy("inDegree").collect()))
    for r in collectors.filter((F.col("inDegree") >= args.collector_min_senders)
                               & (F.col("montant_moyen_recu") <= args.collector_max_avg_amount)).collect():
        anomalies.append(("collector_account", r.id, [r.id], "nb_emetteurs", float(r.inDegree),
                          float(args.collector_min_senders),
                          f"{r.inDegree} emetteurs distincts, {r.nb_virements_recus} virements recus de "
                          f"{r.montant_moyen_recu} MAD en moyenne (seuils {args.collector_min_senders} emetteurs, "
                          f"moyenne <= {args.collector_max_avg_amount:.0f} MAD)"))
    collector_ids = [a[1] for a in anomalies if a[0] == "collector_account"]

    section("3d. Motif (a)-[e1]->(b); (b)-[e2]->(m) : chaînes compte -> compte -> commerçant")
    with timed("gf_motif_chains"):
        chains = (g.find("(a)-[e1]->(b); (b)-[e2]->(m)")
                  .filter("e1.relationship = 'TRANSFERS_TO' AND e2.relationship = 'PAYS'")
                  .select(F.col("a.id").alias("emetteur"), F.col("b.id").alias("relais"), F.col("m.id").alias("commercant"),
                          F.col("m.category").alias("categorie"), F.col("e1.total_amount").alias("montant_transfert"),
                          F.col("e2.total_amount").alias("montant_paiement")))
        n_chains = chains.count()
    print(f"{n_chains} chaines compte -> compte -> commercant")
    via = (chains.filter(F.col("relais").isin(collector_ids)
                         & (F.col("montant_paiement") >= args.cash_out_min_amount))
                 .groupBy("relais", "commercant", "categorie", "montant_paiement")
                 .agg(F.count("*").alias("nb_emetteurs_amont")))
    via.show(truncate=False)
    for r in via.collect():
        anomalies.append(("collector_cash_out", f"{r.relais}->{r.commercant}", [r.relais, r.commercant],
                          "montant_paiement_mad", float(r.montant_paiement), args.cash_out_min_amount,
                          f"compte collecteur alimente par {r.nb_emetteurs_amont} emetteurs puis paiement de "
                          f"{r.montant_paiement} MAD chez {r.commercant} ({r.categorie})"))

    section("3e. Composantes connexes du graphe des transferts")
    with timed("gf_connected_components"):
        # AQE re-sérialise le plan (qui grossit à chaque itération) dans les événements UI : désactivé ici
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        cc = transfers_g.dropIsolatedVertices().connectedComponents().cache()
        sizes = cc.groupBy("component").agg(F.count("*").alias("taille")).cache()
        sizes.count()
        spark.conf.unset("spark.sql.adaptive.enabled")
    sizes.orderBy(F.desc("taille")).show(10)
    print("Distribution des tailles : " + ", ".join(
        f"{r[0]}->{r[1]}" for r in sizes.groupBy("taille").count().orderBy("taille").collect()))
    top_members = (cc.join(degrees.select("id", "degree"), "id")
                     .withColumn("rk", F.row_number().over(
                         Window.partitionBy("component").orderBy(F.desc("degree"), "id")))
                     .filter("rk <= 5").groupBy("component").agg(F.collect_list("id").alias("hubs")))
    for r in sizes.filter(F.col("taille") >= args.component_min_size).join(top_members, "component").collect():
        anomalies.append(("large_component", str(r.component), r.hubs, "taille", float(r.taille),
                          float(args.component_min_size),
                          f"composante de {r.taille} comptes relies par des transferts (seuil "
                          f"{args.component_min_size}) ; comptes les plus connectes : {', '.join(r.hubs)}"))

    section("4. Liste motivée des cas inhabituels -> lake/gold/graph_analysis/")
    result = anomalies_df(spark, anomalies)
    result.groupBy("anomaly_type").count().show()
    result.select("anomaly_type", "entity_id", "metric_value", "reason").show(50, truncate=110)
    result.write.mode("overwrite").partitionBy("anomaly_type").parquet(GRAPH_ANALYSIS)

    section("5. Vérification des scénarios injectés par producer.py et faux positifs")
    truth = json.loads(open(args.truth, encoding="utf-8").read())
    flagged = {t: {a[1] for a in anomalies if a[0] == t} for t in ("shared_device", "collector_account")}
    flagged_cycles = {frozenset(a[2]) for a in anomalies if a[0] == "transfer_cycle"}
    checks = {
        "shared_device": ({d["device_id"] for d in truth["shared_devices"]}, flagged["shared_device"]),
        "collector_account": ({c["wallet"] for c in truth["collectors"]}, flagged["collector_account"]),
        "transfer_cycle": ({frozenset(c) for c in truth["cycles"]}, flagged_cycles),
    }
    validation = {}
    print(f"{'type':<20}{'injectes':>9}{'retrouves':>10}{'faux positifs':>15}{'manques':>9}")
    def fmt_ids(items):
        return sorted("/".join(sorted(i)) if isinstance(i, frozenset) else i for i in items)

    for t, (expected, found) in checks.items():
        tp, fp, fn = expected & found, found - expected, expected - found
        validation[t] = {"injectes": len(expected), "retrouves": len(tp), "faux_positifs": len(fp),
                         "manques": len(fn), "faux_positifs_ids": fmt_ids(fp), "manques_ids": fmt_ids(fn)}
        print(f"{t:<20}{len(expected):>9}{len(tp):>10}{len(fp):>15}{len(fn):>9}")
    all_cycles = {frozenset((r.a, r.b, r.c)) for r in cycle_rows}
    validation["transfer_cycle"]["cycles_sous_le_seuil"] = len(all_cycles - flagged_cycles)
    print(f"Cycles naturels ecartes par le seuil de montant : {len(all_cycles - flagged_cycles)}")

    section("6. Métriques pour le tableau de bord conformité")
    metrics = {
        "sommets": {r["type"]: r["count"] for r in g.vertices.groupBy("type").count().collect()},
        "aretes": {r["relationship"]: r["count"] for r in g.edges.groupBy("relationship").count().collect()},
        "aretes_orphelines_exclues": orphans.count(),
        "anomalies": {r["anomaly_type"]: r["count"] for r in result.groupBy("anomaly_type").count().collect()},
        "cycles_total": len(cycle_rows), "chaines_compte_compte_commercant": n_chains,
        "max_comptes_par_appareil": shared_rows[0].nb_comptes if shared_rows else 0,
        "max_in_degree": degrees.agg(F.max("inDegree")).first()[0],
        "plus_grande_composante": sizes.agg(F.max("taille")).first()[0],
        "seuils": {k: v for k, v in vars(args).items() if k not in ("source", "truth")},
        "validation": validation,
    }
    print(json.dumps(metrics, indent=2, default=str))
    spark.createDataFrame([(json.dumps(metrics, default=str),)], "metrics_json string") \
         .withColumn("computed_at", F.current_timestamp()) \
         .write.mode("overwrite").parquet(lake("gold", "graph_metrics"))
    save_json("part6_metrics.json", metrics)
    save_json("part6_graphframes_timings.json", TIMINGS)

    section("7. Visualisation d'un sous-graphe suspect")
    # un appareil partagé, un cycle et un collecteur (avec ses émetteurs et le commerçant de sortie)
    d, cycle, c = truth["shared_devices"][-1], truth["cycles"][0], truth["collectors"][0]
    focus = [d["device_id"], *d["wallets"], *cycle, c["wallet"], c["cash_out_merchant"]]
    sub_edges = g.edges.filter(
        (F.col("src").isin(focus) & F.col("dst").isin(focus))
        | ((F.col("relationship") == "TRANSFERS_TO") & (F.col("dst") == c["wallet"])))
    sub_edges = [r.asDict() for r in sub_edges.collect()]
    node_ids = {e["src"] for e in sub_edges} | {e["dst"] for e in sub_edges}
    nodes = [r.asDict() for r in g.vertices.filter(F.col("id").isin(list(node_ids))).collect()]
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    draw_subgraph(nodes, sub_edges, OUTPUTS / "part6_subgraph_suspect.png", highlight=set(cycle))
    spark.stop()


if __name__ == "__main__":
    main()
