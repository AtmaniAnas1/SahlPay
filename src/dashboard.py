"""Tableau de bord conformité : page HTML statique générée depuis la couche Gold.

Sources : Delta kpi_city_10min et kpi_city_reject_rate_sliding, Parquet graph_analysis et graph_metrics.
docker compose exec jupyter spark-submit /opt/project/src/dashboard.py   ->  outputs/dashboard_conformite.html
"""
import base64
import html
import io
import json
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import (GOLD_KPI_SLIDING, GOLD_KPI_TUMBLING, GRAPH_ANALYSIS, OUTPUTS, get_spark, lake)  # noqa: E402


def to_img(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return f'<img src="data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()}">'


def table(pdf, max_rows=30):
    return pdf.head(max_rows).to_html(index=False, border=0, classes="t", escape=True)


def read_delta(spark, path):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return None


def main():
    spark = get_spark("SahlPay-Dashboard")
    parts = []

    kpi = read_delta(spark, GOLD_KPI_TUMBLING)
    if kpi is not None:
        by_city = (kpi.groupBy("city")
                      .agg(F.sum("nb_transactions").alias("transactions"), F.sum("nb_rejected").alias("rejets"),
                           F.round(F.sum("montant_total_mad"), 2).alias("montant_mad"))
                      .withColumn("taux_rejet_%", F.round(100 * F.col("rejets") / F.col("transactions"), 2))
                      .orderBy(F.desc("montant_mad")).toPandas())
        fig, ax = plt.subplots(figsize=(9, 3.6))
        ax.bar(by_city["city"], by_city["montant_mad"], color="#4C78A8")
        ax.set_ylabel("MAD encaissés"), ax.set_title("Montant encaissé par ville (fenêtres de 10 min cumulées)")
        ax.tick_params(axis="x", rotation=30)
        parts += ["<h2>Volumes par ville</h2>", to_img(fig), table(by_city)]

        recent = kpi.orderBy(F.desc("window_start")).limit(60).toPandas().sort_values("window_start")
        series = recent.groupby("window_start")["nb_transactions"].sum()
        fig, ax = plt.subplots(figsize=(9, 3))
        ax.plot(series.index, series.values, marker="o", color="#4C78A8")
        ax.set_title("Transactions par fenêtre de 10 minutes (toutes villes)")
        fig.autofmt_xdate()
        parts += ["<h2>Activité récente</h2>", to_img(fig)]

    sliding = read_delta(spark, GOLD_KPI_SLIDING)
    if sliding is not None:
        alerts = (sliding.filter((F.col("nb_transactions") >= 10) & (F.col("taux_rejet") >= 0.15))
                         .orderBy(F.desc("window_start")).toPandas())
        parts += ["<h2>Alertes taux de rejet (fenêtres glissantes, &ge; 15 %, &ge; 10 transactions)</h2>",
                  table(alerts) if len(alerts) else "<p>Aucune fenêtre au-dessus du seuil.</p>"]

    try:
        anomalies = spark.read.parquet(GRAPH_ANALYSIS)
        summary = anomalies.groupBy("anomaly_type").count().orderBy("anomaly_type").toPandas()
        detail = (anomalies.select("anomaly_type", "entity_id", "metric_name", "metric_value", "threshold", "reason")
                           .orderBy("anomaly_type", F.desc("metric_value")).toPandas())
        parts += ["<h2>Structures inhabituelles du graphe (à examiner, pas des fraudes avérées)</h2>",
                  table(summary), table(detail, 60)]
        metrics = json.loads(spark.read.parquet(lake("gold", "graph_metrics")).first()["metrics_json"])
        parts += ["<h3>Validation sur les scénarios injectés</h3>",
                  "<pre>" + html.escape(json.dumps(metrics.get("validation", {}), indent=2)) + "</pre>"]
        img = OUTPUTS / "part6_subgraph_suspect.png"
        if img.exists():
            parts += ["<h3>Sous-graphe suspect</h3>",
                      f'<img src="data:image/png;base64,{base64.b64encode(img.read_bytes()).decode()}">']
    except Exception as exc:
        parts.append(f"<p>Analyse de graphe indisponible ({html.escape(type(exc).__name__)}).</p>")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    page = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>SahlPay conformité</title>
<style>
body{{font-family:system-ui,sans-serif;margin:24px auto;max-width:1100px;padding:0 16px;color:#1f2933;background:#fff}}
h1{{margin-bottom:0}} .sub{{color:#616e7c;margin-top:4px}} img{{max-width:100%}}
table.t{{border-collapse:collapse;font-size:13px;margin:8px 0 20px}} table.t th,table.t td{{padding:4px 10px;border-bottom:1px solid #e4e7eb;text-align:left}}
pre{{background:#f5f7fa;padding:12px;font-size:12px;overflow:auto}}
</style></head><body>
<h1>SahlPay — tableau de bord conformité</h1><p class="sub">Généré le {now} depuis la couche Gold du lac MinIO</p>
{''.join(parts)}
</body></html>"""
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    out = OUTPUTS / "dashboard_conformite.html"
    out.write_text(page, encoding="utf-8")
    print(f"-> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
