"""Éléments partagés par les scripts PySpark SahlPay : session, chemins du lac, schémas, enrichissement."""
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType, TimestampType)

ROOT = Path(__file__).resolve().parent.parent
OUTPUTS = Path(os.environ.get("SAHLPAY_OUTPUTS", ROOT / "outputs"))
LAKE = os.environ.get("SAHLPAY_LAKE", "s3a://lake").rstrip("/")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "transactions"
STATUSES = ["INITIATED", "AUTHORIZED", "COMPLETED", "REJECTED", "REVERSED"]

TX_SCHEMA = StructType([
    StructField("tx_id", StringType(), False),
    StructField("type", StringType(), True),
    StructField("payer_wallet", StringType(), True),
    StructField("payee_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_mad", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])

MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])


def lake(*parts):
    return "/".join([LAKE, *parts])


def checkpoint(name):
    return lake("checkpoints", name)


HISTORY_RAW = lake("bronze", "history")
MERCHANTS_PATH = lake("bronze", "referential", "merchants.json")
SILVER_HISTORY = lake("silver", "transactions_history")
SILVER_CLEAN = lake("silver", "transactions_clean")
BRONZE_RAW = lake("bronze", "transactions_raw")
GOLD_KPI_TUMBLING = lake("gold", "kpi_city_10min")
GOLD_KPI_SLIDING = lake("gold", "kpi_city_reject_rate_sliding")
GOLD_SESSIONS = lake("gold", "device_sessions")
GRAPH_MODEL = lake("gold", "graph_model")
GRAPH_ANALYSIS = lake("gold", "graph_analysis")
GRAPH_COMPARE = lake("gold", "graph_compare")


def get_spark(app_name, **conf):
    builder = SparkSession.builder.appName(app_name).master(
        os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def amount_band_col(amount):
    """Tranche de montant en expression native (optimisable par Catalyst, sans UDF)."""
    return (F.when(amount < 50, "micro")
             .when(amount <= 2000, "standard")
             .otherwise("eleve"))


def read_merchants(spark):
    return spark.read.schema(MERCHANT_SCHEMA).json(MERCHANTS_PATH)


def enrich(tx, merchants):
    """Jointure au référentiel (P2M seulement), tranche de montant et date de partition.

    Fonctionne en batch comme en streaming (jointure stream-static)."""
    m = merchants.select(
        F.col("merchant_id"),
        F.col("name").alias("merchant_name"),
        F.col("category").alias("merchant_category"),
        F.col("city").alias("merchant_city"),
    )
    return (tx.join(F.broadcast(m), (tx.type == "P2M") & (tx.payee_id == m.merchant_id), "left")
              .drop("merchant_id")
              .withColumn("amount_band", amount_band_col(F.col("amount_mad")))
              .withColumn("date", F.to_date("event_time")))


def read_kafka_stream(spark, starting_offsets="latest", max_offsets=None, group_prefix="sahlpay"):
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", starting_offsets)
              .option("failOnDataLoss", "false")
              .option("groupIdPrefix", group_prefix))
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    return reader.load()


def parse_kafka(raw):
    """value (bytes JSON) -> colonnes typées + métadonnées Kafka."""
    return (raw.select(
                F.from_json(F.col("value").cast("string"), TX_SCHEMA).alias("e"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"))
               .select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
               .filter(F.col("tx_id").isNotNull()))


TIMINGS = {}


@contextmanager
def timed(label):
    start = time.perf_counter()
    yield
    TIMINGS[label] = round(time.perf_counter() - start, 3)
    print(f"[TEMPS] {label} : {TIMINGS[label]} s")


def save_json(name, obj):
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    path = OUTPUTS / name
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"-> {path}")
    return path


def save_text(name, text):
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    path = OUTPUTS / name
    path.write_text(text, encoding="utf-8")
    print(f"-> {path}")
    return path


def load_json(name, default=None):
    path = OUTPUTS / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def hold(seconds):
    """Garde l'application vivante pour capturer la Spark UI (http://localhost:4040)."""
    if seconds > 0:
        print(f"Application maintenue {seconds} s : Spark UI sur http://localhost:4040")
        time.sleep(seconds)


def section(title):
    print("\n" + "=" * 100 + f"\n{title}\n" + "=" * 100)
