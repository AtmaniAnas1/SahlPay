"""Partie 1 — SparkSession connectée au Master standalone et vérification de l'environnement.

docker compose exec jupyter spark-submit /opt/project/src/part1_cluster.py
"""
import argparse
import time

from common import KAFKA_BOOTSTRAP, LAKE, TOPIC, get_spark, hold, save_json, section


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold", type=int, default=0, help="secondes pendant lesquelles garder la Spark UI ouverte")
    args = ap.parse_args()

    spark = get_spark("SahlPay-Partie1-Cluster")
    sc = spark.sparkContext

    section("Partie 1 — SparkSession")
    # les executors s'enregistrent quelques secondes après la création du contexte
    for _ in range(30):
        if sc._jsc.sc().getExecutorMemoryStatus().size() >= 3:     # driver + 2 executors
            break
        time.sleep(1)
    executors = sc._jsc.sc().getExecutorMemoryStatus()   # driver + executors (adresse -> mémoire)
    info = {
        "spark_version": spark.version,
        "master": sc.master,
        "app_name": sc.appName,
        "application_id": sc.applicationId,
        "default_parallelism": sc.defaultParallelism,
        "driver_host": sc.getConf().get("spark.driver.host"),
        "block_managers (driver + executors)": executors.size(),
        "ui": sc.uiWebUrl,
    }
    for key, value in info.items():
        print(f"{key:<38} {value}")

    # job de contrôle : 8 tâches réparties sur les executors des deux Workers
    hosts = (sc.parallelize(range(8), 8)
               .map(lambda _: __import__("socket").gethostname())
               .distinct().collect())
    print(f"{'hotes des executors':<38} {sorted(hosts)}")
    info["executor_hosts"] = sorted(hosts)

    # accès au lac MinIO (bucket lake) et au topic Kafka
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(LAKE + "/")
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    print(f"{'bucket ' + LAKE:<38} existe={fs.exists(path)} contenu={[s.getPath().getName() for s in fs.listStatus(path)]}")
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    partitions = sorted(consumer.partitions_for_topic(TOPIC) or [])
    consumer.close()
    print(f"{'topic ' + TOPIC:<38} partitions={partitions}")
    info["kafka_partitions"] = partitions

    save_json("part1_cluster.json", info)
    hold(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
