# Image unique pour Spark Master, Workers, History Server et Jupyter :
# même Spark (3.5.1), même Scala (2.12), même Python (3.10) côté driver et executors.
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Connecteurs ajoutés au classpath de tous les noeuds (pas de --packages à l'exécution)
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar ; do \
      curl -fsSLO "$MAVEN/$jar"; \
    done; \
    curl -fsSLO https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
 && pip3 install --no-cache-dir --no-deps delta-spark==3.2.0

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/spark-events /home/spark /opt/project /opt/spark/work-dir \
 && chown -R spark:spark /opt/spark-events /home/spark /opt/spark/work-dir

# pyspark + py4j depuis la distribution Spark ; le module Python graphframes est contenu dans son jar
ENV SPARK_HOME=/opt/spark \
    HOME=/home/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/jars/graphframes-0.8.3-spark3.5-s_2.12.jar:/opt/project/src

WORKDIR /opt/project
USER spark
