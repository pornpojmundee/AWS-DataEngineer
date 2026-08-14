# Extends the official Airflow image with Java (required by Spark) and
# PySpark, so the `spark-submit` command used in fintech_pipeline_dag.py
# actually works inside the Airflow containers.
FROM apache/airflow:2.9.3

USER root

# Spark needs a JVM. default-jdk-headless keeps the image smaller than the
# full JDK.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Match the pyspark version already used elsewhere in this repo
# (requirements.txt, tests) so behavior is consistent.
RUN pip install --no-cache-dir pyspark==3.5.1
