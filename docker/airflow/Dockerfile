FROM apache/airflow:2.10.5-python3.11

USER root
# Java for Spark; JAVA_HOME symlink works on both amd64 and arm64
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# Add PySpark / Delta / pytest (same file as local development), pinned by
# Airflow's official constraints. The base image already contains Airflow 2.10.5.
ARG PYTHON_VERSION=3.11
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Download the Delta Lake jars at build time instead of on the first task run.
COPY docker/airflow/warmup_spark.py /tmp/warmup_spark.py
# Containers run as the host user ID (AIRFLOW_UID) in group 0, so the jar cache
# must be group-writable or Spark fails with "Permission denied" on startup.
RUN python /tmp/warmup_spark.py && chmod -R g+rwX /home/airflow/.ivy2
