"""SparkSession factory with Delta Lake (and optional S3/MinIO) support."""
from __future__ import annotations

import os
from typing import List

from pyspark.sql import SparkSession

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"  # matches PySpark 3.5.x


def get_spark(app_name: str = "ecommerce-batch") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        # Invalid dates/numbers become NULL and are caught by data-quality rules
        # instead of crashing the job.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.showConsoleProgress", "false")
    )

    extra_packages: List[str] = []
    endpoint = os.getenv("ECOM_S3_ENDPOINT")
    if endpoint:
        extra_packages.append(HADOOP_AWS_PACKAGE)
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
