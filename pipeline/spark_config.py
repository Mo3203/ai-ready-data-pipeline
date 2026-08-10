"""Shared Spark configuration for the streaming jobs."""
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructType,
    TimestampType,
)

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")

SPARK_VERSION = pyspark.__version__
SCALA_VERSION = "2.13" if int(SPARK_VERSION.split(".")[0]) >= 4 else "2.12"
KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)

ORDER_SCHEMA = (
    StructType()
    .add("order_id", StringType())
    .add("restaurant_id", StringType())
    .add("total", DoubleType())
    .add("status", StringType())
    .add("created_at", TimestampType())
)


def build_session(app_name):
    """Create a Spark session with the matching Kafka connector."""
    session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def read_orders_stream(session):
    """Return the raw orders topic as a streaming DataFrame."""
    return (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
    )
