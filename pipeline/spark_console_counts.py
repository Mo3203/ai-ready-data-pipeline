"""Count orders per restaurant in five minute windows, printed to the console.

Aggregation uses event time (created_at) rather than arrival time, so results
stay correct when processing lags. The watermark bounds how long a window
accepts late-arriving events before it is finalised.

Run pipeline/spark_windowed_counts.py instead to persist results.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql.functions import col, count, from_json, window

from spark_config import (
    KAFKA_PACKAGE,
    ORDER_SCHEMA,
    SPARK_VERSION,
    build_session,
    read_orders_stream,
)

WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "2 minutes"
TRIGGER_INTERVAL = "10 seconds"


def main():
    print(f"\nspark {SPARK_VERSION}")
    print(f"connector {KAFKA_PACKAGE}")
    print("the first run downloads the connector and is slow\n")

    spark = build_session("orders-per-restaurant-console")

    orders = (
        read_orders_stream(spark)
        .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("order"))
        .select("order.*")
        .withWatermark("created_at", WATERMARK_DELAY)
    )

    counts = (
        orders
        .groupBy(window(col("created_at"), WINDOW_DURATION), col("restaurant_id"))
        .agg(count("*").alias("orders"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("restaurant_id"),
            col("orders"),
        )
    )

    query = (
        counts.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"counting orders, table prints every {TRIGGER_INTERVAL}. "
          f"Ctrl+C to stop.\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
