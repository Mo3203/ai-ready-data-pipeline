"""Maintain a live summary table of orders per restaurant per five minute window.

Spark provides no built-in database sink for streaming queries, so each
micro-batch is written through foreachBatch. Window totals are revised as new
orders arrive, so rows are upserted on (window_start, restaurant_id) rather
than inserted, keeping exactly one row per window and restaurant.

Results are queryable while the job runs:
    SELECT * FROM order_counts ORDER BY window_start DESC, orders DESC;
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import psycopg
from pyspark.sql.functions import col, count, from_json, window
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

from spark_config import ORDER_SCHEMA, build_session, read_orders_stream

DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", "/tmp/spark-order-counts-checkpoint")

WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "2 minutes"
TRIGGER_INTERVAL = "10 seconds"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS order_counts (
    window_start  TIMESTAMPTZ   NOT NULL,
    window_end    TIMESTAMPTZ   NOT NULL,
    restaurant_id TEXT          NOT NULL,
    orders        INTEGER       NOT NULL,
    revenue       NUMERIC(10,2) NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, restaurant_id)
)
"""

UPSERT_COUNTS = """
INSERT INTO order_counts
    (window_start, window_end, restaurant_id, orders, revenue)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (window_start, restaurant_id) DO UPDATE
SET orders     = EXCLUDED.orders,
    revenue    = EXCLUDED.revenue,
    updated_at = now()
"""


def create_table():
    with psycopg.connect(DSN) as conn:
        conn.execute(CREATE_TABLE)
        conn.commit()
    print("table 'order_counts' ready")


def write_batch(batch_df, batch_id):
    """Upsert one micro-batch of window totals into PostgreSQL."""
    rows = batch_df.collect()
    if not rows:
        print(f"batch {batch_id}: no changes")
        return

    values = [
        (r["window_start"], r["window_end"], r["restaurant_id"],
         r["orders"], r["revenue"])
        for r in rows
    ]

    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_COUNTS, values)
        conn.commit()

    windows = sorted({r["window_start"].strftime("%H:%M") for r in rows})
    print(f"batch {batch_id}: wrote {len(rows)} row(s) for "
          f"window(s) {', '.join(windows)}")


def main():
    create_table()
    spark = build_session("order-counts-to-postgres")

    orders = (
        read_orders_stream(spark)
        .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("order"))
        .select("order.*")
        .withWatermark("created_at", WATERMARK_DELAY)
    )

    counts = (
        orders
        .groupBy(window(col("created_at"), WINDOW_DURATION), col("restaurant_id"))
        .agg(
            count("*").alias("orders"),
            spark_round(spark_sum("total"), 2).alias("revenue"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("restaurant_id"),
            col("orders"),
            col("revenue"),
        )
    )

    query = (
        counts.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("\nwriting windowed counts to postgres   (Ctrl+C to stop)")
    print("inspect them with:  python tools/show_counts.py\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
