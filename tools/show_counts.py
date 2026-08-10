"""Display the windowed order counts maintained by the Spark job.

Run repeatedly while the job is running to watch open windows update while
completed windows stay fixed.

Timestamps are TIMESTAMPTZ, so they render in whichever timezone the client
requests. Set TIMEZONE=local to use the machine timezone instead of UTC.
"""
import os

import psycopg

DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)
TIMEZONE = os.getenv("TIMEZONE", "UTC")

RECENT_WINDOWS = """
SELECT window_start, window_end, restaurant_id, orders, revenue
FROM order_counts
ORDER BY window_start DESC, orders DESC
LIMIT 30
"""

TOTALS = """
SELECT COUNT(DISTINCT window_start), COUNT(*), SUM(orders)
FROM order_counts
"""

BUSIEST = """
SELECT restaurant_id, SUM(orders) AS total
FROM order_counts
GROUP BY restaurant_id
ORDER BY total DESC
LIMIT 1
"""


def main():
    conn = psycopg.connect(DSN)
    if TIMEZONE.lower() != "local":
        conn.execute(f"SET TIME ZONE '{TIMEZONE}'")

    label = "local time" if TIMEZONE.lower() == "local" else TIMEZONE
    print()
    print("=" * 66)
    print(f"  ORDER COUNTS BY FIVE MINUTE WINDOW   ({label})")
    print("=" * 66)

    rows = conn.execute(RECENT_WINDOWS).fetchall()

    if not rows:
        print("\n  no rows yet, is pipeline/spark_windowed_counts.py running?\n")
        conn.close()
        return

    print(f"\n  {'window':<16}{'restaurant':<14}{'orders':>8}{'revenue':>12}")
    print(f"  {'-' * 50}")

    previous = None
    for start, end, restaurant, orders, revenue in rows:
        label = f"{start:%H:%M}-{end:%H:%M}" if start != previous else ""
        previous = start
        print(f"  {label:<16}{restaurant:<14}{orders:>8}{revenue:>12}")

    windows, row_count, order_total = conn.execute(TOTALS).fetchone()
    print(f"\n  windows: {windows}   rows: {row_count}   "
          f"orders counted: {order_total}")

    busiest = conn.execute(BUSIEST).fetchone()
    if busiest:
        print(f"  busiest restaurant: {busiest[0]} with {busiest[1]} orders")

    print()
    conn.close()


if __name__ == "__main__":
    main()
