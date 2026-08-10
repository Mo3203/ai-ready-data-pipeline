"""Display the contents of both storage views side by side.

PostgreSQL holds the complete order history. Redis holds only the current
position of each driver. Both are derived from the same event log.
"""
import os

import psycopg
import redis

DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

REVENUE_BY_RESTAURANT = """
SELECT restaurant_id,
       COUNT(*)             AS orders,
       ROUND(SUM(total), 2) AS revenue
FROM orders
GROUP BY restaurant_id
ORDER BY revenue DESC
"""

DUPLICATE_CHECK = """
SELECT COUNT(*) FROM (
    SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1
) AS duplicates
"""


def show_postgres():
    print("=" * 62)
    print("  POSTGRESQL   complete order history")
    print("=" * 62)

    try:
        with psycopg.connect(DSN) as conn:
            total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            print(f"\n  rows: {total}\n")

            print(f"  {'restaurant':<14}{'orders':>8}{'revenue':>12}")
            print(f"  {'-' * 34}")
            for restaurant, orders, revenue in conn.execute(REVENUE_BY_RESTAURANT):
                print(f"  {restaurant:<14}{orders:>8}{revenue:>12}")

            duplicates = conn.execute(DUPLICATE_CHECK).fetchone()[0]
            print(f"\n  duplicate order ids: {duplicates}   (expected 0)")
    except Exception as exc:
        print(f"\n  could not read postgres: {exc}")


def show_redis():
    print("\n" + "=" * 62)
    print("  REDIS   current driver positions")
    print("=" * 62)

    try:
        client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        keys = sorted(client.keys("driver:*:location"))
        print(f"\n  keys: {len(keys)}\n")

        print(f"  {'driver':<12}{'lat':>12}{'lng':>13}{'updated':>16}")
        print(f"  {'-' * 53}")
        for key in keys:
            value = client.hgetall(key)
            driver = key.split(":")[1]
            print(f"  {driver:<12}{value.get('lat', ''):>12}"
                  f"{value.get('lng', ''):>13}{value.get('ts', '')[11:19]:>16}")
    except Exception as exc:
        print(f"\n  could not read redis: {exc}")


if __name__ == "__main__":
    print()
    show_postgres()
    show_redis()
    print("\n  One event log, two shapes of answer.\n")
