"""Persist every order from the log into PostgreSQL.

Runs in its own consumer group and starts from the earliest available offset,
so no order is missed. Inserts are idempotent because delivery is at-least-once
and offset commits are periodic, meaning a restart can replay recent messages.
"""
import json
import os

import psycopg
from confluent_kafka import Consumer

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT         PRIMARY KEY,
    restaurant_id TEXT         NOT NULL,
    total         NUMERIC(8,2) NOT NULL,
    status        TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL
)
"""

INSERT_ORDER = """
INSERT INTO orders (order_id, restaurant_id, total, status, created_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO NOTHING
"""


def main():
    conn = psycopg.connect(DSN)
    conn.execute(CREATE_TABLE)
    conn.commit()
    print("table 'orders' ready")

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "orders-to-postgres",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["orders"])
    print("writing orders to postgres   (Ctrl+C to stop)\n")

    inserted = 0
    skipped = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            order = json.loads(msg.value())
            cur = conn.execute(INSERT_ORDER, (
                order["order_id"],
                order["restaurant_id"],
                order["total"],
                order["status"],
                order["created_at"],
            ))
            conn.commit()

            if cur.rowcount == 1:
                inserted += 1
                print(f"  inserted  {order['restaurant_id']:<8} "
                      f"${order['total']:<7} inserted={inserted}")
            else:
                skipped += 1
                print(f"  duplicate skipped   skipped={skipped}")

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\ninserted={inserted}  duplicates skipped={skipped}\n")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
