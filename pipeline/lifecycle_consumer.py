"""Consume lifecycle events and maintain both storage views.

PostgreSQL receives one row per order, updated as each state transition
arrives, so every interval is recorded. Redis holds the current position of
each driver. Both views come from the same event stream.

    python demo_consumer.py
"""
import json
import os

import psycopg
import redis
from confluent_kafka import Consumer

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)

CREATE = """
CREATE TABLE IF NOT EXISTS demo_orders (
    order_id      TEXT PRIMARY KEY,
    restaurant_id TEXT,
    driver_id     TEXT,
    zone          TEXT,
    total         NUMERIC(8,2),
    status        TEXT,
    batch_id      TEXT,
    batch_size    INTEGER DEFAULT 1,
    placed_at     TIMESTAMPTZ,
    accepted_at   TIMESTAMPTZ,
    ready_at      TIMESTAMPTZ,
    picked_up_at  TIMESTAMPTZ,
    delivered_at  TIMESTAMPTZ
)
"""

# Each event type updates its own timestamp column. The order row is built up
# over time rather than written once, which is why the insert is an upsert.
STAGE_COLUMN = {
    "order.placed": "placed_at",
    "order.accepted": "accepted_at",
    "order.ready": "ready_at",
    "order.picked_up": "picked_up_at",
    "order.delivered": "delivered_at",
}
STAGE_STATUS = {
    "order.placed": "placed",
    "order.accepted": "preparing",
    "order.ready": "ready",
    "order.picked_up": "in_transit",
    "order.delivered": "delivered",
}


def main():
    conn = psycopg.connect(DSN)
    conn.execute(CREATE)
    conn.commit()
    cache = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                        port=int(os.getenv("REDIS_PORT", "6379")),
                        decode_responses=True)
    cache.ping()
    print("stores ready")

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "demo-consumer",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["orders", "driver-locations"])
    print("consuming events   (Ctrl+C to stop)\n")

    seen = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value())
            seen += 1

            if msg.topic() == "driver-locations":
                cache.hset(
                    f"driver:{payload['driver_id']}:location",
                    mapping={"lat": payload["lat"], "lng": payload["lng"],
                             "ts": payload["ts"]},
                )
                continue

            stage = payload.get("event_type")
            column = STAGE_COLUMN.get(stage)
            if not column:
                continue

            conn.execute(
                f"""
                INSERT INTO demo_orders
                    (order_id, restaurant_id, driver_id, zone, total, status,
                     batch_id, batch_size, {column})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE
                SET {column}   = EXCLUDED.{column},
                    status     = EXCLUDED.status,
                    driver_id  = COALESCE(EXCLUDED.driver_id, demo_orders.driver_id),
                    batch_id   = COALESCE(EXCLUDED.batch_id, demo_orders.batch_id),
                    batch_size = GREATEST(EXCLUDED.batch_size, demo_orders.batch_size)
                """,
                (payload["order_id"], payload["restaurant_id"],
                 payload.get("driver_id"), payload.get("zone"),
                 payload.get("total"), STAGE_STATUS[stage],
                 payload.get("batch_id"), payload.get("batch_size", 1),
                 payload["occurred_at"]),
            )
            conn.commit()

            if seen % 25 == 0:
                print(f"  events processed: {seen}")

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n{seen} events processed\n")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
