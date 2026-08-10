"""Generate food delivery events and publish them to the event log.

Publishes to two topics:
    orders            keyed by restaurant_id
    driver-locations  keyed by driver_id

Message keys determine partition assignment, which preserves ordering of
events belonging to the same restaurant or driver.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
ORDER_RATE = float(os.getenv("ORDER_RATE", "0.3"))
PINGS_PER_TICK = int(os.getenv("PINGS_PER_TICK", "3"))

RESTAURANTS = [f"rest_{i}" for i in range(1, 6)]
DRIVERS = [f"driver_{i}" for i in range(1, 11)]
ITEMS = ["Burger", "Fries", "Pizza", "Salad", "Sushi", "Tacos", "Pad Thai"]

fake = Faker()
producer = Producer({"bootstrap.servers": BROKER})


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def on_delivery(err, _msg):
    if err is not None:
        print(f"  delivery failed: {err}")


def build_order():
    return {
        "order_id": str(uuid.uuid4()),
        "restaurant_id": random.choice(RESTAURANTS),
        "items": random.sample(ITEMS, random.randint(1, 3)),
        "total": round(random.uniform(8, 60), 2),
        "customer": fake.name(),
        "status": "placed",
        "created_at": utc_now(),
    }


def build_ping(driver_id):
    return {
        "driver_id": driver_id,
        "lat": round(random.uniform(40.70, 40.80), 6),
        "lng": round(random.uniform(-74.02, -73.93), 6),
        "ts": utc_now(),
    }


def main():
    print(f"producing events to {BROKER}   (Ctrl+C to stop)\n")
    orders_sent = 0
    pings_sent = 0

    try:
        while True:
            if random.random() < ORDER_RATE:
                order = build_order()
                producer.produce(
                    "orders",
                    key=order["restaurant_id"],
                    value=json.dumps(order),
                    callback=on_delivery,
                )
                orders_sent += 1
                print(f"  order  {order['restaurant_id']:<8} "
                      f"${order['total']:<7} | orders={orders_sent}")

            for driver_id in random.sample(DRIVERS, PINGS_PER_TICK):
                producer.produce(
                    "driver-locations",
                    key=driver_id,
                    value=json.dumps(build_ping(driver_id)),
                    callback=on_delivery,
                )
                pings_sent += 1

            producer.poll(0)
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"\nsent {orders_sent} orders and {pings_sent} driver pings\n")


if __name__ == "__main__":
    main()
