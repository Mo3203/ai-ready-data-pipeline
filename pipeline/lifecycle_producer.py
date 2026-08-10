"""Simulate a food delivery operation, including batched deliveries.

Time is compressed so a demonstration runs in minutes rather than hours. One
simulated second represents TIME_COMPRESSION real seconds, so a preparation
that takes forty seconds here represents thirty real minutes. Every duration
shown on the board is converted back to real-world minutes.

Orders are genuinely batched: when two orders from the same restaurant, going
to the same zone, become ready close enough together, one driver collects both
and they share a delivery trip. This is what the board counts and prices.
"""
import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TIME_COMPRESSION = float(os.getenv("TIME_COMPRESSION", "45"))

# Real-world durations, in minutes, converted to simulated seconds.
def sim(real_minutes):
    return real_minutes * 60 / TIME_COMPRESSION

# Restaurants differ in speed and, more importantly, in consistency. Median
# preparation and spread are given in real-world minutes.
RESTAURANTS = {
    "rest_1": {"name": "Shawarma House", "prep": 14, "spread": 2.5},
    "rest_2": {"name": "Pizza Corner",   "prep": 25, "spread": 4.0},
    "rest_3": {"name": "Burger Yard",    "prep": 11, "spread": 1.8},
    "rest_4": {"name": "Sushi Bar",      "prep": 29, "spread": 11.0},
    "rest_5": {"name": "Grill Station",  "prep": 19, "spread": 6.5},
}
DRIVERS = [f"driver_{i}" for i in range(1, 9)]
ZONES = ["zone_north", "zone_central", "zone_east"]
ITEMS = ["Burger", "Fries", "Pizza", "Salad", "Shawarma", "Grill Plate", "Sushi Set"]

# Order arrival is calibrated against driver capacity: roughly 21 orders per
# hour across eight drivers, giving about 2.6 orders per driver per hour and
# 500 to 600 per month, which is the operating target for a salaried fleet.
ORDER_INTERVAL_MIN, ORDER_INTERVAL_MAX = 3.1, 4.6  # simulated seconds

# A driver waits this long at the restaurant, then travels to the customer.
HANDOVER_MIN, HANDOVER_MAX = 2.0, 6.0        # real minutes
TRAVEL_MIN, TRAVEL_MAX = 9.0, 19.0           # real minutes
SECOND_DROP_MIN, SECOND_DROP_MAX = 4.0, 9.0  # extra leg for a batched order
BATCH_HOLD = 5.0                             # willing to wait this long to pair

producer = Producer({"bootstrap.servers": BROKER})

# Orders that are ready and waiting for a driver, keyed by restaurant and zone.
ready_pool = defaultdict(list)
pool_lock = threading.Lock()
stats = {"placed": 0, "delivered": 0, "batched": 0, "trips": 0}
stats_lock = threading.Lock()


def now():
    return datetime.now(timezone.utc).isoformat()


def emit(order, event_type, extra=None):
    payload = {
        "event_type": event_type,
        "order_id": order["order_id"],
        "restaurant_id": order["restaurant_id"],
        "zone": order["zone"],
        "occurred_at": now(),
    }
    if event_type == "order.placed":
        payload.update({"items": order["items"], "total": order["total"]})
    if extra:
        payload.update(extra)
    producer.produce("orders", key=order["restaurant_id"], value=json.dumps(payload))
    producer.poll(0)


def deliver(orders, driver_id):
    """Take one or two ready orders on a single trip."""
    batch_id = str(uuid.uuid4())[:8] if len(orders) > 1 else None

    for order in orders:
        emit(order, "order.picked_up", {
            "driver_id": driver_id,
            "batch_id": batch_id,
            "batch_size": len(orders),
        })

    time.sleep(sim(random.uniform(TRAVEL_MIN, TRAVEL_MAX)))
    emit(orders[0], "order.delivered", {
        "driver_id": driver_id, "batch_id": batch_id, "batch_size": len(orders),
    })

    if len(orders) > 1:
        # The second customer is nearby, so only a short extra leg.
        time.sleep(sim(random.uniform(SECOND_DROP_MIN, SECOND_DROP_MAX)))
        emit(orders[1], "order.delivered", {
            "driver_id": driver_id, "batch_id": batch_id, "batch_size": len(orders),
        })

    with stats_lock:
        stats["trips"] += 1
        stats["delivered"] += len(orders)
        if len(orders) > 1:
            stats["batched"] += len(orders)


def dispatch(order):
    """Decide whether this ready order travels alone or waits for a partner."""
    key = (order["restaurant_id"], order["zone"])

    with pool_lock:
        waiting = ready_pool[key]
        if waiting:
            partner = waiting.pop(0)
            pair = [partner, order]
        else:
            waiting.append(order)
            pair = None

    if pair:
        time.sleep(sim(random.uniform(HANDOVER_MIN, HANDOVER_MAX)))
        deliver(pair, random.choice(DRIVERS))
        return

    # Give a partner a short chance to appear before travelling alone.
    time.sleep(sim(BATCH_HOLD))
    with pool_lock:
        if order in ready_pool[key]:
            ready_pool[key].remove(order)
        else:
            return  # someone else paired with it

    time.sleep(sim(random.uniform(HANDOVER_MIN, HANDOVER_MAX)))
    deliver([order], random.choice(DRIVERS))


def run_order():
    profile = random.choice(list(RESTAURANTS))
    spec = RESTAURANTS[profile]
    order = {
        "order_id": str(uuid.uuid4())[:8],
        "restaurant_id": profile,
        "zone": random.choice(ZONES),
        "items": random.sample(ITEMS, random.randint(1, 3)),
        "total": round(random.uniform(28, 145), 2),
    }

    emit(order, "order.placed")
    with stats_lock:
        stats["placed"] += 1

    time.sleep(sim(random.uniform(1.0, 3.0)))
    emit(order, "order.accepted")

    prep = max(sim(5), sim(random.gauss(spec["prep"], spec["spread"])))
    time.sleep(prep)
    emit(order, "order.ready")

    dispatch(order)


def driver_pings():
    while True:
        for driver_id in random.sample(DRIVERS, 4):
            producer.produce("driver-locations", key=driver_id, value=json.dumps({
                "driver_id": driver_id,
                "lat": round(random.uniform(24.60, 24.80), 6),
                "lng": round(random.uniform(46.60, 46.80), 6),
                "ts": now(),
            }))
        producer.poll(0)
        time.sleep(2)


def report():
    while True:
        time.sleep(15)
        with stats_lock:
            share = (stats["batched"] / stats["delivered"] * 100) if stats["delivered"] else 0
            print(f"  placed={stats['placed']}  delivered={stats['delivered']}  "
                  f"batched={stats['batched']} ({share:.0f}%)  trips={stats['trips']}")


def main():
    print(f"lifecycle producer -> {BROKER}")
    print(f"time compression {TIME_COMPRESSION}x, so one simulated second "
          f"represents {TIME_COMPRESSION:.0f} real seconds\n")
    threading.Thread(target=driver_pings, daemon=True).start()
    threading.Thread(target=report, daemon=True).start()
    try:
        while True:
            threading.Thread(target=run_order, daemon=True).start()
            time.sleep(random.uniform(ORDER_INTERVAL_MIN, ORDER_INTERVAL_MAX))
    except KeyboardInterrupt:
        producer.flush()
        print("\nstopped\n")


if __name__ == "__main__":
    main()
