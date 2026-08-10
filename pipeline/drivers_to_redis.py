"""Maintain the current location of each driver in Redis.

Runs in its own consumer group and starts from the latest offset, since only
the present position is useful. Each ping overwrites the same key, so the
keyspace stays proportional to the number of drivers rather than to the volume
of events. Full history remains available in the log.
"""
import json
import os

import redis
from confluent_kafka import Consumer

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def main():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    client.ping()
    print(f"connected to redis at {REDIS_HOST}:{REDIS_PORT}")

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "drivers-to-redis",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["driver-locations"])
    print("writing driver locations to redis   (Ctrl+C to stop)\n")

    updates = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            ping = json.loads(msg.value())
            key = f"driver:{ping['driver_id']}:location"
            client.hset(key, mapping={
                "lat": ping["lat"],
                "lng": ping["lng"],
                "ts": ping["ts"],
            })
            updates += 1

            stored = len(client.keys("driver:*:location"))
            print(f"  {key:<28} lat={ping['lat']:<10} lng={ping['lng']:<11} "
                  f"| updates={updates} keys={stored}")

    except KeyboardInterrupt:
        pass
    finally:
        stored = len(client.keys("driver:*:location"))
        print(f"\n{updates} pings processed, {stored} driver keys stored\n")
        consumer.close()


if __name__ == "__main__":
    main()
