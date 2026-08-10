"""Operations board served over HTTP. No dependencies beyond the pipeline's own.

    python dashboard.py     then open http://localhost:8090

Three views from one event stream:
    Operations   the business reading, for a non-technical audience
    Pipeline     the machinery underneath, for a technical audience
    Intelligence what the captured timestamps make possible
"""
import json
import os
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import psycopg
import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "analytics"))
import readiness_model

PORT = int(os.getenv("DASHBOARD_PORT", "8090"))
DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)
BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")

# Stated on screen so the arithmetic is auditable.
DRIVER_SALARY_SAR = 3000
SHIFT_HOURS = 8
WORKING_DAYS = 26

# The simulation runs compressed: a preparation taking ~25 minutes in a real
# kitchen completes in ~25 seconds here, so one simulated hour represents roughly
# TIME_COMPRESSION real hours. Projections divide by it, otherwise the headline
# figures come out an order of magnitude too high.
TIME_COMPRESSION = float(os.getenv("TIME_COMPRESSION", "45"))


RATE_WINDOW_SECONDS = 300  # measure throughput over recent activity only

# A delivery consumes more driver time than the pickup-to-drop leg. The driver
# must first travel to the restaurant, and afterwards reposition for the next
# job. Both are paid time and both belong in the cost of an order.
APPROACH_MINUTES = 8.0     # depot or previous drop, to the restaurant
REPOSITION_MINUTES = 7.0   # after the drop, before the next pickup

# Paid minutes available per driver per month.
DRIVER_MINUTES_PER_MONTH = 8 * 26 * 60


def operations_metrics():
    with psycopg.connect(DSN) as conn:
        totals = conn.execute("""
            SELECT COUNT(*) FILTER (WHERE delivered_at IS NOT NULL),
                   COUNT(*) FILTER (WHERE status <> 'delivered'),
                   COUNT(DISTINCT driver_id) FILTER (WHERE driver_id IS NOT NULL),
                   COALESCE(SUM(total) FILTER (WHERE delivered_at IS NOT NULL), 0)
            FROM demo_orders
        """).fetchone()
        delivered, active, drivers, revenue = totals

        # Throughput is measured over a recent rolling window rather than the
        # whole table, so idle gaps between sessions cannot distort the rate.
        recent = conn.execute("""
            SELECT COUNT(*),
                   COUNT(DISTINCT driver_id),
                   EXTRACT(EPOCH FROM (MAX(delivered_at) - MIN(delivered_at)))
            FROM demo_orders
            WHERE delivered_at > now() - make_interval(secs => %s)
        """, (RATE_WINDOW_SECONDS,)).fetchone()
        recent_delivered = recent[0] or 0
        recent_drivers = recent[1] or 0
        recent_span = float(recent[2] or 0)

        # Batched against single deliveries, and what each is worth.
        split = conn.execute("""
            SELECT COALESCE(SUM(CASE WHEN batch_size > 1 THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN batch_size > 1 THEN total ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN batch_size = 1 THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN batch_size = 1 THEN total ELSE 0 END), 0),
                   COUNT(DISTINCT batch_id) FILTER (WHERE batch_id IS NOT NULL)
            FROM demo_orders WHERE delivered_at IS NOT NULL
        """).fetchone()
        batched_orders, batched_revenue, single_orders, single_revenue, batch_trips = split

        # Measured driver time per trip, single against batched. A batched trip
        # runs from the first pickup to the last drop, and covers two orders.
        trip_times = conn.execute("""
            SELECT
              AVG(CASE WHEN batch_size = 1
                       THEN EXTRACT(EPOCH FROM (delivered_at - picked_up_at)) END),
              (SELECT AVG(span) FROM (
                  SELECT EXTRACT(EPOCH FROM (MAX(delivered_at) - MIN(picked_up_at))) AS span
                  FROM demo_orders
                  WHERE batch_id IS NOT NULL AND delivered_at IS NOT NULL
                  GROUP BY batch_id
              ) AS batched_spans)
            FROM demo_orders
            WHERE delivered_at IS NOT NULL AND picked_up_at IS NOT NULL
        """).fetchone()
        single_trip_seconds = float(trip_times[0] or 0)
        batch_trip_seconds = float(trip_times[1] or 0)

        intervals = [
            float(v) if v is not None else 0.0
            for v in conn.execute("""
                SELECT AVG(EXTRACT(EPOCH FROM (ready_at - accepted_at))),
                       AVG(EXTRACT(EPOCH FROM (picked_up_at - ready_at))),
                       AVG(EXTRACT(EPOCH FROM (delivered_at - placed_at)))
                FROM demo_orders WHERE delivered_at IS NOT NULL
            """).fetchone()
        ]

        rail = conn.execute("""
            SELECT order_id, restaurant_id, status, total, batch_size
            FROM demo_orders
            ORDER BY COALESCE(delivered_at, picked_up_at, ready_at,
                              accepted_at, placed_at) DESC
            LIMIT 14
        """).fetchall()

        by_stage = dict(conn.execute("""
            SELECT status, COUNT(*) FROM demo_orders GROUP BY status
        """).fetchall())

    warming = recent_delivered < 5 or recent_span < 60 or not recent_drivers

    # Every batched pair replaces two driver trips with one.
    trips_saved = int(batched_orders) - int(batch_trips or 0)
    total_delivered = int(batched_orders) + int(single_orders)
    batch_share = (batched_orders / total_delivered) if total_delivered else 0

    # Convert measured intervals from simulated seconds into real minutes.
    def real_minutes(seconds):
        return seconds * TIME_COMPRESSION / 60.0

    wait_minutes = real_minutes(intervals[1])          # ready to picked up
    travel_minutes = real_minutes(single_trip_seconds)  # picked up to delivered
    second_leg = max(0.0, real_minutes(batch_trip_seconds) - travel_minutes)

    # A full driver cycle, not just the delivery leg.
    cycle_single = APPROACH_MINUTES + wait_minutes + travel_minutes + REPOSITION_MINUTES
    cycle_batched = cycle_single + second_leg

    cost_per_minute = DRIVER_SALARY_SAR / DRIVER_MINUTES_PER_MONTH

    if warming or cycle_single <= 0:
        cost_single_order = cost_batched_order = cost_per_order = 0.0
        projected_month = per_driver_hour = 0.0
        minutes_per_single = minutes_per_batched = 0.0
        saving_per_batched = 0.0
        cost_at_40 = cost_without_batching = 0.0
    else:
        # Two orders share one batched cycle, so each carries half of it.
        minutes_per_single = cycle_single
        minutes_per_batched = cycle_batched / 2.0

        cost_single_order = minutes_per_single * cost_per_minute
        cost_batched_order = minutes_per_batched * cost_per_minute

        # Driver capacity, not order arrivals, sets how many orders a driver
        # completes. Batching raises it because one cycle serves two customers.
        avg_minutes_per_order = (
            (1 - batch_share) * minutes_per_single + batch_share * minutes_per_batched
        )
        projected_month = DRIVER_MINUTES_PER_MONTH / avg_minutes_per_order
        per_driver_hour = projected_month / (SHIFT_HOURS * WORKING_DAYS)
        cost_per_order = DRIVER_SALARY_SAR / projected_month

        saving_per_batched = 1 - minutes_per_batched / minutes_per_single
        cost_without_batching = cost_single_order
        cost_at_40 = cost_single_order * 0.60 + cost_batched_order * 0.40

    driver_minutes_actual = 0.0
    driver_minutes_if_single = 0.0
    time_saved_seconds = 0.0

    return {
        "warming": warming,
        "delivered": delivered,
        "active": active,
        "drivers": drivers,
        "revenue": float(revenue or 0),
        "per_driver_hour": per_driver_hour,
        "projected_month": projected_month,
        "cost_per_order": cost_per_order,
        "avg_prep": intervals[0],
        "avg_wait": intervals[1],
        "avg_total": intervals[2],
        "batched_orders": int(batched_orders),
        "batched_revenue": float(batched_revenue),
        "single_orders": int(single_orders),
        "single_revenue": float(single_revenue),
        "batch_trips": int(batch_trips or 0),
        "trips_saved": trips_saved,
        "batch_share": batch_share,
        "cost_single_order": cost_single_order,
        "cost_batched_order": cost_batched_order,
        "minutes_per_single": minutes_per_single,
        "minutes_per_batched": minutes_per_batched,
        "cost_without_batching": cost_without_batching,
        "cost_at_40_share": cost_at_40,
        "saving_per_batched": saving_per_batched,
        "cycle_single": cycle_single if not warming else 0,
        "cycle_batched": cycle_batched if not warming else 0,
        "wait_minutes": wait_minutes,
        "travel_minutes": travel_minutes,
        "projected_month_at_40": (
            DRIVER_MINUTES_PER_MONTH / (0.60 * minutes_per_single + 0.40 * minutes_per_batched)
            if minutes_per_batched else 0
        ),
        "time_compression": TIME_COMPRESSION,
        "rail": [
            {"order_id": o, "restaurant_id": r, "status": s,
             "total": float(t or 0), "batch_size": int(bs or 1)}
            for o, r, s, t, bs in rail
        ],
        "by_stage": by_stage,
    }


def pipeline_metrics():
    info = {"broker": BROKER, "topics": [], "groups": [], "redis_keys": 0,
            "postgres_rows": 0}
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BROKER})
        meta = admin.list_topics(timeout=5)
        for name in ["orders", "driver-locations"]:
            entry = meta.topics.get(name)
            if entry and not entry.error:
                info["topics"].append({
                    "name": name,
                    "partitions": len(entry.partitions),
                })
    except Exception as exc:
        info["broker_error"] = str(exc)

    try:
        cache = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                            port=int(os.getenv("REDIS_PORT", "6379")),
                            decode_responses=True)
        keys = cache.keys("driver:*:location")
        info["redis_keys"] = len(keys)
        info["drivers"] = []
        for key in sorted(keys)[:8]:
            value = cache.hgetall(key)
            info["drivers"].append({
                "driver": key.split(":")[1],
                "lat": value.get("lat"), "lng": value.get("lng"),
                "ts": (value.get("ts") or "")[11:19],
            })
    except Exception as exc:
        info["redis_error"] = str(exc)

    try:
        with psycopg.connect(DSN) as conn:
            info["postgres_rows"] = conn.execute(
                "SELECT COUNT(*) FROM demo_orders").fetchone()[0]
            info["complete_rows"] = conn.execute(
                "SELECT COUNT(*) FROM demo_orders WHERE delivered_at IS NOT NULL"
            ).fetchone()[0]
            sample = conn.execute("""
                SELECT order_id, restaurant_id, placed_at, accepted_at,
                       ready_at, picked_up_at, delivered_at
                FROM demo_orders WHERE delivered_at IS NOT NULL
                ORDER BY delivered_at DESC LIMIT 1
            """).fetchone()
            if sample:
                info["sample_order"] = {
                    "order_id": sample[0], "restaurant_id": sample[1],
                    "placed": str(sample[2])[11:19],
                    "accepted": str(sample[3])[11:19] if sample[3] else None,
                    "ready": str(sample[4])[11:19] if sample[4] else None,
                    "picked_up": str(sample[5])[11:19] if sample[5] else None,
                    "delivered": str(sample[6])[11:19] if sample[6] else None,
                }
    except Exception as exc:
        info["postgres_error"] = str(exc)

    return info


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, payload):
        body = json.dumps(payload, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            if self.path.startswith("/api/operations"):
                return self._json(operations_metrics())
            if self.path.startswith("/api/pipeline"):
                return self._json(pipeline_metrics())
            if self.path.startswith("/api/intelligence"):
                snap = readiness_model.snapshot()
                snap["time_compression"] = TIME_COMPRESSION
                return self._json(snap)
        except Exception as exc:
            return self._json({"error": str(exc)})

        html = (Path(__file__).parent / "board.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)


if __name__ == "__main__":
    print(f"\n  operations board:  http://localhost:{PORT}\n")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
