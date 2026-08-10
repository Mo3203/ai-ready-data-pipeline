"""Preparation-time prediction, and the two decisions that depend on it.

Everything here follows from one learned quantity: how long a given restaurant
takes to prepare an order. Because accepted_at and ready_at are captured as
separate events, that interval is measurable and a distribution can be fitted
per restaurant. Without those two timestamps none of this module can exist.

From that single prediction come two operational outputs:

    Batchability   two orders are batchable when their predicted ready times
                   fall close enough together to justify one driver trip.

    Customer ETA   predicted ready time, plus the handover delay and the travel
                   time for the delivery zone, both also learned from history.

Predictions are distributions rather than point estimates, because both
decisions are risk decisions. A promise of "25 minutes" is worth little without
knowing how often it holds.
"""
import os
import statistics
from datetime import datetime, timezone

import psycopg

DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)

MIN_SAMPLES = 5   # below this, the model declines to predict

# The simulation runs compressed: a preparation taking ~25 minutes in a real
# kitchen completes in ~25 seconds here. Horizons and tolerances are expressed
# in real-world terms and divided by this factor so the questions asked are
# proportionate to the simulated clock.
TIME_COMPRESSION = float(os.getenv("TIME_COMPRESSION", "45"))

# Confidence answers "how much can this prediction be trusted", not "is it
# nearly ready". It is the probability that the actual ready time falls within
# this tolerance of the predicted time, which is what a dispatcher needs before
# committing a driver or promising a customer.
ACCURACY_TOLERANCE = 300 / TIME_COMPRESSION  # represents 5 real minutes
OVERLAP_TOLERANCE = 240 / TIME_COMPRESSION   # represents 4 real minutes
STALE_AFTER = 180  # an order cooking longer was abandoned by a stopped producer


def _percentile(values, p):
    if not values:
        return None
    idx = max(0, min(len(values) - 1, int(len(values) * p) - 1))
    return sorted(values)[idx]


# --------------------------------------------------------------------------
# Learning
# --------------------------------------------------------------------------

def learn_prep_profiles(conn):
    """Fit a preparation-time distribution per restaurant."""
    rows = conn.execute("""
        SELECT restaurant_id,
               EXTRACT(EPOCH FROM (ready_at - accepted_at))
        FROM demo_orders
        WHERE ready_at IS NOT NULL AND accepted_at IS NOT NULL
          AND ready_at > accepted_at
    """).fetchall()

    samples = {}
    for restaurant_id, prep in rows:
        samples.setdefault(restaurant_id, []).append(float(prep))

    profiles = {}
    for restaurant_id, values in samples.items():
        if len(values) < MIN_SAMPLES:
            profiles[restaurant_id] = {"samples": len(values), "ready": False}
            continue
        mean = statistics.mean(values)
        sd = statistics.pstdev(values) or 1.0
        profiles[restaurant_id] = {
            "samples": len(values),
            "ready": True,
            "mean": mean,
            "sd": sd,
            "p10": _percentile(values, 0.10),
            "p50": statistics.median(values),
            "p90": _percentile(values, 0.90),
            # Coefficient of variation. Low means a consistent kitchen, which is
            # what allows batching to be aggressive there.
            "variability": sd / mean,
            "values": sorted(values),
        }
    return profiles


def learn_delivery_profiles(conn):
    """Learn handover delay, and travel time per delivery zone."""
    handover = [
        float(v) for (v,) in conn.execute("""
            SELECT EXTRACT(EPOCH FROM (picked_up_at - ready_at))
            FROM demo_orders
            WHERE picked_up_at IS NOT NULL AND ready_at IS NOT NULL
              AND picked_up_at > ready_at
        """).fetchall()
    ]

    rows = conn.execute("""
        SELECT zone, EXTRACT(EPOCH FROM (delivered_at - picked_up_at))
        FROM demo_orders
        WHERE delivered_at IS NOT NULL AND picked_up_at IS NOT NULL
          AND delivered_at > picked_up_at
    """).fetchall()

    travel = {}
    for zone, seconds in rows:
        travel.setdefault(zone, []).append(float(seconds))

    return {
        "handover_p50": statistics.median(handover) if len(handover) >= MIN_SAMPLES else None,
        "travel": {
            zone: {
                "p50": statistics.median(values),
                "p90": _percentile(values, 0.90),
                "samples": len(values),
            }
            for zone, values in travel.items() if len(values) >= MIN_SAMPLES
        },
    }


def backtest_prep(conn, profiles):
    """How far off the prediction is, in seconds, on completed orders.

    The restaurant median is used as the prediction and compared with what
    actually happened. This is the model's credibility figure.
    """
    rows = conn.execute("""
        SELECT restaurant_id,
               EXTRACT(EPOCH FROM (ready_at - accepted_at))
        FROM demo_orders
        WHERE ready_at IS NOT NULL AND accepted_at IS NOT NULL
          AND ready_at > accepted_at
        ORDER BY ready_at DESC
        LIMIT 200
    """).fetchall()

    errors = []
    for restaurant_id, actual in rows:
        profile = profiles.get(restaurant_id)
        if not profile or not profile.get("ready"):
            continue
        errors.append(abs(float(actual) - profile["p50"]))

    if len(errors) < MIN_SAMPLES:
        return {"ready": False, "samples": len(errors)}

    within = sum(1 for e in errors if e <= ACCURACY_TOLERANCE) / len(errors)
    return {
        "ready": True,
        "samples": len(errors),
        "mae": statistics.mean(errors),
        "within_5min": within,
    }


# --------------------------------------------------------------------------
# Prediction
# --------------------------------------------------------------------------

def conditional_forecast(profile, elapsed):
    """Predict the remaining preparation time for an order already cooking.

    Conditioning on elapsed time matters: an order twenty minutes into a
    kitchen with a fifteen minute median is not average, it is running late,
    and only the slower part of the distribution is still possible.

    Returns the median and ninetieth percentile of remaining time, plus the
    probability that the actual result lands within tolerance of the median.
    """
    if not profile.get("ready"):
        return None

    remaining = [v for v in profile["values"] if v >= elapsed]
    if len(remaining) < 3:
        # Past almost all recorded preparations for this restaurant.
        return None

    remaining.sort()
    p50 = remaining[len(remaining) // 2]
    p90 = remaining[max(0, int(len(remaining) * 0.9) - 1)]

    hits = sum(1 for v in remaining if abs(v - p50) <= ACCURACY_TOLERANCE)
    return {
        "ready_p50": max(0.0, p50 - elapsed),
        "ready_p90": max(0.0, p90 - elapsed),
        "confidence": hits / len(remaining),
    }


def in_flight_orders(conn):
    rows = conn.execute("""
        SELECT order_id, restaurant_id, zone, accepted_at
        FROM demo_orders
        WHERE accepted_at IS NOT NULL AND ready_at IS NULL
          AND accepted_at > now() - make_interval(secs => %s)
        ORDER BY accepted_at
    """, (STALE_AFTER,)).fetchall()

    now = datetime.now(timezone.utc)
    return [
        {"order_id": o, "restaurant_id": r, "zone": z,
         "elapsed": (now - a).total_seconds()}
        for o, r, z, a in rows
    ]


def predict(conn, profiles, delivery):
    """For every order in a kitchen: when ready, how trustworthy, and the ETA."""
    predictions = []
    for order in in_flight_orders(conn):
        profile = profiles.get(order["restaurant_id"], {})
        forecast = conditional_forecast(profile, order["elapsed"])

        if forecast is None:
            order["known"] = False
            predictions.append(order)
            continue

        handover = delivery["handover_p50"]
        travel = delivery["travel"].get(order["zone"])

        if handover is not None and travel:
            eta_p50 = forecast["ready_p50"] + handover + travel["p50"]
            eta_p90 = forecast["ready_p90"] + handover + travel["p90"]
        else:
            eta_p50 = eta_p90 = None

        order.update({
            "known": True,
            "ready_p50": forecast["ready_p50"],
            "ready_p90": forecast["ready_p90"],
            "confidence": forecast["confidence"],
            "eta_p50": eta_p50,
            "eta_p90": eta_p90,
            "variability": profile["variability"],
        })
        predictions.append(order)
    return predictions


def find_batch_pairs(predictions):
    """Pairs whose predicted ready times are close enough for one trip."""
    known = [p for p in predictions if p.get("known") and p.get("confidence") is not None]
    pairs = []
    for i, a in enumerate(known):
        for b in known[i + 1:]:
            if a["restaurant_id"] != b["restaurant_id"] or a["zone"] != b["zone"]:
                continue
            gap = abs(a["ready_p50"] - b["ready_p50"])
            if gap > OVERLAP_TOLERANCE:
                continue
            # The pair is only as reliable as the less predictable of the two.
            pairs.append({
                "order_a": a["order_id"], "order_b": b["order_id"],
                "restaurant_id": a["restaurant_id"], "zone": a["zone"],
                "gap_seconds": gap,
                "confidence": min(a["confidence"], b["confidence"]),
                "saved_trip": True,
            })
    pairs.sort(key=lambda p: -p["confidence"])
    return pairs


def snapshot():
    with psycopg.connect(DSN) as conn:
        profiles = learn_prep_profiles(conn)
        delivery = learn_delivery_profiles(conn)
        predictions = predict(conn, profiles, delivery)
        accuracy = backtest_prep(conn, profiles)

    pairs = find_batch_pairs(predictions)

    # Flag each order that appears in at least one candidate pair.
    batchable = {p["order_a"] for p in pairs} | {p["order_b"] for p in pairs}
    for order in predictions:
        order["batchable"] = order["order_id"] in batchable

    return {
        "profiles": {
            k: {kk: vv for kk, vv in v.items() if kk != "values"}
            for k, v in profiles.items()
        },
        "delivery": delivery,
        "predictions": predictions,
        "pairs": pairs,
        "accuracy": accuracy,
        "restaurants_modelled": sum(1 for p in profiles.values() if p.get("ready")),
        "observations": sum(p["samples"] for p in profiles.values()),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(snapshot(), indent=2, default=str))
