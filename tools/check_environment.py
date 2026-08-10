"""Verify that required packages and services are available.

Exits non-zero if anything is missing, with a suggested fix for each failure.
"""
import os
import sys

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=delivery user=delivery password=delivery",
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

TOPICS = ["orders", "driver-locations"]

failures = 0


def passed(message):
    print(f"  PASS   {message}")


def failed(message, fix):
    global failures
    failures += 1
    print(f"  FAIL   {message}")
    print(f"         fix: {fix}")


def check_packages():
    print("\nPython packages")
    install = "pip install -r requirements.txt"

    try:
        import confluent_kafka
        passed(f"confluent-kafka {confluent_kafka.version()[0]}")
    except ImportError:
        failed("confluent-kafka not installed", install)

    try:
        import psycopg
        passed(f"psycopg {psycopg.__version__}")
    except ImportError:
        failed("psycopg not installed", install)

    try:
        import redis
        passed(f"redis {redis.__version__}")
    except ImportError:
        failed("redis not installed", install)

    try:
        import faker  # noqa: F401
        passed("faker installed")
    except ImportError:
        failed("faker not installed", install)


def check_broker():
    try:
        from confluent_kafka.admin import AdminClient
        metadata = AdminClient({"bootstrap.servers": BROKER}).list_topics(timeout=10)
        passed(f"broker reachable at {BROKER}")

        for topic in TOPICS:
            entry = metadata.topics.get(topic)
            if entry and not entry.error:
                passed(f"topic '{topic}' has {len(entry.partitions)} partition(s)")
            else:
                failed(f"topic '{topic}' missing", "bash scripts/setup.sh")
    except Exception as exc:
        failed(f"cannot reach broker: {exc}", "bash scripts/setup.sh")


def check_postgres():
    try:
        import psycopg
        with psycopg.connect(DSN, connect_timeout=5) as conn:
            conn.execute("SELECT 1")
        passed("postgres reachable")
    except Exception as exc:
        failed(f"cannot reach postgres: {exc}", "bash scripts/setup.sh")


def check_redis():
    try:
        import redis
        redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=5
        ).ping()
        passed(f"redis reachable at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as exc:
        failed(f"cannot reach redis: {exc}", "bash scripts/setup.sh")


def main():
    check_packages()
    if failures:
        print("\nInstall the missing packages, then run this again.\n")
        return 1

    print("\nServices")
    check_broker()
    check_postgres()
    check_redis()

    print()
    if failures:
        print("Fix the failures above, then run this again.\n")
        return 1

    print("Environment ready. Start with:  python pipeline/producer.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
