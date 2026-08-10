"""Verify that Spark can run on this machine.

Checks the Java runtime version and the installed PySpark build, and reports
which Kafka connector coordinates will be used.
"""
import os
import re
import subprocess
import sys

SUPPORTED_JAVA = (11, 17, 21)

failures = 0


def passed(message):
    print(f"  PASS   {message}")


def failed(message, fix):
    global failures
    failures += 1
    print(f"  FAIL   {message}")
    print(f"         fix: {fix}")


def note(message):
    print(f"  NOTE   {message}")


def java_major(output):
    match = re.search(r'version "(\d+)(?:\.(\d+))?', output)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2) or 0) if major == 1 else major


def check_java():
    print("\nJava runtime")
    fix = "install Java 17 and set JAVA_HOME, see docs/setup.md"

    try:
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=15
        )
    except FileNotFoundError:
        failed("java not found on PATH", fix)
        return
    except Exception as exc:
        failed(f"could not run java: {exc}", fix)
        return

    major = java_major((result.stderr or "") + (result.stdout or ""))
    if major is None:
        failed("could not parse the java version", fix)
    elif major in SUPPORTED_JAVA:
        passed(f"java {major}")
    elif major > max(SUPPORTED_JAVA):
        failed(f"java {major} is newer than Spark supports", fix)
    else:
        failed(f"java {major} is too old", fix)

    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        passed(f"JAVA_HOME set to {java_home}")
    else:
        note("JAVA_HOME is not set, usually fine but set it if Spark fails")


def check_pyspark():
    print("\nPySpark")
    try:
        import pyspark
    except ImportError:
        failed("pyspark not installed", "pip install -r requirements.txt")
        return

    version = pyspark.__version__
    passed(f"pyspark {version}")

    major = int(version.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    note(f"connector org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}")

    if major < 4 and sys.version_info.minor >= 12:
        note(f"python 3.{sys.version_info.minor} with spark {major}.x can be "
             f"unreliable, prefer 'pip install pyspark>=4.0'")


def main():
    check_java()
    check_pyspark()

    print()
    if failures:
        print("Fix the failures above, then run this again.\n")
        return 1

    print("Spark ready. Next:  python pipeline/spark_windowed_counts.py")
    print("The first run downloads the connector and needs network access.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
