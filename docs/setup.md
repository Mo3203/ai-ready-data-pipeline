# Setup notes

## Services

`scripts/setup.sh` starts four containers and creates the topics:

| Service          | Port  | Role                                  |
| ---------------- | ----- | ------------------------------------- |
| Redpanda         | 19092 | Event log, Kafka-compatible           |
| Redpanda Console | 8080  | Web interface for topics and messages |
| PostgreSQL       | 5432  | Order history and window summaries    |
| Redis            | 6379  | Current driver positions              |

Clients on the host connect to Redpanda on `localhost:19092`. Containers reach
it internally as `redpanda:9092`.

## Java for Spark

Spark requires a Java 17 runtime. On macOS:

```bash
brew install openjdk@17
```

Homebrew does not link it automatically. Add the following to `~/.zshrc` and
open a new terminal:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

On Intel Macs replace `/opt/homebrew` with `/usr/local`. On Debian or Ubuntu:

```bash
sudo apt install openjdk-17-jdk
```

Verify with `python tools/check_spark.py`.

## Kafka connector

The Spark jobs derive the connector coordinates from the installed PySpark
version, so the two cannot drift apart. The first run downloads the connector
from Maven Central and takes a few minutes.

## Resetting

```bash
bash scripts/reset_data.sh        # clear tables, Redis keys and topics
bash scripts/teardown.sh         # stop services
bash scripts/teardown.sh --volumes  # stop and delete all stored data
```

## Troubleshooting

| Symptom                                     | Cause and resolution                                                                            |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `Connection refused` on port 19092          | Services not running. `bash scripts/setup.sh`                                                   |
| Topic missing                               | `bash scripts/setup.sh` recreates both topics                                                   |
| Consumer receives nothing                   | Confirm the producer is running. Groups using `latest` only see new events                      |
| `relation "orders" does not exist`          | Start `pipeline/orders_to_postgres.py`, which creates it                                        |
| `java: command not found`                   | Install Java 17 and set `JAVA_HOME`                                                             |
| `ClassNotFoundException` mentioning kafka   | Connector download failed. Check network access and remove the Ivy cache in your home directory |
| Spark hangs at dependency resolution        | First download in progress                                                                      |
| Port already allocated                      | Another process holds 5432, 6379 or 8080. Stop it or remap in `docker-compose.yml`              |
| `confluent-kafka` fails to install on macOS | `brew install librdkafka`, then reinstall                                                       |
| Containers exit immediately                 | Allocate at least 4 GB to Docker                                                                |
