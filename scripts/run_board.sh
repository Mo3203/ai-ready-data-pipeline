#!/usr/bin/env bash
# Start everything needed for the demo, in order, in one command.
#
#   bash run_demo.sh
#
# Stops cleanly with Ctrl+C.

set -u
cd "$(dirname "$0")/.."

PIDS=()
cleanup() {
  echo ""
  echo "stopping..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null
  echo "stopped"
  exit 0
}
trap cleanup INT TERM

echo ""
echo "1. checking services"
if ! docker exec redpanda rpk cluster info >/dev/null 2>&1; then
  echo "   starting containers"
  docker compose up -d >/dev/null 2>&1 || docker start redpanda postgres redis >/dev/null 2>&1
  for _ in $(seq 1 30); do
    docker exec redpanda rpk cluster info >/dev/null 2>&1 && break
    sleep 2
  done
fi
docker exec redpanda rpk cluster info >/dev/null 2>&1 \
  && echo "   broker ready" \
  || { echo "   BROKER NOT REACHABLE - run: docker compose up -d"; exit 1; }
docker exec postgres pg_isready -U delivery >/dev/null 2>&1 \
  && echo "   postgres ready" || { echo "   POSTGRES NOT REACHABLE"; exit 1; }
docker exec redis redis-cli ping >/dev/null 2>&1 \
  && echo "   redis ready" || { echo "   REDIS NOT REACHABLE"; exit 1; }

echo ""
echo "2. creating topics"
for topic in orders driver-locations; do
  docker exec redpanda rpk topic create "$topic" -p 3 >/dev/null 2>&1 \
    && echo "   created $topic" || echo "   $topic exists"
done

echo ""
echo "3. starting pipeline"
python pipeline/lifecycle_consumer.py > /tmp/board_consumer.log 2>&1 &
PIDS+=($!); echo "   consumer started"
sleep 2
python pipeline/lifecycle_producer.py > /tmp/board_producer.log 2>&1 &
PIDS+=($!); echo "   producer started"
sleep 2
python dashboard/server.py > /tmp/board_server.log 2>&1 &
PIDS+=($!); echo "   dashboard started"
sleep 2

echo ""
echo "   ---------------------------------------------"
echo "     open  http://localhost:8090"
echo "   ---------------------------------------------"
echo ""
echo "   Let it run 3-4 minutes before presenting, so the"
echo "   Intelligence view has enough history to model."
echo ""
echo "   logs: /tmp/board_*.log      Ctrl+C to stop"
echo ""

wait
