#!/usr/bin/env bash
# Clear all demo data for a clean run. Starts the containers first if needed,
# since the data lives inside them.
#
#   bash reset_demo.sh

set -u
cd "$(dirname "$0")/.."

echo ""
echo "1. making sure the services are up"
if ! docker exec postgres pg_isready -U delivery >/dev/null 2>&1; then
  docker compose up -d >/dev/null 2>&1 || docker start redpanda postgres redis redpanda-console >/dev/null 2>&1
  for _ in $(seq 1 30); do
    docker exec postgres pg_isready -U delivery >/dev/null 2>&1 && break
    sleep 2
  done
fi

if ! docker exec postgres pg_isready -U delivery >/dev/null 2>&1; then
  echo "   postgres is not reachable. Is Docker Desktop running?"
  exit 1
fi
echo "   ready"

echo ""
echo "2. clearing data"
docker exec postgres psql -U delivery -d delivery -c \
  "DROP TABLE IF EXISTS demo_orders;" >/dev/null 2>&1 \
  && echo "   postgres: demo_orders dropped" \
  || echo "   postgres: nothing to drop"

docker exec redis redis-cli FLUSHALL >/dev/null 2>&1 \
  && echo "   redis: cleared" \
  || echo "   redis: not reachable"

# Recreate topics so the event log starts empty too.
for topic in orders driver-locations; do
  docker exec redpanda rpk topic delete "$topic" >/dev/null 2>&1
  docker exec redpanda rpk topic create "$topic" -p 3 >/dev/null 2>&1 \
    && echo "   topic recreated: $topic"
done

echo ""
echo "clean. now run:  bash scripts/run_board.sh"
echo ""
