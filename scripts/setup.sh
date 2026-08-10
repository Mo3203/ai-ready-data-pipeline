#!/usr/bin/env bash
# Start the local services and create the required topics.
# Safe to run repeatedly.

set -u

TOPICS=(orders driver-locations)
PARTITIONS=3

wait_for() {
  local name="$1" attempts="$2"
  shift 2
  echo ""
  echo "waiting for ${name}"
  for _ in $(seq 1 "${attempts}"); do
    if "$@" >/dev/null 2>&1; then
      echo "  ready"
      return 0
    fi
    sleep 2
  done
  echo "  timed out, check: docker compose logs ${name}"
  exit 1
}

echo ""
echo "starting containers"
docker compose up -d
sleep 5

wait_for redpanda 30 docker exec redpanda rpk cluster info
wait_for postgres 30 docker exec postgres pg_isready -U delivery
wait_for redis 15 docker exec redis redis-cli ping

echo ""
echo "creating topics"
for topic in "${TOPICS[@]}"; do
  if docker exec redpanda rpk topic list 2>/dev/null | awk '{print $1}' | grep -qx "${topic}"; then
    echo "  ${topic} already exists"
  else
    docker exec redpanda rpk topic create "${topic}" -p "${PARTITIONS}" >/dev/null 2>&1 \
      && echo "  created ${topic} with ${PARTITIONS} partitions"
  fi
done

echo ""
echo "setup complete, verify with: python tools/check_environment.py"
echo ""
