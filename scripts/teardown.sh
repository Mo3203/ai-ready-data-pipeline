#!/usr/bin/env bash
# Stop the local services. Pass --volumes to also delete stored data.

set -u

if [ "${1:-}" = "--volumes" ]; then
  echo "stopping containers and removing volumes"
  docker compose down -v
else
  echo "stopping containers, data is preserved"
  docker compose down
fi

echo "done"
