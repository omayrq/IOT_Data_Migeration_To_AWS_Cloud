#!/usr/bin/env bash
# Registers Kafka Connect connectors against the local (or MSK Connect) REST API.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/connectors"

register () {
  local file=$1
  local name
  name=$(basename "$file" .json)
  echo "==> Registering $name"
  curl -s -X POST -H "Content-Type: application/json" \
    --data @"$file" "$CONNECT_URL/connectors" | python3 -m json.tool
  echo
}

echo "Waiting for Kafka Connect REST API at $CONNECT_URL ..."
until curl -s -o /dev/null "$CONNECT_URL/connectors"; do sleep 3; done

# Phase 2 required connectors for the CDC -> Snowflake path
register "$DIR/debezium-postgres-source.json"
register "$DIR/snowflake-sink.json"

# Phase 1 connectors (uncomment once MSK/JDBC plugin + secrets are configured)
# register "$DIR/jdbc-sink-postgres.json"
# register "$DIR/s3-sink-backup.json"

echo "Current connector status:"
curl -s "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool
