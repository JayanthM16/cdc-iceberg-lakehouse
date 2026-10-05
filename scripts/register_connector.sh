#!/usr/bin/env bash
set -euo pipefail
echo "Waiting for Kafka Connect..."
until curl -sf http://localhost:8083/ >/dev/null; do sleep 3; done
curl -s -X POST -H "Content-Type: application/json" \
  --data @connector/orders-connector.json http://localhost:8083/connectors | python3 -m json.tool || true
echo
curl -s http://localhost:8083/connectors/orders-connector/status | python3 -m json.tool
