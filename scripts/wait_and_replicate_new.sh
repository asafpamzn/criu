#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

MAX_WAIT=${MAX_WAIT:-600}

echo "Waiting for Valkey to respond to PING..."
for i in $(seq 1 "$MAX_WAIT"); do
  if valkey-cli ping &>/dev/null; then
    echo "Valkey is responsive"
    break
  fi
  sleep 0.1
done

if ! valkey-cli ping &>/dev/null; then
  echo "ERROR: Valkey not responsive after ${MAX_WAIT}x0.1s"
  exit 1
fi

echo "Configuring as replica of ${PRIMARY_IP}:${VALKEY_PORT}..."
valkey-cli replicaof "$PRIMARY_IP" "$VALKEY_PORT"

echo "Replica configured"
