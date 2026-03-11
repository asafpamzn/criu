#!/bin/bash
set -euo pipefail

# Post-restore configuration for CRIU-migrated Valkey.
# Run on REPLICA machine after CRIU restore + SIGCONT.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

MAX_WAIT_PING=${MAX_WAIT_PING:-600}      # 600 × 0.1s = 60s

echo "Waiting for Valkey server to start responding to PING..."

for i in $(seq 1 "$MAX_WAIT_PING"); do
  if valkey-cli ping &>/dev/null; then
    echo "Valkey is responsive!"
    break
  fi
  sleep 0.02
done

if ! valkey-cli ping &>/dev/null; then
  echo "Timeout: Valkey did not become responsive."
  exit 1
fi

# CRIU already transferred all data — no REPLICAOF needed.
# REPLICAOF would trigger BGSAVE fork() on the source (~1.7s
# mmap_lock stall for 100GB+ process), defeating zero-downtime.
echo "Configuring as standalone (CRIU data already transferred)..."
valkey-cli config set save "" >/dev/null 2>&1 || true
valkey-cli config set appendonly no >/dev/null 2>&1 || true
echo "Replica ready (standalone, data from CRIU migration)"
