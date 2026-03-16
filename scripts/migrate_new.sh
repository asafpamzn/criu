#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
PID=$(pgrep -x valkey-server | head -n1)

if [ -z "$PID" ]; then
  echo "ERROR: valkey-server not running"
  exit 1
fi

echo "Starting CRIU dump for PID $PID..."

sudo "$CRIU_BIN" dump \
  --tree "$PID" \
  --images-dir "$IMAGES_DIR" \
  --cow-dump \
  --lazy-pages \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --tcp-close \
  --skip-in-flight \
  --ext-unix-sk \
  --leave-running \
  --display-stats \
  -v2 -o "$IMAGES_DIR/lazy-primary.log"
