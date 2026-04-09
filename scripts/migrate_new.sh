#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA_SSH_HOST="${REPLICA_IP:-$REPLICA_HOST}"

PID=$(pgrep -x valkey-server | head -n1)

if [ -z "$PID" ]; then
  echo "ERROR: valkey-server not running"
  exit 1
fi

# Step 0: Kill leftover ssh/restore processes
echo "Step 0: Killing leftover ssh restore processes..."
sudo pkill -9 -f "ssh.*restore_new.sh" 2>/dev/null || true
sleep 0.1

# Step 1: Clean images dir
echo "Step 1: Cleaning $IMAGES_DIR..."
sudo rm -rf "$IMAGES_DIR"/*

# Step 2: Start restore on replica (it will wait for page server)
echo "Step 2: Starting restore on replica..."
stdbuf -oL $SSH ubuntu@$REPLICA_SSH_HOST "sudo $SCRIPT_DIR/restore_new.sh" 2>&1 | stdbuf -oL sed 's/^/[replica] /' &
REPLICA_PID=$!

# Step 3: Wait for replica ready signal
echo "Step 3: Waiting for replica ready signal..."
READY_FILE="$IMAGES_DIR/ready.log"
for i in $(seq 1 60); do
  if [ -f "$READY_FILE" ]; then
    echo "Replica ready"
    break
  fi
  sleep 0.5
done
if [ ! -f "$READY_FILE" ]; then
  echo "ERROR: replica ready signal not found at $READY_FILE"
  exit 1
fi

# Step 4: CRIU dump
echo "Step 4: Starting CRIU dump for PID $PID..."

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
  -v3 -o "$IMAGES_DIR/lazy-primary.log"

# Step 5: Wait for replica master_link_status:up
echo "Step 5: Waiting for replica master_link_status:up..."
REPLICA_PORT="${REPLICA_PORT:-6379}"
START_TIME=$(date +%s%3N)
for i in $(seq 1 120); do
  STATUS=$($SSH ubuntu@$REPLICA_SSH_HOST "valkey-cli -p $REPLICA_PORT info replication 2>/dev/null | grep master_link_status" || true)
  if [[ "$STATUS" == *"master_link_status:up"* ]]; then
    END_TIME=$(date +%s%3N)
    ELAPSED=$((END_TIME - START_TIME))
    echo "Replica master_link_status:up after ${ELAPSED}ms"
    break
  fi
  sleep 0.5
done

if [[ "$STATUS" != *"master_link_status:up"* ]]; then
  echo "WARNING: master_link_status:up not reached within 60s"
fi

echo "Migration complete"
