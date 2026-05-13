#!/bin/bash
# Copy test files to remote and run them.
# Usage: ./run-remote.sh

set -e

SSH_KEY=~/.ssh/mac.pem
REMOTE="ubuntu@ec2-3-82-38-179.compute-1.amazonaws.com"
REMOTE_DIR="/tmp/tls-conn-test"

echo "=== Deploying TLS test to remote ==="

# Create remote directory and copy files
ssh -i "$SSH_KEY" "$REMOTE" "rm -rf $REMOTE_DIR && mkdir -p $REMOTE_DIR"
scp -i "$SSH_KEY" test-tls-conn.c gen-certs.sh Makefile "$REMOTE:$REMOTE_DIR/"

echo "=== Installing dependencies (if needed) ==="
ssh -i "$SSH_KEY" "$REMOTE" "
    dpkg -l | grep -q libgnutls28-dev || sudo apt-get install -y libgnutls28-dev openssl 2>/dev/null
"

echo "=== Building and running test ==="
ssh -i "$SSH_KEY" "$REMOTE" "
    cd $REMOTE_DIR
    chmod +x gen-certs.sh
    bash gen-certs.sh
    make clean all
    echo ''
    echo '=== Running with 8 threads (default P3 count) ==='
    ./test-tls-conn 8
    echo ''
    echo '=== Running with 15 threads (large profile P3 count) ==='
    ./test-tls-conn 15
"
