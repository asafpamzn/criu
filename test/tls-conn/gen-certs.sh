#!/bin/bash
# Generate test certificates for TLS per-connection test.
# Creates a self-signed CA and a server cert signed by it.

set -e

CERTS_DIR="$(dirname "$0")/certs"
mkdir -p "$CERTS_DIR"

echo "Generating test certificates in $CERTS_DIR ..."

# CA key and self-signed cert
openssl genrsa -out "$CERTS_DIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days 365 -key "$CERTS_DIR/ca.key" \
    -out "$CERTS_DIR/ca.crt" \
    -subj "/CN=CRIU-Test-CA/O=CRIU-Test" 2>/dev/null

# Server key and CSR
openssl genrsa -out "$CERTS_DIR/server.key" 2048 2>/dev/null
openssl req -new -key "$CERTS_DIR/server.key" \
    -out "$CERTS_DIR/server.csr" \
    -subj "/CN=localhost/O=CRIU-Test" 2>/dev/null

# Sign server cert with CA (add SAN for localhost)
openssl x509 -req -days 365 \
    -in "$CERTS_DIR/server.csr" \
    -CA "$CERTS_DIR/ca.crt" -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERTS_DIR/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
    2>/dev/null

# Cleanup CSR
rm -f "$CERTS_DIR/server.csr" "$CERTS_DIR/ca.srl"

echo "Done. Files:"
ls -la "$CERTS_DIR"
