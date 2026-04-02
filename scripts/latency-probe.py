#!/usr/bin/env python3
"""Persistent-connection PING latency probe for Valkey.
Outputs: timestamp_sec latency_us [ERR]
"""
import socket, time, sys, os

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379
interval = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1

def connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect((host, port))
    return s

sock = connect()
while True:
    try:
        t0 = time.monotonic()
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        data = b""
        while b"\r\n" not in data:
            data += sock.recv(64)
        t1 = time.monotonic()
        lat_us = int((t1 - t0) * 1_000_000)
        sys.stdout.write(f"{time.time():.3f} {lat_us}\n")
        sys.stdout.flush()
    except Exception as e:
        t1 = time.monotonic()
        lat_us = int((t1 - t0) * 1_000_000)
        sys.stdout.write(f"{time.time():.3f} {lat_us} ERR\n")
        sys.stdout.flush()
        try:
            sock.close()
        except Exception:
            pass
        time.sleep(0.5)
        try:
            sock = connect()
        except Exception:
            pass
    time.sleep(interval)
