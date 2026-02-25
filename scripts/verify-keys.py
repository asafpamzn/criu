#!/usr/bin/env python3
"""Verify keys on a Valkey instance against a sample file.

Usage: verify-keys.py <sample-file> [host] [port]

Sample file format (TSV): key\tsize\tmd5
Prints: <failed_count>
Exit 0 if all match, 1 otherwise.
"""
import socket
import hashlib
import sys

sample_file = sys.argv[1]
host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
port = int(sys.argv[3]) if len(sys.argv) > 3 else 6379

s = socket.socket()
s.settimeout(10)
s.connect((host, port))


def send_cmd(s, *args):
    req = f"*{len(args)}\r\n"
    for a in args:
        a = str(a)
        req += f"${len(a)}\r\n{a}\r\n"
    s.sendall(req.encode())


def recv_line(s):
    buf = b""
    while b"\r\n" not in buf:
        buf += s.recv(4096)
    return buf


passed = 0
failed = 0
missing = 0
checked = 0

with open(sample_file) as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) != 3:
            continue
        key, expected_size, expected_md5 = parts
        expected_size = int(expected_size)
        if expected_size < 0:
            continue

        send_cmd(s, "GET", key)

        hdr = recv_line(s)
        size_line = hdr.split(b"\r\n")[0]

        if not size_line.startswith(b"$") or size_line == b"$-1":
            missing += 1
            checked += 1
            continue

        nbytes = int(size_line[1:])
        data = hdr[hdr.index(b"\r\n") + 2 :]
        while len(data) < nbytes + 2:
            data += s.recv(65536)
        actual_md5 = hashlib.md5(data[:nbytes]).hexdigest()

        if nbytes == expected_size and actual_md5 == expected_md5:
            passed += 1
        else:
            failed += 1
            if failed <= 5:
                print(
                    f"MISMATCH: {key} size={nbytes}/{expected_size} "
                    f"md5={actual_md5}/{expected_md5}",
                    file=sys.stderr,
                )
        checked += 1

s.close()

print(f"{passed}/{checked} passed, {failed} failed, {missing} missing")
sys.exit(1 if failed > 0 else 0)
