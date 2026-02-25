#!/usr/bin/env python3
"""Sample random keys from Valkey and record key/size/md5.

Usage: sample-keys.py <output-file> [count] [host] [port]
"""
import socket
import hashlib
import random
import sys

outfile = sys.argv[1]
count = int(sys.argv[2]) if len(sys.argv) > 2 else 500
host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
port = int(sys.argv[4]) if len(sys.argv) > 4 else 6379

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


def recv_bulk(s):
    hdr = recv_line(s)
    first = hdr.split(b"\r\n")[0]
    if first.startswith(b"$-1"):
        return None
    if first.startswith(b"$"):
        nbytes = int(first[1:])
        data = hdr[hdr.index(b"\r\n") + 2 :]
        while len(data) < nbytes + 2:
            data += s.recv(65536)
        return data[:nbytes]
    return hdr


# Collect keys via SCAN
keys = []
cursor = "0"
while True:
    send_cmd(s, "SCAN", cursor, "COUNT", "10000")
    # Parse array response: *2\r\n$cursor_len\r\ncursor\r\n*count\r\n...
    resp = b""
    while resp.count(b"\r\n") < 6:
        resp += s.recv(65536)
    lines = resp.decode(errors="replace").split("\r\n")

    # Find cursor (after first *2 and $len)
    idx = 0
    for i, l in enumerate(lines):
        if l.startswith("*2"):
            idx = i
            break
    cursor = lines[idx + 2] if len(lines) > idx + 2 else "0"

    # Find keys array
    for i in range(idx + 3, len(lines)):
        if lines[i].startswith("*"):
            key_count = int(lines[i][1:])
            for j in range(key_count):
                ki = i + 1 + j * 2 + 1
                if ki < len(lines) and lines[ki]:
                    keys.append(lines[ki])
            break

    if cursor == "0":
        break
    if len(keys) >= count * 4:
        break

sample = random.sample(keys, min(count, len(keys)))

# Get values and compute checksums
with open(outfile, "w") as f:
    for key in sample:
        send_cmd(s, "GET", key)
        val = recv_bulk(s)
        if val is None:
            f.write(f"{key}\t-1\tNIL\n")
        else:
            md5 = hashlib.md5(val).hexdigest()
            f.write(f"{key}\t{len(val)}\t{md5}\n")

s.close()
print(f"Sampled {len(sample)} keys to {outfile}")
