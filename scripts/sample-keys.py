#!/usr/bin/env python3
"""Sample random keys from Valkey and record key/size/md5.

Usage: sample-keys.py <output-file> [count] [host] [port]
"""
import hashlib
import random
import socket
import sys


class ValkeyConn:
    """Minimal Valkey client using buffered socket I/O."""

    def __init__(self, host, port):
        self.sock = socket.socket()
        self.sock.settimeout(30)
        self.sock.connect((host, port))
        self.f = self.sock.makefile("rb")

    def cmd(self, *args):
        req = f"*{len(args)}\r\n"
        for a in args:
            a = str(a)
            req += f"${len(a)}\r\n{a}\r\n"
        self.sock.sendall(req.encode())

    def read_line(self):
        return self.f.readline().rstrip(b"\r\n")

    def read_resp(self):
        line = self.read_line()
        t, data = line[0:1], line[1:]
        if t == b"+":
            return data.decode()
        if t == b"-":
            raise Exception(data.decode())
        if t == b":":
            return int(data)
        if t == b"$":
            n = int(data)
            if n < 0:
                return None
            val = self.f.read(n + 2)  # +2 for \r\n
            return val[:n]
        if t == b"*":
            n = int(data)
            if n < 0:
                return None
            return [self.read_resp() for _ in range(n)]
        return line

    def close(self):
        self.f.close()
        self.sock.close()


def main():
    outfile = sys.argv[1]
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 6379

    c = ValkeyConn(host, port)

    keys = []
    cursor = "0"
    while True:
        c.cmd("SCAN", cursor, "COUNT", "10000")
        result = c.read_resp()
        cursor = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
        for k in result[1]:
            keys.append(k.decode() if isinstance(k, bytes) else str(k))
        if cursor == "0":
            break
        if len(keys) >= count * 4:
            break

    sample = random.sample(keys, min(count, len(keys)))

    with open(outfile, "w") as f:
        for key in sample:
            c.cmd("GET", key)
            val = c.read_resp()
            if val is None:
                f.write(f"{key}\t-1\tNIL\n")
            else:
                md5 = hashlib.md5(val).hexdigest()
                f.write(f"{key}\t{len(val)}\t{md5}\n")

    c.close()
    print(f"Sampled {len(sample)} keys to {outfile}")


main()
