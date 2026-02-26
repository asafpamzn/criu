#!/usr/bin/env python3
"""Verify keys on a Valkey instance against a sample file.

Usage: verify-keys.py <sample-file> [host] [port]

Sample file format (TSV): key\tsize\tmd5
Exit 0 if all match, 1 otherwise.
"""
import hashlib
import socket
import sys


class ValkeyConn:
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
            val = self.f.read(n + 2)
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
    sample_file = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 6379

    c = ValkeyConn(host, port)

    passed = failed = missing = 0
    with open(sample_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 3:
                continue
            key, expected_size, expected_md5 = parts
            expected_size = int(expected_size)
            if expected_size < 0:
                continue

            c.cmd("GET", key)
            val = c.read_resp()

            if val is None:
                missing += 1
                continue

            actual_md5 = hashlib.md5(val).hexdigest()
            if len(val) == expected_size and actual_md5 == expected_md5:
                passed += 1
            else:
                failed += 1
                if failed <= 5:
                    print(
                        f"MISMATCH: {key} size={len(val)}/{expected_size} "
                        f"md5={actual_md5}/{expected_md5}",
                        file=sys.stderr,
                    )

    checked = passed + failed + missing
    print(f"{passed}/{checked} passed, {failed} failed, {missing} missing")
    c.close()
    sys.exit(1 if failed > 0 else 0)


main()
