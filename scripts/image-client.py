#!/usr/bin/env python3
"""TCP image file client. Downloads all image files from image-server
and writes them to a local directory.

Usage: image-client.py <host> <port> <output_dir>
"""

import os, socket, struct, sys, time

def recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"connection closed, got {len(buf)}/{n} bytes")
        buf += chunk
    return buf

def main():
    host = sys.argv[1]
    port = int(sys.argv[2])
    output_dir = sys.argv[3]

    os.makedirs(output_dir, exist_ok=True)

    # Retry connect (server may not be listening yet)
    sk = None
    for attempt in range(100):
        try:
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.connect((host, port))
            break
        except ConnectionRefusedError:
            sk.close()
            sk = None
            time.sleep(0.1)

    if sk is None:
        print("image-client: failed to connect after 10s", file=sys.stderr)
        sys.exit(1)

    # Receive file count
    file_count = struct.unpack('!I', recv_exact(sk, 4))[0]

    total_bytes = 0
    for _ in range(file_count):
        # Receive header
        name_len = struct.unpack('!H', recv_exact(sk, 2))[0]
        name = recv_exact(sk, name_len).decode()
        file_size = struct.unpack('!Q', recv_exact(sk, 8))[0]

        # Receive file data
        path = os.path.join(output_dir, name)
        with open(path, 'wb') as f:
            remaining = file_size
            while remaining > 0:
                chunk = sk.recv(min(65536, remaining))
                if not chunk:
                    raise ConnectionError(f"connection closed during {name}")
                f.write(chunk)
                remaining -= len(chunk)
        total_bytes += file_size

    sk.close()
    print(f"image-client: received {file_count} files ({total_bytes} bytes) to {output_dir}", file=sys.stderr)

if __name__ == '__main__':
    main()
