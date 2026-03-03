#!/usr/bin/env python3
"""TCP image file server. Sends all .img and stats-* files from a
directory to one connecting client, then exits.

Usage: image-server.py <images_dir> <port>

Protocol:
  [4 bytes] file_count (uint32 big-endian)
  For each file:
    [2 bytes] name_length (uint16 big-endian)
    [name_length bytes] filename
    [8 bytes] file_size (uint64 big-endian)
    [file_size bytes] file data
"""

import os, socket, struct, sys

def main():
    images_dir = sys.argv[1]
    port = int(sys.argv[2])

    # Collect files to send
    files = []
    for name in sorted(os.listdir(images_dir)):
        if name.endswith('.img') or name.startswith('stats-'):
            path = os.path.join(images_dir, name)
            if os.path.isfile(path):
                files.append((name, path))

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', port))
    srv.listen(1)

    conn, addr = srv.accept()
    srv.close()

    # Send file count
    conn.sendall(struct.pack('!I', len(files)))

    total_bytes = 0
    for name, path in files:
        size = os.path.getsize(path)
        name_bytes = name.encode()
        # Send header: name_length + name + file_size
        conn.sendall(struct.pack('!H', len(name_bytes)))
        conn.sendall(name_bytes)
        conn.sendall(struct.pack('!Q', size))
        # Send file data
        with open(path, 'rb') as f:
            sent = 0
            while sent < size:
                chunk = f.read(min(65536, size - sent))
                if not chunk:
                    break
                conn.sendall(chunk)
                sent += len(chunk)
        total_bytes += size

    conn.close()
    print(f"image-server: sent {len(files)} files ({total_bytes} bytes) to {addr[0]}", file=sys.stderr)

if __name__ == '__main__':
    main()
