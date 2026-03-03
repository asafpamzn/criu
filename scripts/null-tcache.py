#!/usr/bin/env python3
"""Null per-thread glibc tcache pointers in a stopped/frozen process.
Usage: null-tcache.py <pid> <images_dir>

Reads each thread's tpidr_el0 from CRIU core images (no ptrace needed),
walks the DTV to find the libc TLS block, scans for tcache-like pointers,
and nulls them. Also re-zeros all glibc arena mutexes.
"""
import struct, os, sys, json, subprocess, glob

pid = int(sys.argv[1])
imgs_dir = sys.argv[2]

# Read TLS (tpidr_el0) from core images
tls_addrs = []
for core_file in sorted(glob.glob(f"{imgs_dir}/core-*.img")):
    try:
        raw = subprocess.check_output(
            [sys.executable, "-m", "crit", "decode", "-i", core_file],
            stderr=subprocess.DEVNULL)
        data = json.loads(raw)
        for entry in data.get("entries", []):
            ti = entry.get("ti_aarch64", {})
            tls = ti.get("tls", 0)
            if tls:
                tls_addrs.append(tls)
    except Exception:
        pass

nulled = 0
arenas_zeroed = 0

with open(f"/proc/{pid}/mem", "r+b") as f:
    for tp in tls_addrs:
        try:
            f.seek(tp)
            dtv = struct.unpack("<Q", f.read(8))[0]
            if not dtv:
                continue
            f.seek(dtv + 16)
            tls_block = struct.unpack("<Q", f.read(8))[0]
            if not tls_block:
                continue
            for off in range(0x480, 0x600, 8):
                f.seek(tls_block + off)
                val = struct.unpack("<Q", f.read(8))[0]
                if val == 0 or val < 0x10000:
                    continue
                try:
                    f.seek(val)
                    counts = struct.unpack("<64H", f.read(128))
                    total = sum(counts)
                    if 0 < total < 1000:
                        f.seek(tls_block + off)
                        f.write(struct.pack("<Q", 0))
                        nulled += 1
                except Exception:
                    pass
        except Exception:
            pass

    # Re-zero all arena mutexes
    for line in open(f"/proc/{pid}/maps"):
        if "libc.so" in line and "rw-" in line:
            start = int(line.split("-")[0], 16)
            arena = start + 0xa50
            seen = set()
            addr = arena
            while addr not in seen and len(seen) < 256:
                seen.add(addr)
                f.seek(addr)
                f.write(struct.pack("<III", 0, 0, 0))
                f.seek(addr + 2160)
                nxt = struct.unpack("<Q", f.read(8))[0]
                addr = nxt
            arenas_zeroed = len(seen)
            break
    f.flush()

print(f"null-tcache: {nulled} tcache, {arenas_zeroed} arenas ({len(tls_addrs)} threads)",
      flush=True)
if nulled == 0 and tls_addrs:
    # Debug: check first thread's TLS chain
    tp = tls_addrs[0]
    try:
        with open(f"/proc/{pid}/mem", "rb") as df:
            df.seek(tp)
            dtv = struct.unpack("<Q", df.read(8))[0]
            df.seek(dtv + 16)
            tls_block = struct.unpack("<Q", df.read(8))[0]
            df.seek(tls_block + 0x548)
            val = struct.unpack("<Q", df.read(8))[0]
            print(f"  DEBUG: TP=0x{tp:x} DTV=0x{dtv:x} TLS=0x{tls_block:x} +0x548=0x{val:x}",
                  flush=True)
    except Exception as e:
        print(f"  DEBUG: exception {e}", flush=True)
