#!/bin/bash
# Build script for BPF dirty page tracker
# Usage: ./build-bpf.sh [--rebuild-criu]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRIU_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$SCRIPT_DIR"

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)
        TARGET_ARCH="x86"
        ;;
    aarch64|arm64)
        TARGET_ARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

echo "=== Building BPF dirty tracker for $ARCH (target: $TARGET_ARCH) ==="

# Step 1: Generate vmlinux.h from kernel BTF
echo "[1/3] Generating vmlinux.h from kernel BTF..."
if [ ! -f /sys/kernel/btf/vmlinux ]; then
    echo "ERROR: /sys/kernel/btf/vmlinux not found. Kernel must have CONFIG_DEBUG_INFO_BTF=y"
    exit 1
fi
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h
echo "      Generated vmlinux.h ($(wc -l < vmlinux.h) lines)"

# Step 2: Compile BPF program
echo "[2/3] Compiling dirty_track.bpf.c..."
clang -g -O2 -target bpf \
    -D__TARGET_ARCH_${TARGET_ARCH} \
    -I "$SCRIPT_DIR" \
    -c dirty_track.bpf.c \
    -o dirty_track.bpf.o
echo "      Generated dirty_track.bpf.o ($(stat -c%s dirty_track.bpf.o 2>/dev/null || stat -f%z dirty_track.bpf.o) bytes)"

# Step 3: Generate skeleton header
echo "[3/3] Generating skeleton header..."
bpftool gen skeleton dirty_track.bpf.o > dirty_track.skel.h
echo "      Generated dirty_track.skel.h ($(wc -l < dirty_track.skel.h) lines)"

echo ""
echo "=== BPF build complete ==="

# Optional: rebuild CRIU
if [ "$1" = "--rebuild-criu" ]; then
    echo ""
    echo "=== Rebuilding CRIU ==="
    cd "$CRIU_ROOT"
    make clean
    make -j$(nproc)
    echo "=== CRIU rebuild complete ==="
fi

echo ""
echo "To view BPF debug output during migration:"
echo "  sudo cat /sys/kernel/debug/tracing/trace_pipe | grep CLONE-BPF"
