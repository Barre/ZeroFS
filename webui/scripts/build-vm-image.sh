#!/bin/bash
# Build the v86 VM image (kernel + initramfs) from Alpine Linux ISO.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WEBUI_DIR="$(dirname "$SCRIPT_DIR")"
OUT_DIR="$WEBUI_DIR/public/v86"
WORK_DIR="/tmp/zerofs-vm-build"
ALPINE_VERSION="3.21"
ALPINE_RELEASE="3.21.3"
ALPINE_URL="https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/releases/x86/alpine-virt-${ALPINE_RELEASE}-x86.iso"
ALPINE_SHA256="e837d1d67aced3157652a51f2c22bee582a95ad03863a6a25b5c1c1bbc9a3d84"
BIOS_URL_BASE="https://raw.githubusercontent.com/copy/v86/4e4bf556350916a9a5139a4b3756af0971d56f8c/bios"
SEABIOS_SHA256="73e3f359102e3a9982c35fce98eb7cd08f18303ac7f1ba6ebfbe6cdc1c244d98"

verify_sha256() {
    local file="$1" expected="$2" name="$3"
    local actual
    actual=$(sha256sum "$file" | cut -d' ' -f1)
    if [ "$actual" != "$expected" ]; then
        echo "ERROR: $name checksum mismatch!"
        echo "  expected: $expected"
        echo "  actual:   $actual"
        exit 1
    fi
}

mkdir -p "$OUT_DIR" "$WORK_DIR"

# BIOS
[ -f "$OUT_DIR/seabios.bin" ] || curl -sL "$BIOS_URL_BASE/seabios.bin" -o "$OUT_DIR/seabios.bin"
verify_sha256 "$OUT_DIR/seabios.bin" "$SEABIOS_SHA256" "seabios.bin"

# v86 runtime
for f in libv86.js v86.wasm; do
    if [ ! -f "$OUT_DIR/$f" ] || [ "$WEBUI_DIR/node_modules/v86/build/$f" -nt "$OUT_DIR/$f" ]; then
        cp "$WEBUI_DIR/node_modules/v86/build/$f" "$OUT_DIR/$f"
    fi
done

# Alpine ISO
ISO="$WORK_DIR/alpine.iso"
[ -f "$ISO" ] || curl -sL "$ALPINE_URL" -o "$ISO"
verify_sha256 "$ISO" "$ALPINE_SHA256" "alpine-virt-${ALPINE_RELEASE}-x86.iso"

# Extract kernel and modloop from ISO
echo "Extracting kernel and modules from ISO..."
bsdtar -xf "$ISO" -C "$WORK_DIR" --strip-components=1 boot/vmlinuz-virt boot/initramfs-virt boot/modloop-virt
cp "$WORK_DIR/vmlinuz-virt" "$OUT_DIR/vmlinuz-virt"

# Extract busybox + musl from the original initramfs
ORIG_DIR="$WORK_DIR/initramfs-orig"
rm -rf "$ORIG_DIR"
mkdir -p "$ORIG_DIR"
cd "$ORIG_DIR"
zcat "$WORK_DIR/initramfs-virt" | cpio -idm 2>/dev/null

# Extract 9P modules from modloop squashfs
SQUASH_DIR="$WORK_DIR/squashfs-root"
rm -rf "$SQUASH_DIR"
unsquashfs -f -d "$SQUASH_DIR" "$WORK_DIR/modloop-virt" > /dev/null 2>&1
KVER=$(ls "$SQUASH_DIR/modules/" | grep -v firmware | head -1)
echo "  Kernel version: $KVER"

# Build minimal initramfs: busybox + musl + 9P modules + init
echo "Building minimal initramfs..."
INITRAMFS_DIR="$WORK_DIR/initramfs"
rm -rf "$INITRAMFS_DIR"
mkdir -p "$INITRAMFS_DIR"/{bin,lib,dev,proc,sys,tmp,run,mnt,etc}

cp "$ORIG_DIR/bin/busybox"         "$INITRAMFS_DIR/bin/"
cp "$ORIG_DIR/lib/ld-musl-i386.so.1" "$INITRAMFS_DIR/lib/"

MODBASE="$SQUASH_DIR/modules/$KVER/kernel"
DESTMOD="$INITRAMFS_DIR/lib/modules/$KVER"
mkdir -p "$DESTMOD/kernel/fs/netfs" "$DESTMOD/kernel/net/9p" "$DESTMOD/kernel/fs/9p"
cp "$MODBASE/fs/netfs/netfs.ko"      "$DESTMOD/kernel/fs/netfs/"
cp "$MODBASE/net/9p/9pnet.ko"        "$DESTMOD/kernel/net/9p/"
cp "$MODBASE/net/9p/9pnet_virtio.ko" "$DESTMOD/kernel/net/9p/"
cp "$MODBASE/fs/9p/9p.ko"            "$DESTMOD/kernel/fs/9p/"

cp "$SCRIPT_DIR/vm-init.sh" "$INITRAMFS_DIR/init"
chmod 755 "$INITRAMFS_DIR/init"

cd "$INITRAMFS_DIR"
find . | cpio -o -H newc 2>/dev/null | xz -9 --check=crc32 > "$OUT_DIR/initramfs-virt"

echo "VM image built: $(du -sh "$OUT_DIR" | cut -f1)"
