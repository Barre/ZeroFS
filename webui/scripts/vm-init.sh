#!/bin/busybox sh

/bin/busybox mkdir -p /bin /sbin /usr/bin /usr/sbin /proc /sys /dev /dev/pts /dev/shm /tmp /run /mnt
/bin/busybox --install -s
export PATH=/bin:/sbin:/usr/bin:/usr/sbin

mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev 2>/dev/null || mount -t tmpfs tmpfs /dev
mkdir -p /dev/pts /dev/shm
mount -t devpts devpts /dev/pts
mount -t tmpfs tmpfs /dev/shm
mount -t tmpfs tmpfs /tmp

# 9P modules for ZeroFS mount
KVER=$(uname -r)
insmod /lib/modules/$KVER/kernel/fs/netfs/netfs.ko
insmod /lib/modules/$KVER/kernel/net/9p/9pnet.ko
insmod /lib/modules/$KVER/kernel/net/9p/9pnet_virtio.ko
insmod /lib/modules/$KVER/kernel/fs/9p/9p.ko

mkdir -p /mnt
if mount -t 9p -o trans=virtio,version=9p2000.L,cache=loose,msize=512000 host9p /mnt; then
    echo "ZeroFS mounted at /mnt"
else
    echo "WARNING: Failed to mount ZeroFS at /mnt"
fi

hostname zerofs-shell
echo "root:x:0:0:root:/mnt:/bin/sh" > /etc/passwd
echo "root:x:0:" > /etc/group
export HOME=/mnt
export TERM=linux
export PS1='\w \$ '
export ENV=/etc/profile
cat > /etc/profile << 'PROFILE'
export PATH=/bin:/sbin:/usr/bin:/usr/sbin
export HOME=/mnt
export TERM=linux
export PS1='\w \$ '
PROFILE

# Sends CPU/IO stats as xterm title so the frontend can display CPU/load indicators
(while true; do
    read cpu user nice system idle rest < /proc/stat 2>/dev/null
    total=$((user + nice + system + idle))
    if [ -n "$prev_total" ]; then
        diff_idle=$((idle - prev_idle))
        diff_total=$((total - prev_total))
        if [ "$diff_total" -gt 0 ]; then
            cpu_pct=$(( (diff_total - diff_idle) * 100 / diff_total ))
        else
            cpu_pct=0
        fi
    else
        cpu_pct=0
    fi
    prev_idle=$idle
    prev_total=$total
    read loadavg rest < /proc/loadavg 2>/dev/null
    printf '\033]0;cpu=%s;load=%s\007' "$cpu_pct" "$loadavg" > /dev/ttyS0
    sleep 2
done) &

printf '\n  \033[1;34mZeroFS Web Shell\033[0m\n  Your filesystem is mounted at /mnt\n\n'

cd /mnt
exec setsid sh -c 'cd /mnt; exec sh -l </dev/ttyS0 >/dev/ttyS0 2>&1'
