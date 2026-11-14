#!/usr/bin/env bash
# Unmount first so the NFS client writes back its cache, then SIGTERM so ZeroFS
# flushes to the object store.
set -uo pipefail

echo "::group::Shutting down ZeroFS"
status=0
state=${ZEROFS_STATE_DIR:-}

# Reads the mount table, so it cannot block on a dead server.
mounted() {
    [ -n "${MOUNT_PATH:-}" ] && mount | grep -qF " on $MOUNT_PATH "
}

if [ -n "$state" ] && [ -f "$state/exit" ]; then
    echo "::error::ZeroFS exited during the job with status $(cat "$state/exit"); writes it had not flushed are lost"
    status=1
    # sync and a plain umount would block forever on a hard mount with no server.
    if mounted; then
        sudo umount -f "$MOUNT_PATH" 2>/dev/null || sudo umount -l "$MOUNT_PATH"
    fi
else
    if mounted; then
        echo "Unmounting $MOUNT_PATH..."
        sync
        if ! sudo umount "$MOUNT_PATH"; then
            echo "::error::$MOUNT_PATH is busy; detaching it, writes made to it after sync are lost"
            sudo umount -l "$MOUNT_PATH" 2>/dev/null || sudo umount -f "$MOUNT_PATH"
            status=1
        fi
    fi

    if [ -n "$state" ] && [ -f "$state/pid" ]; then
        timeout=300
        pid=$(cat "$state/pid")
        echo "Stopping ZeroFS (PID $pid)..."
        kill -TERM "$pid" 2>/dev/null
        for ((i = 0; i < timeout; i++)); do
            [ -f "$state/exit" ] && break
            sleep 1
        done
        if [ ! -f "$state/exit" ]; then
            echo "::error::ZeroFS did not exit within ${timeout}s; killing it, unflushed writes are lost"
            kill -KILL "$pid" 2>/dev/null
            status=1
        elif [ "$(cat "$state/exit")" != 0 ]; then
            echo "::error::ZeroFS shutdown failed with status $(cat "$state/exit"); unflushed writes are lost"
            status=1
        fi
    fi
fi

log="$state/zerofs.log"
if [ -n "$state" ] && [ -f "$log" ]; then
    if [ "$status" -ne 0 ]; then
        tail -50 "$log"
    elif grep -qi error "$log"; then
        echo "::warning::ZeroFS logged errors during execution:"
        grep -i error "$log" | head -20
    fi
fi

echo "::endgroup::"
exit "$status"
