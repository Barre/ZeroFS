#!/bin/sh
# Runs as: deb prerm ("remove"/"upgrade"/...), rpm preun (0=erase, 1=upgrade).
# Stop and disable only on a real removal, never on an upgrade.
set -e

if [ "$1" = "remove" ] || [ "$1" = "purge" ] || [ "$1" = "0" ]; then
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --no-reload disable --now zerofs.service >/dev/null 2>&1 || true
    fi
fi

exit 0
