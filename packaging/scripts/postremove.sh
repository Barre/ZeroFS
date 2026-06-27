#!/bin/sh
# Runs as: deb postrm ("remove"/"purge"/"upgrade"/...), rpm postun (0=erase, 1=upgrade).
set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

# The zerofs user, /etc/zerofs, and /var/cache/zerofs are intentionally left
# behind so a reinstall keeps its cache and config. Remove them by hand if you
# really want a clean slate.

exit 0
