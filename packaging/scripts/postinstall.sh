#!/bin/sh
# Runs as: deb postinst ("configure"), rpm post (1=install, 2=upgrade).
set -e

# Dedicated unprivileged system user the service runs as.
if ! getent group zerofs >/dev/null 2>&1; then
    groupadd --system zerofs
fi
if ! getent passwd zerofs >/dev/null 2>&1; then
    nologin_shell="$(command -v nologin || echo /usr/sbin/nologin)"
    useradd --system --gid zerofs --no-create-home \
        --home-dir /var/cache/zerofs --shell "$nologin_shell" \
        --comment "ZeroFS service" zerofs
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

# First install only (deb: $1=configure with no $2; rpm: $1=1).
if [ "$1" = "configure" ] && [ -z "$2" ] || [ "$1" = "1" ]; then
    cat <<'EOF'

ZeroFS installed. Before starting:
  1. Edit /etc/zerofs/zerofs.env   - set ZEROFS_PASSWORD and S3 credentials
  2. Edit /etc/zerofs/config.toml  - set [storage] url (and protocols/ports)
Then:
  systemctl enable --now zerofs

EOF
fi

exit 0
