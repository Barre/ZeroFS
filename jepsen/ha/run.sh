#!/usr/bin/env bash
# Run the ZeroFS HA jepsen test, using a user-local JDK + Leiningen + MinIO tools
# under $ZEROFS_JEPSEN_TOOLS (default ~/jepsen-ha), then execs lein. Example:
#
#   ./run.sh run test --time-limit 120 --concurrency 10
#   ./run.sh run serve            # browse results at http://localhost:8080
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS="${ZEROFS_JEPSEN_TOOLS:-$HOME/jepsen-ha}"
export JAVA_HOME="$TOOLS/jdk"
export PATH="$JAVA_HOME/bin:$TOOLS/bin:$PATH"
# Default the cluster work dir to the tools dir; minio/mc resolve from $TOOLS/bin
# on PATH. Override any of these with the env vars or the --work-dir/--*-bin flags.
export ZEROFS_JEPSEN_WORK="${ZEROFS_JEPSEN_WORK:-$TOOLS}"
cd "$here"
exec lein "$@"
