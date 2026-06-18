#!/usr/bin/env bash
# Wrapper that runs the suite with the environment local-fs expects.
#
# The local-fs client shells out to coreutils (ln, mkdir, mv, ...) and parses
# their error *messages*. Those messages must come from GNU coreutils: recent
# Ubuntu ships uutils coreutils by default, which phrases errors differently
# (e.g. "Already exists" vs "File exists") and would surface as spurious
# divergences. If the default coreutils aren't GNU but the `gnu-coreutils`
# package is installed (gnu-prefixed binaries), we build a shim dir of GNU
# tools and put it first on PATH.
#
# Requires a JDK (set JAVA_HOME) and Leiningen (`lein`) on PATH.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v lein >/dev/null 2>&1; then
  echo "run.sh: 'lein' not found on PATH (install Leiningen)." >&2; exit 1
fi
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  export PATH="$JAVA_HOME/bin:$PATH"
  export JAVA_CMD="$JAVA_HOME/bin/java"
elif ! command -v java >/dev/null 2>&1; then
  echo "run.sh: no java found; set JAVA_HOME to a JDK." >&2; exit 1
fi

# Ensure GNU coreutils.
if ! ln --version 2>/dev/null | grep -q 'GNU coreutils'; then
  shim="$here/.gnubin"
  if [ -x /usr/bin/gnuln ]; then
    mkdir -p "$shim"
    for t in cat ln mkdir mv rm stat sync touch truncate; do
      [ -x "/usr/bin/gnu$t" ] && ln -sf "/usr/bin/gnu$t" "$shim/$t"
    done
    export PATH="$shim:$PATH"
    echo "run.sh: using GNU coreutils shim at $shim" >&2
  else
    echo "run.sh: WARNING: default coreutils are not GNU and gnu-coreutils is" >&2
    echo "        not installed; error-string checks may report false anomalies." >&2
  fi
fi

cd "$here"
exec lein "$@"
