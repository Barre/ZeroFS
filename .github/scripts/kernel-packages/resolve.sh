#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
repo_root=$(cd -- "$script_dir/../../.." && pwd -P)
readonly repo_root

: "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"
cd -- "$repo_root"

node \
    .github/scripts/kernel-target-updates/tests/reconcile-update-pull-request.test.js
bash \
    .github/scripts/kernel-target-updates/tests/ensure-ci.test.sh
python3 -m unittest discover \
    -s kernel/ci/tests \
    -p 'test_*.py'
python3 -m unittest discover \
    -s packaging/kernel/tests \
    -p 'test_*.py'

matrix=$(packaging/kernel/kernel-targets.py \
    --manifest packaging/kernel/kernels.lock.json matrix)

printf 'matrix=%s\n' "$matrix" >>"$GITHUB_OUTPUT"
