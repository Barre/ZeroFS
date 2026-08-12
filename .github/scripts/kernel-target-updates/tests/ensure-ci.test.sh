#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
subject=$script_dir/../ensure-ci.sh

temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT

cat >"$temporary/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1 $2" == "run list" ]]; then
  printf '%s\n' "$GH_RUNS"
elif [[ "$1 $2" == "workflow run" ]]; then
  printf '%s\0' "$@" >"$GH_DISPATCH_ARGUMENTS"
fi
EOF
chmod +x "$temporary/gh"

head_sha=0123456789abcdef0123456789abcdef01234567
export GH_DISPATCH_ARGUMENTS=$temporary/dispatch-arguments
export GH_RUNS='[]'
export GH_TOKEN=test-token
export HEAD_SHA=$head_sha
export PATH=$temporary:$PATH
export UPDATE_BRANCH=automation/kernel-target-updates

"$subject"
mapfile -d '' -t arguments <"$GH_DISPATCH_ARGUMENTS"
expected=(
  workflow run ci.yml --ref "$UPDATE_BRANCH"
  -f "expected_sha=$HEAD_SHA"
)
[[ "${arguments[*]}" == "${expected[*]}" ]]

export GH_RUNS="[{\"event\":\"pull_request\",\"headSha\":\"$HEAD_SHA\"}]"
: >"$GH_DISPATCH_ARGUMENTS"
"$subject"
mapfile -d '' -t arguments <"$GH_DISPATCH_ARGUMENTS"
[[ "${arguments[*]}" == "${expected[*]}" ]]

export GH_RUNS="[{\"event\":\"workflow_dispatch\",\"headSha\":\"$HEAD_SHA\"}]"
: >"$GH_DISPATCH_ARGUMENTS"
output=$("$subject")
[[ "$output" == "CI already exists for $HEAD_SHA" ]]
[[ ! -s "$GH_DISPATCH_ARGUMENTS" ]]
