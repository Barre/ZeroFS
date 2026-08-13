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
  printf '%s\0' "$@" >"$GH_RUN_ARGUMENTS"
  printf '%s\n' "$GH_RUNS"
elif [[ "$1" == "api" ]]; then
  printf '%s\0' "$@" >"$GH_API_ARGUMENTS"
  cat >"$GH_API_INPUT"
  printf '%s\n' "$GH_DISPATCH_RESPONSE"
else
  echo "unexpected gh command: $*" >&2
  exit 1
fi
EOF
chmod +x "$temporary/gh"

head_sha=0123456789abcdef0123456789abcdef01234567
run_id=31650879787
export GH_API_ARGUMENTS=$temporary/api-arguments
export GH_API_INPUT=$temporary/api-input
export GH_DISPATCH_RESPONSE="{\"workflow_run_id\":$run_id}"
export GH_RUN_ARGUMENTS=$temporary/run-arguments
export GH_RUNS='[]'
export GH_TOKEN=test-token
export GITHUB_OUTPUT=$temporary/github-output
export GITHUB_REPOSITORY=Barre/ZeroFS
export HEAD_SHA=$head_sha
export PATH=$temporary:$PATH
export UPDATE_BRANCH=automation/kernel-target-updates

reset_outputs() {
  : >"$GH_API_ARGUMENTS"
  : >"$GH_API_INPUT"
  : >"$GH_RUN_ARGUMENTS"
  : >"$GITHUB_OUTPUT"
}

assert_arguments() {
  local file=$1
  shift
  local -a actual
  local expected
  local index

  mapfile -d '' -t actual <"$file"
  [[ "${#actual[@]}" -eq "$#" ]]
  index=0
  for expected in "$@"; do
    [[ "${actual[$index]}" == "$expected" ]]
    ((index += 1))
  done
}

reset_outputs
output=$("$subject")
[[ "$output" == "Dispatched CI for $HEAD_SHA: $run_id" ]]
assert_arguments "$GH_RUN_ARGUMENTS" \
  run list \
  --workflow ci.yml \
  --branch "$UPDATE_BRANCH" \
  --commit "$HEAD_SHA" \
  --limit 100 \
  --json createdAt,databaseId,event,headSha
assert_arguments "$GH_API_ARGUMENTS" \
  api --method POST \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2026-03-10" \
  "repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/dispatches" \
  --input -
jq -e \
  --arg ref "$UPDATE_BRANCH" \
  --arg expected_sha "$HEAD_SHA" \
  '.ref == $ref and
   .inputs.expected_sha == $expected_sha and
   (keys | sort) == ["inputs", "ref"]' \
  "$GH_API_INPUT" >/dev/null
[[ "$(<"$GITHUB_OUTPUT")" == "run_id=$run_id" ]]

export GH_RUNS="[{\"event\":\"pull_request\",\"headSha\":\"$HEAD_SHA\"}]"
reset_outputs
"$subject" >/dev/null
[[ -s "$GH_API_ARGUMENTS" ]]
[[ "$(<"$GITHUB_OUTPUT")" == "run_id=$run_id" ]]

export GH_RUNS="[{
  \"event\": \"workflow_dispatch\",
  \"headSha\": \"$HEAD_SHA\",
  \"createdAt\": \"2026-08-12T23:30:00Z\",
  \"databaseId\": $run_id
}]"
reset_outputs
output=$("$subject")
[[ "$output" == "CI already exists for $HEAD_SHA: $run_id" ]]
[[ ! -s "$GH_API_ARGUMENTS" ]]
[[ "$(<"$GITHUB_OUTPUT")" == "run_id=$run_id" ]]

newer_id=31650879999
export GH_RUNS="[
  {
    \"event\": \"workflow_dispatch\",
    \"headSha\": \"$HEAD_SHA\",
    \"createdAt\": \"2026-08-12T23:30:00Z\",
    \"databaseId\": $run_id
  },
  {
    \"event\": \"workflow_dispatch\",
    \"headSha\": \"$HEAD_SHA\",
    \"createdAt\": \"2026-08-12T23:31:00Z\",
    \"databaseId\": $newer_id
  }
]"
reset_outputs
"$subject" >/dev/null
[[ "$(<"$GITHUB_OUTPUT")" == "run_id=$newer_id" ]]
[[ ! -s "$GH_API_ARGUMENTS" ]]

export GH_RUNS='[]'
export GH_DISPATCH_RESPONSE='{"workflow_run_id":"not-a-run"}'
reset_outputs
if "$subject" >/dev/null 2>&1; then
  echo "accepted a malformed CI run ID" >&2
  exit 1
fi
[[ ! -s "$GITHUB_OUTPUT" ]]

export GH_RUNS='not-json'
reset_outputs
if "$subject" >/dev/null 2>&1; then
  echo "accepted a malformed CI run listing" >&2
  exit 1
fi
[[ ! -s "$GH_API_ARGUMENTS" ]]
[[ ! -s "$GITHUB_OUTPUT" ]]

export GH_RUNS='[]'
export GH_DISPATCH_RESPONSE='{}'
reset_outputs
if "$subject" >/dev/null 2>&1; then
  echo "accepted a dispatch response without a run ID" >&2
  exit 1
fi
[[ ! -s "$GITHUB_OUTPUT" ]]
