#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"
: "${UPDATE_BRANCH:?UPDATE_BRANCH is required}"

if [[ ! "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "invalid HEAD_SHA: $HEAD_SHA" >&2
  exit 1
fi
if [[ -z "${UPDATE_BRANCH//[[:space:]]/}" ]]; then
  echo "UPDATE_BRANCH must not be blank" >&2
  exit 1
fi
if [[ ! "$GITHUB_REPOSITORY" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]]; then
  echo "invalid GITHUB_REPOSITORY: $GITHUB_REPOSITORY" >&2
  exit 1
fi

record_run_id() {
  local run_id=$1

  if [[ ! "$run_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid CI run ID: $run_id" >&2
    exit 1
  fi
  echo "run_id=$run_id" >>"$GITHUB_OUTPUT"
}

runs=$(gh run list \
  --workflow ci.yml \
  --branch "$UPDATE_BRANCH" \
  --commit "$HEAD_SHA" \
  --limit 100 \
  --json createdAt,databaseId,event,headSha)
run_id=$(jq -r --arg head_sha "$HEAD_SHA" '
  map(select(
    .headSha == $head_sha and
    .event == "workflow_dispatch"
  )) |
  sort_by(.createdAt, .databaseId) |
  last |
  .databaseId // empty
' <<<"$runs")
if [[ -n "$run_id" ]]; then
  record_run_id "$run_id"
  echo "CI already exists for $HEAD_SHA: $run_id"
  exit 0
fi

response=$(jq -n \
  --arg ref "$UPDATE_BRANCH" \
  --arg expected_sha "$HEAD_SHA" \
  '{
    ref: $ref,
    inputs: {expected_sha: $expected_sha}
  }' |
  gh api --method POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2026-03-10" \
    "repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/dispatches" \
    --input -)
run_id=$(jq -er '
  .workflow_run_id |
  select(type == "number" and . > 0 and . == floor) |
  tostring
' <<<"$response")
record_run_id "$run_id"
echo "Dispatched CI for $HEAD_SHA: $run_id"
