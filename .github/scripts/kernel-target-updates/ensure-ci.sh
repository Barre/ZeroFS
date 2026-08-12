#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
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

runs=$(gh run list \
  --workflow ci.yml \
  --branch "$UPDATE_BRANCH" \
  --commit "$HEAD_SHA" \
  --limit 100 \
  --json event,headSha)
if jq -e --arg head_sha "$HEAD_SHA" \
    'any(.[]; .headSha == $head_sha and .event == "workflow_dispatch")' \
    <<<"$runs" >/dev/null; then
  echo "CI already exists for $HEAD_SHA"
else
  gh workflow run ci.yml --ref "$UPDATE_BRANCH" \
    -f "expected_sha=$HEAD_SHA"
fi
