#!/usr/bin/env bash
set -euo pipefail

: "${BASE_SHA:?BASE_SHA is required}"
: "${DEFAULT_BRANCH:?DEFAULT_BRANCH is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
: "${UPDATE_BRANCH:?UPDATE_BRANCH is required}"

readonly manifest=packaging/kernel/kernels.lock.json
readonly candidate_dir=kernel-candidates
readonly remote=origin

if [[ ! "$BASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "invalid BASE_SHA: $BASE_SHA" >&2
  exit 1
fi
default_ref="refs/heads/$DEFAULT_BRANCH"
git check-ref-format "$default_ref" >/dev/null || {
  echo "invalid DEFAULT_BRANCH: $DEFAULT_BRANCH" >&2
  exit 1
}
if [[ -z "${UPDATE_BRANCH//[[:space:]]/}" ]]; then
  echo "UPDATE_BRANCH must not be blank" >&2
  exit 1
fi
current_head=$(git rev-parse --verify 'HEAD^{commit}')
if [[ "$current_head" != "$BASE_SHA" ]]; then
  echo "HEAD $current_head does not match BASE_SHA $BASE_SHA" >&2
  exit 1
fi
if ! git diff --cached --quiet --; then
  echo "refusing to reconcile with pre-existing staged changes" >&2
  exit 1
fi

require_current_base() {
  local advertised

  advertised=$(git ls-remote --heads "$remote" "$default_ref" | cut -f1)
  if [[ "$advertised" != "$BASE_SHA" ]]; then
    echo "$DEFAULT_BRANCH moved from $BASE_SHA to ${advertised:-missing}" >&2
    echo "aborting stale kernel discovery; the next run will recompute it" >&2
    exit 1
  fi
}

require_current_base

shopt -s nullglob
candidates=("$candidate_dir"/*.json)
shopt -u nullglob

pending_arguments=()
remote_sha=$(git ls-remote --heads "$remote" \
  "refs/heads/$UPDATE_BRANCH" | cut -f1)
if [[ -n "$remote_sha" ]]; then
  pending_ref=refs/zerofs/kernel-target-update-pending
  git fetch --no-tags "$remote" \
    "+refs/heads/$UPDATE_BRANCH:$pending_ref"
  fetched_sha=$(git rev-parse --verify "$pending_ref^{commit}")
  if [[ "$fetched_sha" != "$remote_sha" ]]; then
    echo "fetched update branch does not match its advertised commit" >&2
    exit 1
  fi

  read -r -a pending_parents <<<"$(git show -s --format=%P "$remote_sha")"
  if ((${#pending_parents[@]} != 1)); then
    echo "update branch head must have exactly one parent" >&2
    exit 1
  fi
  pending_parent=${pending_parents[0]}
  if ! git merge-base --is-ancestor "$pending_parent" "$BASE_SHA"; then
    echo "update branch parent is not an ancestor of BASE_SHA" >&2
    exit 1
  fi

  mapfile -t pending_paths < <(
    git diff-tree --no-commit-id --name-only -r "$remote_sha" --
  )
  if ((${#pending_paths[@]} != 1)) ||
     [[ "${pending_paths[0]}" != "$manifest" ]]; then
    echo "update branch must change only $manifest" >&2
    printf 'changed path: %s\n' "${pending_paths[@]}" >&2
    exit 1
  fi

  pending_dir=$(mktemp -d)
  trap 'rm -rf -- "$pending_dir"' EXIT
  pending_base=$pending_dir/base.json
  pending_head=$pending_dir/head.json
  git show "$pending_parent:$manifest" >"$pending_base"
  git show "$remote_sha:$manifest" >"$pending_head"
  pending_arguments=(
    --pending-base "$pending_base"
    --pending-head "$pending_head"
  )
fi

packaging/kernel/kernel-targets.py \
  --manifest "$manifest" \
  reconcile "${pending_arguments[@]}" "${candidates[@]}"

if git diff --quiet -- "$manifest"; then
  if [[ -n "$remote_sha" ]]; then
    require_current_base
    git push \
      --force-with-lease="refs/heads/$UPDATE_BRANCH:$remote_sha" \
      "$remote" ":refs/heads/$UPDATE_BRANCH"
  fi
  echo "changed=false" >>"$GITHUB_OUTPUT"
  exit 0
fi

mapfile -t changed_paths < <(git diff --name-only "$BASE_SHA" --)
if ((${#changed_paths[@]} != 1)) ||
   [[ "${changed_paths[0]}" != "$manifest" ]]; then
  echo "refusing to publish changes outside $manifest" >&2
  printf 'changed path: %s\n' "${changed_paths[@]}" >&2
  exit 1
fi

if [[ -n "$remote_sha" ]]; then
  if [[ "$pending_parent" == "$BASE_SHA" ]] &&
     git diff --quiet "$remote_sha" --; then
    require_current_base
    git push \
      --force-with-lease="refs/heads/$UPDATE_BRANCH:$remote_sha" \
      "$remote" "$remote_sha:refs/heads/$UPDATE_BRANCH"
    {
      echo "changed=true"
      echo "head_sha=$remote_sha"
    } >>"$GITHUB_OUTPUT"
    exit 0
  fi
fi

git config user.name "github-actions[bot]"
git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"
git add -- "$manifest"
git commit -m "Update distro kernel package targets"

require_current_base
git push \
  --force-with-lease="refs/heads/$UPDATE_BRANCH:$remote_sha" \
  "$remote" "HEAD:refs/heads/$UPDATE_BRANCH"
{
  echo "changed=true"
  echo "head_sha=$(git rev-parse --verify 'HEAD^{commit}')"
} >>"$GITHUB_OUTPUT"
