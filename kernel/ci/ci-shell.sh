#!/usr/bin/env bash
#
# GitHub Actions shell adapter. Use as: shell: kernel/ci/ci-shell.sh {0}

set -euo pipefail

readonly script_name=${0##*/}

usage() {
    echo "usage: $script_name <github-temp-script>" >&2
}

run_guest_step() {
    local log_dir=${TMPDIR:-/tmp}/zerofs-ci-step-logs
    local step_script
    local step_log
    local command_pid
    local relay_pid
    local command_status
    local relay_status

    install -d -m 0700 -- "$log_dir"
    step_script=$(mktemp "$log_dir/step.XXXXXX.sh")
    step_log=${step_script%.sh}.log
    trap 'rm -f -- "$step_script"' EXIT

    cat >"$step_script"
    chmod 0600 "$step_script"
    : >"$step_log"
    chmod 0600 "$step_log"

    # The foreground shell writes to a regular file so processes started with
    # `&` cannot retain the SSH channel after the shell exits. Relay that file
    # while the shell is alive so step output remains live; later daemon output
    # stays available in the guest log.
    bash --noprofile --norc -e -o pipefail "$step_script" \
        </dev/null >"$step_log" 2>&1 &
    command_pid=$!
    tail --pid="$command_pid" -n +1 -f "$step_log" &
    relay_pid=$!

    set +e
    wait "$command_pid"
    command_status=$?
    wait "$relay_pid"
    relay_status=$?
    set -e

    rm -f -- "$step_script"
    trap - EXIT

    if ((command_status != 0)); then
        return "$command_status"
    fi
    return "$relay_status"
}

if [[ ${1:-} == --guest-step ]]; then
    [[ $# -eq 1 ]] || {
        usage
        exit 2
    }
    run_guest_step
    exit
fi

[[ $# -eq 1 ]] || {
    usage
    exit 2
}

github_script=$1
[[ -r "$github_script" ]] || {
    echo "$script_name: script is not readable: $github_script" >&2
    exit 1
}

if [[ ${ZEROFS_CI_GUEST:-0} != 1 ]]; then
    exec bash --noprofile --norc -e -o pipefail "$github_script"
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec "$script_dir/qemu-vm.sh" exec "$PWD" <"$github_script"
