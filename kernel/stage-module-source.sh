#!/usr/bin/env bash
#
# Assemble a clean, self-contained module source tree.

set -euo pipefail

usage() {
    echo "usage: $0 <destination>" >&2
    exit 2
}

if [[ $# -ne 1 ]]; then
    usage
fi

destination_input=$1
marker_name=.zerofs-module-source
marker_value=zerofs-module-source-v1

script_path=$(realpath -- "${BASH_SOURCE[0]}")
script_dir=$(dirname -- "$script_path")
repository_root=$(realpath -- "$script_dir/..")

if [[ -z "$destination_input" ]]; then
    echo "destination must not be empty" >&2
    exit 2
fi
while [[ "$destination_input" != / && "$destination_input" == */ ]]; do
    destination_input=${destination_input%/}
done
if [[ -L "$destination_input" ]]; then
    echo "destination must not be a symlink: $destination_input" >&2
    exit 2
fi
destination=$(realpath -m -- "$destination_input")

case "$destination" in
    / | "$repository_root" | "$repository_root/"*)
        echo "refusing to stage over the repository sources: $destination" >&2
        exit 2
        ;;
esac

if [[ -e "$destination" && ! -d "$destination" ]]; then
    echo "destination must be a directory: $destination" >&2
    exit 2
fi
if [[ -d "$destination" ]]; then
    if ! first_entry=$(find "$destination" -mindepth 1 -print -quit); then
        echo "cannot inspect existing destination: $destination" >&2
        exit 2
    fi
    if [[ -n "$first_entry" ]]; then
        marker_path=$destination/$marker_name
        if [[ -L "$marker_path" || ! -f "$marker_path" ]] ||
            [[ "$(<"$marker_path")" != "$marker_value" ]]; then
            echo "refusing to replace an unowned nonempty directory: $destination" >&2
            exit 2
        fi
    fi
fi

destination_parent=$(dirname -- "$destination")
destination_name=$(basename -- "$destination")
if [[ ! -d "$destination_parent" ]]; then
    install -d -m 0755 "$destination_parent"
fi

stage_directory=$(mktemp -d "$destination_parent/.${destination_name}.stage.XXXXXX")
backup_directory=

cleanup() {
    if [[ -n "$stage_directory" && -d "$stage_directory" ]]; then
        rm -rf -- "$stage_directory"
    fi
    if [[ -n "$backup_directory" && -d "$backup_directory" ]] &&
        [[ ! -e "$destination" && ! -L "$destination" ]]; then
        mv -- "$backup_directory" "$destination"
    fi
}
trap cleanup EXIT

chmod 0755 "$stage_directory"

install -d -m 0755 \
    "$stage_directory/kernel/client" \
    "$stage_directory/kernel/netfs" \
    "$stage_directory/kernel/self_contained" \
    "$stage_directory/kernel/vfs" \
    "$stage_directory/zerofs/ninep-proto/src"

install -m 0644 \
    "$repository_root/LICENSE" \
    "$stage_directory/"

install -m 0644 \
    "$repository_root/kernel/Kbuild" \
    "$repository_root/kernel/Makefile" \
    "$repository_root/kernel/README.md" \
    "$stage_directory/kernel/"

# Kbuild recreates generated bindings against the exact target. Stage every
# authored root Rust/C source and build-time header.
shopt -s nullglob
for source in \
    "$repository_root"/kernel/*.rs \
    "$repository_root"/kernel/*.c \
    "$repository_root"/kernel/*.h; do
    case "$source" in
        *_bindings.rs | *.mod.c)
            continue
            ;;
    esac
    install -m 0644 "$source" "$stage_directory/kernel/"
done
shopt -u nullglob

# Module declarations own their source lists. Copy complete module trees so
# future child modules and transitive imports are staged automatically.
cp -R "$repository_root/kernel/client/." "$stage_directory/kernel/client/"
cp -R "$repository_root/kernel/netfs/." "$stage_directory/kernel/netfs/"
# A local module build generates bindings and Kbuild intermediates inside the
# source module trees. Regenerate them against the target kernel.
rm -f -- "$stage_directory/kernel/netfs/bindings.rs"
find "$stage_directory/kernel/netfs" -type f \
    \( -name '*.o' -o -name '.*.cmd' -o -name '.*.o.d' \) -delete
cp -R \
    "$repository_root/kernel/self_contained/." \
    "$stage_directory/kernel/self_contained/"
find "$stage_directory/kernel/self_contained" -type d \
    -name __pycache__ -prune -exec rm -rf -- {} +
find "$stage_directory/kernel/self_contained" -type f \
    \( -name '*.bc' -o -name '*.o' -o -name '.*.cmd' \
       -o -name '.*.o.d' -o -name '*.pyc' -o -name '*.pyo' \) -delete
cp -R "$repository_root/kernel/vfs/." "$stage_directory/kernel/vfs/"
rm -f -- "$stage_directory/kernel/vfs/layout_bindings.rs"
find "$stage_directory/kernel/vfs" -type f \
    \( -name '*.o' -o -name '.*.cmd' -o -name '.*.o.d' \) -delete
cp -R \
    "$repository_root/zerofs/ninep-proto/src/." \
    "$stage_directory/zerofs/ninep-proto/src/"

printf '%s\n' "$marker_value" > "$stage_directory/$marker_name"

# cp preserves source modes while install only controls its final component.
# Normalize the complete source tree so staging is independent of the source
# checkout's modes and the caller's umask.
find "$stage_directory" -type d -exec chmod 0755 {} +
find "$stage_directory" -type f -exec chmod 0644 {} +
chmod 0755 \
    "$stage_directory/kernel/self_contained/build.sh" \
    "$stage_directory/kernel/self_contained/runtime_compat.py"

if [[ -d "$destination" ]]; then
    backup_directory=$(mktemp -d "$destination_parent/.${destination_name}.old.XXXXXX")
    rmdir -- "$backup_directory"
    mv -- "$destination" "$backup_directory"
fi
mv -- "$stage_directory" "$destination"
stage_directory=

if [[ -n "$backup_directory" ]]; then
    rm -rf -- "$backup_directory"
    backup_directory=
fi
trap - EXIT

echo "staged module source in $destination"
