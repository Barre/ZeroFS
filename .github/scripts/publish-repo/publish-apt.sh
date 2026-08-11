#!/usr/bin/env bash

set -euo pipefail

readonly codename=stable
readonly component=main
readonly repository_prefix=deb

repository_args=(
    --bucket "$R2_BUCKET"
    --endpoint "$R2_ENDPOINT"
    --s3-region auto
    --force-path-style
    --prefix "$repository_prefix"
    --codename "$codename"
    --suite "$codename"
    --component "$component"
)
upload_args=(
    "${repository_args[@]}"
    --visibility private
    --sign "$GPG_FINGERPRINT"
    --gpg-options="--pinentry-mode=loopback --batch --yes"
)
if [[ "$PROBE_PACKAGE" == zerofs-kernel-client ]]; then
    declare -A incoming_probe_digest=()
    for package in out/*.deb; do
        name=$(dpkg-deb -f "$package" Package)
        if [[ "$name" == "$PROBE_PACKAGE" ]]; then
            architecture=$(dpkg-deb -f "$package" Architecture)
            [[ -z "${incoming_probe_digest[$architecture]:-}" ]] || {
                echo "multiple $PROBE_PACKAGE packages for $architecture" >&2
                exit 1
            }
            incoming_probe_digest["$architecture"]=$(
                sha256sum "$package" | awk '{ print $1 }'
            )
        fi
    done

    read -r -a repository_architectures <<<"$ARCHITECTURES"
    for architecture in "${repository_architectures[@]}"; do
        [[ -n "${incoming_probe_digest[$architecture]:-}" ]] || {
            echo "no $PROBE_PACKAGE package for $architecture" >&2
            exit 1
        }
        existing_files="$RUNNER_TEMP/apt-existing-$architecture"
        manifest_root="dists/$codename/$component"
        manifest_pattern="$manifest_root/binary-$architecture/Packages"
        if [[ "$architecture" == all ]]; then
            manifest_pattern="$manifest_root/binary-*/Packages"
        fi
        rclone lsf "r2:$R2_BUCKET/$repository_prefix" \
            --recursive --files-only --include "$manifest_pattern" \
            >"$existing_files"

        policy_manifests=()
        manifest_index=0
        while IFS= read -r manifest_key; do
            manifest_architecture=${manifest_key#"$manifest_root/binary-"}
            manifest_architecture=${manifest_architecture%/Packages}
            [[ "$manifest_key" == \
                   "$manifest_root/binary-$manifest_architecture/Packages" &&
               "$manifest_architecture" =~ ^[A-Za-z0-9_]+$ ]] || {
                echo "unexpected APT manifest key: $manifest_key" >&2
                exit 1
            }
            manifest="$RUNNER_TEMP/apt-Packages-$architecture-$manifest_index"
            rclone copyto \
                "r2:$R2_BUCKET/$repository_prefix/$manifest_key" "$manifest"
            policy_manifests+=(--manifest "$manifest")
            ((manifest_index += 1))
        done <"$existing_files"
        if ((!manifest_index)); then
            manifest="$RUNNER_TEMP/apt-Packages-$architecture-empty"
            : >"$manifest"
            policy_manifests+=(--manifest "$manifest")
        fi
        python3 packaging/kernel/repository_policy.py check-apt \
            "${policy_manifests[@]}" \
            --package "$PROBE_PACKAGE" \
            --architecture "$architecture" \
            --incoming "$PROBE_VERSION" \
            --incoming-sha256 "${incoming_probe_digest[$architecture]}"
    done
fi

deb-s3 upload "${upload_args[@]}" --preserve-versions out/*.deb
deb-s3 verify "${repository_args[@]}"
