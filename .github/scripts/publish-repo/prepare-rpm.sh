#!/usr/bin/env bash

set -euo pipefail

install -d -m 0755 rpmrepo rpm-new
rclone lsf "r2:$R2_BUCKET" \
    --recursive --files-only --include 'rpm/**' \
    >"$RUNNER_TEMP/rpm-existing-files"
if [[ -s "$RUNNER_TEMP/rpm-existing-files" ]]; then
    rclone copy "r2:$R2_BUCKET/rpm" rpmrepo
fi
if find rpmrepo -type l -print -quit | grep -q .; then
    echo "existing RPM repository contains a symbolic link" >&2
    exit 1
fi

rpm_db="$RUNNER_TEMP/zerofs-publish-rpmdb"
install -d -m 0700 "$rpm_db"
rpm --dbpath "$rpm_db" --initdb
rpm --dbpath "$rpm_db" --import zerofs.gpg

verify_signed_rpm() {
    local verification

    verification=$(rpm --dbpath "$rpm_db" --checksig "$1")
    printf '%s\n' "$verification"
    [[ "$verification" == *"signatures OK"* ]]
}

shopt -s nullglob
mapfile -d '' historical_packages < <(
    find rpmrepo -type f -name '*.rpm' -print0 | sort -z
)
for package in "${historical_packages[@]}"; do
    verify_signed_rpm "$package"
done
new_packages=(out/*.rpm)

# These two digests cover the immutable RPM header (including scriptlets and
# dependency metadata) and its complete payload while deliberately excluding
# the repository signature.
rpm_content_id() {
    local identifier

    identifier=$(rpm -qp --qf '%{SHA256HEADER}:%{PAYLOADDIGEST}' "$1")
    [[ "$identifier" =~ ^[A-Fa-f0-9]{64}:[A-Fa-f0-9]{64}$ ]] || {
        echo "$1: RPM has no usable SHA-256 content identity" >&2
        return 1
    }
    printf '%s\n' "${identifier,,}"
}

declare -A skip_new_packages=()
if [[ "$PROBE_PACKAGE" == zerofs-kernel-client ]]; then
    declare -A incoming_probe_content=()
    declare -A incoming_probe_filename=()
    for package in "${new_packages[@]}"; do
        mapfile -t identity < <(
            rpm -qp --qf '%{NAME}\n%{ARCH}\n%{VERSION}-%{RELEASE}\n' \
                "$package"
        )
        ((${#identity[@]} == 3))
        if [[ "${identity[0]}" == "$PROBE_PACKAGE" ]]; then
            architecture=${identity[1]}
            [[ "${identity[2]}" == "$PROBE_VERSION" ]]
            [[ -z "${incoming_probe_filename[$architecture]:-}" ]] || {
                echo "multiple $PROBE_PACKAGE packages for $architecture" >&2
                exit 1
            }
            incoming_probe_content["$architecture"]=$(
                rpm_content_id "$package"
            )
            incoming_probe_filename["$architecture"]=${package##*/}
        fi
    done

    for package in "${historical_packages[@]}"; do
        mapfile -t identity < <(
            rpm -qp --qf '%{NAME}\n%{ARCH}\n%{VERSION}-%{RELEASE}\n' \
                "$package"
        )
        ((${#identity[@]} == 3))
        if [[ "${identity[0]}" == "$PROBE_PACKAGE" &&
              -n "${incoming_probe_content[${identity[1]}]:-}" ]]; then
            python3 packaging/kernel/repository_policy.py check-version \
                "$PROBE_VERSION" "${identity[2]}"
            if [[ "${identity[2]}" == "$PROBE_VERSION" ]]; then
                historical_content=$(rpm_content_id "$package")
                [[ "$historical_content" == \
                   "${incoming_probe_content[${identity[1]}]}" ]] || {
                    echo "refusing to replace equal-version $PROBE_PACKAGE "\
                         "for ${identity[1]} with different content" >&2
                    exit 1
                }
                filename=${incoming_probe_filename[${identity[1]}]}
                skip_new_packages["$filename"]=1
            fi
        fi
    done
fi

for package in "${new_packages[@]}"; do
    filename=${package##*/}
    if [[ -n "${skip_new_packages[$filename]:-}" ]]; then
        echo "$filename is already published with identical content"
        continue
    fi
    existing="rpmrepo/$filename"
    if [[ -e "$existing" ]]; then
        [[ -f "$existing" && ! -L "$existing" ]]
        if [[ "$PROBE_PACKAGE" == zerofs-kernel-client ]]; then
            echo "refusing to overwrite historical RPM $filename" >&2
            exit 1
        fi
    fi
    rpmsign --addsign \
        --define "_openpgp_sign_id $GPG_FINGERPRINT" \
        --define "_gpg_name $GPG_FINGERPRINT" \
        "$package"
    verify_signed_rpm "$package"
    cp "$package" "rpm-new/$filename"
    cp "$package" "$existing"
done

createrepo_c --update rpmrepo
repomd=rpmrepo/repodata/repomd.xml
[[ -s "$repomd" ]]
gpg --batch --yes --pinentry-mode loopback --passphrase '' \
    --local-user "$GPG_FINGERPRINT" \
    --detach-sign --armor \
    --output "$repomd.asc" "$repomd"
gpg --batch --verify "$repomd.asc" "$repomd"
