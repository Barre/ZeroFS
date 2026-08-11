#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}

die() {
    printf '%s: %s\n' "$script_name" "$*" >&2
    exit 1
}

source_unavailable() {
    # DKMS reports every failed MAKE command with the same outer status. Leave
    # a machine-readable signal so the package installer can distinguish a
    # recoverable missing distribution input from a real compile failure.
    install -d -m 0755 "${source_unavailable_marker%/*}"
    if [[ -e "$source_unavailable_marker" ||
          -L "$source_unavailable_marker" ]]; then
        [[ -f "$source_unavailable_marker" &&
           ! -L "$source_unavailable_marker" ]] ||
            die "unsafe source-unavailable marker: $source_unavailable_marker"
    fi
    printf '%s\n' "$kernel_release" >"$source_unavailable_marker"
    chmod 0644 "$source_unavailable_marker"
    printf '%s: %s\n' "$script_name" "$*" >&2
    exit 75
}

[[ $# -eq 3 ]] || {
    printf 'usage: %s KERNEL_RELEASE KERNEL_BUILD EXTRACTION_DIRECTORY\n' \
        "$script_name" >&2
    exit 2
}
kernel_release=$1
kernel_build=$(realpath -e -- "$2")
extraction_directory=$(realpath -m -- "$3")
[[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~:-]*$ ]] ||
    die "unsafe kernel release: $kernel_release"
case $extraction_directory in
    / | /usr | /usr/* | "$kernel_build" | "$kernel_build"/*)
        die "unsafe kernel source extraction directory: $extraction_directory"
        ;;
esac
source_unavailable_marker="${extraction_directory}.source-unavailable"
if [[ -e "$source_unavailable_marker" ||
      -L "$source_unavailable_marker" ]]; then
    [[ -f "$source_unavailable_marker" &&
       ! -L "$source_unavailable_marker" ]] ||
        die "unsafe source-unavailable marker: $source_unavailable_marker"
    rm -f -- "$source_unavailable_marker"
fi

kernel_source=
provenance=
candidate_error=
explicit_source_version=${ZEROFS_KERNEL_SOURCE_PACKAGE_VERSION:-}
explicit_header_version=${ZEROFS_KERNEL_HEADERS_PACKAGE_VERSION:-}
trusted_package_versions=false
if [[ -n "$explicit_source_version" || -n "$explicit_header_version" ]]; then
    [[ "$explicit_source_version" =~ ^[0-9A-Za-z.+:~_-]+$ &&
       "$explicit_header_version" =~ ^[0-9A-Za-z.+:~_-]+$ ]] ||
        die "both explicit kernel source and header package versions are required"
fi

package_version_for_path() {
    local path=$1
    local owner
    local version

    if command -v dpkg-query >/dev/null 2>&1; then
        if owner=$(dpkg-query -S "$path" 2>/dev/null |
            sed -n '1s/: \/.*//p') && [[ -n "$owner" ]]; then
            if version=$(dpkg-query -W -f='${Version}\n' "$owner" \
                2>/dev/null); then
                printf '%s\n' "$version"
            fi
            return
        fi
    fi
    if command -v rpm >/dev/null 2>&1; then
        if version=$(rpm -qf \
            --qf '%{EPOCHNUM}:%{VERSION}-%{RELEASE}\n' "$path" \
            2>/dev/null); then
            printf '%s\n' "$version"
        fi
    fi
}

validate_packaging_revision() {
    local source=$1
    local origin=$2
    local changelog
    local changelog_version=
    local first_line
    local header_version
    local source_version
    local ubuntu_abi
    local changelog_pattern='^[^[:space:]]+[[:space:]]+\(([^)]+)\)'

    # Prefer local package ownership. CI may supply the exact versions from its
    # authenticated target lock when a prepared source tree is intentionally
    # outside the package database.
    changelog=$(find "$source" -maxdepth 2 -type f \
        \( -path '*/debian/changelog' -o -path '*/debian.master/changelog' \) \
        -print -quit)
    if [[ -n "$changelog" ]]; then
        IFS= read -r first_line <"$changelog"
        if [[ "$first_line" =~ $changelog_pattern ]]; then
            changelog_version=${BASH_REMATCH[1]}
        fi
    fi
    source_version=$(package_version_for_path "$origin" || true)
    if [[ -n "$changelog_version" && -n "$source_version" &&
          "$changelog_version" != "$source_version" ]]; then
        candidate_error="source changelog $changelog_version disagrees with package $source_version"
        return 1
    fi
    [[ -n "$source_version" ]] || source_version=$changelog_version
    header_version=$(package_version_for_path \
        "$kernel_build/include/config/kernel.release" || true)

    if [[ -n "$explicit_source_version" ]]; then
        if [[ -n "$source_version" &&
              "$source_version" != "$explicit_source_version" ]]; then
            candidate_error="source package $source_version does not match trusted source version $explicit_source_version"
            return 1
        fi
        if [[ -n "$header_version" &&
              "$header_version" != "$explicit_header_version" ]]; then
            candidate_error="headers $header_version do not match trusted header version $explicit_header_version"
            return 1
        fi
        source_version=$explicit_source_version
        header_version=$explicit_header_version
        trusted_package_versions=true
    fi
    if [[ -z "$source_version" || -z "$header_version" ]]; then
        candidate_error="cannot prove the source and header package revisions for $kernel_release"
        return 1
    fi
    if [[ "$trusted_package_versions" == false &&
          "$source_version" != "$header_version" ]]; then
        candidate_error="source package $source_version does not match headers $header_version"
        return 1
    fi

    # Ubuntu encodes the kernel ABI in both the uname release and its source
    # package version. This catches a newer same-series source package even when
    # it was copied outside dpkg's ownership database.
    if [[ "$source_version" =~ ^([0-9]+\.[0-9]+\.[0-9]+-[0-9]+)\.[0-9]+ ]]; then
        ubuntu_abi=${BASH_REMATCH[1]}
        if [[ "$kernel_release" != "$ubuntu_abi"-* ]]; then
            candidate_error="source package $source_version does not match $kernel_release"
            return 1
        fi
    fi
}

source_matches_kernel() {
    local source=$1
    local origin=$2
    local version

    [[ -f "$source/Makefile" &&
       -f "$source/rust/Makefile" &&
       -f "$source/rust/kernel/lib.rs" &&
       -f "$source/scripts/Makefile.build" ]] || return 1
    version=$(make -s -C "$source" kernelversion 2>/dev/null) || return 1
    [[ -n "$version" ]] || return 1
    case $kernel_release in
        "$version" | "$version"[-+._~]*) ;;
        *) return 1 ;;
    esac
    validate_packaging_revision "$source" "$origin" || return 1
}

use_source_directory() {
    local candidate=$1
    local resolved

    [[ -d "$candidate" ]] || return 1
    resolved=$(realpath -e -- "$candidate")
    source_matches_kernel "$resolved" "$resolved/Makefile" || return 1
    kernel_source=$resolved
    provenance="directory:$resolved"
}

extract_source_archive() {
    local archive=$1
    local makefile
    local marker="$extraction_directory/.zerofs-kernel-source"
    local source
    local -a candidates=()

    [[ -f "$archive" && ! -L "$archive" ]] || return 1
    install -d -m 0755 "$extraction_directory"
    if [[ -e "$marker" || -L "$marker" ]]; then
        [[ -f "$marker" && ! -L "$marker" &&
           "$(<"$marker")" == zerofs-kernel-source-v1 ]] ||
            die "invalid ownership marker below $extraction_directory"
    elif [[ -n $(find "$extraction_directory" -mindepth 1 -print -quit) ]]; then
        die "refusing to replace an unowned kernel source directory"
    else
        printf '%s\n' zerofs-kernel-source-v1 >"$marker"
        chmod 0644 "$marker"
    fi
    find "$extraction_directory" -mindepth 1 \
        ! -path "$marker" -delete
    tar --extract --file "$archive" --directory "$extraction_directory" \
        --no-same-owner --no-same-permissions
    while IFS= read -r -d '' makefile; do
        source=${makefile%/rust/Makefile}
        source_matches_kernel "$source" "$archive" && candidates+=("$source")
    done < <(find "$extraction_directory" -mindepth 2 -maxdepth 4 \
        -type f -path '*/rust/Makefile' -print0)
    if [[ ${#candidates[@]} -gt 1 ]]; then
        die "multiple matching kernel source trees in $archive"
    fi
    if [[ ${#candidates[@]} -eq 0 ]]; then
        [[ -n "$candidate_error" ]] ||
            candidate_error="cannot select one matching kernel source tree from $archive"
        return 1
    fi
    kernel_source=${candidates[0]}
    provenance="archive:$archive"
}

requested=${ZEROFS_KERNEL_SOURCE:-}
if [[ -n "$requested" ]]; then
    if [[ -d "$requested" ]]; then
        use_source_directory "$requested" ||
            source_unavailable \
                "${candidate_error:-ZEROFS_KERNEL_SOURCE is not a matching source tree}"
    elif [[ -f "$requested" ]]; then
        extract_source_archive "$requested" ||
            source_unavailable \
                "${candidate_error:-ZEROFS_KERNEL_SOURCE is not a matching source archive}"
    else
        die "ZEROFS_KERNEL_SOURCE does not exist: $requested"
    fi
else
    for candidate in \
        "$kernel_build/source" \
        "/usr/src/linux-$kernel_release" \
        /usr/src/linux; do
        if use_source_directory "$candidate"; then
            break
        fi
    done

    if [[ -z "$kernel_source" ]]; then
        kernel_version=${kernel_release%%-*}
        if [[ "$kernel_version" =~ ^([0-9]+\.[0-9]+) ]]; then
            major_minor=${BASH_REMATCH[1]}
        else
            major_minor=$kernel_version
        fi
        declare -A unique_archives=()
        shopt -s nullglob
        for archive in \
            "/usr/src/linux-source-$kernel_version".tar.* \
            "/usr/src/linux-source-$major_minor".tar.*; do
            unique_archives["$archive"]=1
        done
        shopt -u nullglob
        while IFS= read -r archive; do
            if extract_source_archive "$archive"; then
                break
            fi
        done < <(printf '%s\n' "${!unique_archives[@]}" | sort)
        [[ -n "$kernel_source" ]] ||
            source_unavailable \
                "${candidate_error:-the exact kernel source package is not installed for $kernel_release}"
    fi
fi

[[ -n "$kernel_source" && -n "$provenance" ]] ||
    die "cannot find matching kernel source for $kernel_release"
[[ "$kernel_source" != *$'\n'* && "$kernel_source" != *$'\t'* &&
   "$provenance" != *$'\n'* && "$provenance" != *$'\t'* ]] ||
    die "kernel source paths may not contain tabs or newlines"
printf '%s\t%s\n' "$kernel_source" "$provenance"
