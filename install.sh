#!/bin/sh
# Install script for ZeroFS

set -e

REPO_OWNER="Barre"
REPO_NAME="ZeroFS"
BINARY_NAME="zerofs"
TARBALL_NAME="zerofs-pgo-multiplatform.tar.gz"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
SKIP_CHECKSUM="${SKIP_CHECKSUM:-false}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' 

info() {
    printf "${GREEN}[INFO]${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$1"
}

error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1" >&2
    exit 1
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin*)
            echo "darwin"
            ;;
        Linux*)
            echo "linux"
            ;;
        FreeBSD*)
            echo "freebsd"
            ;;
        *)
            error "Unsupported operating system: $(uname -s)"
            ;;
    esac
}

detect_arch() {
    local os="$1"
    arch="$(uname -m)"
    
    case "$arch" in
        x86_64|amd64)
            if [ "$os" = "darwin" ]; then
                echo "x86_64"
            else
                echo "amd64"
            fi
            ;;
        aarch64|arm64)
            if [ "$os" = "darwin" ]; then
                echo "aarch64"
            else
                echo "arm64"
            fi
            ;;
        armv7l|armv7)
            echo "armv7"
            ;;
        i386|i686)
            echo "i686"
            ;;
        ppc64le)
            echo "ppc64le"
            ;;
        riscv64)
            echo "riscv64"
            ;;
        s390x)
            echo "s390x"
            ;;
        *)
            error "Unsupported architecture: $arch"
            ;;
    esac
}

get_download_url() {
    local version="${1:-latest}"
    
    if [ "$version" = "latest" ]; then
        echo "https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/latest/download/${TARBALL_NAME}"
    else
        echo "https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/download/${version}/${TARBALL_NAME}"
    fi
}

get_binary_filename() {
    local os="$1"
    local arch="$2"
    echo "${BINARY_NAME}-${os}-${arch}-pgo"
}

verify_checksum() {
    local tarball="$1"
    local checksum_url="$2"
    local tmp_dir="$3"
    
    if [ "$SKIP_CHECKSUM" = "true" ]; then
        warn "Skipping checksum verification (not recommended)"
        return 0
    fi
    
    local checksum_file="$tmp_dir/checksum.sha256"
    
    info "Downloading checksum file..."
    
    if command -v curl >/dev/null 2>&1; then
        if ! curl -fSL "$checksum_url" -o "$checksum_file"; then
            warn "Failed to download checksum file, skipping verification"
            return 0
        fi
    elif command -v wget >/dev/null 2>&1; then
        if ! wget -q "$checksum_url" -O "$checksum_file"; then
            warn "Failed to download checksum file, skipping verification"
            return 0
        fi
    fi
    
    info "Verifying checksum..."
    
    cd "$tmp_dir" || exit 1
    local base_tarball
    base_tarball="$(basename "$tarball")"
    
    if command -v sha256sum >/dev/null 2>&1; then
        if sha256sum -c "$checksum_file" --status; then
            info "Checksum verification passed"
        else
            error "Checksum verification failed! The downloaded file may be corrupted or tampered with."
        fi
    elif command -v shasum >/dev/null 2>&1; then
        if shasum -a 256 -c "$checksum_file" --status; then
            info "Checksum verification passed"
        else
            error "Checksum verification failed! The downloaded file may be corrupted or tampered with."
        fi
    else
        warn "No SHA256 tool found (sha256sum or shasum), skipping checksum verification"
    fi
    cd - >/dev/null || exit 1
}

install_binary() {
    local os="$1"
    local arch="$2"
    local version="${VERSION:-latest}"
    
    info "Detected OS: $os"
    info "Detected Architecture: $arch"
    info "Installing ${BINARY_NAME}..."
    
    if [ ! -d "$INSTALL_DIR" ]; then
        if mkdir -p "$INSTALL_DIR" 2>/dev/null; then
            info "Created install directory: $INSTALL_DIR"
        else
            warn "Cannot create ${INSTALL_DIR}, trying fallback locations..."
            if [ -d "/usr/local/bin" ] && [ -w "/usr/local/bin" ]; then
                INSTALL_DIR="/usr/local/bin"
                info "Using fallback directory: $INSTALL_DIR"
            elif [ -d "/usr/bin" ]; then
                INSTALL_DIR="/usr/bin"
                info "Using fallback directory: $INSTALL_DIR (may require sudo)"
            else
                error "No suitable install directory found. Please specify with: INSTALL_DIR=\$HOME/.local/bin"
            fi
        fi
    fi
    
    local download_url
    download_url="$(get_download_url "$version")"
    
    local checksum_url="${download_url}.sha256"
    
    local binary_filename
    binary_filename="$(get_binary_filename "$os" "$arch")"
    
    info "Downloading from: $download_url"
    info "Looking for binary: $binary_filename"
    
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    
    local tarball="$tmp_dir/${TARBALL_NAME}"
    
    if command -v curl >/dev/null 2>&1; then
        if ! curl -fSL "$download_url" -o "$tarball"; then
            error "Failed to download tarball. Please check your internet connection."
        fi
    elif command -v wget >/dev/null 2>&1; then
        if ! wget -q "$download_url" -O "$tarball"; then
            error "Failed to download tarball. Please check your internet connection."
        fi
    else
        error "Neither curl nor wget found. Please install one of them and try again."
    fi
    
    verify_checksum "$tarball" "$checksum_url" "$tmp_dir"
    
    info "Extracting tarball..."
    
    if ! tar -xzf "$tarball" -C "$tmp_dir"; then
        error "Failed to extract tarball"
    fi
    
    if [ ! -f "$tmp_dir/$binary_filename" ]; then
        error "Binary $binary_filename not found in tarball. Your platform may not be supported."
    fi
    
    chmod +x "$tmp_dir/$binary_filename"
    
    local source_binary="$tmp_dir/$binary_filename"
    local dest_binary="$INSTALL_DIR/$BINARY_NAME"
    
    if [ -w "$INSTALL_DIR" ]; then
        mv "$source_binary" "$dest_binary"
        info "Successfully installed ${BINARY_NAME} to ${dest_binary}"
    else
        if command -v sudo >/dev/null 2>&1; then
            warn "Installation to ${INSTALL_DIR} requires elevated privileges"
            info "Using sudo to install..."
            if sudo mv "$source_binary" "$dest_binary"; then
                info "Successfully installed ${BINARY_NAME} to ${dest_binary}"
            else
                error "Failed to install with sudo. Please check your permissions."
            fi
        else
            error "No write permission to ${INSTALL_DIR} and sudo is not available. Try: INSTALL_DIR=\$HOME/.local/bin sh install.sh"
        fi
    fi
}

check_path() {
    case "$INSTALL_DIR" in
        /usr/local/bin|/usr/bin|/bin)
            info "${INSTALL_DIR} is a standard system directory"
            ;;
        *)
            # For user directories, check PATH
            if ! echo "$PATH" | grep -q "$INSTALL_DIR"; then
                warn "${INSTALL_DIR} is not in your PATH"
                info "Add the following to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
                echo ""
                echo "    export PATH=\"${INSTALL_DIR}:\$PATH\""
                echo ""
            fi
            ;;
    esac
}

verify_installation() {
    local binary_path="$INSTALL_DIR/$BINARY_NAME"
    
    if [ -f "$binary_path" ] && [ -x "$binary_path" ]; then
        info "Installation verified!"
        
        if "$binary_path" --version >/dev/null 2>&1; then
            info "Run '${BINARY_NAME} --version' to verify"
        else
            info "Run '${BINARY_NAME}' to get started"
        fi
    else
        error "Installation verification failed - binary not found or not executable at ${binary_path}"
    fi
}

main() {
    info "Starting ${BINARY_NAME} installation..."
    
    local os
    local arch
    
    os="$(detect_os)"
    arch="$(detect_arch "$os")"
    
    install_binary "$os" "$arch"
    check_path
    verify_installation
    
    echo ""
    info "Installation complete!"
}

main
