#!/bin/sh
# Kalo CLI Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.sh | sh
#    or: wget -qO- https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.sh | sh
# Pin a version (e.g. a preview build): curl -fsSL .../install.sh | KALO_VERSION=v0.2.0-agent.1 sh

set -e

REPO="kalo-build/kalo-cli"
BINARY_NAME="kalo"
INSTALL_DIR="${KALO_INSTALL_DIR:-/usr/local/bin}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    printf "${GREEN}[INFO]${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$1"
}

error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1"
    exit 1
}

# Detect OS
detect_os() {
    OS="$(uname -s)"
    case "$OS" in
        Linux*)     OS="linux";;
        Darwin*)    OS="darwin";;
        MINGW*|MSYS*|CYGWIN*) OS="windows";;
        *)          error "Unsupported operating system: $OS";;
    esac
    echo "$OS"
}

# Detect architecture
detect_arch() {
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64|amd64)   ARCH="x86_64";;
        arm64|aarch64)  ARCH="arm64";;
        *)              error "Unsupported architecture: $ARCH";;
    esac
    echo "$ARCH"
}

# Get latest release version
get_latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
    else
        error "curl or wget is required"
    fi
}

# Download file
download() {
    URL="$1"
    DEST="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$URL" -o "$DEST"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$DEST" "$URL"
    else
        error "curl or wget is required"
    fi
}

main() {
    info "Detecting system..."
    OS=$(detect_os)
    ARCH=$(detect_arch)
    info "OS: $OS, Architecture: $ARCH"

    if [ -n "${KALO_VERSION:-}" ]; then
        VERSION="$KALO_VERSION"
        case "$VERSION" in v*) ;; *) VERSION="v$VERSION" ;; esac
        info "Requested version: $VERSION"
    else
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
        if [ -z "$VERSION" ]; then
            error "Could not determine latest version"
        fi
        info "Latest version: $VERSION"
    fi

    # Construct download URL
    VERSION_NUM="${VERSION#v}"
    FILENAME="${BINARY_NAME}_${VERSION_NUM}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}"

    info "Downloading $FILENAME..."
    TMP_DIR=$(mktemp -d)
    trap "rm -rf $TMP_DIR" EXIT

    download "$DOWNLOAD_URL" "$TMP_DIR/$FILENAME"

    info "Extracting..."
    tar -xzf "$TMP_DIR/$FILENAME" -C "$TMP_DIR"

    info "Installing to $INSTALL_DIR..."
    if [ ! -d "$INSTALL_DIR" ]; then
        mkdir -p "$INSTALL_DIR" 2>/dev/null || sudo mkdir -p "$INSTALL_DIR"
    fi
    if [ -w "$INSTALL_DIR" ]; then
        mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
        chmod +x "$INSTALL_DIR/$BINARY_NAME"
    else
        warn "Permission denied. Trying with sudo..."
        sudo mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
        sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"
    fi

    info "Kalo CLI installed successfully!"
    info "Run 'kalo --help' to get started"
    
    # Verify the binary we just installed, not whichever kalo comes first in PATH
    "$INSTALL_DIR/$BINARY_NAME" --version

    FOUND=$(command -v "$BINARY_NAME" 2>/dev/null || true)
    case ":$PATH:" in
        *":$INSTALL_DIR:"*) ON_PATH=1 ;;
        *) ON_PATH=0 ;;
    esac
    if [ "$ON_PATH" -eq 0 ]; then
        warn "$INSTALL_DIR is not in your PATH. Run this, then use kalo:"
        warn "  export PATH=\"$INSTALL_DIR:\$PATH\""
    fi
    if [ -n "$FOUND" ] && [ "$FOUND" != "$INSTALL_DIR/$BINARY_NAME" ]; then
        warn "'kalo' currently runs $FOUND, not the version installed here."
        if [ "$ON_PATH" -eq 1 ]; then
            warn "Put $INSTALL_DIR before $(dirname "$FOUND") in your PATH, or remove $FOUND."
        fi
    fi
}

main "$@"
