#!/bin/sh
# Kalo CLI Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.sh | sh
#    or: wget -qO- https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.sh | sh

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

    info "Fetching latest version..."
    VERSION=$(get_latest_version)
    if [ -z "$VERSION" ]; then
        error "Could not determine latest version"
    fi
    info "Latest version: $VERSION"

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
    
    # Verify installation
    if command -v kalo >/dev/null 2>&1; then
        kalo --version
    else
        warn "kalo is not in PATH. Add $INSTALL_DIR to your PATH."
    fi
}

main "$@"
