#!/bin/bash
set -euo pipefail

REPO="ops0-ai/oxid"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
  linux)  OS="linux" ;;
  darwin) OS="darwin" ;;
  *) echo "Error: Unsupported OS: $OS" >&2; exit 1 ;;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64)  ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "Error: Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

# Detect musl (Alpine, Void, etc.) vs glibc
LIBC=""
if [ "$OS" = "linux" ]; then
  if ldd --version 2>&1 | grep -qi musl || [ -f /etc/alpine-release ]; then
    LIBC="-musl"
  fi
fi

ARTIFACT="oxid-${OS}${LIBC}-${ARCH}"

# Get latest version
echo "Fetching latest oxid release..."
VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)

if [ -z "$VERSION" ]; then
  echo "Error: Could not determine latest version" >&2
  exit 1
fi

echo "Installing oxid ${VERSION} (${OS}/${ARCH})..."

URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARTIFACT}.tar.gz"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Download and extract
curl -fsSL "$URL" -o "${TMPDIR}/${ARTIFACT}.tar.gz"
tar xzf "${TMPDIR}/${ARTIFACT}.tar.gz" -C "$TMPDIR"

# Install
if [ -w "$INSTALL_DIR" ]; then
  mv "${TMPDIR}/oxid" "${INSTALL_DIR}/oxid"
else
  echo "Installing to ${INSTALL_DIR} (requires sudo)..."
  sudo mv "${TMPDIR}/oxid" "${INSTALL_DIR}/oxid"
fi

chmod +x "${INSTALL_DIR}/oxid"

echo ""
echo "oxid ${VERSION} installed to ${INSTALL_DIR}/oxid"
echo "Run 'oxid --version' to verify."
