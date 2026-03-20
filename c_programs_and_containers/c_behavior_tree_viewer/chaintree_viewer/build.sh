#!/bin/bash
# ChainTree Viewer - Build Script
# Requires: Qt 6 dev packages, CMake 3.16+, C++17 compiler

set -e

# --- Install dependencies (Ubuntu/Debian) ---
# Uncomment if needed:
# sudo apt update
# sudo apt install -y qt6-base-dev cmake g++ make

# --- Build ---
BUILD_DIR="build"

echo "=== ChainTree Viewer Build ==="
echo ""

# Check for Qt 6
if ! pkg-config --exists Qt6Widgets 2>/dev/null; then
    if ! dpkg -l qt6-base-dev &>/dev/null; then
        echo "ERROR: Qt 6 not found."
        echo "Install with:  sudo apt install qt6-base-dev"
        echo ""
        echo "On Fedora:     sudo dnf install qt6-qtbase-devel"
        echo "On Arch:       sudo pacman -S qt6-base"
        exit 1
    fi
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

echo ""
echo "=== Build complete ==="
echo "Binary: $BUILD_DIR/chaintree_viewer"
echo "Run:    ./$BUILD_DIR/chaintree_viewer"
