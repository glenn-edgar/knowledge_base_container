#!/bin/bash
# setup_and_build.sh
# Run this once to set up the Gradle wrapper, then build the project

set -e

echo "=== Step 1: Generate Gradle wrapper ==="
# Need system gradle to bootstrap the wrapper
if ! command -v gradle &> /dev/null; then
    echo "Installing system gradle..."
    sudo apt install -y gradle
fi

gradle wrapper --gradle-version 8.5

echo ""
echo "=== Step 2: Build the debug APK ==="
./gradlew assembleDebug

echo ""
echo "=== Step 3: Done! ==="
APK_PATH="app/build/outputs/apk/debug/app-debug.apk"
if [ -f "$APK_PATH" ]; then
    echo "APK built successfully: $APK_PATH"
    echo ""
    echo "To install on your phone via WiFi:"
    echo "  1. Enable Wireless Debugging on your phone"
    echo "  2. adb pair <ip>:<port>     (enter pairing code)"
    echo "  3. adb connect <ip>:<port>"
    echo "  4. adb install $APK_PATH"
    echo ""
    echo "To see logs:"
    echo "  adb logcat | grep myapp"
else
    echo "Build may have failed - check output above"
fi
