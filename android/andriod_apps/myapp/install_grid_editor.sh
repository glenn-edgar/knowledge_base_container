#!/bin/bash
# install_grid_editor.sh
# Copies grid editor files into the myapp project.
# Run from wherever this script lives. Adjust PROJECT if needed.

PROJECT="$HOME/knowledge_base_assembly/android/andriod_apps/myapp"

JAVA_DIR="$PROJECT/app/src/main/java/com/example/myapp"
RES_LAYOUT="$PROJECT/app/src/main/res/layout"
RES_DRAWABLE="$PROJECT/app/src/main/res/drawable"
RES_VALUES="$PROJECT/app/src/main/res/values"
MANIFEST_DIR="$PROJECT/app/src/main"

# Source directory (where this script + files live)
SRC="$(cd "$(dirname "$0")" && pwd)"

echo "Installing grid editor into: $PROJECT"

# Create directories
mkdir -p "$JAVA_DIR"
mkdir -p "$RES_LAYOUT"
mkdir -p "$RES_DRAWABLE"
mkdir -p "$RES_VALUES"

# Java files
cp "$SRC/java/com/example/myapp/GridModel.java"       "$JAVA_DIR/"
cp "$SRC/java/com/example/myapp/GridAdapter.java"      "$JAVA_DIR/"
cp "$SRC/java/com/example/myapp/ConfigFileManager.java" "$JAVA_DIR/"
cp "$SRC/java/com/example/myapp/GridActivity.java"     "$JAVA_DIR/"
cp "$SRC/java/com/example/myapp/MainActivity.java"     "$JAVA_DIR/"

# Layouts
cp "$SRC/res/layout/activity_main.xml"       "$RES_LAYOUT/"
cp "$SRC/res/layout/activity_grid.xml"       "$RES_LAYOUT/"
cp "$SRC/res/layout/grid_cell_controls.xml"  "$RES_LAYOUT/"
cp "$SRC/res/layout/grid_cell_key.xml"       "$RES_LAYOUT/"
cp "$SRC/res/layout/grid_cell_type.xml"      "$RES_LAYOUT/"

# Drawable
cp "$SRC/res/drawable/cell_border.xml"       "$RES_DRAWABLE/"

# Values
cp "$SRC/res/values/strings.xml"             "$RES_VALUES/"

# Manifest
cp "$SRC/AndroidManifest.xml"                "$MANIFEST_DIR/"

echo ""
echo "Done. Files installed:"
echo "  Java:     $JAVA_DIR/ (5 files)"
echo "  Layout:   $RES_LAYOUT/ (5 files)"
echo "  Drawable: $RES_DRAWABLE/ (1 file)"
echo "  Values:   $RES_VALUES/ (1 file)"
echo "  Manifest: $MANIFEST_DIR/AndroidManifest.xml"
echo ""
echo "Next steps:"
echo "  1. Check app/build.gradle has: implementation 'androidx.recyclerview:recyclerview:1.3.2'"
echo "  2. ./gradlew assembleDebug --no-daemon"
echo "  3. cp app/build/outputs/apk/debug/app-debug.apk /mnt/c/Users/glenn/Downloads/"
