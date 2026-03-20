# Grid Editor Integration Guide

## File Placement

Copy these files into your existing project at:
`~/knowledge_base_assembly/android/andriod_apps/myapp/`

### Java source files → `app/src/main/java/com/example/myapp/`

| File                  | Description                                    |
|-----------------------|------------------------------------------------|
| `GridModel.java`      | Data model: rows, keys, types, JSON, validation |
| `GridAdapter.java`    | RecyclerView adapter with 3 view types          |
| `ConfigFileManager.java` | Internal save/load, SAF import/export        |
| `GridActivity.java`   | Main grid editor activity                       |
| `MainActivity.java`   | Updated: adds navigation button to grid editor  |

### Layout files → `app/src/main/res/layout/`

| File                       | Description                              |
|----------------------------|------------------------------------------|
| `activity_main.xml`        | Updated: centered text + navigation btn  |
| `activity_grid.xml`        | Toolbar + RecyclerView                   |
| `grid_cell_controls.xml`   | Row number + insert/delete buttons       |
| `grid_cell_key.xml`        | Numeric EditText for key (0-255)         |
| `grid_cell_type.xml`       | Spinner dropdown for type selection      |

### Drawable → `app/src/main/res/drawable/`

| File             | Description                  |
|------------------|------------------------------|
| `cell_border.xml` | Thin border shape for cells |

### Values → `app/src/main/res/values/`

| File           | Description                      |
|----------------|----------------------------------|
| `strings.xml`  | Updated: adds grid_editor_title  |

### Manifest → `app/src/main/`

| File                   | Description                                         |
|------------------------|-----------------------------------------------------|
| `AndroidManifest.xml`  | Both activities, intent filters for JSON import      |

## Build Dependencies

Your existing `app/build.gradle` should already have these from the initial setup:

```groovy
dependencies {
    implementation 'androidx.appcompat:appcompat:1.6.1'
    implementation 'com.google.android.material:material:1.11.0'
    implementation 'androidx.constraintlayout:constraintlayout:2.1.4'
    implementation 'androidx.recyclerview:recyclerview:1.3.2'  // ADD THIS IF MISSING
}
```

**Check**: if `recyclerview` is not already in dependencies, add that line.

## Quick Deploy

```bash
cd ~/knowledge_base_assembly/android/andriod_apps/myapp

# If you cleared gradle cache or changed AGP, re-patch aapt2:
find ~/.gradle -name 'aapt2-*-linux.jar' -type f | xargs -I{} jar -u -f {} -C /usr/bin aapt2

# Build
./gradlew assembleDebug --no-daemon

# Copy to Windows
cp app/build/outputs/apk/debug/app-debug.apk /mnt/c/Users/glenn/Downloads/
```

## What You Get

- **MainActivity**: Shows app name + "Message Format Editor" button
- **GridActivity**: Full grid editor with:
  - Scrollable grid: controls column + key column + 5 type columns
  - Per-row insert/delete with confirmation dialogs
  - Add row via toolbar
  - Save/Load to internal app storage (named configs)
  - Import/Export via Android file picker (SAF)
  - Email attachment import (ACTION_VIEW, ACTION_SEND for JSON)
  - Validation: key range, uniqueness, type continuity
  - Unsaved changes guard on load/import

## JSON Format

Saved/exported files look like:

```json
{
  "cols": 6,
  "data": [
    { "key": 0,   "types": ["u8", "u16", "f32", "", ""] },
    { "key": 10,  "types": ["i32", "",    "",    "", ""] },
    {}
  ]
}
```

## Notes

- `s0` type is included in the valid set — this is a null-terminated string marker.
  On the parser side, you'll need length/delimiter handling for this variable-length type.
- The grid defaults to 6 columns (1 key + 5 types). This is set in `GridModel()` constructor.
  Change the default in `GridModel.createDefault()` if you want more/fewer type columns.
- `adjustPan` on GridActivity prevents the soft keyboard from resizing the grid.
- Intent filters use `application/json` MIME type. If you later switch to a custom
  extension like `.smfconfig`, update the manifest filters and the SAF launcher MIME.
