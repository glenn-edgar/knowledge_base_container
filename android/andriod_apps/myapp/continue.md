# Serial Terminal App - Project State & Continue Notes

## Date: March 18, 2026

## Project Location
```
~/knowledge_base_assembly/android/andriod_apps/myapp
```

## What's Working
- **WSL2 mirrored networking** configured (`%USERPROFILE%\.wslconfig` has `networkingMode=mirrored`)
- **Windows Firewall rules** added for NATS (4222) and test (9999)
- **ADB wireless debugging** from WSL2 to phone (Motorola, device guid: `adb-ZY22K44GBF-Kbh5mv`)
  - ADB pairing works but is finicky — `adb pair` sometimes hangs, retry with fresh codes
  - `export ADB_MDNS_OPENSCREEN=0` before starting adb helps
  - Pairing port and connection port are different — check phone's Wireless Debugging screen each time
- **App builds and runs** via `./gradlew assembleDebug` + `adb install -r app/build/outputs/apk/debug/app-debug.apk`
- **Phone IP**: 192.168.1.202 (may change)
- **Laptop IP**: 192.168.1.158 (confirmed reachable from phone and external devices)
- Localhost access works from Windows; LAN access works from external devices (hairpin to own IP does not work — known Windows quirk)

## App Architecture
- **Package**: `com.example.myapp`
- **Language**: Java
- **Build**: Gradle, compileSdk 34, minSdk 24, Java 17
- **3 tabs** using ViewPager2 + TabLayout:
  1. **USB Serial** (`UsbSerialFragment.java`) — FTDI, CP210x, CH340, PL2303, ESP32 via `usb-serial-for-android:3.7.3` (jitpack)
  2. **BLE** (`BleTerminalFragment.java`) — GATT terminal, auto-detects Nordic UART Service, falls back to any writable/notifiable characteristic
  3. **BT SPP** (`BtSppFragment.java`) — Classic Bluetooth RFCOMM serial, lists paired devices

## Key Files Modified/Created
```
settings.gradle                          — added jitpack repo
app/build.gradle                         — added viewpager2, usb-serial-for-android deps
app/src/main/AndroidManifest.xml         — USB host, BLE, BT permissions, USB device filter
app/src/main/res/xml/device_filter.xml   — USB vendor IDs (FTDI, CP210x, CH340, PL2303, Arduino, ESP)
app/src/main/res/layout/activity_main.xml           — TabLayout + ViewPager2
app/src/main/res/layout/fragment_usb_serial.xml     — USB terminal UI
app/src/main/res/layout/fragment_ble_terminal.xml   — BLE terminal UI
app/src/main/res/layout/fragment_bt_spp.xml         — SPP terminal UI
app/src/main/java/com/example/myapp/MainActivity.java       — Tab setup, permissions
app/src/main/java/com/example/myapp/UsbSerialFragment.java  — USB serial logic
app/src/main/java/com/example/myapp/BleTerminalFragment.java — BLE GATT logic
app/src/main/java/com/example/myapp/BtSppFragment.java      — Bluetooth SPP logic
```

## ADB Reconnection Cheat Sheet
```bash
# Kill any hung adb
pkill -9 adb

# Set environment
export ADB_MDNS_OPENSCREEN=0
adb start-server

# On phone: Settings > Developer Options > Wireless Debugging
# Tap "Pair device with pairing code" — note the PORT and CODE
adb pair 192.168.1.202:<pairing-port>
# Enter the code when prompted

# Then connect using the MAIN port shown on Wireless Debugging screen (different from pairing port)
adb connect 192.168.1.202:<connection-port>

# Verify
adb devices
```

## Build & Deploy Cheat Sheet
```bash
cd ~/knowledge_base_assembly/android/andriod_apps/myapp
./gradlew assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

## NEXT: Setup Debug Environment

### What needs to be done:
1. **Logcat from WSL2** — basic real-time log viewing:
   ```bash
   adb logcat -s "com.example.myapp" --pid=$(adb shell pidof com.example.myapp)
   # Or filter by tag:
   adb logcat *:E   # errors only
   adb logcat | grep -i "myapp"
   ```

2. **Android SDK debug tools** — ensure the following are available in WSL2:
   - `adb` (installed, version 34.0.4)
   - Android SDK (check: `echo $ANDROID_HOME` or `echo $ANDROID_SDK_ROOT`)
   - Consider installing command-line tools if not present

3. **Remote debugging options to evaluate**:
   - **JDWP debugging** — `adb forward tcp:5005 jdwp:<pid>` then attach from an IDE or jdb
   - **VS Code Java debugger** — install Extension Pack for Java, configure launch.json for remote attach
   - **Android Studio on Windows** (x86 emulated on Snapdragon) — heavier but full GUI debugger
   - **Logcat viewer** — consider a TUI like `pidcat` or a simple filtered logcat wrapper script

4. **Add debug logging to the app** — instrument each fragment with `Log.d()` calls for:
   - Connection state changes
   - Data sent/received (hex dump option)
   - Permission grant/deny events
   - Error details

5. **Possible debug features in-app**:
   - Hex view toggle for received data
   - Connection status bar with device info
   - Log export (save terminal output to file)

## Known Issues / Quirks
- ADB wireless pairing is unreliable from WSL2 — sometimes hangs, retry with fresh codes
- `adb kill-server` can hang — use `pkill -9 adb` instead
- No Android emulator available on Windows ARM64 — must use physical device
- Laptop cannot reach itself via LAN IP (192.168.1.158) — use localhost from Windows, LAN IP from external devices
- BLE chunking is hardcoded to 20 bytes (default MTU) — should negotiate MTU after connection for better throughput


