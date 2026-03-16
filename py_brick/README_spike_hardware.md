# SPIKE Prime Hub — Hardware Description and Pybricks Command Reference

Detailed hardware specifications for the LEGO® SPIKE Prime Hub (45601 / Technic
Large Hub) and every Pybricks v3.6 command that interacts with the physical
hardware, including BLE wireless, USB serial, and persistent flash storage.

---

## Table of Contents

- [Hardware Overview](#hardware-overview)
- [Processor and Memory](#processor-and-memory)
- [I/O Ports (LPF2)](#io-ports-lpf2)
  - [Port Pin Assignments](#port-pin-assignments)
  - [LEGO UART Messaging Protocol](#lego-uart-messaging-protocol)
  - [Pybricks Port Commands](#pybricks-port-commands)
- [5×5 LED Light Matrix](#5×5-led-light-matrix)
  - [Hardware](#led-matrix-hardware)
  - [Pybricks Display Commands](#pybricks-display-commands)
- [Status Light (RGB LED)](#status-light-rgb-led)
  - [Pybricks Light Commands](#pybricks-light-commands)
- [Buttons](#buttons)
  - [Pybricks Button Commands](#pybricks-button-commands)
- [6-Axis IMU](#6-axis-imu-accelerometer--gyroscope)
  - [Hardware](#imu-hardware)
  - [Pybricks IMU Commands](#pybricks-imu-commands)
- [Speaker](#speaker)
  - [Pybricks Speaker Commands](#pybricks-speaker-commands)
- [Rechargeable Battery](#rechargeable-battery)
  - [Pybricks Battery Commands](#pybricks-battery-commands)
- [Bluetooth Low Energy (BLE)](#bluetooth-low-energy-ble)
  - [Hardware](#ble-hardware)
  - [Pybricks BLE Broadcast/Observe Commands](#pybricks-ble-broadcastobserve-commands)
  - [BLE Connection to Pybricks Code IDE](#ble-connection-to-pybricks-code-ide)
- [USB Serial Port](#usb-serial-port)
  - [Hardware](#usb-hardware)
  - [Pybricks USB/Serial Commands](#pybricks-usbserial-commands)
  - [USB vs BLE for Development](#usb-vs-ble-for-development)
- [Persistent Flash Storage (512 Bytes)](#persistent-flash-storage-512-bytes)
  - [How It Works](#how-storage-works)
  - [Pybricks Storage Commands](#pybricks-storage-commands)
  - [Flash Wear and Write Endurance](#flash-wear-and-write-endurance)
  - [What Else Is Stored in Flash](#what-else-is-stored-in-flash)
- [System Control](#system-control)
  - [Pybricks System Commands](#pybricks-system-commands)

---

## Hardware Overview

The SPIKE Prime Hub (LEGO part 45601, also called the Technic Large Hub) is a
programmable brick-shaped controller with the following physical features:

| Feature | Description |
|---------|-------------|
| Processor | STM32F413VG — ARM Cortex-M4 with FPU, 100 MHz |
| Flash (internal) | 1 MB on-chip NOR flash |
| RAM | 320 KB SRAM (256 KB SRAM1 + 64 KB SRAM2) |
| External flash | 32 MB SPI flash (Winbond W25Q256) |
| I/O ports | 6 × LPF2 (LEGO Power Functions 2.0), labeled A–F |
| Display | 5×5 white LED matrix |
| Status LED | 1 × RGB LED (around the center button) |
| Buttons | 3 — Center, Left, Right |
| IMU | 6-axis — 3-axis accelerometer + 3-axis gyroscope (ST LSM6DS3TR) |
| Speaker | Piezoelectric buzzer |
| Bluetooth | TI CC2564C — Bluetooth Classic + BLE 4.2 |
| USB | Micro-USB — USB 2.0 Full Speed |
| Battery | Rechargeable lithium-ion, 2000 mAh, 7.4V nominal |
| Dimensions | Approximately 56 × 80 × 40 mm (brick form factor with Technic mounting) |

---

## Processor and Memory

The STM32F413VG is an ARM Cortex-M4 running at 100 MHz with hardware floating
point (FPU). It delivers 125 DMIPS and includes ST's ART Accelerator for
zero-wait-state flash execution.

| Resource | Size | Used For |
|----------|------|----------|
| Internal flash | 1 MB | Pybricks firmware, compiled MicroPython bytecode, user program storage, persistent user data (512 bytes), IMU calibration settings |
| SRAM1 | 256 KB | Runtime memory — MicroPython heap, stack, motor control state, BLE buffers |
| SRAM2 | 64 KB | Connected via I/D bus for faster access — used by time-critical control loops |
| External SPI flash | 32 MB | Shared with stock LEGO firmware filesystem. Pybricks currently uses less than 1 MB of this to avoid corrupting the LEGO filesystem if you restore stock firmware. |

The 320 KB total RAM is shared between the Pybricks firmware (C runtime, motor
controllers, BLE stack, IMU processing) and the MicroPython VM (your program's
variables, objects, and stack). Typical user programs have access to roughly
200–250 KB of heap depending on how many devices are initialized.

---

## I/O Ports (LPF2)

### Hardware

The hub has 6 identical LPF2 (LEGO Power Functions 2.0) ports labeled A through F.
Each port is a 6-pin connector that supports:

| Pin | Function | Description |
|-----|----------|-------------|
| 1 | M1 (PWM) | Motor drive output 1 — H-bridge PWM |
| 2 | M2 (PWM) | Motor drive output 2 — H-bridge PWM |
| 3 | GND | Ground |
| 4 | VCC | 3.3V power supply to sensor/motor |
| 5 | ID1 / TX | Device identification / UART transmit |
| 6 | ID2 / RX | Device identification / UART receive |

### Port Pin Assignments

Each port maps to specific STM32 GPIO pins:

| Port | PWM1 | PWM2 | UART TX | UART RX |
|------|------|------|---------|---------|
| A | PE9 | PE11 | PD7 | PD8 |
| B | PE13 | PE14 | PD9 | PD10 |
| C | PB6 | PB7 | PD11 | PE4 |
| D | PB8 | PB9 | PC15 | PC14 |
| E | PA0 | PA1 | PD3 | PD4 |
| F | PA2 | PA3 | PD5 | PD6 |

### LEGO UART Messaging Protocol

When a sensor or smart motor is connected, the port auto-detects the device via
the ID pins and switches to UART mode. The LEGO UART Messaging Protocol (LPF2)
runs at 115200 baud with the following characteristics:

- The motor/sensor has its own MCU that handles low-level functions (encoder
  counting, LED driving, etc.)
- Communication is polled at approximately 100 Hz (~10 ms intervals)
- Data is reported in modes — each device has multiple modes exposing different
  measurements
- The hub sends mode-switch commands; the device responds with data in the
  requested mode

This protocol layer is why you cannot get raw encoder tick counts — the motor's
internal MCU processes the quadrature encoder and reports processed values (whole
degrees) over the UART link.

### Pybricks Port Commands

All port devices are accessed through the `pybricks.pupdevices` module. See the
main I/O command reference for full details on each class. The port assignment
uses `Port.A` through `Port.F`:

```python
from pybricks.pupdevices import Motor, ColorSensor, UltrasonicSensor, ForceSensor
from pybricks.parameters import Port

motor = Motor(Port.A)
color = ColorSensor(Port.B)
ultrasonic = UltrasonicSensor(Port.C)
force = ForceSensor(Port.D)
```

For raw protocol access to any device:

```python
from pybricks.iodevices import PUPDevice
device = PUPDevice(Port.A)
print(device.info())        # {"id": device_type_id}
values = device.read(0)     # read mode 0
device.write(5, [1,1,1,1])  # write to mode 5
```

---

## 5×5 LED Light Matrix

### LED Matrix Hardware

A 25-pixel white LED display arranged in a 5×5 grid on the front face of the hub.
Each LED has individually controllable brightness from 0 (off) to 100 (full).
The LEDs are driven via GPIO multiplexing — the STM32 scans rows and columns to
address individual pixels.

This display is only present on the PrimeHub (SPIKE Prime). The EssentialHub
(SPIKE Essential) does not have a matrix display.

### Pybricks Display Commands

```python
from pybricks.hubs import PrimeHub
hub = PrimeHub()
```

| Command | Description |
|---------|-------------|
| `hub.display.off()` | Turn off all 25 pixels. |
| `hub.display.pixel(row, col, brightness)` | Set one pixel. `row` 0–4 (top to bottom), `col` 0–4 (left to right), `brightness` 0–100. |
| `hub.display.image(matrix)` | Show a complete image. `matrix` is a `Matrix` of 5×5 brightness values (each 0–100). |
| `hub.display.animate(images, interval)` | Cycle through a list of images, each shown for `interval` ms. Runs in background. |
| `hub.display.number(n)` | Display a number (-99 to 99). Larger numbers scroll. |
| `hub.display.char(c)` | Display a single character. |
| `hub.display.text(text, on, off)` | Scroll text across the display. `on` = ms per character, `off` = ms gap between characters. |
| `hub.display.orientation(side)` | Rotate the display. `Side.TOP` (default), `Side.LEFT`, `Side.RIGHT`, `Side.BOTTOM`. |

---

## Status Light (RGB LED)

A single RGB LED surrounds the center button. It can display any color from
the `Color` palette or custom HSV values with brightness control.

### Pybricks Light Commands

| Command | Description |
|---------|-------------|
| `hub.light.on(color)` | Turn on at the given color. Supports `Color.RED`, custom `Color(h=30, s=100, v=50)`, and brightness scaling `Color.RED * 0.3`. |
| `hub.light.off()` | Turn off. |
| `hub.light.blink(color, durations)` | Blink with a list of on/off durations in ms (e.g., `[500, 500]`). Runs in background. |
| `hub.light.animate(colors, interval)` | Cycle through a list of colors, each shown for `interval` ms. Runs in background. |

---

## Buttons

Three physical buttons on the top face of the hub:

| Button | Position | Pybricks Constant |
|--------|----------|-------------------|
| Center | Middle (surrounded by RGB LED) | `Button.CENTER` |
| Left | Left of center | `Button.LEFT` |
| Right | Right of center | `Button.RIGHT` |

### Pybricks Button Commands

| Command | Description |
|---------|-------------|
| `hub.buttons.pressed()` | Returns a `set` of currently pressed buttons. Empty set if none pressed. |
| `hub.system.set_stop_button(button)` | Change which button stops the program. `None` to disable, `Button.CENTER` (default), or a list like `[Button.LEFT, Button.RIGHT]`. |

```python
pressed = hub.buttons.pressed()
if Button.LEFT in pressed and Button.RIGHT in pressed:
    # both buttons held simultaneously
```

---

## 6-Axis IMU (Accelerometer + Gyroscope)

### IMU Hardware

| Parameter | Value |
|-----------|-------|
| IC | ST LSM6DS3TR |
| Axes | 3-axis accelerometer + 3-axis gyroscope (6 DOF) |
| Interface | SPI to STM32 |
| Accelerometer range | ±2g / ±4g / ±8g / ±16g (firmware selects appropriate range) |
| Gyroscope range | ±250 / ±500 / ±1000 / ±2000 deg/s |
| Sample rate | Up to 6.66 kHz (firmware samples at lower rate for control loop) |

The IMU is mounted inside the hub. Its orientation relative to the robot is
configured by the `top_side` and `front_side` parameters in the hub constructor.

### Pybricks IMU Commands

| Command | Description |
|---------|-------------|
| `hub.imu.up()` | Which side faces upward — returns `Side.TOP`, `Side.BOTTOM`, `Side.LEFT`, `Side.RIGHT`, `Side.FRONT`, or `Side.BACK`. |
| `hub.imu.tilt()` | Returns `(pitch, roll)` in degrees. |
| `hub.imu.acceleration()` | Returns `(x, y, z)` in mm/s². At rest, one axis reads ~9810 (gravity). |
| `hub.imu.angular_velocity()` | Returns `(x, y, z)` in deg/s. |
| `hub.imu.heading()` | Accumulated heading angle (deg) since last reset. Positive = clockwise from top. |
| `hub.imu.reset_heading(angle)` | Reset heading reference to given angle (default 0). |
| `hub.imu.rotation(axis)` | Rotation around a specific `Axis.X`, `Axis.Y`, or `Axis.Z` in degrees. |
| `hub.imu.ready()` | `True` when calibrated and stable. `False` if just booted or not calibrated for >10 minutes. |
| `hub.imu.stationary()` | `True` if the hub has been still for at least one second. |
| `hub.imu.settings(...)` | Get/set: `angular_velocity_threshold`, `acceleration_threshold`, `heading_correction`, `angular_velocity_bias`, `angular_velocity_scale`, `acceleration_correction`. Settings are saved to flash. |

---

## Speaker

### Hardware

A piezoelectric buzzer driven by a PWM timer output. It produces square-wave tones —
it is not a full audio speaker and cannot play WAV files or complex waveforms. Sound
quality is comparable to a simple alarm buzzer.

### Pybricks Speaker Commands

| Command | Description |
|---------|-------------|
| `hub.speaker.volume(vol)` | Get or set volume (0–100). No argument returns current value. |
| `hub.speaker.beep(frequency, duration)` | Play a tone at `frequency` Hz for `duration` ms. |
| `hub.speaker.play_notes(notes, tempo)` | Play a list of musical notes at the given tempo (BPM). Notes: `"C4/4"` (quarter note), `"D4/8"` (eighth), `"R/4"` (rest). |

---

## Rechargeable Battery

### Hardware

| Parameter | Value |
|-----------|-------|
| Type | Lithium-ion rechargeable |
| Capacity | 2000 mAh |
| Nominal voltage | 7.4V (2S configuration) |
| Charging | Via Micro-USB port, managed by MPS MP2639A IC |
| Charging time | Approximately 3–4 hours from empty |
| Runtime | 2–4 hours depending on motor load and BLE activity |

The battery is not user-removable under normal use (it is inside the sealed hub
casing). The hub cannot run without the battery — USB power alone is not
sufficient to operate the hub; it charges the battery while connected.

### Pybricks Battery Commands

| Command | Description |
|---------|-------------|
| `hub.battery.voltage()` | Battery voltage in mV. Full charge ≈ 8300 mV, low ≈ 6500 mV. |
| `hub.battery.current()` | Battery current draw in mA. Useful for monitoring motor load. |

---

## Bluetooth Low Energy (BLE)

### BLE Hardware

| Parameter | Value |
|-----------|-------|
| IC | TI CC2564C |
| Protocols | Bluetooth Classic (BR/EDR) + Bluetooth Low Energy 4.2 |
| Max connections | 4 BLE + 1 Bluetooth Classic simultaneously |
| Range | ~10 m typical (unobstructed), varies with environment |
| Antenna | Internal PCB antenna |

Under Pybricks, BLE serves two distinct purposes:

1. **IDE connection** — the Pybricks Code web IDE connects to the hub via BLE to
   download programs, start/stop execution, and stream `print()` output.
2. **Hub-to-hub communication** — the broadcast/observe API for peer-to-peer data
   exchange between Pybricks hubs (no pairing required).

### Pybricks BLE Broadcast/Observe Commands

Configured in the hub constructor:

```python
from pybricks.hubs import PrimeHub
hub = PrimeHub(
    broadcast_channel=1,          # 0–255, or None to disable
    observe_channels=[2, 3]       # list of channels to listen on
)
```

| Command | Description |
|---------|-------------|
| `hub.ble.broadcast(data)` | Broadcast `data` on the configured channel. Accepts `int`, `float`, `str`, `bytes`, `bool`, or `tuple`/`list` of these. Pass `None` to stop. Updates ~every 100 ms. |
| `hub.ble.observe(channel)` | Read the most recent data from another hub on the given channel. Returns `None` if no recent data. Channel must be in `observe_channels`. |
| `hub.ble.signal_strength(channel)` | RSSI of last observed signal in dBm. Nearby ≈ -40, far ≈ -70. Returns -128 if no data. |
| `hub.ble.version()` | Bluetooth firmware version string. |

**Payload size constraints (total ~26 bytes max):**

| Type | Bytes |
|------|-------|
| `True` / `False` | 1 |
| `float` | 5 |
| `int` | 2–5 (depending on magnitude) |
| `str` / `bytes` | length + 1 |

**Important:** When the hub is connected to the Pybricks Code IDE via BLE, the
BLE radio is shared. Broadcast/observe works best when the hub is running
autonomously (not connected to a computer). During IDE connection, observe
reliability may be reduced.

Only one task can call `broadcast()` at a time when multitasking.

### BLE Connection to Pybricks Code IDE

The Pybricks Code web IDE (code.pybricks.com) uses Web Bluetooth to connect
to the hub. The connection protocol uses a custom Pybricks BLE service for:

- Program download (compiled MicroPython bytecode)
- Start/stop control
- `print()` output streaming (stdout)
- `input()` streaming (stdin)
- Hub status monitoring

The hub's BLE name can be read or changed with `hub.system.name()`. The hub
is discoverable (and connectable) only while its status LED is blinking — once
a program is running, it is no longer discoverable by the IDE.

---

## USB Serial Port

### USB Hardware

| Parameter | Value |
|-----------|-------|
| Connector | Micro-USB Type B |
| Protocol | USB 2.0 Full Speed (12 Mbit/s) |
| Device class | CDC ACM (virtual serial port) |
| Baud rate | Not applicable (USB CDC is packetized, not baud-rate limited) |

When connected via USB, the hub appears as a virtual COM port on the host
computer. On Windows this is typically `COMx`, on Linux `/dev/ttyACMx`, on
macOS `/dev/cu.usbmodemxxxx`.

### Pybricks USB/Serial Commands

Pybricks does not expose a direct `serial.write()` / `serial.read()` API for the
USB port. Instead, USB communication happens through these mechanisms:

**Program output — `print()` and `input()`:**

```python
# Output goes to the Pybricks Code terminal (via USB or BLE)
print("Distance:", db.distance())

# Input comes from the Pybricks Code terminal
name = input("Enter your name: ")
```

**Low-level byte reading — `read_input_byte()`:**

```python
from pybricks.tools import read_input_byte

# Non-blocking read of one byte from stdin (USB or BLE terminal)
byte_val = read_input_byte()              # oldest byte as int (0–255), or None
char_val = read_input_byte(chr=True)      # as one-character string, or None
latest = read_input_byte(last=True)       # newest byte, discard older buffered
```

**Low-level stream access — `usys.stdin` / `usys.stdout`:**

```python
import usys

# These are stream objects (uio.FileIO) connected to the USB/BLE terminal
usys.stdin   # input stream
usys.stdout  # output stream
usys.stderr  # alias for stdout
```

**The Pybricks Code IDE** handles program upload and execution control over the
same USB (or BLE) link. You do not need to manage the serial connection manually —
the IDE handles framing and multiplexing between control messages and stdio.

### USB vs BLE for Development

| Aspect | USB | BLE |
|--------|-----|-----|
| Connection | Physical cable | Wireless, ~10 m range |
| Reliability | Very reliable, no dropouts | Can drop in noisy RF environments |
| Bandwidth | Higher (USB 2.0 Full Speed) | Lower (BLE 4.2, ~20 kbit/s effective) |
| Charging | Charges battery while connected | No charging |
| Latency | Lower | Higher (~30–60 ms connection interval) |
| Mobility | Tethered | Untethered — robot can drive freely |
| `print()` throughput | Higher — suitable for fast data logging | Lower — may drop lines if printing too fast |
| Broadcast/observe | Not affected | Shares radio — may reduce observe reliability |

For development, USB is faster and more reliable. For competition or demo, BLE
allows the robot to run untethered. Many users develop over USB and then
disconnect for autonomous runs.

---

## Persistent Flash Storage (512 Bytes)

### How Storage Works

The SPIKE Prime Hub provides 512 bytes of persistent user data storage. This
data is stored in a reserved region of the STM32F413's internal 1 MB NOR flash.

Key behavior:

- **Data is buffered in RAM during operation.** When you call
  `hub.system.storage(offset, write=data)`, the data is written to a RAM buffer.
- **Data is committed to flash on normal shutdown.** When you turn off the hub
  normally (long-press the center button, or call `hub.system.shutdown()`), the
  firmware writes the RAM buffer to flash.
- **Data is NOT saved if batteries are removed while running.** If the hub
  loses power abruptly (battery disconnect, crash), uncommitted changes are lost.
- **Data survives power cycles.** Once written to flash, the data persists even
  if you remove the battery for an extended period.
- **Data is cleared on firmware update.** Updating the Pybricks firmware erases
  the user data region.
- **Data is cleared by `hub.system.reset_storage()`.** This explicitly erases
  all user data and resets IMU calibration settings.

### Pybricks Storage Commands

```python
from pybricks.hubs import PrimeHub
hub = PrimeHub()
```

**Writing data:**

```python
# Write 4 bytes starting at offset 0
hub.system.storage(0, write=b'\x01\x02\x03\x04')

# Write a packed structure
import ustruct
data = ustruct.pack('hh', 357, 112)  # two signed 16-bit integers
hub.system.storage(0, write=data)
```

**Reading data:**

```python
# Read 4 bytes starting at offset 0
raw = hub.system.storage(0, read=4)
print(raw)  # b'\x01\x02\x03\x04'

# Unpack the structure
import ustruct
raw = hub.system.storage(0, read=4)
heading_correction, axle_track = ustruct.unpack('hh', raw)
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `offset` | Byte offset into the 512-byte region (0–511). |
| `read=N` | Read `N` bytes starting at offset. Returns `bytes`. |
| `write=data` | Write `data` (bytes) starting at offset. Returns `None`. |

Raises `ValueError` if you attempt to read or write outside the 0–511 range.

**Practical usage patterns:**

```python
# Store calibration values for your robot
import ustruct

def save_calibration(wheel_diameter, axle_track, heading_correction):
    data = ustruct.pack('ffh', wheel_diameter, axle_track, heading_correction)
    hub.system.storage(0, write=data)

def load_calibration():
    raw = hub.system.storage(0, read=10)  # 4+4+2 = 10 bytes
    return ustruct.unpack('ffh', raw)

# Store a run counter
def increment_run_counter():
    raw = hub.system.storage(100, read=4)
    count = ustruct.unpack('I', raw)[0]
    count += 1
    hub.system.storage(100, write=ustruct.pack('I', count))
    return count
```

### Flash Wear and Write Endurance

**This is a critical concern for embedded systems.**

The storage uses the STM32F413's internal NOR flash memory. The key endurance
characteristics are:

| Parameter | Value |
|-----------|-------|
| Technology | NOR flash (internal to STM32F413) |
| Endurance | 10,000 erase cycles minimum (per sector), per STM32F4 datasheet |
| Erase granularity | Sector-based — the smallest erasable unit is a full sector (16 KB for the first four sectors, 64 KB or 128 KB for others) |
| Write granularity | Byte/word level — individual bytes can be written without erasing (bits can only be cleared 0→1 requires erase) |
| Data retention | 20 years at 55°C (per datasheet) after endurance limit |

**How Pybricks manages wear:**

The firmware does NOT write to flash on every `storage()` call. Instead:

1. `storage(offset, write=data)` writes to a RAM buffer only.
2. The RAM buffer is committed to flash **once** on shutdown.
3. Each shutdown performs one erase-then-write cycle on the storage sector.

This means the **number of flash erase cycles equals the number of hub
power-off events**, not the number of `storage()` calls in your program. You
can call `storage()` thousands of times in a single run without additional
wear — only the final shutdown commits to flash.

**Wear calculation:**

| Usage Pattern | Power Cycles | Years to 10,000 Cycles |
|---------------|-------------|------------------------|
| Classroom use: 5 on/off per day, 200 days/year | 1,000/year | 10 years |
| Competition: 50 on/off per day, 50 days/year | 2,500/year | 4 years |
| Development: 100 on/off per day, 365 days/year | 36,500/year | ~0.27 years (100 days) |
| Stress test: continuous power cycling | — | Avoid this |

**Practical advice:**

- **Normal use is fine.** At typical classroom or competition usage rates, the
  flash will long outlast the hub's mechanical life.
- **Do not write in a tight loop then immediately shut down repeatedly.** If
  your workflow involves rapid program-run-shutdown-program-run-shutdown cycles
  during development, the flash wear accumulates. But even 100 cycles per day
  gives you 100 days before reaching the 10,000 cycle minimum — and the
  actual endurance is typically 2–5× the minimum spec.
- **The erase is what wears the flash, not the write.** Each shutdown that
  commits storage performs one erase cycle. Multiple writes to the RAM buffer
  between shutdowns do not cause additional wear.
- **If you only read, there is zero wear.** Read operations do not degrade flash.
- **IMU calibration settings also share this flash region.** The `imu.settings()`
  values (heading_correction, angular_velocity_bias, etc.) are stored in the same
  flash sector. They are committed on shutdown alongside user storage data.
- **Abrupt power loss does not cause flash corruption** — it simply means the
  latest changes are lost (they were only in RAM). The previous valid flash
  contents remain intact.

### What Else Is Stored in Flash

The 1 MB internal flash is partitioned roughly as follows:

| Region | Approximate Size | Contents |
|--------|-----------------|----------|
| Pybricks firmware | ~400–600 KB | C runtime, motor controllers, BLE stack, MicroPython VM |
| User program | Variable | Compiled MicroPython bytecode (your `.py` compiled) |
| User data | 512 bytes | `hub.system.storage()` data |
| IMU calibration | Shared with user data sector | `hub.imu.settings()` values |
| System settings | Small | Hub name, stop button config |

The 32 MB external SPI flash stores the LEGO stock firmware filesystem. Pybricks
uses less than 1 MB of this external flash to avoid corrupting the LEGO
filesystem, preserving the ability to restore stock firmware.

---

## System Control

### Pybricks System Commands

| Command | Description |
|---------|-------------|
| `hub.system.name()` | Get the hub's Bluetooth name (as a string). |
| `hub.system.name("MyRobot")` | Set the hub's Bluetooth name. This is the name that appears in the Pybricks Code IDE during connection. |
| `hub.system.storage(offset, read=N)` | Read `N` bytes from persistent storage at `offset`. See [Persistent Flash Storage](#persistent-flash-storage-512-bytes). |
| `hub.system.storage(offset, write=data)` | Write `data` to persistent storage at `offset`. See [Persistent Flash Storage](#persistent-flash-storage-512-bytes). |
| `hub.system.set_stop_button(button)` | Set which button(s) terminate the program. `None` = disable stop button. `Button.CENTER` = default. `[Button.LEFT, Button.RIGHT]` = require both. |
| `hub.system.shutdown()` | Power off the hub. This triggers the flash commit of user storage data. |
| `hub.system.reset_storage()` | Erase all user data, IMU calibration settings, and user programs. Resets to factory defaults. |
| `hub.system.reset_reason()` | Returns why the hub last rebooted (e.g., user shutdown, watchdog reset, power loss). Useful for diagnostics. |

---

## Hub Constructor — Putting It All Together

```python
from pybricks.hubs import PrimeHub
from pybricks.parameters import Side

hub = PrimeHub(
    top_side=Side.TOP,            # Which hub face points up in your build
    front_side=Side.FRONT,        # Which hub face points forward (USB port side)
    broadcast_channel=1,          # BLE broadcast channel (0–255), None to disable
    observe_channels=[2, 3]       # BLE channels to listen on
)
```

The `top_side` and `front_side` parameters are critical for correct IMU readings.
If the hub is mounted sideways or inverted in your robot, set these to match the
actual physical orientation. This affects `imu.up()`, `imu.tilt()`,
`imu.heading()`, and gyro-assisted `DriveBase.use_gyro(True)`.

---

## References

- [Pybricks PrimeHub Documentation](https://docs.pybricks.com/en/latest/hubs/primehub.html)
- [Pybricks GitHub — pybricks-micropython](https://github.com/pybricks/pybricks-micropython)
- [STM32F413 Datasheet](https://www.st.com/en/microcontrollers-microprocessors/stm32f413-423.html)
- [SPIKE Prime Technical Info (gpdaniels)](https://github.com/gpdaniels/spike-prime)
- [SPIKE Prime Hub Pinout (GianCann)](https://github.com/GianCann/SpikePrimeHub)

---

*Based on Pybricks v3.6.1 documentation, STM32F413 datasheet, and community
hardware analysis. LEGO® is a trademark of the LEGO Group.*

