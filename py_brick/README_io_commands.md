# Pybricks v3 — SPIKE Prime & SPIKE Essential I/O Command Reference

Complete API reference for all I/O commands available under Pybricks MicroPython v3.x
on LEGO® SPIKE Prime (PrimeHub) and SPIKE Essential (EssentialHub).

All commands below are from the Pybricks v3.6 documentation.
Import paths and hub availability are noted per section.

---

## Table of Contents

- [Hub — PrimeHub / EssentialHub](#hub--primehub--essentialhub)
  - [Hub Light](#hub-light)
  - [Hub Display (PrimeHub only)](#hub-display-primehub-only--5×5-led-matrix)
  - [Hub Buttons](#hub-buttons)
  - [Hub IMU](#hub-imu-accelerometer--gyroscope)
    - [Gyro Drift Calibration](#gyro-drift-calibration)
  - [Hub Speaker](#hub-speaker)
  - [Hub Battery](#hub-battery)
  - [Hub BLE](#hub-ble-bluetooth-broadcastobserve)
  - [Hub System](#hub-system)
- [Port Devices — pupdevices](#port-devices--from-pybrickspupdevices-import-)
  - [Motor](#motorport-positive_direction-gears-reset_angle-profile)
  - [DCMotor](#dcmotorport-positive_direction)
  - [ColorSensor](#colorsensorport)
  - [UltrasonicSensor](#ultrasonicsensorport)
  - [ForceSensor](#forcesensorport)
  - [ColorLightMatrix](#colorlightmatrixport)
  - [Light](#lightport)
  - [Remote](#remote)
- [Low-Level Generic I/O — iodevices](#low-level-generic-io--from-pybricksiodevices-import-)
  - [PUPDevice](#pupdeviceport)
  - [LWP3Device](#lwp3devicehub_kind-name-timeout)
- [Robotics — DriveBase & Car](#robotics--from-pybricksrobotics-import-)
  - [DriveBase](#drivebaseleft_motor-right_motor-wheel_diameter-axle_track)
    - [Wheel Slip Detection and Recovery](#wheel-slip-detection-and-recovery)
  - [Car](#carsteer_motor-drive_motors-torque_limit)
- [Utility Tools](#utility--from-pybrickstools-import-)
  - [wait](#waittime)
  - [StopWatch](#stopwatch)
  - [multitask](#multitaskcoroutines-racefalse)
  - [run_task](#run_taskcoroutine)
  - [hub_menu](#hub_menusymbols)
  - [read_input_byte](#read_input_bytelastfalse-chrfalse)

---

## Hub — PrimeHub / EssentialHub

```python
from pybricks.hubs import PrimeHub    # SPIKE Prime
from pybricks.hubs import EssentialHub # SPIKE Essential

hub = PrimeHub(
    top_side=Side.TOP,            # Which hub face points up in your build
    front_side=Side.FRONT,        # Which hub face points forward
    broadcast_channel=0,          # BLE broadcast channel (0–255), None to disable
    observe_channels=[]           # List of BLE channels to listen on
)
```

The constructor's `top_side` and `front_side` parameters tell the IMU how the hub
is oriented in your robot. This is critical for `use_gyro(True)` on a DriveBase
and for correct `imu.up()` / `imu.tilt()` readings.

---

### Hub Light

The single RGB status LED on the hub.

| Method | Description |
|--------|-------------|
| `hub.light.on(color)` | Turn on the light at the given `Color`. Example: `Color.RED`, `Color.GREEN`, `Color(h=30, s=100, v=50)` for custom hue. Brightness can be scaled: `Color.RED * 0.3` for 30%. |
| `hub.light.off()` | Turn off the light. |
| `hub.light.blink(color, durations)` | Blink at the given color. `durations` is a list of on/off times in ms that repeats indefinitely. Example: `[500, 500]` for even blink, `[500, 500, 50, 900]` for a fast-then-slow pattern. Runs in background. |
| `hub.light.animate(colors, interval)` | Cycle through a list of `Color` values, showing each for `interval` ms. Runs in background. Useful for rainbows: `[Color(h=i*8) for i in range(45)]`. |

---

### Hub Display (PrimeHub only) — 5×5 LED Matrix

The EssentialHub does not have a display matrix.

| Method | Description |
|--------|-------------|
| `hub.display.orientation(side)` | Set which direction is "up" for the display. `Side.TOP` (default), `Side.LEFT`, `Side.RIGHT`, `Side.BOTTOM`. |
| `hub.display.off()` | Turn off all 25 pixels. |
| `hub.display.pixel(row, col, brightness)` | Set one pixel. `row` 0–4 (top to bottom), `col` 0–4 (left to right), `brightness` 0–100. |
| `hub.display.image(image)` | Show a complete image. `image` is a `Matrix` of brightness values (5 rows × 5 cols, each 0–100). |
| `hub.display.animate(images, interval)` | Animate a list of `Matrix` images, each shown for `interval` ms. Runs in background. |
| `hub.display.number(n)` | Display a number from -99 to 99. For numbers > 99, it scrolls. |
| `hub.display.char(c)` | Display a single character. |
| `hub.display.text(text, on, off)` | Scroll text across the display. `on` = ms each character is shown, `off` = ms pause between characters. |

---

### Hub Buttons

| Method | Description |
|--------|-------------|
| `hub.buttons.pressed()` | Returns a `set` of currently pressed buttons. Possible values: `Button.LEFT`, `Button.RIGHT`, `Button.CENTER` (PrimeHub) or `Button.CENTER` only (EssentialHub). |

Usage pattern:
```python
pressed = hub.buttons.pressed()
if Button.LEFT in pressed:
    # left button is held down
```

The center button is the default "stop" button. You can change this with
`hub.system.set_stop_button(None)` to disable, or pass a button/list to reassign.

---

### Hub IMU (Accelerometer + Gyroscope)

The built-in 6-axis IMU (3-axis accelerometer + 3-axis gyroscope). All axes follow
the hub's coordinate system, adjusted by `top_side`/`front_side` in the constructor.

| Method | Description |
|--------|-------------|
| `hub.imu.up()` | Returns which side of the hub currently faces upward. Returns `Side.TOP`, `Side.BOTTOM`, `Side.LEFT`, `Side.RIGHT`, `Side.FRONT`, or `Side.BACK`. |
| `hub.imu.tilt()` | Returns `(pitch, roll)` in degrees relative to the user-specified neutral orientation. Order of rotation is pitch-then-roll. |
| `hub.imu.acceleration()` | Returns `(x, y, z)` acceleration in mm/s². At rest, one axis reads ~9810 mm/s² (gravity). Changed from m/s² to mm/s² in v3.2. |
| `hub.imu.angular_velocity()` | Returns `(x, y, z)` angular velocity in deg/s. |
| `hub.imu.heading()` | Returns the accumulated heading angle in degrees since last reset. Uses the gyroscope. Positive = clockwise (viewed from top). |
| `hub.imu.reset_heading(angle)` | Resets the heading reference to the given angle (default 0). |
| `hub.imu.rotation(axis)` | Returns the rotation around a specific axis (Axis.X, Axis.Y, or Axis.Z) in degrees. |
| `hub.imu.ready()` | Returns `True` if the IMU is calibrated and producing stable readings. May take a moment after power-on. Keep the hub stationary during calibration. |
| `hub.imu.stationary()` | Returns `True` if the hub is currently not moving. Useful for waiting until the robot settles. |
| `hub.imu.settings(...)` | Get or set heading correction parameters. If no arguments, returns current values. |

#### Gyro Drift Calibration

Pybricks handles gyro drift through a multi-layered approach — automatic runtime
bias estimation, persistent per-hub scale correction, and a manual full-calibration
routine.

**Layer 1: Automatic Runtime Bias Calibration (zero-rate offset)**

This is the primary drift correction and it is fully automatic. The firmware
continuously monitors the accelerometer to detect when the hub is stationary.
When it determines the hub is not moving, it recalibrates the gyro bias in the
background.

The stationarity detection uses two thresholds tunable via `imu.settings()`:

- `angular_velocity_threshold` (deg/s) — variations in angular velocity below
  this value mean the hub is considered stationary enough to calibrate.
  Default after reset: 2 deg/s.
- `acceleration_threshold` (mm/s²) — variations in acceleration below this
  value mean the hub is considered stationary enough to calibrate.

The method `hub.imu.ready()` tells you when initial calibration has completed —
it returns `True` when the robot has been sitting stationary for a few seconds.
It returns `False` if the hub has just been started, or if it has not had a
chance to calibrate for more than 10 minutes.

The calibration runs continuously. It works more aggressively at first (the first
stationary zones soon after boot), and then more gradually over time. It also
works more aggressively the longer the hub is stationary. The firmware buffers
samples and discards recent ones retroactively if motion is detected, to avoid
contaminating the bias estimate with the onset of movement.

**Practical consequence:** You do not need to do anything special at boot. Just
let the hub sit still for a few seconds and `ready()` will tell you when the
bias is locked in. Every time the robot stops moving during operation, the
firmware re-estimates the bias.

**Layer 2: Per-Hub Scale Factor Correction (heading_correction)**

This addresses a different problem than bias drift. Due to variations in the
manufacturing process, each hub consistently reports a different value for a
full rotation. For example, your hub might consistently report 357 degrees for
every 360 degree turn. This is a multiplicative error (gain error), not an
additive one.

You calibrate it once per hub:

1. Make the robot perform a known rotation (e.g., 10 full turns).
2. Read the uncalibrated result with
   `hub.imu.rotation(-Axis.Z, calibrated=False)`.
3. Compute and apply the correction factor:

```python
# If your hub reads 357 for a true 360:
hub.imu.settings(heading_correction=360/357)
```

These IMU settings are saved persistently on the hub. They keep their values
until you change them again. The values are reset to defaults if you update
the hub to a different firmware version or call `hub.system.reset_storage()`.

**Layer 3: Full Factory-Style Calibration (manual, optional)**

For the best accuracy, there is a calibration routine you run on the REPL:

```python
import _imu_calibrate
```

Follow the on-screen instructions. This calibrates both the accelerometer and
gyroscope simultaneously. The procedure involves placing the hub in indicated
starting positions and rolling it 4 times along each axis without lifting it.
It measures bias and scale on all three gyro axes plus accelerometer offsets.
The values remain on the hub until you install a new firmware.

**What Gyro Calibration Cannot Correct**

Since there is no compass (magnetometer), long-term yaw errors cannot be
corrected with an external reference. The accelerometer can anchor pitch and
roll (gravity provides a reference vector), but heading has no such anchor.

| Error Source | Correction |
|-------------|------------|
| Bias drift (zero-rate offset) | Automatic recalibration during stationary periods |
| Scale error (gain error) | One-time `heading_correction` setting per hub |
| Accumulated yaw drift during continuous motion | Uncorrectable without external reference — the longer you drive without stopping, the more it accumulates |

There are also edge cases: for now, the `heading()` method only tracks heading
while the robot is on a flat surface. If you lift the robot from the table and
put it back, the heading value will no longer be correct. A 3D heading mode
(`hub.imu.heading('3D')`) is in development.

---

### Hub Speaker

| Method | Description |
|--------|-------------|
| `hub.speaker.volume(vol)` | Get or set volume (0–100). If no argument, returns current volume. |
| `hub.speaker.beep(frequency, duration)` | Play a tone at `frequency` Hz for `duration` ms. |
| `hub.speaker.play_notes(notes, tempo)` | Play a list of musical notes at the given `tempo` (beats per minute). Notes are strings like `"C4/4"` (C in octave 4, quarter note), `"D4/8"` (eighth note), `"R/4"` (quarter rest). |

---

### Hub Battery

| Method | Description |
|--------|-------------|
| `hub.battery.voltage()` | Returns the battery voltage in millivolts (mV). A full charge is typically ~8300 mV. |
| `hub.battery.current()` | Returns the battery current draw in milliamps (mA). Useful for monitoring load. |

---

### Hub BLE (Bluetooth Broadcast/Observe)

Lightweight peer-to-peer communication between Pybricks hubs using BLE advertising.
No pairing required. One hub broadcasts data; others observe on the same channel.

| Method | Description |
|--------|-------------|
| `hub.ble.broadcast(data)` | Broadcast `data` on the `broadcast_channel` set in the constructor. Data can be `int`, `float`, `str`, `bytes`, `True`, `False`, or a `tuple`/`list` of these. Pass `None` to stop broadcasting. Total payload is limited to ~26 bytes. Broadcasts update every ~100 ms. |
| `hub.ble.observe(channel)` | Read the most recently received data from another hub broadcasting on `channel`. Returns the data in the same format it was sent, or `None` if no recent data is available. The `channel` must be in the `observe_channels` list from the constructor. |
| `hub.ble.signal_strength(channel)` | Returns the RSSI in dBm of the last observed signal on the given channel. Nearby ≈ -40 dBm, far ≈ -70 dBm. Returns -128 if no data available. |
| `hub.ble.version()` | Returns the Bluetooth firmware version string. |

**Data size constraints:**

- `True` / `False`: 1 byte each
- `float`: 5 bytes
- `int`: 2–5 bytes depending on magnitude
- `str` / `bytes`: length in bytes plus overhead
- Total maximum: ~26 bytes

When multitasking, only one task can call `broadcast()` at a time.

---

### Hub System

| Method | Description |
|--------|-------------|
| `hub.system.name()` | Get or set the Bluetooth name of the hub. This is the name that appears in the Pybricks Code IDE. |
| `hub.system.storage(offset, read=N)` | Read `N` bytes from persistent user storage starting at `offset`. This storage survives power cycles. |
| `hub.system.storage(offset, write=data)` | Write `data` (bytes) to persistent user storage at `offset`. Limited total storage (typically 128 bytes). Useful for saving calibration values. |
| `hub.system.set_stop_button(button)` | Set which button(s) terminate the program. Pass `None` to disable the stop button entirely. Pass `Button.CENTER` (default) or a list like `[Button.LEFT, Button.RIGHT]`. |
| `hub.system.shutdown()` | Power off the hub. |
| `hub.system.reset_reason()` | Returns the reason for the last reboot (e.g. user, watchdog, etc.). |

---

## Port Devices — `from pybricks.pupdevices import ...`

These are the typed, high-level device classes for motors, sensors, and lights
that plug into the hub's I/O ports (Port.A through Port.F depending on hub).

---

### `Motor(port, positive_direction, gears, reset_angle, profile)`

Smart motors with built-in rotation sensors (encoders). This covers all SPIKE angular motors:
Small (ID 65), Medium (ID 48), and Large (ID 49).

```python
from pybricks.pupdevices import Motor
from pybricks.parameters import Port, Direction

motor = Motor(
    Port.A,
    positive_direction=Direction.CLOCKWISE,  # Default. Use COUNTERCLOCKWISE for mirrored mounting
    gears=None,       # Gear train list, e.g. [12, 36] for 3:1 reduction. Adjusts all angle/speed values.
    reset_angle=True,  # True = reset to absolute encoder position on init
    profile=None       # Acceleration profile (advanced)
)
```

#### Measuring

| Method | Description |
|--------|-------------|
| `motor.angle()` | Current angle in degrees. Continuous (can exceed 360). Positive = positive_direction. |
| `motor.reset_angle(angle=0)` | Reset the angle reference. If `angle` is given, the current position becomes that value. If `None`, resets to the motor's absolute zero (from the built-in absolute encoder). |
| `motor.speed(window=100)` | Current speed in deg/s. `window` (ms) controls the measurement averaging time. Short window = responsive but noisy. Long window = stable but laggy. |
| `motor.load()` | Estimated load torque in mNm (millinewton-meters) opposing the motor. Positive when the motor is being resisted. Useful for detecting when a mechanism hits a hard stop. |
| `motor.stalled()` | Returns `True` if the motor cannot reach its target speed/position even at maximum actuation. |

#### Stopping

| Method | Description |
|--------|-------------|
| `motor.stop()` | Coast — cut power and let friction slow the motor. The motor spins freely. |
| `motor.brake()` | Passive electric brake — motor terminals are short-circuited. Stronger than `stop()` but doesn't actively hold position. |
| `motor.hold()` | Active hold — PID controller keeps the motor at its current angle. Uses power continuously. |

#### Running (all support `await` for multitasking)

| Method | Description |
|--------|-------------|
| `motor.run(speed)` | Run at constant `speed` (deg/s) indefinitely. Accelerates smoothly. Does not block — returns immediately and the motor keeps running. |
| `motor.dc(duty)` | Run at raw duty cycle (%) — direct voltage control without PID. -100 to +100. Useful for simple power control or custom feedback loops. |
| `motor.run_time(speed, time, then=Stop.HOLD, wait=True)` | Run at `speed` for `time` ms, then stop. `then` controls stop behavior. `wait=False` to make it non-blocking. |
| `motor.run_angle(speed, rotation_angle, then=Stop.HOLD, wait=True)` | Run at `speed` for a relative `rotation_angle` degrees. Always positive speed; sign of angle determines direction. |
| `motor.run_target(speed, target_angle, then=Stop.HOLD, wait=True)` | Run to an absolute `target_angle`. Direction is chosen automatically. `speed` must be positive. |
| `motor.run_until_stalled(speed, then=Stop.COAST, duty_limit=None)` | Run at `speed` until the motor stalls. Returns the stall angle. `duty_limit` (%) caps the torque to protect geared mechanisms. |
| `motor.track_target(target_angle)` | Move to `target_angle` as fast as possible (no smooth acceleration). Designed for tight control loops where you update the target every iteration. |
| `motor.done()` | Returns `True` if the current `run_*` command has finished. |

#### Configuration

| Method | Description |
|--------|-------------|
| `motor.settings(max_voltage)` | Get or set the maximum voltage. No arguments = return current value. |
| `motor.close()` | Close the motor object, releasing the port. Allows re-initialization with different settings (e.g., different `gears`). |
| `motor.control.limits(speed, acceleration, torque)` | Get or set PID limits. `acceleration` can be a tuple `(accel, decel)` for asymmetric profiles. `torque` in mNm. |
| `motor.control.pid(kp, ki, kd, ...)` | Get or set PID controller gains. |
| `motor.control.target_tolerances(speed, position)` | Get or set how close the motor must be to the target before `done()` returns `True`. |
| `motor.control.stall_tolerances(speed, time)` | Get or set when the motor is considered stalled (speed below threshold for a given time). |
| `motor.control.scale` | Number of degrees that the motor turns for one degree at the output. Automatically set by the `gears` parameter. |
| `motor.model(...)` | Get or set internal motor model parameters (debug only). |

---

### `DCMotor(port, positive_direction)`

Simple motors without rotation sensors — train motors, WeDo motors, etc.

```python
from pybricks.pupdevices import DCMotor
motor = DCMotor(Port.A, positive_direction=Direction.CLOCKWISE)
```

| Method | Description |
|--------|-------------|
| `motor.dc(duty)` | Run at the given duty cycle, -100% to +100%. Direct voltage control. |
| `motor.stop()` | Coast — cut power, spin freely. |
| `motor.brake()` | Passive electric braking. |
| `motor.settings(max_voltage)` | Get or set the maximum voltage. |

---

### `ColorSensor(port)`

LEGO® SPIKE Color Sensor (device ID 61). Has 3 built-in white LEDs for illumination.

```python
from pybricks.pupdevices import ColorSensor
sensor = ColorSensor(Port.A)
```

| Method | Description |
|--------|-------------|
| `sensor.color(surface=True)` | Detected color, rounded to the nearest detectable color. Returns a `Color` value. `surface=True` for objects/surfaces, `surface=False` for screens/external light sources. Default detectable colors: `Color.RED`, `Color.YELLOW`, `Color.GREEN`, `Color.BLUE`, `Color.WHITE`, `Color.NONE`. |
| `sensor.reflection()` | Surface reflection 0–100%. How much of the sensor's emitted light bounces back. |
| `sensor.ambient()` | Ambient light intensity 0–100%. Measures external light (sensor LEDs off during measurement). |
| `sensor.hsv(surface=True)` | Raw HSV color measurement. Returns a `Color` object with `.h` (hue 0–359), `.s` (saturation 0–100), `.v` (brightness/value 0–100). No rounding to detectable colors. |
| `sensor.detectable_colors(colors)` | Configure which colors `color()` can return. Pass a list/tuple of `Color` objects. The sensor maps raw HSV measurements to the nearest color in this list. |
| `sensor.lights.on(brightness)` | Control the 3 built-in LEDs. Single value = all same brightness. Tuple of 3 = individual control. Each 0–100%. |
| `sensor.lights.off()` | Turn off all sensor LEDs. |

**Custom color calibration pattern:**
```python
# Measure your actual colors with hsv() first, then override:
Color.GREEN = Color(h=132, s=94, v=26)
Color.MAGENTA = Color(h=348, s=96, v=40)
my_colors = (Color.GREEN, Color.MAGENTA, Color.RED, Color.NONE)
sensor.detectable_colors(my_colors)
# Now sensor.color() only returns these colors
```

---

### `UltrasonicSensor(port)`

LEGO® SPIKE Ultrasonic Sensor (device ID 62). Measures distance via ultrasonic sound.
Has 4 built-in LEDs around the "eyes."

```python
from pybricks.pupdevices import UltrasonicSensor
sensor = UltrasonicSensor(Port.A)
```

| Method | Description |
|--------|-------------|
| `sensor.distance()` | Distance to nearest object in mm. Range ~40–2000 mm. Returns 2000 if nothing is detected. |
| `sensor.presence()` | Returns `True` if another ultrasonic sensor is detected nearby (listens for ultrasonic pulses from other sensors). |
| `sensor.lights.on(brightness)` | Control the 4 built-in LEDs. Single value = all same. Tuple of 4 = individual. Each 0–100%. |
| `sensor.lights.off()` | Turn off all sensor LEDs. |

---

### `ForceSensor(port)`

LEGO® SPIKE Force Sensor (device ID 63). A button with force and displacement measurement.

```python
from pybricks.pupdevices import ForceSensor
sensor = ForceSensor(Port.A)
```

| Method | Description |
|--------|-------------|
| `sensor.force()` | Measured force in Newtons (N). Range 0 to ~10 N. |
| `sensor.distance()` | Button travel distance in mm. Range 0 to ~8 mm. |
| `sensor.pressed()` | Returns `True` if the button is firmly pressed down. |
| `sensor.touched()` | Returns `True` if the button is even slightly deflected. More sensitive than `pressed()` — detects initial contact before measurable force. |

**Practical difference between `pressed()` and `touched()`:**
`touched()` triggers on the slightest physical contact, even when `force()` still reads ~0.
`pressed()` requires meaningful force. Use `touched()` for bump detection, `pressed()` for
deliberate user input.

---

### `ColorLightMatrix(port)`

LEGO® SPIKE 3×3 Color Light Matrix (device ID 64). A 3×3 grid of individually
addressable color LEDs.

```python
from pybricks.pupdevices import ColorLightMatrix
matrix = ColorLightMatrix(Port.A)
```

| Method | Description |
|--------|-------------|
| `matrix.on(colors)` | Set the 3×3 grid. `colors` is a list/tuple of 9 `Color` values (row-major order: top-left to bottom-right). Each pixel can be any `Color` including brightness scaling. |
| `matrix.off()` | Turn off all 9 pixels. |

---

### `Light(port)`

Simple single Powered Up light (device ID 8).

```python
from pybricks.pupdevices import Light
light = Light(Port.A)
```

| Method | Description |
|--------|-------------|
| `light.on(brightness)` | Turn on at the given brightness (0–100%). |
| `light.off()` | Turn off. |

---

### `Remote()`

LEGO® Powered Up Remote (Handset). Connects via Bluetooth.
The constructor blocks until a remote is found and connected.

```python
from pybricks.pupdevices import Remote
remote = Remote(name=None, timeout=10000)
# name: connect to a specific remote by Bluetooth name
# timeout: ms to wait before raising an error
```

| Method | Description |
|--------|-------------|
| `remote.name(name=None)` | Get or set the Bluetooth name. If no argument, returns the current name. |
| `remote.light.on(color)` | Set the remote's status LED color. |
| `remote.light.off()` | Turn off the remote's LED. |
| `remote.buttons.pressed()` | Returns a `set` of currently pressed buttons: `Button.LEFT_PLUS`, `Button.LEFT_MINUS`, `Button.RIGHT_PLUS`, `Button.RIGHT_MINUS`, `Button.CENTER`. |

---

## Low-Level Generic I/O — `from pybricks.iodevices import ...`

These classes provide raw access to any Powered Up device, bypassing the typed
sensor/motor classes. Useful for third-party sensors, undocumented modes,
or devices Pybricks doesn't have a specific class for.

---

### `PUPDevice(port)`

Raw Powered Up Protocol device access. Works with any sensor or motor connected
to a hub port.

```python
from pybricks.iodevices import PUPDevice
device = PUPDevice(Port.A)
```

| Method | Description |
|--------|-------------|
| `device.read(mode)` | Read values at the given mode number. Returns a tuple of integers. Each device has multiple modes exposing different measurements (e.g., the distance sensor has modes for distance, presence, LED control, etc.). |
| `device.write(mode, data)` | Write a list/tuple of values to the given mode. Only certain sensors/modes support writes (e.g., setting LED brightness on the ultrasonic sensor via mode 5). **Caution:** Switching between read and write on different modes in quick succession can trigger `EPERM` errors. |
| `device.info()` | Returns a dict with device metadata. Key `"id"` gives the device type ID (e.g., 61 for ColorSensor, 62 for UltrasonicSensor). |

**Known device IDs:**

| ID | Device | Pybricks Class |
|----|--------|----------------|
| 1 | WeDo 2.0 Medium Motor | `DCMotor` |
| 2 | Powered Up Train Motor | `DCMotor` |
| 8 | Powered Up Light | `Light` |
| 34 | WeDo 2.0 Tilt Sensor | `TiltSensor` |
| 35 | WeDo 2.0 Infrared Sensor | `InfraredSensor` |
| 37 | BOOST Color Distance Sensor | `ColorDistanceSensor` |
| 38 | BOOST Interactive Motor | `Motor` |
| 46 | Technic Large Motor | `Motor` |
| 47 | Technic Extra Large Motor | `Motor` |
| 48 | SPIKE Medium Angular Motor | `Motor` |
| 49 | SPIKE Large Angular Motor | `Motor` |
| 61 | SPIKE Color Sensor | `ColorSensor` |
| 62 | SPIKE Ultrasonic Sensor | `UltrasonicSensor` |
| 63 | SPIKE Force Sensor | `ForceSensor` |
| 64 | SPIKE 3×3 Color Light Matrix | `ColorLightMatrix` |
| 65 | SPIKE Small Angular Motor | `Motor` |
| 75 | Technic Medium Angular Motor | `Motor` |
| 76 | Technic Large Angular Motor | `Motor` |

---

### `LWP3Device(hub_kind, name, timeout)`

Bluetooth connection to a hub running official LEGO firmware (LEGO Wireless Protocol v3).
Allows a Pybricks hub to send/receive raw binary messages to/from a stock LEGO hub.

```python
from pybricks.iodevices import LWP3Device
device = LWP3Device(hub_kind, name=None, timeout=10000)
# hub_kind: type of hub to connect to
# name: Bluetooth name filter
# timeout: connection timeout in ms
```

| Method | Description |
|--------|-------------|
| `device.name(name=None)` | Get or set the Bluetooth name of the remote device. No argument = return current name. |
| `device.write(data)` | Send a raw binary message (bytes) to the remote hub. The message must conform to the LWP3 protocol. |
| `device.read()` | Receive the next message from the remote hub. Blocks until a message arrives. Returns raw bytes. |

---

## Robotics — `from pybricks.robotics import ...`

Higher-level classes for controlling complete robot drive systems.

---

### `DriveBase(left_motor, right_motor, wheel_diameter, axle_track)`

Differential-drive robot controller. Converts desired distances (mm) and angles (deg)
into coordinated left/right motor commands using kinematics derived from `wheel_diameter`
and `axle_track`.

```python
from pybricks.pupdevices import Motor
from pybricks.parameters import Port, Direction
from pybricks.robotics import DriveBase

left = Motor(Port.A, Direction.COUNTERCLOCKWISE)
right = Motor(Port.B)
drive_base = DriveBase(left, right, wheel_diameter=56, axle_track=112)
```

**Constructor parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `left_motor` | Motor | Motor object for the left wheel |
| `right_motor` | Motor | Motor object for the right wheel |
| `wheel_diameter` | Number (mm) | Diameter of the drive wheels. SPIKE large tires are 56 mm. |
| `axle_track` | Number (mm) | Distance between the two wheel-ground contact points. |

**Sign conventions:**

| Quantity | Positive | Negative |
|----------|----------|----------|
| Distance / speed | Forward | Backward |
| Angle / turn rate | Right (clockwise from top) | Left (counterclockwise) |
| Radius | Arc curves right | Arc curves left |

**Motor lock:** While the DriveBase is active (driving or holding position after a
command), the individual motor objects cannot be used directly. Call `stop()` to
release them.

#### Command Methods

All command methods support `await` for multitasking.

**`straight(distance, then=Stop.HOLD, wait=True)`**

Drive in a straight line for the given distance, then stop.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `distance` | Number (mm) | required | Positive = forward, negative = backward |
| `then` | Stop | `Stop.HOLD` | What to do after: `Stop.HOLD` (actively maintain position), `Stop.BRAKE` (passive braking), `Stop.COAST` (cut power) |
| `wait` | bool | `True` | `True` = block until done. `False` = return immediately; check `done()` for completion. |

---

**`turn(angle, then=Stop.HOLD, wait=True)`**

Pivot in place (zero-radius turn) by the given angle.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `angle` | Number (deg) | required | Positive = clockwise (right), negative = counterclockwise (left) |
| `then` | Stop | `Stop.HOLD` | Stop behavior after turn completes |
| `wait` | bool | `True` | Blocking behavior |

Both wheels turn in opposite directions. The robot rotates around the midpoint
between the wheels. Speed and acceleration are controlled by `settings()`.

---

**`curve(radius, angle, then=Stop.HOLD, wait=True)`**

Drive along an arc (partial circle) at the given radius.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `radius` | Number (mm) | required | Radius of curvature. Positive = arc right, negative = arc left. Cannot be zero (use `turn()` for that). |
| `angle` | Number (deg) | required | How far along the arc. Positive = forward, negative = reverse. |
| `then` | Stop | `Stop.HOLD` | Stop behavior after arc completes |
| `wait` | bool | `True` | Blocking behavior |

The inner wheel moves slower and the outer wheel faster, computed from the radius
and axle track.

**Note (v3.6+):** `curve()` is being superseded by `arc()`, which has clearer
direction semantics. Existing `curve()` code continues to work.

**`arc(radius, angle=None, distance=None, then=Stop.HOLD, wait=True)`** (v3.6+)

Same concept as `curve()` but you can specify travel either as `angle` (degrees)
or `distance` (mm along the arc), not both. Raises `ValueError` if you specify
both or neither.

---

**`drive(speed, turn_rate)`**

Begin driving at the given speed and turn rate. The robot continues indefinitely
until you call `stop()`, `brake()`, or issue another `drive()`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `speed` | Number (mm/s) | Forward/backward speed at the robot's center point |
| `turn_rate` | Number (deg/s) | Rotational rate. Positive = clockwise (right), negative = counterclockwise (left) |

This is the method for sensor-driven control loops. The `settings()` speed/acceleration
values do **not** apply here — you provide explicit values.

`drive(0, 0)` decelerates to a controlled stop (smoother than `stop()` which
cuts power immediately).

---

#### Stop Methods

| Method | Description |
|--------|-------------|
| `stop()` | Coast — cut motor power, let friction stop the robot. Also deactivates the DriveBase, releasing individual motor objects for direct use. |
| `brake()` | Passive electric braking. Stronger than `stop()` but does not actively hold position. DriveBase remains active. |

---

#### Measurement Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `distance()` | Number (mm) | Estimated distance driven since last `reset()`. Computed from wheel encoder rotations. Positive = net forward. |
| `angle()` | Number (deg) | Estimated heading angle since last `reset()`. Positive = net clockwise. Uses gyro when `use_gyro(True)`. |
| `state()` | Tuple | `(distance_mm, speed_mm_s, angle_deg, turn_rate_deg_s)`. Real-time snapshot of all odometry. |
| `stalled()` | bool | `True` if the drive base cannot reach its target speed/position at maximum actuation. |
| `done()` | bool | `True` if the current `straight()`/`turn()`/`curve()` command has completed. Used with `wait=False`. |

**`reset(angle=0)`**

Reset distance to 0 and heading angle to the given value (default 0).
Also calls `stop()` to halt ongoing movements. When `use_gyro(True)` is active,
this also resets the gyro heading reference.

---

#### Configuration

**`settings(straight_speed, straight_acceleration, turn_rate, turn_acceleration)`**

Configure speeds and accelerations for `straight()`, `turn()`, and `curve()`.
If called with no arguments, returns the current values as a 4-tuple.

| Parameter | Type | Description |
|-----------|------|-------------|
| `straight_speed` | Number (mm/s) | Straight-line driving speed |
| `straight_acceleration` | Number (mm/s²) | Linear acceleration. Can be a tuple `(accel, decel)` for asymmetric profiles. |
| `turn_rate` | Number (deg/s) | Turning speed |
| `turn_acceleration` | Number (deg/s²) | Angular acceleration. Also supports `(accel, decel)` tuple. |

Initial values are automatically computed from `wheel_diameter` and `axle_track`,
targeting about 40% of maximum speed. These settings do **not** affect `drive()`.

Can only be changed while the robot is stopped.

---

**`use_gyro(bool)`**

| Value | Behavior |
|-------|----------|
| `True` | Use the hub's IMU gyroscope for heading control. Significantly more accurate for turns and straight driving since it is not affected by wheel slip. |
| `False` | Use only motor encoder odometry (default). |

When using gyro, make sure `top_side` and `front_side` are set correctly in the
hub constructor so the system knows which axis corresponds to yaw rotation.

The gyro has small drift that accumulates, so very large turns or many small turns
in the same direction may be slightly off. You may need to empirically tune values
(e.g., `turn(357)` instead of `turn(360)`).

After a command completes, the DriveBase actively maintains the heading. This means
the wheels will spin if you lift the robot off the ground.

---

#### Advanced Control Attributes

| Attribute | Description |
|-----------|-------------|
| `drive_base.distance_control` | PID controller for distance/speed. Has `.limits()`, `.pid()`, `.target_tolerances()`, `.stall_tolerances()`. |
| `drive_base.heading_control` | PID controller for heading/turn rate. Same sub-methods as distance_control. Settings apply per degree of whole-robot rotation, not per-motor degree. |

---

#### Wheel Slip Detection and Recovery

Wheel slip is the fundamental problem with dead-reckoning from wheel encoders.
Here is what you can do with the Pybricks resources on the SPIKE.

##### Detection

Gyro vs. encoder disagreement is the primary slip detector. When `use_gyro(True)`
is active, the DriveBase has both data streams available simultaneously:

| Signal | What It Tells You |
|--------|-------------------|
| `hub.imu.heading()` | What the robot *actually* did rotationally (gyro ground truth) |
| `drive_base.distance()` / `drive_base.angle()` | What the *wheels* report happened (encoder estimate) |
| `motor.load()` | Estimated torque on each motor. A sudden drop to near-zero while commanding significant power is a strong slip indicator — the wheel lost traction. |
| `motor.stalled()` | The inverse case: wheels cannot turn at all (stuck, not slipping). |
| `motor.speed()` vs commanded speed | If actual speed significantly exceeds commanded speed during a turn, the wheel is spinning freely. |
| `hub.imu.acceleration()` | A sudden lateral acceleration spike that does not match the commanded trajectory indicates slip or external disturbance. |

##### Strategy 1: Let the Gyro-Fused DriveBase Handle It (simplest)

When you call `drive_base.use_gyro(True)`, the heading PID controller uses the
gyro as truth, not the encoders. If one wheel slips during a `straight()` command:

1. The gyro detects an unwanted yaw.
2. The PID corrects by applying differential motor speed.
3. The robot straightens out automatically.

This does not fix the *distance* error (the encoders still think they traveled
farther than they did), but it keeps the heading accurate. For many competition
scenarios, heading accuracy matters more than distance accuracy.

##### Strategy 2: Sensor-Driven Closed Loop with `drive()`

Instead of using the "go X mm" commands, use `drive(speed, turn_rate)` inside
your own control loop that reads external sensors:

```python
from pybricks.pupdevices import Motor, ColorSensor
from pybricks.parameters import Port, Direction
from pybricks.robotics import DriveBase
from pybricks.tools import wait

left = Motor(Port.A, Direction.COUNTERCLOCKWISE)
right = Motor(Port.B)
color = ColorSensor(Port.C)
db = DriveBase(left, right, 56, 112)
db.use_gyro(True)

# Line following — wheel slip doesn't matter because
# the sensor continuously corrects the trajectory
while True:
    reflection = color.reflection()
    error = reflection - 50        # target is edge of line
    turn_rate = error * 2.0        # proportional gain
    db.drive(150, turn_rate)
    wait(10)
```

Here wheel slip is irrelevant because the robot's position is continuously
corrected by an external sensor (the color sensor reading the line). The
encoders are only used for motor speed control, not navigation.

##### Strategy 3: Detect-and-Retry with Gyro Cross-Check

```python
db.use_gyro(True)

target_heading = 90
db.turn(target_heading)

# After turn completes, check actual heading
actual = hub.imu.heading()
error = target_heading - actual

if abs(error) > 2:  # more than 2 degrees off
    db.turn(error)   # correction turn
```

For straight-line distance, cross-check with the ultrasonic sensor if there is
a known wall ahead:

```python
# Drive toward a wall, use ultrasonic for ground truth
sensor = UltrasonicSensor(Port.D)
target_distance = 100  # mm from wall

db.drive(100, 0)
while sensor.distance() > target_distance:
    wait(10)
db.stop()
```

##### Strategy 4: Reduce Slip via Acceleration Control

Most wheel slip happens during hard acceleration or deceleration. The `settings()`
method lets you limit this:

```python
# Gentle acceleration reduces slip on smooth surfaces
db.settings(
    straight_speed=200,
    straight_acceleration=(300, 500),  # slow accel, faster decel
    turn_rate=120,
    turn_acceleration=200
)
```

Also `motor.control.limits(speed, acceleration, torque)` on the individual
motors — capping torque directly limits the force at the wheel-ground contact
patch, reducing the likelihood of breaking traction.

##### Strategy 5: Load Monitoring for Real-Time Slip Detection

Use multitasking to run a background monitor alongside your drive mission:

```python
async def monitor_slip():
    while True:
        left_load = left.load()
        right_load = right.load()
        # If commanding motion but load drops near zero,
        # the wheel has lost traction
        if abs(left_load) < 5 and abs(left.speed()) > 50:
            print("Left wheel slip detected")
            # React: reduce speed, pause, etc.
        await wait(20)

async def main():
    await multitask(
        drive_mission(),
        monitor_slip()
    )
run_task(main())
```

##### What Slip Recovery Cannot Do

The fundamental limitation: **there is no way to recover distance accuracy from
wheel slip using only onboard resources.** The gyro corrects heading, but the
hub has no independent distance measurement. The ultrasonic sensor can give
range-to-obstacle, and the color sensor can detect line positions or colored
landmarks, but there is no odometry correction for distance traveled over open
floor.

The practical recovery approach is a supervision strategy — when slip is
detected, switch from dead-reckoning to sensor-relative navigation, or
re-acquire a known landmark and reset the position estimate.

---

### `Car(steer_motor, drive_motors, torque_limit=100)`

Ackermann-style vehicle with a dedicated steering motor and one or more drive motors.
On initialization, the steering motor automatically sweeps to both mechanical endpoints
to find center and maximum steering angle.

```python
from pybricks.pupdevices import Motor
from pybricks.parameters import Port, Direction
from pybricks.robotics import Car

steer = Motor(Port.C, Direction.CLOCKWISE)
front = Motor(Port.A, Direction.COUNTERCLOCKWISE)
rear = Motor(Port.B, Direction.COUNTERCLOCKWISE)
car = Car(steer, [front, rear], torque_limit=100)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `steer_motor` | Motor | The steering motor |
| `drive_motors` | Motor or list | One or more drive motors |
| `torque_limit` | Number (%) | Maximum torque during initialization endpoint sweep. Lower values protect delicate steering mechanisms. |

---

**`steer(steering)`**

Set the steering position.

| Parameter | Type | Description |
|-----------|------|-------------|
| `steering` | Number (%) | -100 = full left, 0 = straight, +100 = full right. The angle corresponding to 100% is determined during initialization. |

This only moves the steering motor — it does not start driving.

---

**`drive_power(power)`**

Drive at a raw power level.

| Parameter | Type | Description |
|-----------|------|-------------|
| `power` | Number (%) | -100 = full reverse, 0 = stop, +100 = full forward. Direct voltage control (no PID). Below 10% power, the car coasts for smooth rollout instead of abrupt braking. |

Best for remote control where instant response to button input is desired.

---

**`drive_speed(speed)`**

Drive at a PID-controlled speed.

| Parameter | Type | Description |
|-----------|------|-------------|
| `speed` | Number (deg/s) | Target motor speed. Positive = forward, negative = backward. |

The PID controller automatically adjusts power to maintain speed across varying
loads (hills, carpet, obstacles). Includes smooth acceleration and deceleration.

---

## Utility — `from pybricks.tools import ...`

---

### `wait(time)`

Pause program execution for `time` milliseconds.

```python
from pybricks.tools import wait
wait(1000)  # wait 1 second
```

Under the multitasking runtime (`run_task`), `wait()` yields control to other
coroutines during the pause. Without multitasking, it simply blocks.

---

### `StopWatch()`

Timer for measuring elapsed time.

```python
from pybricks.tools import StopWatch

watch = StopWatch()       # starts automatically at 0
elapsed = watch.time()    # returns ms since creation/reset
```

| Method | Description |
|--------|-------------|
| `watch.time()` | Elapsed time in milliseconds since creation or last `reset()`. |
| `watch.pause()` | Freeze the timer. Subsequent `time()` calls return the same value. |
| `watch.resume()` | Un-freeze the timer. It continues from where it was paused. |
| `watch.reset()` | Reset to 0. If paused, stays paused at 0. If running, keeps running from 0. |

Common patterns:
```python
# Timed loop
watch = StopWatch()
while watch.time() < 5000:
    # do something for 5 seconds
    wait(10)

# Measure execution time
watch = StopWatch()
do_something()
print("Took", watch.time(), "ms")
```

---

### `multitask(*coroutines, race=False)`

Run multiple `await`-able coroutines concurrently using cooperative multitasking.
Must be used inside an `async def` function under `run_task()`.

```python
from pybricks.tools import multitask, run_task

async def task_a():
    await motor_a.run_angle(500, 360)

async def task_b():
    await motor_b.run_angle(500, -360)

async def main():
    # Run both motors simultaneously
    await multitask(task_a(), task_b())

run_task(main())
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `*coroutines` | coroutine | required | One or more coroutine objects (async function calls) |
| `race` | bool | `False` | `False` = wait for **all** to complete. `True` = wait for the **first** to complete, then cancel the rest ("race" mode). |

**Returns:** Tuple of return values from each coroutine. Unfinished coroutines
(when `race=True`) have `None` as their return value.

**Cooperative scheduling:** Coroutines yield control at every `await` point.
A coroutine that does CPU-intensive work without `await` will block all others.
Always include `await wait(...)` in tight loops.

---

### `run_task(coroutine)`

Entry point for the async multitasking runtime. Runs a coroutine from start to
finish, blocking all non-async code.

```python
from pybricks.tools import run_task

async def main():
    await drive_base.straight(500)
    await drive_base.turn(90)

run_task(main())
```

| Parameter | Description |
|-----------|-------------|
| `coroutine` | The main coroutine to execute. Typically `main()`. |

Cannot be nested (no `run_task()` inside another `run_task()`).

When called with no arguments, returns `True` if the async run loop is currently
active, `False` otherwise.

**Key behavior:** When `run_task` is not used, all `await`-prefixed methods
(like `motor.run_angle()`, `drive_base.straight()`, etc.) act as normal blocking
functions. You can ignore `await` entirely if you don't need multitasking.

---

### `hub_menu(*symbols)`

Display an interactive menu on the hub's LED matrix. The user scrolls through
symbols using the left/right buttons and selects with the center button.
Blocks until a selection is made.

```python
from pybricks.tools import hub_menu

selected = hub_menu("A", "B", "C", 1, 2, 3)

if selected == "A":
    import mission_a
elif selected == "B":
    import mission_b
```

| Parameter | Description |
|-----------|-------------|
| `*symbols` | One or more single characters or small numbers to display as choices |

**Returns:** The selected symbol (matching the type passed in — `str` or `int`).

This is a convenience function built on top of the display and button APIs.
It can be used anywhere in a program, not just at startup. Useful for
competition robots that run multiple missions from a single program slot.

---

### `read_input_byte(last=False, chr=False)`

Non-blocking read of one byte from standard input (the Pybricks Code terminal).
Useful for receiving commands or data from a connected computer while the
program is running.

```python
from pybricks.tools import read_input_byte

byte_val = read_input_byte()           # oldest byte, as int 0–255
char_val = read_input_byte(chr=True)   # oldest byte, as string
latest = read_input_byte(last=True)    # newest byte, discard older
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `last` | bool | `False` | `True` = read the most recent byte and discard all older buffered bytes. `False` = read the oldest byte (FIFO). |
| `chr` | bool | `False` | `True` = return a one-character string. `False` = return numeric value 0–255. |

**Returns:** The byte value, or `None` if no data is available. When `chr=True`,
also returns `None` if the byte is not a printable character.

---

## Parameters Reference — `from pybricks.parameters import ...`

Quick reference for the most commonly used parameter constants.

### Port

`Port.A`, `Port.B`, `Port.C`, `Port.D`, `Port.E`, `Port.F`

Availability depends on the hub (PrimeHub has A–F, EssentialHub has A–B).

### Direction

`Direction.CLOCKWISE`, `Direction.COUNTERCLOCKWISE`

### Stop

| Constant | Description |
|----------|-------------|
| `Stop.COAST` | Cut power, spin freely |
| `Stop.BRAKE` | Passive electric braking |
| `Stop.HOLD` | Active PID position hold |
| `Stop.COAST_SMART` | Coast, but remember the target for the next command |

### Button

`Button.CENTER`, `Button.LEFT`, `Button.RIGHT`,
`Button.LEFT_PLUS`, `Button.LEFT_MINUS`, `Button.RIGHT_PLUS`, `Button.RIGHT_MINUS`
(last four are Remote-only)

### Side

`Side.TOP`, `Side.BOTTOM`, `Side.LEFT`, `Side.RIGHT`, `Side.FRONT`, `Side.BACK`

### Color

Built-in: `Color.RED`, `Color.ORANGE`, `Color.YELLOW`, `Color.GREEN`,
`Color.CYAN`, `Color.BLUE`, `Color.VIOLET`, `Color.MAGENTA`,
`Color.WHITE`, `Color.BLACK`, `Color.GRAY`, `Color.NONE`

Custom: `Color(h=0, s=100, v=100)` — hue 0–359, saturation 0–100, value 0–100.

Brightness scaling: `Color.RED * 0.5` gives red at 50% brightness.

### Axis

`Axis.X`, `Axis.Y`, `Axis.Z` — used with `imu.rotation()`.

---

## Calibration Tips

### Wheel Diameter

1. Mark a starting line on the floor.
2. Command `drive_base.straight(1000)` for exactly 1000 mm.
3. Measure the actual distance traveled.
4. Adjust: `actual_diameter = programmed_diameter × (programmed_distance / actual_distance)`

### Axle Track

1. Command `drive_base.turn(3600)` for 10 full rotations.
2. Measure how far off the final heading is.
3. Adjust `axle_track` proportionally:
   - Turns too far → decrease `axle_track`
   - Turns not enough → increase `axle_track`

Always calibrate `wheel_diameter` first, then `axle_track`.

---

## References

- [Pybricks Documentation](https://docs.pybricks.com/)
- [Pybricks GitHub](https://github.com/pybricks/pybricks-micropython)
- [Pybricks Code IDE](https://code.pybricks.com/)

---

*Based on Pybricks v3.6.1 documentation. LEGO® is a trademark of the LEGO Group.*

