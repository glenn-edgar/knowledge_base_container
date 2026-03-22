# Pybricks MicroPython — Motor & Sensor Command Reference

> Pybricks v3.6.x API for LEGO SPIKE Prime, MINDSTORMS Robot Inventor, Technic, BOOST, and City hubs.
> Full documentation: <https://docs.pybricks.com/en/latest/>

---

## Table of Contents

- [Common Imports](#common-imports)
- [Motor (with rotation sensor)](#motor-with-rotation-sensor)
  - [Initialization](#motor-initialization)
  - [Measuring](#motor-measuring)
  - [Running](#motor-running)
  - [Stopping](#motor-stopping)
  - [Motor Settings](#motor-settings)
  - [Control Settings (PID)](#control-settings-pid)
- [DCMotor (no rotation sensor)](#dcmotor-no-rotation-sensor)
- [ColorSensor](#colorsensor)
- [UltrasonicSensor](#ultrasonicsensor)
- [ForceSensor](#forcesensor)
- [ColorDistanceSensor](#colordistancesensor)
- [Hub IMU](#hub-imu)
- [DriveBase](#drivebase)
- [Stop Types](#stop-types)
- [Utilities](#utilities)

---

## Common Imports

```python
from pybricks.hubs import PrimeHub          # or InventorHub, TechnicHub, etc.
from pybricks.pupdevices import (
    Motor, DCMotor,
    ColorSensor, UltrasonicSensor, ForceSensor,
    ColorDistanceSensor, ColorLightMatrix, Light,
)
from pybricks.parameters import Button, Color, Direction, Port, Side, Stop, Axis
from pybricks.robotics import DriveBase
from pybricks.tools import wait, StopWatch, multitask, run_task

hub = PrimeHub()
```

---

## Motor (with rotation sensor)

Use for all SPIKE angular motors and Technic motors that have built-in rotation encoders.

### Motor Initialization

```python
Motor(port, positive_direction=Direction.CLOCKWISE, gears=None, reset_angle=True, profile=None)
```

| Parameter            | Type        | Description |
|----------------------|-------------|-------------|
| `port`               | `Port`      | Port the motor is connected to (`Port.A` – `Port.F`). |
| `positive_direction` | `Direction` | Which way is "positive." Default `Direction.CLOCKWISE`. |
| `gears`              | `list`      | Optional gear train, e.g. `[12, 36]` (12-tooth drives 36-tooth). Pybricks auto-compensates speed/angle. |
| `reset_angle`        | `bool`      | Reset the angle to 0 on init. Set `False` to keep absolute encoder value. |
| `profile`            | `int`       | Smoothness profile (higher = smoother, slower response). |

```python
# Basic
arm = Motor(Port.A)

# Reversed, with gear train
lift = Motor(Port.B, Direction.COUNTERCLOCKWISE, gears=[12, 36])
```

### Motor Measuring

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `angle()` | `int` | deg | Current rotation angle of the output shaft. |
| `reset_angle(angle)` | — | deg | Reset angle to given value. Pass `None` to reset to absolute encoder position. |
| `speed(window=100)` | `int` | deg/s | Speed averaged over `window` ms. Short window = responsive; long window = steady. |
| `load()` | `int` | mNm | Estimated load torque resisting the motor. Only valid during active control. |
| `stalled()` | `bool` | — | `True` if the motor cannot reach its target even at max actuation. |

```python
motor = Motor(Port.A)
motor.reset_angle(0)
motor.run(300)
wait(500)
print("Angle:", motor.angle(), "Speed:", motor.speed(), "Load:", motor.load())
print("Stalled:", motor.stalled())
```

### Motor Running

All `speed` parameters are in deg/s. Angle/rotation parameters are in deg. Time is in ms.

| Method | Description |
|--------|-------------|
| `run(speed)` | Run at constant speed forever (until next command). |
| `dc(duty)` | Raw duty cycle (−100 to 100%). No PID — direct voltage control. |
| `run_time(speed, time, then=Stop.HOLD, wait=True)` | Run for a duration in ms. Total time includes accel/decel. |
| `run_angle(speed, rotation_angle, then=Stop.HOLD, wait=True)` | Run a relative angle from current position. |
| `run_target(speed, target_angle, then=Stop.HOLD, wait=True)` | Run to an absolute target angle. |
| `run_until_stalled(speed, then=Stop.COAST, duty_limit=None)` | Run until stalled. Returns the stall angle. `duty_limit` (%) caps torque. |
| `track_target(target_angle)` | Immediately move to target angle (no accel ramp). Good for real-time tracking loops. |
| `done()` | Returns `True` if the last command has completed. |

```python
motor = Motor(Port.A)

# Run 360 degrees, then hold position
motor.run_angle(500, 360, then=Stop.HOLD)

# Run to absolute 0, then coast
motor.run_target(200, 0, then=Stop.COAST)

# Find mechanical endpoint
stall_angle = motor.run_until_stalled(-200, duty_limit=30)
motor.reset_angle(0)

# Non-blocking: start motion, do other things, check done()
motor.run_angle(500, 720, wait=False)
while not motor.done():
    print("Moving...", motor.angle())
    wait(100)
```

### Motor Stopping

| Method | Behavior |
|--------|----------|
| `stop()` | Coast — motor spins freely, decelerates by friction. |
| `brake()` | Passive brake — shorts motor leads, resists motion but does not hold position. |
| `hold()` | Active hold — PID controller holds current angle. |

### Motor Settings

```python
motor.settings(max_voltage)             # Get or set max voltage (mV)
motor.close()                           # Release motor; allows re-init with new gears
```

### Control Settings (PID)

Pybricks uses a built-in PID controller for all position and speed commands (`run_target`, `run_angle`, `run_time`, `hold`, etc.). The controller is accessed through `motor.control`.

> **Important:** You can only change control settings while the motor is stopped. Call `motor.stop()` first, change settings, then issue new commands.

#### `control.limits(speed, acceleration, torque)`

Sets the maximum speed, acceleration, and feedback torque the controller is allowed to use.

| Parameter      | Units   | Description |
|----------------|---------|-------------|
| `speed`        | deg/s   | Maximum speed. All commands capped to this value. |
| `acceleration` | deg/s²  | Ramp slope for accel/decel. Pass a `(accel, decel)` tuple for separate values. |
| `torque`       | mNm     | Maximum feedback torque during PID control. |

Call with no arguments to read current values.

```python
motor.stop()
motor.control.limits(speed=800, acceleration=1500, torque=300)
```

#### `control.pid(kp, ki, kd, integral_deadzone, integral_rate)`

Gets or sets the PID gains for the internal position/speed controller. Call with no arguments to read current values.

| Parameter           | Units          | Description |
|---------------------|----------------|-------------|
| `kp`                | µNm/deg        | **Proportional gain.** Feedback torque per degree of position error. Higher = stiffer response, more oscillation risk. |
| `ki`                | µNm/(deg·s)    | **Integral gain.** Feedback torque per accumulated degree-seconds of error. Eliminates steady-state error. Higher = faster correction of static offsets, but can cause windup overshoot. |
| `kd`                | µNm/(deg/s)    | **Derivative gain.** Feedback torque per unit of speed (acts as damping). Higher = more damping, less oscillation, but slower settling. Also serves as proportional speed control. |
| `integral_deadzone` | deg            | Zone around the target where the integral term does **not** accumulate error. Prevents integral windup when the motor is close to target. |
| `integral_rate`     | deg/s          | Maximum rate at which the integral error is allowed to grow. Limits windup during large transients. |

**Default values** (SPIKE Medium/Large Angular Motor, typical):

| Parameter           | Default Value |
|---------------------|---------------|
| `kp`                | ~42484        |
| `ki`                | ~21242        |
| `kd`                | ~5310         |
| `integral_deadzone` | ~8            |
| `integral_rate`     | ~15           |

> Defaults vary by motor type. Always read current values with `motor.control.pid()` before tuning.

```python
motor = Motor(Port.A)
motor.stop()

# Read current PID values
kp, ki, kd, deadzone, rate = motor.control.pid()
print(f"kp={kp}, ki={ki}, kd={kd}, deadzone={deadzone}, rate={rate}")

# Soften response for a heavy geared mechanism
motor.control.pid(kp // 2, ki // 2, kd, deadzone, rate)

# Restore defaults by reading from a fresh motor or noting originals
```

**PID Control Law:**

The controller computes a torque command at each control cycle:

```
error        = target_angle - measured_angle
d_error      = -measured_speed            (derivative of error ≈ negative speed)
integral    += error * dt                  (clamped by integral_deadzone and integral_rate)

torque_cmd   = kp * error  +  ki * integral  +  kd * d_error
```

The torque command is then clamped to the limits set by `control.limits(torque=...)`.

**Tuning Guidelines:**

| Symptom | Adjustment |
|---------|------------|
| Motor oscillates around target | Decrease `kp`, increase `kd` |
| Motor is sluggish / doesn't reach target | Increase `kp` |
| Motor overshoots target | Increase `kd`, decrease `kp` |
| Steady-state error (doesn't quite reach target) | Increase `ki` (slightly) |
| Motor vibrates or buzzes when holding | Increase `integral_deadzone`, decrease `kp` |
| Heavy/geared load — jerky motion | Halve `kp` and `ki`; increase motor profile or use `run_time` |

**Practical Example — Compliant Gripper:**

```python
from pybricks.pupdevices import Motor
from pybricks.parameters import Port, Stop
from pybricks.tools import wait

gripper = Motor(Port.B)
gripper.stop()

# Read defaults
defaults = gripper.control.pid()
print("Defaults:", defaults)

# Make the gripper compliant: reduce proportional gain
# so it doesn't crush objects, keep damping high
kp, ki, kd, dz, rate = defaults
gripper.control.pid(kp // 3, ki // 4, kd, dz * 2, rate)

# Close the gripper gently until stalled
gripper.run_until_stalled(200, then=Stop.HOLD, duty_limit=40)
wait(2000)

# Open back up
gripper.run_target(300, 0)
```

#### `control.target_tolerances(speed, position)`

Defines when `done()` returns `True` — i.e., when the motor is considered to have reached its target.

| Parameter  | Units | Description |
|------------|-------|-------------|
| `speed`    | deg/s | Max speed deviation from zero before motion is "done." |
| `position` | deg   | Max position deviation from target before motion is "done." |

```python
motor.stop()
# Tighten tolerances for precision
motor.control.target_tolerances(speed=5, position=2)
```

#### `control.stall_tolerances(speed, time)`

Defines when `stalled()` returns `True`.

| Parameter | Units | Description |
|-----------|-------|-------------|
| `speed`   | deg/s | If actual speed stays below this value under max actuation, motor is stalled. |
| `time`    | ms    | How long the speed must stay below the threshold before declaring a stall. |

```python
motor.stop()
# More sensitive stall detection
motor.control.stall_tolerances(speed=20, time=200)
```

#### `control.scale`

Read-only. The gear ratio determined from the `gears=` argument at init. Number of internal encoder degrees per output degree.

#### `model.state()` / `model.settings()`

Advanced — returns the internal motor model estimates (angle, speed, current, stall state) updated faster than real measurements. Useful for custom PID loops.

```python
est_angle, est_speed, est_current, est_stalled = motor.model.state()
```

---

## DCMotor (no rotation sensor)

For train motors, Powered Up motors, and other motors without encoders.

```python
DCMotor(port, positive_direction=Direction.CLOCKWISE)
```

| Method | Description |
|--------|-------------|
| `dc(duty)` | Set duty cycle (−100 to 100%). Positive = forward. |
| `stop()` | Coast. |
| `brake()` | Passive brake. |
| `settings(max_voltage)` | Get/set max voltage. |

```python
train = DCMotor(Port.A)
train.dc(75)       # 75% forward
wait(3000)
train.brake()
```

---

## ColorSensor

SPIKE Color Sensor (device ID 61). Has 3 built-in lights.

```python
sensor = ColorSensor(Port.A)
```

| Method | Returns | Description |
|--------|---------|-------------|
| `color()` | `Color` | Nearest detectable color (rounded). |
| `hsv()` | `Color` | Raw hue (0–359), saturation (0–100), value/brightness (0–100). |
| `reflection()` | `int` (%) | Surface reflectance, 0–100. |
| `ambient()` | `int` (%) | Ambient light intensity, 0–100. |
| `detectable_colors(colors)` | `tuple` | Set or get the list of colors to detect. |
| `lights.on(brightness)` | — | Turn on LEDs. Single int (all 3) or 3-tuple for individual control. |
| `lights.off()` | — | Turn off LEDs. |

**Custom Color Detection:**

```python
sensor = ColorSensor(Port.A)

# Measure your actual objects with sensor.hsv(), then define:
Color.GREEN  = Color(h=132, s=94, v=26)
Color.BROWN  = Color(h=17,  s=78, v=15)
Color.RED    = Color(h=359, s=97, v=39)

sensor.detectable_colors([Color.GREEN, Color.BROWN, Color.RED, Color.NONE])

while True:
    print(sensor.color())
    wait(200)
```

---

## UltrasonicSensor

SPIKE Ultrasonic Sensor (device ID 62). Has 4 built-in lights.

```python
sensor = UltrasonicSensor(Port.A)
```

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `distance()` | `int` | mm | Distance to nearest object. |
| `presence()` | `bool` | — | `True` if another ultrasonic sensor is detected nearby. |
| `lights.on(brightness)` | — | % | Single int (all 4) or 4-tuple for individual LED control. |
| `lights.off()` | — | — | Turn off LEDs. |

```python
sensor = UltrasonicSensor(Port.D)
while True:
    dist = sensor.distance()
    print("Distance:", dist, "mm")
    wait(100)
```

---

## ForceSensor

SPIKE Force Sensor (device ID 63).

```python
sensor = ForceSensor(Port.A)
```

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `force()` | `float` | N | Measured push force. |
| `distance()` | `float` | mm | How far the button is deflected. |
| `pressed(force=3)` | `bool` | — | `True` if force exceeds threshold (default 3 N). |
| `touched()` | `bool` | — | `True` if any touch is detected (more sensitive than `pressed`). |

```python
sensor = ForceSensor(Port.C)
while True:
    print(f"Force: {sensor.force():.1f} N, Dist: {sensor.distance():.1f} mm, "
          f"Pressed: {sensor.pressed()}, Touched: {sensor.touched()}")
    wait(200)
```

---

## ColorDistanceSensor

BOOST Color Distance Sensor (device ID 37). Combines color, distance (IR), and ambient light. Single built-in LED.

```python
sensor = ColorDistanceSensor(Port.A)
```

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `color()` | `Color` | — | Nearest detectable color. |
| `hsv()` | `Color` | — | Raw HSV measurement. |
| `reflection()` | `int` | % | Surface reflectance, 0–100. |
| `ambient()` | `int` | % | Ambient light, 0–100. |
| `distance()` | `int` | % | Relative distance via IR, 0 (near) to 100 (far). |
| `detectable_colors(colors)` | `tuple` | — | Set/get colors to detect. |
| `light.on(color)` | — | — | LED on (Color.RED, Color.GREEN, Color.BLUE, etc.). |
| `light.off()` | — | — | LED off. |

---

## Hub IMU

Accessed via `hub.imu` on PrimeHub / InventorHub / TechnicHub.

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `heading()` | `float` | deg | Accumulated heading (yaw). Persists across rotations. |
| `reset_heading(angle)` | — | deg | Reset heading reference. |
| `rotation(axis)` | `float` | deg | Rotation around a given axis. |
| `angular_velocity(axis)` | `float` | deg/s | Rotational speed around a given axis. |
| `acceleration(axis)` | `float` | mm/s² | Linear acceleration along a given axis. |
| `ready()` | `bool` | — | `True` if gyro is calibrated and ready. |

```python
hub = PrimeHub()
if not hub.imu.ready():
    print("Calibrating...")
    while not hub.imu.ready():
        wait(100)

hub.imu.reset_heading(0)
while True:
    print("Heading:", hub.imu.heading())
    wait(100)
```

---

## DriveBase

Two-wheeled robot base from `pybricks.robotics`. Manages two motors with odometry.

```python
DriveBase(left_motor, right_motor, wheel_diameter, axle_track)
```

| Parameter        | Units | Description |
|------------------|-------|-------------|
| `wheel_diameter` | mm    | Diameter of the drive wheels. |
| `axle_track`     | mm    | Distance between the wheel contact patches. |

### Movement Commands

| Method | Description |
|--------|-------------|
| `straight(distance, then=Stop.HOLD, wait=True)` | Drive forward (+) or backward (−) in mm. |
| `turn(angle, then=Stop.HOLD, wait=True)` | Turn in place. Positive = right (clockwise from top). |
| `curve(radius, angle, then=Stop.HOLD, wait=True)` | Drive an arc. |
| `drive(speed, turn_rate)` | Continuous drive (mm/s, deg/s). Keeps going until next command. |

### Stopping

| Method | Description |
|--------|-------------|
| `stop()` | Coast both motors. |
| `brake()` | Passive brake. |
| `coast()` | Explicitly coast. |

### Measuring

| Method | Returns | Units | Description |
|--------|---------|-------|-------------|
| `distance()` | `int` | mm | Estimated distance driven since reset. |
| `angle()` | `int` | deg | Estimated heading angle since reset. |
| `state()` | `tuple` | — | `(distance_mm, speed_mm_s, angle_deg, turn_rate_deg_s)` |
| `done()` | `bool` | — | `True` if last command is complete. |
| `stalled()` | `bool` | — | `True` if drive base cannot reach target. |
| `reset()` | — | — | Zero distance and angle. |

### Settings

```python
drive.settings(straight_speed, straight_acceleration, turn_rate, turn_acceleration)
drive.use_gyro(True)   # Use hub IMU for heading (much more accurate)
```

### DriveBase PID Control

The DriveBase has two independent PID controllers:

- `drive.distance_control` — controls distance/speed (same interface as `motor.control`)
- `drive.heading_control` — controls heading/turn rate (same interface as `motor.control`)

Both expose `.pid()`, `.limits()`, `.target_tolerances()`, and `.stall_tolerances()` with the same signatures as the motor control, but units are mm and mm/s for distance, and deg and deg/s for heading.

```python
left  = Motor(Port.A, Direction.COUNTERCLOCKWISE)
right = Motor(Port.B)
drive = DriveBase(left, right, wheel_diameter=56, axle_track=112)
drive.use_gyro(True)

drive.straight(500)    # Drive 500 mm forward
drive.turn(90)         # Turn 90° right
drive.straight(-200)   # Back up 200 mm
```

---

## Stop Types

Imported from `pybricks.parameters.Stop`:

| Constant | Behavior |
|----------|----------|
| `Stop.COAST` | Let motors spin freely. For relative angle maneuvers, the last target angle is used as the next start point (reduces cumulative error). |
| `Stop.BRAKE` | Passively resist motion (short motor leads). |
| `Stop.HOLD` | Actively hold at commanded angle using PID. **Default for most commands.** |
| `Stop.NONE` | Do not decelerate at end — continue at speed. Use to chain maneuvers. |

---

## Utilities

From `pybricks.tools`:

| Function / Class | Description |
|------------------|-------------|
| `wait(time)` | Pause execution for `time` ms. |
| `StopWatch()` | Timer with `.time()` (ms), `.pause()`, `.resume()`, `.reset()`. |
| `multitask(coroutine1, coroutine2, ...)` | Run multiple async tasks concurrently. |
| `run_task(main_task)` | Entry point for async programs. |

```python
from pybricks.tools import wait, StopWatch, multitask, run_task

timer = StopWatch()
while timer.time() < 5000:
    print("Elapsed:", timer.time(), "ms")
    wait(500)
```

---

## Device ID Reference

| ID | Class | Device |
|----|-------|--------|
| 1  | `DCMotor` | WeDo 2.0 Medium Motor |
| 2  | `DCMotor` | Powered Up Train Motor |
| 8  | `Light` | Powered Up Light |
| 38 | `Motor` | BOOST Interactive Motor |
| 46 | `Motor` | Technic Large Motor |
| 47 | `Motor` | Technic Extra Large Motor |
| 48 | `Motor` | SPIKE Medium Angular Motor |
| 49 | `Motor` | SPIKE Large Angular Motor |
| 65 | `Motor` | SPIKE Small Angular Motor |
| 75 | `Motor` | Technic Medium Angular Motor |
| 76 | `Motor` | Technic Large Angular Motor |
| 34 | `TiltSensor` | WeDo 2.0 Tilt Sensor |
| 35 | `InfraredSensor` | WeDo 2.0 Infrared Motion Sensor |
| 37 | `ColorDistanceSensor` | BOOST Color Distance Sensor |
| 61 | `ColorSensor` | SPIKE Color Sensor |
| 62 | `UltrasonicSensor` | SPIKE Ultrasonic Sensor |
| 63 | `ForceSensor` | SPIKE Force Sensor |
| 64 | `ColorLightMatrix` | SPIKE 3×3 Color Light Matrix |

---

## References

- Pybricks Documentation: <https://docs.pybricks.com/en/latest/>
- Pybricks Code IDE: <https://code.pybricks.com/>
- Pybricks GitHub: <https://github.com/pybricks>
- Pybricks API Source: <https://github.com/pybricks/pybricks-api>

