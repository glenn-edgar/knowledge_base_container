# General-Purpose Robotics Platform: Zephyr + Pybricks C Libraries on XIAO MG24

## Overview

This document describes an architecture for a general-purpose robotics platform that combines:

- **Zephyr RTOS** — real-time kernel, BLE, Thread mesh, device driver framework
- **Pybricks pbio C library** — proven motor control, drivebase kinematics, PUP device protocol, sensor processing
- **Seeed XIAO MG24** (Silicon Labs EFR32MG24) — initial target hardware
- **Bosch BNO085/BNO086** — 9-DOF IMU with onboard Kalman filter and quaternion output

The central insight is that Pybricks' C stack is already cleanly layered and hardware-agnostic. The SPIKE-RT project (Nagoya University, Japan) already proved that pbio runs outside of MicroPython on a different RTOS (TOPPERS/ASP3). Porting to Zephyr follows the same pattern with a much larger ecosystem.

---

## Pybricks C Stack — Three Clean Layers

```
+----------------------------------+
|  Application / User Code         |  <-- currently MicroPython or SPIKE-RT C tasks
+----------------------------------+
|  pbio  (platform-independent)    |  <-- motor control, drivebase, IMU, color,
|                                  |      light matrix, battery, sensors
+----------------------------------+
|  pbdrv  (hardware abstraction)   |  <-- thin HAL: GPIO, UART, I2C, SPI, timers
+----------------------------------+
|  MCU / Board                     |  <-- STM32F413 (SPIKE), STM32L431 (Technic), etc.
+----------------------------------+
```

**pbio does not know or care what RTOS is running.** It needs a timer tick, some interrupt routing, and a main polling loop. Only `pbdrv` touches hardware directly.

---

## Zephyr-Based System Architecture

```
+----------------------------------------------------+
|  Application Layer                                  |
|  +----------+ +----------+ +--------------------+  |
|  | Your C   | | MicroPy  | | ChainTree/         |  |
|  | app      | | (optional)| | S-Engine           |  |
|  +----------+ +----------+ +--------------------+  |
+----------------------------------------------------+
|  Robotics Service Layer (Zephyr subsystem)          |
|  +------------------------------------------------+ |
|  | pbio (motor control, drivebase, sensors,        | |
|  |       IMU fusion, PID, color, light)            | |
|  +------------------------------------------------+ |
+----------------------------------------------------+
|  Zephyr HAL / pbdrv bridge                          |
|  +------+ +------+ +-----+ +-----+ +--------+     |
|  | GPIO | | UART | | I2C | | SPI | | Timers |     |
|  +------+ +------+ +-----+ +-----+ +--------+     |
+----------------------------------------------------+
|  Zephyr Kernel                                      |
|  threads, semaphores, timers, BLE, USB, logging     |
+----------------------------------------------------+
|  Hardware (any Zephyr-supported board)              |
|  XIAO MG24, STM32, nRF52/53, ESP32, RP2040, etc.  |
+----------------------------------------------------+
```

---

## The Four Pieces to Build

### 1. pbdrv-zephyr — the HAL bridge (core engineering work)

Reimplement `pbdrv` functions using Zephyr's device API. This is the only layer that requires real engineering.

**Current pbdrv (STM32 direct HAL):**
```c
HAL_GPIO_WritePin(GPIOA, GPIO_PIN_5, GPIO_PIN_SET);
```

**Zephyr pbdrv:**
```c
gpio_pin_set_dt(&motor_enable, 1);
```

#### pbdrv to Zephyr API mapping

| pbdrv needs | Zephyr provides |
|---|---|
| GPIO | `gpio_dt_spec` / `gpio_pin_configure_dt()` |
| UART (PUP device protocol) | `uart_irq_*` or `uart_async_*` API |
| I2C (IMU, etc.) | `i2c_dt_spec` / `i2c_transfer_dt()` |
| SPI (display) | `spi_dt_spec` / `spi_transceive_dt()` |
| Hardware timers | `counter` API or `k_timer` |
| DMA | Zephyr DMA API (less mature, may need direct HAL) |
| ADC (battery) | `adc_dt_spec` |

The Zephyr Devicetree approach makes this more portable than the current pbdrv — pin mapping is defined in DT overlays per board instead of in C `#ifdef` blocks.

### 2. Board definitions — one per target hardware

```
boards/
  spike_prime/
    spike_prime.dts          # pin map, peripherals, clocks
    spike_prime_defconfig    # kernel options
    board.cmake              # flash/debug config
```

Since the system is general-purpose, targets include:
- XIAO MG24 (initial platform)
- Custom STM32 boards with LEGO PUP ports wired up
- nRF52840 boards (native Zephyr BLE, could talk to PUP devices)
- Any board with UART ports for the PUP protocol

### 3. Robotics Zephyr module — packaging pbio as a subsystem

```
modules/robotics/
  CMakeLists.txt
  Kconfig                    # CONFIG_PBIO, CONFIG_PBIO_DRIVEBASE, etc.
  zephyr/module.yml
  lib/pbio/                  # pulled from pybricks, untouched
  drivers/pbdrv-zephyr/      # your HAL bridge
  include/robotics/          # clean public API
```

Registers as a proper Zephyr module:
```yaml
# west.yml
- name: robotics
  url: https://github.com/yourorg/zephyr-robotics
```

### 4. Application layer

User apps become simple Zephyr threads:

```c
#include <zephyr/kernel.h>
#include <robotics/motor.h>
#include <robotics/drivebase.h>
#include <robotics/color_sensor.h>

void main(void) {
    // pbio handles PID, acceleration curves, stall detection
    struct drivebase *db = drivebase_init(PORT_A, PORT_B,
                                          56.0,   // wheel diameter mm
                                          128.0); // axle track mm

    drivebase_straight(db, 500, 200, 100);  // 500mm, speed, accel
    drivebase_turn(db, 90, 150, 75);        // 90 deg, speed, accel

    // Meanwhile another thread runs sensors
    // Meanwhile another thread runs your planner/state machine
}
```

---

## What Each Side Provides for Free

### From Pybricks pbio (proven, tested)

- PID motor control with anti-windup
- Drivebase kinematics (straight, turn, arc, curves)
- IMU sensor fusion (gyro-assisted turns)
- PUP device protocol (auto-detection of motors/sensors)
- Color sensor with HSV processing
- Stall detection, acceleration profiling

### From Zephyr (massive ecosystem)

- BLE stack (native, no BTstack dependency)
- USB device stack (CDC, DFU, mass storage)
- Shell subsystem (debug CLI over serial/BLE)
- Logging framework
- Settings/NVS (persistent config storage)
- MCUboot (OTA firmware updates)
- Network stack if you go ESP32/nRF (WiFi, Thread, Matter)
- Supports 500+ boards out of the box
- `west` build system, CI/CD friendly

---

## Initial Platform: Seeed XIAO MG24 (Silicon Labs EFR32MG24)

### Chip specifications

| Feature | EFR32MG24 Spec | Robotics relevance |
|---|---|---|
| Core | Cortex-M33 @ 78 MHz | More than enough for pbio PID loops |
| Flash | 1536 KB | Plenty — SPIKE Prime runs pbio in ~1MB |
| RAM | 256 KB | Generous — SPIKE has 320KB total, shares with MicroPython |
| BLE | 5.3 native | Hub-to-hub comms, phone/PC control |
| 802.15.4 | Thread / Zigbee | Multi-robot mesh networking |
| GPIO | 22 pins on XIAO | Enough for 2-3 PUP ports |
| UART | 3x EUSART | Key — PUP device protocol runs over UART |
| I2C | 2x | External IMU, additional sensors |
| SPI | 2x | Display, flash |
| ADC | 16-bit | Battery monitoring |
| Size | 21x17.5mm | Fits inside custom robot chassis |

### Why XIAO MG24 is a good initial platform

- **Zephyr already supports it** — `xiao_mg24` board definition exists upstream, no custom BSP needed to start
- **BLE is native Silicon Labs** — EFR32 series is one of Zephyr's best-supported BLE targets, no BTstack or external BLE module needed
- **Thread/802.15.4 for multi-robot** — something none of the three SPIKE runtimes offer; multiple robots can coordinate over Thread mesh while each runs pbio motor control locally
- **3 EUSARTs map to 3 PUP ports** — LEGO PUP protocol runs at 115200 baud over single-wire UART; three is enough for a meaningful robot (2 drive motors + 1 sensor)
- **Cheap ($5-6)** — iterate fast, break things
- **USB-C** — easy debug and DFU flashing
- **Small enough** to embed in custom chassis

### Wiring PUP devices to XIAO

LEGO PUP connectors use 6 wires. For basic motor/sensor control:

```
PUP Connector Pin    Signal        XIAO Connection
-----------------    ------        ---------------
1                    M1 (motor+)   H-bridge driver (external)
2                    M2 (motor-)   H-bridge driver (external)
3                    GND           GND
4                    VCC (3.3V)    3V3
5                    UART TX/RX    EUSART TX pin (via level shift if needed)
6                    ID/Power      GPIO + ADC (device ID detection)
```

External motor drivers (H-bridge) are required since the XIAO can't source motor current directly. A DRV8833 or TB6612 breakout works — this is what the SPIKE hub does internally (the STM32F413 drives H-bridges through GPIO, not directly).

### First milestone — minimal working system

```
XIAO MG24
  +-- EUSART0 --> PUP Port A (motor)
  +-- EUSART1 --> PUP Port B (motor)
  +-- EUSART2 --> PUP Port C (sensor)
  +-- I2C0    --> BNO086 IMU
  +-- GPIO    --> DRV8833 motor driver x2
  +-- BLE     --> Control from phone/PC
  +-- USB-C   --> Debug serial + DFU flash
```

### Build order

1. **Zephyr app skeleton** — blink LED, serial console on XIAO MG24
2. **PUP UART driver** — implement pbdrv UART using Zephyr's EUSART API, detect a motor plugged in (the real first test)
3. **Motor PWM** — GPIO control of external H-bridge, pbio takes over PID
4. **pbio integration** — run `pbio_do_one_event()` in a dedicated Zephyr thread, hook up timer tick
5. **Second motor + drivebase** — differential drive working
6. **BLE control** — send commands from phone/PC
7. **Sensor support** — color, ultrasonic, force via PUP protocol
8. **IMU integration** — BNO086 over I2C, quaternion-to-heading for pbio
9. **ChainTree/S-Engine** — run planner as another Zephyr thread

### Performance note

78 MHz Cortex-M33 vs SPIKE's 100 MHz Cortex-M4 — the MG24 is slightly slower clock but the M33 is a newer, more efficient architecture. For pbio's PID loops (which run at 1 kHz on SPIKE), this is fine. For the S-Engine/ChainTree planner — depends on tree complexity, but likely fine for real-time control decisions.

---

## Bosch BNO085/BNO086 IMU Integration

### Why the BNO08x

The BNO08x performs all sensor fusion on-chip:

| What you'd normally implement | BNO08x handles it |
|---|---|
| Accelerometer calibration | Onboard |
| Gyroscope drift compensation | Onboard |
| Magnetometer soft/hard iron correction | Onboard |
| Kalman filter (sensor fusion) | Onboard (Hillcrest Labs SH-2 firmware) |
| Quaternion computation | Onboard — you just read the result |

You read pre-cooked orientation data over I2C or SPI. No DSP code on your M33.

### BNO08x vs SPIKE Prime's IMU

SPIKE Prime has an LSM6DS3TR-C (6-DOF only — accel + gyro, no magnetometer). pbio does its own simpler fusion in `lib/pbio/src/imu.c`:

```
SPIKE (current):     LSM6DS3 raw data --> pbio's own fusion --> heading angle
BNO08x (your board): 9-DOF raw data  --> onboard Kalman    --> quaternion out
```

The BNO08x output is strictly better — 9 degrees of freedom, proper Kalman filter, quaternion representation (no gimbal lock), and heading relative to magnetic north.

### Integration layer 1: Zephyr driver

For the BNO085/BNO086, use the SH-2 protocol over I2C:

```dts
/* Devicetree overlay for XIAO MG24 */
&i2c0 {
    bno086: bno086@4a {
        compatible = "bosch,bno08x";
        reg = <0x4a>;
        int-gpios = <&gpio0 3 GPIO_ACTIVE_LOW>;
        reset-gpios = <&gpio0 4 GPIO_ACTIVE_LOW>;
    };
};
```

The SH-2 protocol is well-documented with open-source implementations (Adafruit's `sh2` library, Hillcrest's reference code). Wrapping it as a Zephyr sensor driver is straightforward — I2C reads of report packets.

### Integration layer 2: Quaternion to pbio heading

pbio's drivebase and motor control primarily use heading angle (yaw) for gyro-assisted turns. The conversion from quaternion is trivial:

```c
// Quaternion (w, x, y, z) --> yaw heading in degrees
float quat_to_heading(float w, float x, float y, float z) {
    float siny_cosp = 2.0f * (w * z + x * y);
    float cosy_cosp = 1.0f - 2.0f * (y * y + z * z);
    return atan2f(siny_cosp, cosy_cosp) * (180.0f / M_PI);
}
```

The full quaternion should also be kept available for applications that need 3D orientation or tilt compensation.

### Integration layer 3: pbio IMU abstraction replacement

pbio's IMU interface in `lib/pbio/src/imu.c` expects:
- Heading angle (for gyro-assisted turns)
- Angular velocity (for rate-based control)
- Acceleration (for tilt detection)

Replace the LSM6DS3 driver calls with BNO08x reads:

```c
// pbdrv_imu_zephyr.c

// BNO08x gives all of these as pre-fused reports:
float heading = quat_to_heading(q.w, q.x, q.y, q.z);  // rotation vector report
float gyro_z  = bno_gyro_report.z;                      // calibrated gyro report
float accel   = bno_accel_report;                        // linear acceleration report
```

### Integration layer 4: Application API

```c
// Clean API for your robotics layer
struct imu_data {
    // Full quaternion (from BNO08x directly)
    float quat[4];          // w, x, y, z

    // Euler angles (derived from quaternion)
    float heading;          // yaw - what pbio drivebase uses
    float pitch;
    float roll;

    // Raw calibrated vectors
    float gyro[3];          // deg/s
    float accel[3];         // m/s^2 (gravity removed)
    float mag[3];           // uT

    // BNO08x also provides these
    float accuracy;         // heading accuracy estimate in radians
    uint8_t cal_status;     // calibration status
};
```

### Wiring BNO086 to XIAO MG24

```
XIAO MG24          BNO086 Breakout
----------         ---------------
3V3          -->   VIN
GND          -->   GND
I2C0 SDA     -->   SDA
I2C0 SCL     -->   SCL
GPIO3        -->   INT  (data ready interrupt)
GPIO4        -->   RST  (optional, for hard reset)
```

Two wires for data (I2C), two for control.

### IMU integration difficulty

| Task | Difficulty | Notes |
|---|---|---|
| SH-2 protocol driver | Low-Medium | Well-documented, open-source references exist |
| Zephyr sensor driver wrapper | Low | Standard Zephyr sensor API pattern |
| Quaternion to heading for pbio | Trivial | Four lines of math |
| Calibration persistence | Low | Save BNO08x cal data to Zephyr NVS on shutdown |
| Timing | Low | BNO08x runs at up to 400 Hz, pbio needs 1 kHz ticks but IMU reads at 100 Hz is fine |

### Advantages over SPIKE Prime's IMU

- **Absolute heading** (magnetometer) — SPIKE only has relative gyro heading that drifts
- **Quaternion output** — no gimbal lock, proper 3D orientation
- **Onboard Kalman** — better fusion than pbio's simple complementary filter
- **Heading accuracy estimate** — the IMU tells you how confident it is
- **Gravity-separated acceleration** — linear acceleration with gravity removed, useful for motion detection

For FLL/competition robots, absolute heading alone is a significant upgrade — no accumulated gyro drift across multiple runs.

---

## Overall Difficulty Assessment

| Challenge | Difficulty | Notes |
|---|---|---|
| pbdrv UART for PUP protocol | Medium | Timing-sensitive, needs interrupt-driven UART |
| pbio polling integration | Low | Run `pbio_do_one_event()` in a dedicated Zephyr thread |
| DMA for display/audio | Medium | Zephyr DMA API is board-dependent |
| BLE (Pybricks protocol compat) | Medium | Reimplement on Zephyr BLE, or define your own protocol |
| BNO08x IMU integration | Low | On-chip fusion, just read quaternions over I2C |
| Testing on real SPIKE hardware | Medium | Need DFU bootloader understanding |
| External H-bridge motor drivers | Low | One-time breakout board design |

---

## Why This Is Worth Doing

Currently three SPIKE Prime runtimes exist (stock LEGO, Pybricks, SPIKE-RT) and all are SPIKE-locked. A Zephyr-based system with pbio as a module enables:

- Run the same motor/sensor/drivebase code on **any hardware** with PUP ports
- Mix LEGO sensors with non-LEGO hardware (cameras, lidar, custom boards)
- Use Zephyr's BLE mesh for multi-hub coordination
- Run the ChainTree/S-Engine planner as a Zephyr thread alongside real-time motor control
- Build custom competition robots not limited to the SPIKE hub form factor
- 9-DOF IMU with absolute heading — a strict upgrade over SPIKE's 6-DOF with drift

The pbio library is the hard-won piece — years of reverse-engineering LEGO protocols and tuning PID loops. Wrapping it in Zephyr is mostly plumbing.
