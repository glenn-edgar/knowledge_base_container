# Replace the simulator with real hardware

Track B in the project roadmap. Architecture is proven on Linux +
containers; this is the jump to embedded silicon.

## The contract `dongle_base` exposes

Three concrete API surfaces define the seam:

1. **External wire** — USB CDC-ACM frames following `comm_manifest`
   framing. Defined in `libcomm/frame.{c,h}` + per-class
   `comm_manifest.lua`.
2. **Internal bus** — `bus_kernel` ABI (`libcomm/bus_kernel.h`). Linux
   uses `bus_kernel_linux.c`; Zephyr will use `bus_kernel_zephyr.c`
   (see Track A scaffolding in `project_dongle_track_a` memory).
3. **Per-class plugin** — `logical_robot_vtable_t`
   (`libcomm/logical_robot.h`). Drive_base implements it for the
   simulated rover; an MGM240 / Pico Zephyr firmware would implement
   it for real motors.

Each port replaces ONE layer:

| Layer | Linux today | Pico hardware (Track B) |
|---|---|---|
| External wire | pty (Linux) | USB CDC-ACM (Zephyr) |
| Internal bus | `bus_kernel_linux` | `bus_kernel_zephyr` |
| logical_robot | `drive_base_robot.c` calling `phys_*` | `drive_base_robot.c` calling motor PWM + encoder ISRs |

## What stays unchanged

- Class image (`lunar_rover-class`) — identity is "I am a drive_base
  rover," not "I am simulated."
- Mission state machine (`remote.json`) — still describes the same
  plan vocabulary.
- Master-side Lua (`dongle_hal`, `mqtt_robot_main`, …) — talks the
  same wire to the same opcodes.
- Test harness (`planner_test_peer`, contract fixture) — broker-level
  testing doesn't care whether the rover is simulated or real.

## What changes

- **`robot_sim` becomes `dongle_proxy`.** The Linux process that
  hosted the dongle threads is replaced with a thin shim that opens
  `/dev/ttyACM0` and shovels frames between USB and the master-side
  libcomm. No `libphysics` link.
- **The dongle firmware ships separately.** A Zephyr project per
  target (Pico, Pico 2, MGM240) ships a binary that runs on the MCU,
  speaks USB CDC on one end, talks to motors/encoders on the other.
- **Class image still ships `tunables.bin`** — physics tunables are
  a useful seed even for real hardware (PID gains, max accel, etc.
  matter as starting points; calibration overrides them at runtime).

## Hardware bring-up sequence

1. Get Pico + Debug Probe + RS-485 breakouts (per
   `project_esp32_dongle_prototype`).
2. Port `bus_kernel_linux.c` patterns to `bus_kernel_zephyr.c`. Track
   A scaffolding already laid down most of the headers
   (`bus_msg`, `ext_bus`, `bus_config` are platform-neutral).
3. Implement the matching ext_bus contract on Zephyr side (3-fn
   contract per Track A.6).
4. Stand up the firmware locally: send a frame from the host, observe
   it on the wire.
5. Wire it through `dongle_proxy` so master-side Lua sees the same
   surface as today.
6. Run `test_random_paths --self-host --mode paths_only` against the
   real rover. Expect motion in the real world.

See [`project_dongle_track_c`](../../docs/architecture/index.md) for
the locked contract details (bus_msg envelope, vtable, NVS layout,
921.6 kbps ext-bus).
