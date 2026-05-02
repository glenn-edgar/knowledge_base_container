# Porting

This rover is **a template for actual hardware**. The Linux + container
shape is a vehicle for proving the architecture; production deployment
swaps out specific layers without touching the rest.

## What's portable

| Layer | Fixed across ports | Varies per port |
|---|---|---|
| Mission state machine (`remote.json`) | shape | per-class entries |
| Worker functions | shape | per-class verbs |
| Wire protocol (`comm_manifest`) | framing | opcode catalogue per class |
| Master-side HAL (`dongle_hal.lua`) | shape | dispatch dispatch entries |
| `bus_kernel` ABI | C API | platform impl (Linux pty, Zephyr UART) |
| `logical_robot` vtable | C API | per-class implementation |
| Physics simulator | unchanged | replaced with real hardware on Track B |
| Container layer | optional | absent on small-RAM Pi (use [tarball](small_pi_deploy.md)) |

## Three port axes

1. **[Add a new robot class](../containers/adding_a_new_class.md)** —
   factory_arm, lidar_pod, etc. Linux + simulated. Reuses everything
   in `dongle_base`.

2. **[Deploy to a small-RAM Pi without containers](small_pi_deploy.md)** —
   Pi Zero 2 / 3B / 5 with Pi OS Lite. Native binaries + Lua,
   `make-deploy-tarball.sh` produces the install bundle.

3. **[Replace the simulator with real hardware](new_hardware_target.md)**
   — Track B (Pico-class dongle running Zephyr). `bus_kernel` + `ext_bus`
   ports to Zephyr; `robot_sim` → `dongle_proxy`. Logical_robot
   implementations stay the same; physics is "the real world."

## Ordering recommendation

Do them in the order above. Each axis is independent but the simpler
ones reduce surface area for the harder ones:

- Adding a class doesn't touch the platform; it's pure data + worker
  Lua + maybe a new `<class>_robot.c`.
- Going containerless on a Pi proves the source tree is build-portable
  without Docker.
- Replacing the simulator is the biggest jump; doing it after the
  others means the protocol surface is locked.
