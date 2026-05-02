# `mycorp/dongle_base:1.0`

USB protocol + internal software bus + virtual-robot framework + all
master-side Lua + the test harness.

The middle layer. Class images consume static archives, headers, and
the Lua module set from here; class images themselves carry only the
*specific* logical_robot impl + tunables + mission.

## Bakes

### C: static archives + headers + .so variants

- `/opt/dongle_base/lib/libcomm.a` — static (linked into class robot_sim)
- `/opt/dongle_base/lib/libphysics.a` — static (linked into class robot_sim)
- `/opt/dongle_base/include/` — full header set (`libcomm/*.h`,
  `comm_manifest.h`, `comm_manifest_bin.h`, `physics_pipe.h`)
- `/usr/local/lib/libcomm.so` — for Lua FFI (`comm_ffi`)
- `/usr/local/lib/libphysics.so` — for Lua FFI (`physics_ffi`)
- `/usr/local/lib/libmqtt_pubsub.so` — third-party-style, links libmosquitto1

### Lua: master-side modules

`/opt/dongle_base/lua/`:

| Module | Role |
|---|---|
| `comm_ffi.lua` | libcomm FFI bindings + load_lib fallback chain |
| `dongle_hal.lua` | master-side HAL backed by libcomm + robot_sim |
| `physics_ffi.lua` | libphysics FFI bindings (sim mode) |
| `ct_comm.lua` | chain-tree comm helpers |
| `drive_base_ffi.lua` | wire opcode constants + builders + decoders |
| `mqtt_robot_main.lua` | rover entrypoint (planner-side handshake + tick) |
| `mqtt_robot_config.lua` | config loader + status publisher |
| `robot_hal.lua` | sim-vs-dongle selector |
| `robot_controller.lua` | mission dispatcher |
| `remote_user_functions.lua` | ChainTree workers (paths, arm cycles, recharge…) |
| `runtime_dict/ct_*.lua` | chain-tree dict-runtime |
| `lib/mqtt_pubsub.lua` | MQTT FFI binding (uses libmqtt_pubsub.so) |
| `command_packets.lua`, `link_client.lua`, `mqtt_transport.lua`, `ct_loader_pure.lua`, `fn_registry.lua`, `json_util.lua` | shared infra |

### Test harness

`/opt/dongle_base/test_harness/`:

- `planner_test_peer.lua` — reusable mission-side peer (handshake, RPC,
  done collection)
- `test_mock_planner.lua` — long-running keepalive (thin wrapper over peer)
- `test_random_paths.lua` — scenario generator + runner (`--self-host`
  drives the entire e2e from one process)
- `robot_controller_test_peer.lua` — Phase-2 contract fixture
- `test_robot_controller_contract.lua` — 37/37 self-test of the fixture

## What's NOT here

- `robot_sim/main.c`, `dongle_threads.c`, `drive_base_robot.c` — these
  live in the class image and final-link against this layer's `.a`
  archives. Why: `main.c` `#include`s class headers (e.g.
  `drive_base_robot.h`); the vtable abstraction that would let `main.c`
  move up is a future refactor.
- Per-class `comm_manifest.bin`, `tunables.bin`, `remote.json` — class.
- Class-image manifests (`config.template.json`, `class_processes.json`)
  — class.

## Build

```bash
bash containers/dongle_base/docker_build.sh
```

Multi-stage:

1. **builder** (`FROM ubuntu:24.04`): build-essential, builds
   `lib{comm,physics}.a` and `lib{comm,physics}.so` from staged sources.
2. **runtime** (`FROM mycorp/robot_base:1.0`): apt-installs
   `libmosquitto1`, copies build artifacts, ldconfig.

## Layer size

≈ 167 MB cumulative; ~2 MB delta over `robot_base` (the C archives,
the Lua module set, libmqtt_pubsub).

## Why a separate layer

Initial design (committed `cb1f5bc9`) baked all this into
`lunar_rover-class`. That's wrong: ~95% of what's here is generic
infrastructure that ANY robot class would re-use. Refactor (committed
same day) extracted `dongle_base` so:

- Future classes (`factory_arm-class`, `lidar_pod-class`) only ship
  ~400 KB of unique content.
- The wire protocol + Lua API is owned by ONE image — no drift.
- The test harness is portable: any class image inherits a runnable
  rover-mocking peer.

For real-hardware retargeting (Track B Pico): `dongle_base` defines the
contract (USB CDC-ACM frames, `bus_kernel` ABI, `logical_robot` vtable).
The Pico Zephyr firmware implements that contract; class images don't
change.
