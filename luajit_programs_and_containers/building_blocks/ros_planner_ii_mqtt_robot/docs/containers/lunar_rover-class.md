# `mycorp/lunar_rover-class:1.0`

Class-specific image. After the dongle_base extraction, this layer
adds only **~400 KB** unique content on top of `dongle_base`.

## Bakes

`/opt/apps/lunar_rover/`:

| File | Role |
|---|---|
| `robot_sim/robot_sim` | Final-linked binary (main.c + dongle_threads.c + drive_base_robot.c + dongle_base's lib*.a) |
| `tunables.bin` | Generated at build time from `physics_config.json` via `build_drive_base_tunables.lua` |
| `comm_manifest.bin` | Class wire-catalogue blob |
| `comm_manifest_ffi.lua` | Lua mirror of the catalogue |
| `remote.json` | Compiled mission state machine (chain-tree IR) |
| `physics_config.json` | Tool slots + chassis/motor/follower/sensor params |
| `sim_map.json` | Stations (charger, load_dock, paint_fixture, assembly_fixture) |
| `capabilities.lua` | List of mission verbs the rover advertises |
| `config.template.json` | `${VAR}` placeholders rendered to `/run/robot/config.json` at boot |
| `class_processes.json` | Tells `robot_base` supervisor what to spawn (robot_sim + mqtt_robot) |

## Identity contract

Class-image-side ENV defaults (set in Dockerfile):

```
ROBOT_CLASS=lunar_rover
ROBOT_CLASS_BAKED=lunar_rover    # immutable; supervisor fail-stops on mismatch
MQTT_HOST=localhost
MQTT_PORT=1883
VMRT_KB_SITE=moonbase.alpha.surface_ops
DONGLE_TYPE=1
SLAVE_ADDR=1
SPEED_FACTOR=1.0
HAL_MODE=dongle
ENERGY_MAX=10000
ENERGY_INFINITE=false
WIRE_FORMAT=json
```

Required at `docker run`:

- `ROBOT_ID` — per-instance identity
- `DONGLE_INSTANCE` — per-instance dongle index

## Build

```bash
bash containers/lunar_rover-class/docker_build.sh
```

Multi-stage:

1. **builder** (`FROM mycorp/dongle_base:1.0`): apt-installs
   build-essential + luajit, final-links `robot_sim/robot_sim` from
   class C against `/opt/dongle_base/lib/lib{comm,physics}.a`,
   bakes `tunables.bin`.
2. **runtime** (`FROM mycorp/dongle_base:1.0`): copies build artifacts
   into `/opt/apps/lunar_rover/`, prepends class app dir to `LUA_PATH`,
   sets `ROBOT_CLASS_BAKED=lunar_rover`.

## Why class-specific only

The split was driven by the realization that "drive_base_robot.c +
tunables + manifest + remote.json + capabilities" are the only truly
class-varying inputs. Everything else (USB protocol, comm framework,
physics, supervisor, test harness) is generic and lives in `dongle_base`.

This lets:

- `factory_arm-class`, `lidar_pod-class`, etc. inherit from `dongle_base`
  with their own slim deltas.
- Drive-base wire-protocol updates (e.g. L6 tool catalogue) land in
  `dongle_base` artifacts ONCE.
- Per-instance identity stays out of the image (ENV at runtime, not
  baked).
