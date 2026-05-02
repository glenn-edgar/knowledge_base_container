# Containers

The rover ships as a 3-layer Docker image stack. Each layer narrows
scope: the base layers carry generic infrastructure, the leaf layer
carries class-specific data only.

## Layer hierarchy

```
nanodatacenter/luajit-base:latest       (existing, NDC platform)
  └── mycorp/robot_base:1.0             supervisor + ENV gate + render-config
       └── mycorp/dongle_base:1.0       USB protocol + bus + virtual-robot
                                        framework + master-side Lua + harness
            └── mycorp/lunar_rover-class:1.0   class-specific: tunables,
                                               mission state machine,
                                               class manifests, robot_sim
```

Future classes (`factory_arm-class`, `lidar_pod-class`, …) inherit
`dongle_base` and add only **~400 KB** of unique content.

## What goes where

| Concern | Layer | Why |
|---|---|---|
| LuaJIT runtime, chain_tree | luajit-base | Cross-app NDC standard |
| Process supervisor, ENV gate, render-config, upward_peer stub | robot_base | Shared by every robot class |
| `libcomm.{a,so}`, `libphysics.{a,so}`, master-side Lua, mqtt_pubsub, test harness | dongle_base | Generic USB-protocol + virtual-robot framework |
| `robot_sim` binary, `tunables.bin`, `comm_manifest.bin`, `remote.json`, capabilities, class processes/template | lunar_rover-class | Class-specific physical model + mission |

## Static-link policy

Everything we build statically links into the rover binary:

- `libcomm.a` + `libphysics.a` produced in `dongle_base` builder stage.
- `robot_sim` final-linked in the class image's builder against those
  archives — single binary, no class-time `.so` deps.
- `libcomm.so` + `libphysics.so` ALSO shipped (`/usr/local/lib`,
  `ldconfig`'d) for Lua FFI consumers (`comm_ffi`, `physics_ffi`).
  This is the only `.so`-load path on the master side.
- `libmqtt_pubsub.so` is third-party-shaped (links libmosquitto1) and
  stays dynamically loaded.

## Image arch

The class image's multi-stage Dockerfile rebuilds C from source so the
image arch matches the **build host**:

- WSL2 dev → x86_64 image
- Pi 4 / 5 build → aarch64 image

No prebuilt cross-arch binaries leak through.

## Running

```bash
docker run -d --add-host=host.docker.internal:host-gateway \
    -e ROBOT_ID=rover_1 \
    -e ROBOT_CLASS=lunar_rover \
    -e DONGLE_INSTANCE=1 \
    -e MQTT_HOST=host.docker.internal \
    --name rover_1 \
    mycorp/lunar_rover-class:1.0
```

Required ENV (fail-stop on missing):

| Variable | Example | Notes |
|---|---|---|
| `ROBOT_ID` | `rover_1` | Per-instance identity |
| `ROBOT_CLASS` | `lunar_rover` | Must match `ROBOT_CLASS_BAKED` |
| `DONGLE_INSTANCE` | `1` | Per-instance dongle index |

Optional with defaults: `MQTT_HOST=localhost`, `MQTT_PORT=1883`,
`VMRT_KB_SITE=moonbase.alpha.surface_ops`, `DONGLE_TYPE=1`,
`SLAVE_ADDR=1`, `SPEED_FACTOR=1.0`, `HAL_MODE=dongle`,
`ENERGY_MAX=10000`, `ENERGY_INFINITE=false`, `WIRE_FORMAT=json`.

## Building (in order)

```bash
# 1. NDC base (separate repo / submodule):
cd <ndc_root>/luajit/luajit_base/container && ./docker_build.sh

# 2. robot_base
bash containers/robot_base/docker_build.sh

# 3. dongle_base
bash containers/dongle_base/docker_build.sh

# 4. lunar_rover-class
bash containers/lunar_rover-class/docker_build.sh
```

See per-layer pages for what each build does.
