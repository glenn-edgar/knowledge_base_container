# Containerless deploy on small-RAM Pis

For Pi Zero 2 (512 MB), Pi 3B (1 GB), Pi 5 (1 GB or larger), where
running Docker eats half your headroom. The source tree builds
natively without modification; this page documents how to extract a
minimal install bundle.

## Footprint

| Item | Native size |
|---|---|
| `libphysics.so` | ~70 KB |
| `libcomm.so` | ~75 KB |
| `robot_sim/robot_sim` | ~75 KB |
| `libmqtt_pubsub.so` | ~30 KB |
| Lua source set | ~250 KB |
| Total deploy bundle | **< 1 MB** |
| Runtime RSS | ~50 MB (LuaJIT process + robot_sim + mosquitto) |

Easily fits a Pi Zero 2 with all of Pi OS Lite still loaded.

## Build the tarball

Top-level helper:

```bash
bash make-deploy-tarball.sh                       # default: out/rover_deploy.tar.gz
bash make-deploy-tarball.sh --out /tmp/rover.tgz  # custom path
```

The script reuses the staging logic from
`containers/dongle_base/docker_build.sh` + `lunar_rover-class/`. It
does NOT call docker. Output is a self-contained tarball with:

```
rover_deploy/
├── README.md                  (install instructions)
├── Makefile                   (target-side rebuild rule)
├── start_rover.sh             (the launcher)
├── physics_config.json
├── sim_map.json
├── tunables.bin               (pre-built; rebuild with luajit on target)
├── comm_manifest.bin
├── remote.json
├── libcomm/                   (C sources + headers, for target rebuild)
├── physics_core.c, physics_pipe.h
├── robot_sim/{main.c, dongle_threads.c, dongle_skeleton.h}
├── lua/
│   ├── (rover-side modules: mqtt_robot_main, dongle_hal, …)
│   └── lib/mqtt_pubsub.lua
└── native_libs/
    └── libmqtt_pubsub.so      (rebuilt on target if arch differs)
```

## Install on target

```bash
# From the dev machine, send the tarball to the Pi:
scp out/rover_deploy.tar.gz pi@rover-1.local:~

# On the Pi:
ssh pi@rover-1.local
sudo apt install -y luajit libmosquitto-dev build-essential
tar xzf rover_deploy.tar.gz
cd rover_deploy
make                                   # rebuilds .so + robot_sim
luajit build_drive_base_tunables.lua physics_config.json tunables.bin
```

Local environment (systemd, runit, monit, whatever you use) is
**your responsibility**. The deploy tarball does NOT install a service
unit — different fleets handle process supervision differently and
the rover doesn't care.

A reasonable hand-launch is:

```bash
ROBOT_ID=rover_1 \
DONGLE_INSTANCE=1 \
MQTT_HOST=10.0.0.5 \
    ./start_rover.sh
```

`start_rover.sh` does the same thing as the in-container supervisor:
spawns `robot_sim`, captures `PTY=…/READY`, exec's `mqtt_robot_main`
with `ROBOT_SIM_PTY=` set. ~30 lines of bash.

## When to use containers vs. tarball

| Scenario | Recommendation |
|---|---|
| Dev / laptop / WSL2 / CI | Containers. The 50 MB image overhead is invisible. |
| Pi 4 / 8 GB+ deploy | Containers. Headroom isn't a constraint. |
| Pi Zero 2 (512 MB) | Tarball. Containers eat half the RAM. |
| Pi 3B (1 GB) | Tarball if you want headroom for telemetry tools, otherwise either. |
| Real hardware (Pico) | Neither — the dongle runs Zephyr; the rover Lua might run on a small Pi co-located with the Pico. Tarball on the Pi. |

## Multi-rover on one Pi

Multiple `start_rover.sh` invocations each take their own
`ROBOT_ID` + `DONGLE_INSTANCE`. They share the broker. With ~25 MB
RSS each, a Pi 5 can host a small fleet directly.
