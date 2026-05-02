#!/usr/bin/env bash
# make-deploy-tarball.sh -- emit a containerless deploy bundle for
# small-RAM Pis (Zero 2, 3B, 5).
#
# What's in the tarball: source-portable everything. C sources +
# headers (rebuilt on target), Lua modules, configs, manifests,
# pre-built tunables.bin, pre-compiled remote.json. Target machine
# rebuilds .so + robot_sim natively via `make` against the included
# Makefile.
#
# What's NOT in the tarball:
#   - No service unit (systemd/runit/etc.). Local environment owns
#     process supervision; this rover doesn't dictate.
#   - No Docker artifacts.
#   - No host-only test harness (test_random_paths/etc.). Tarball is
#     for production deploy; testing is host-side.
#
# Usage:
#   bash make-deploy-tarball.sh                      # out/rover_deploy.tar.gz
#   bash make-deploy-tarball.sh --out /tmp/r.tgz     # custom path
#   bash make-deploy-tarball.sh --class lunar_rover  # which class image to mirror

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT="$SCRIPT_DIR/out/rover_deploy.tar.gz"
CLASS="lunar_rover"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out)   OUT="$2"; shift 2 ;;
        --class) CLASS="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

STAGE="$(mktemp -d -t rover_deploy.XXXXXX)"
ROOT="$STAGE/rover_deploy"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "$ROOT"
mkdir -p "$ROOT/libcomm" "$ROOT/robot_sim" "$ROOT/lua" \
         "$ROOT/lua/runtime_dict" "$ROOT/lua/lib" "$ROOT/native_libs"

echo "=== Staging $CLASS deploy bundle into $ROOT ==="

# ---- C build inputs (rebuilt on target) ----
cp "$SCRIPT_DIR/libcomm/"*.c           "$ROOT/libcomm/"
cp "$SCRIPT_DIR/libcomm/"*.h           "$ROOT/libcomm/"
cp "$SCRIPT_DIR/robot_sim/main.c"            "$ROOT/robot_sim/"
cp "$SCRIPT_DIR/robot_sim/dongle_threads.c"  "$ROOT/robot_sim/"
cp "$SCRIPT_DIR/robot_sim/dongle_skeleton.h" "$ROOT/robot_sim/"
cp "$SCRIPT_DIR/physics_core.c"        "$ROOT/"
cp "$SCRIPT_DIR/physics_pipe.h"        "$ROOT/"

# ---- pre-staged generated artifacts ----
cp "$SCRIPT_DIR/comm_manifest.h"       "$ROOT/"
cp "$SCRIPT_DIR/comm_manifest_bin.h"   "$ROOT/"
cp "$SCRIPT_DIR/comm_manifest.bin"     "$ROOT/"
cp "$SCRIPT_DIR/comm_manifest_ffi.lua" "$ROOT/lua/"
cp "$SCRIPT_DIR/remote.json"           "$ROOT/"

# ---- physics + tunables (target rebuilds tunables.bin via `make tunables`) ----
cp "$SCRIPT_DIR/physics_config.json"            "$ROOT/"
cp "$SCRIPT_DIR/sim_map.json"                   "$ROOT/"
cp "$SCRIPT_DIR/build_drive_base_tunables.lua"  "$ROOT/"

# ---- master-side Lua (mirror dongle_base's lua_modules) ----
for f in mqtt_robot_main.lua mqtt_robot_config.lua robot_hal.lua \
         dongle_hal.lua comm_ffi.lua ct_comm.lua drive_base_ffi.lua \
         physics_ffi.lua robot_controller.lua remote_user_functions.lua \
         capabilities.lua; do
    cp "$SCRIPT_DIR/$f" "$ROOT/lua/"
done

# ---- json_util + chain_tree dict-runtime ----
cp "$BB_ROOT/chain_tree_luajit/lua_dsl/luajit_pipeline/json_util.lua" \
   "$ROOT/lua/"
cp "$BB_ROOT/chain_tree_luajit/runtime_dict/"*.lua \
   "$ROOT/lua/runtime_dict/"

# ---- ros_planner_ii master-side helpers ----
for f in ct_loader_pure.lua fn_registry.lua link_client.lua mqtt_transport.lua; do
    cp "$BB_ROOT/ros_planner_ii/runtime/$f" "$ROOT/lua/"
done
cp "$BB_ROOT/ros_planner_ii/hub_dsl/protocol/command_packets.lua" \
   "$ROOT/lua/"

# ---- mqtt_pubsub: Lua + native lib + C source for target rebuild ----
cp "$BB_ROOT/knowledge_base/mqtt/lib/mqtt_pubsub.lua"   "$ROOT/lua/lib/"
cp "$BB_ROOT/knowledge_base/mqtt/libmqtt_pubsub.so"     "$ROOT/native_libs/"
cp "$BB_ROOT/knowledge_base/mqtt/mqtt_pubsub.c"         "$ROOT/native_libs/"
cp "$BB_ROOT/knowledge_base/mqtt/include/mqtt_pubsub.h" "$ROOT/native_libs/" 2>/dev/null || true

# ---- top-level launcher + Makefile + README ----

cat > "$ROOT/Makefile" <<'MAKEFILE'
# Makefile -- containerless rover deploy.
#
# Targets:
#   make            rebuild libphysics.so + libcomm.so + robot_sim/robot_sim
#   make tunables   regenerate tunables.bin from physics_config.json
#   make pubsub     rebuild libmqtt_pubsub.so against target's libmosquitto-dev
#   make clean      remove build outputs
#
# Pre-req on the target Pi:
#   sudo apt install -y luajit libmosquitto-dev build-essential

CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Wno-unused-parameter -std=c99
LDFLAGS = -shared -lm

LIBPHYSICS = libphysics.so
LIBCOMM    = libcomm.so
ROBOT_SIM  = robot_sim/robot_sim

LIBCOMM_SRCS = libcomm/comm.c libcomm/frame.c libcomm/transport_inproc.c \
               libcomm/transport_uart.c libcomm/ext_bus_linux_pty.c \
               libcomm/manifest.c libcomm/router.c libcomm/link.c \
               libcomm/bus_kernel_linux.c libcomm/logical_robot.c

ROBOT_SIM_SRCS = robot_sim/main.c robot_sim/dongle_threads.c \
                 libcomm/frame.c libcomm/bus_kernel_linux.c \
                 libcomm/logical_robot.c libcomm/drive_base_robot.c

.PHONY: all clean tunables pubsub
all: $(LIBPHYSICS) $(LIBCOMM) $(ROBOT_SIM)

$(LIBPHYSICS): physics_core.c
	$(CC) $(CFLAGS) physics_core.c -o $@ $(LDFLAGS)

$(LIBCOMM): $(LIBCOMM_SRCS)
	$(CC) $(CFLAGS) -Ilibcomm -I. $(LIBCOMM_SRCS) -o $@ $(LDFLAGS) -lpthread

$(ROBOT_SIM): $(ROBOT_SIM_SRCS) $(LIBPHYSICS)
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(ROBOT_SIM_SRCS) -o $@ \
	    -L. -lphysics -lm -lpthread -Wl,-rpath,'$$ORIGIN/..'

tunables: tunables.bin
tunables.bin: physics_config.json build_drive_base_tunables.lua
	LUA_PATH="lua/?.lua;;" luajit build_drive_base_tunables.lua \
	    physics_config.json tunables.bin

# Rebuild libmqtt_pubsub.so from source (only if the bundled prebuilt
# was for a different arch or libmosquitto major version).
pubsub: native_libs/libmqtt_pubsub.so.local
native_libs/libmqtt_pubsub.so.local: native_libs/mqtt_pubsub.c
	$(CC) -Wall -Wextra -std=c11 -O2 -fPIC \
	    -Inative_libs -shared \
	    native_libs/mqtt_pubsub.c \
	    -o native_libs/libmqtt_pubsub.so.local \
	    -lmosquitto -lpthread
	@echo "Built native_libs/libmqtt_pubsub.so.local"
	@echo "If the prebuilt native_libs/libmqtt_pubsub.so is wrong arch, replace it:"
	@echo "  mv native_libs/libmqtt_pubsub.so.local native_libs/libmqtt_pubsub.so"

clean:
	rm -f $(LIBPHYSICS) $(LIBCOMM) $(ROBOT_SIM) tunables.bin \
	      native_libs/libmqtt_pubsub.so.local
MAKEFILE

cat > "$ROOT/start_rover.sh" <<'STARTSH'
#!/usr/bin/env bash
# start_rover.sh -- containerless rover launcher.
#
# Equivalent to what robot_base's chain-tree supervisor does in the
# container: spawn robot_sim, capture PTY=…/READY from its stdout,
# exec mqtt_robot_main with ROBOT_SIM_PTY set.
#
# Required env:    ROBOT_ID, DONGLE_INSTANCE
# Optional env:    ROBOT_CLASS=lunar_rover, MQTT_HOST=localhost,
#                  MQTT_PORT=1883, VMRT_KB_SITE=moonbase.alpha.surface_ops,
#                  DONGLE_TYPE=1, SLAVE_ADDR=1, SPEED_FACTOR=1.0,
#                  ENERGY_MAX=10000, ENERGY_INFINITE=false,
#                  WIRE_FORMAT=json
#
# Process supervision (systemd, runit, monit) is the local
# environment's responsibility; this script just runs once.

set -euo pipefail
SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SELF_DIR"

: "${ROBOT_ID:?ROBOT_ID is required}"
: "${DONGLE_INSTANCE:?DONGLE_INSTANCE is required}"
: "${ROBOT_CLASS:=lunar_rover}"
: "${MQTT_HOST:=localhost}"
: "${MQTT_PORT:=1883}"
: "${VMRT_KB_SITE:=moonbase.alpha.surface_ops}"
: "${DONGLE_TYPE:=1}"
: "${SLAVE_ADDR:=1}"
: "${SPEED_FACTOR:=1.0}"
: "${HAL_MODE:=dongle}"
: "${ENERGY_MAX:=10000}"
: "${ENERGY_INFINITE:=false}"
: "${WIRE_FORMAT:=json}"

# Build artifacts on first run (idempotent).
[[ -f libphysics.so ]] && [[ -f libcomm.so ]] && [[ -x robot_sim/robot_sim ]] || make
[[ -f tunables.bin ]] || make tunables

mkdir -p run
RUN_DIR="$SELF_DIR/run"
CONFIG="$RUN_DIR/config.json"

# Render the per-instance config from the template-shaped values.
# Plain shell; no lua dep.
cat > "$CONFIG" <<EOF
{
    "robot_id": "$ROBOT_ID",
    "site": "$VMRT_KB_SITE",
    "mqtt_host": "$MQTT_HOST",
    "mqtt_port": $MQTT_PORT,
    "robot_class": "$ROBOT_CLASS",
    "remote_json": "$SELF_DIR/remote.json",
    "physics_dir": "$SELF_DIR",
    "energy_max": $ENERGY_MAX,
    "energy_infinite": $ENERGY_INFINITE,
    "wire_format": "$WIRE_FORMAT",
    "speed_factor": $SPEED_FACTOR,
    "capabilities": [
        "init_check", "path_spline", "path_line",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle"
    ]
}
EOF

# Start robot_sim, capture its first two stdout lines (PTY=… + READY).
mkfifo "$RUN_DIR/sim_out.fifo" 2>/dev/null || true
exec 3< <(./robot_sim/robot_sim --type "$DONGLE_TYPE" \
                                 --instance "$DONGLE_INSTANCE" \
                                 --addr "$SLAVE_ADDR" \
                                 --tunables "$SELF_DIR/tunables.bin" 2>&1)
SIM_PID=$!
trap 'kill $SIM_PID 2>/dev/null; exit 0' INT TERM

PTY=""
READY=0
while read -r line <&3; do
    echo "[sim] $line"
    case "$line" in
        PTY=*) PTY="${line#PTY=}" ;;
        READY) READY=1 ;;
    esac
    [[ -n "$PTY" && $READY -eq 1 ]] && break
done

if [[ -z "$PTY" || $READY -eq 0 ]]; then
    echo "rover: robot_sim never published PTY+READY" >&2
    kill $SIM_PID 2>/dev/null || true
    exit 1
fi

# Hand off to mqtt_robot_main. The Lua process runs until SIGTERM /
# planner-lost / mission completion. When it exits, we kill robot_sim.
LUA_PATH="lua/?.lua;lua/runtime_dict/?.lua;lua/lib/?.lua;./?.lua;;"
LD_LIBRARY_PATH="$SELF_DIR:$SELF_DIR/native_libs:${LD_LIBRARY_PATH:-}"

ROBOT_SIM_PTY="$PTY" \
HAL_MODE="$HAL_MODE" \
LUA_PATH="$LUA_PATH" \
LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    luajit "$SELF_DIR/lua/mqtt_robot_main.lua" "$CONFIG"

RC=$?
kill $SIM_PID 2>/dev/null || true
exit $RC
STARTSH
chmod +x "$ROOT/start_rover.sh"

cat > "$ROOT/README.md" <<EOF
# rover deploy bundle ($CLASS)

Generated by \`make-deploy-tarball.sh\`. Containerless install for
small-RAM Pis. See \`docs/porting/small_pi_deploy.md\` in the source
repo for the full story; this README is the install crib.

## Install

\`\`\`bash
sudo apt install -y luajit libmosquitto-dev build-essential
tar xzf rover_deploy.tar.gz
cd rover_deploy
make                                       # build .so + robot_sim
make tunables                              # bake physics_config.json -> tunables.bin
\`\`\`

If the bundled \`native_libs/libmqtt_pubsub.so\` was built for the
wrong arch (e.g. you bundled on x86_64 but deploy on aarch64),
rebuild it on the target:

\`\`\`bash
make pubsub
mv native_libs/libmqtt_pubsub.so.local native_libs/libmqtt_pubsub.so
\`\`\`

## Run

\`\`\`bash
ROBOT_ID=rover_1 DONGLE_INSTANCE=1 MQTT_HOST=10.0.0.5 ./start_rover.sh
\`\`\`

Local environment is responsible for process supervision (systemd
unit, runit service, etc.) — the rover doesn't ship a service file.

## Footprint

- ~1 MB on disk after build
- ~50 MB RSS at runtime (LuaJIT + robot_sim + libmosquitto)

## Layout

- \`Makefile\`               rebuild rules
- \`start_rover.sh\`         launcher (replaces in-container supervisor)
- \`*.json\`, \`tunables.bin\`, \`remote.json\`  rover config + manifests
- \`libcomm/\`, \`physics_core.c\`, \`robot_sim/\`  C sources, rebuilt by \`make\`
- \`lua/\`                  master-side Lua modules
- \`native_libs/\`          libmqtt_pubsub.so + sources for target rebuild
EOF

# ---- emit ----
mkdir -p "$(dirname "$OUT")"
tar -C "$STAGE" -czf "$OUT" rover_deploy
echo
echo "=== Done ==="
echo "  Bundle: $OUT"
ls -lh "$OUT"
echo
echo "Ship to a Pi:"
echo "  scp $OUT pi@rover-1.local:~"
echo "  ssh pi@rover-1.local 'tar xzf rover_deploy.tar.gz && cd rover_deploy && make && make tunables'"
