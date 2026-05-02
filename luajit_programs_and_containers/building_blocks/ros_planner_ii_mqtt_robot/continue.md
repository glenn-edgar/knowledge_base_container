# continue.md — 2026-05-01 end-of-session handoff (L5 closed; container design locked)

**Open here next session.** Two pieces of state to be aware of:

1. **L5 paths_only milestone is achieved** — Linux mission planner drives
   the four-thread dongle architecture end-to-end. Working tree is clean,
   all commits pushed.

2. **Robot container design is locked** in `project_robot_container_design.md`
   memory (six-question design session at end of 2026-05-01).
   **Coding the container is the recommended next session.**

## Verify on resume

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e
# → 306 unit checks across 14 groups, all green
```

E2E (paths_only scenarios green; mixed_* fails are the L6 boundary):

```bash
HAL_MODE=dongle bash run_tests.sh --speed 10 --count 5
```

## Next-session options (in order of recommendation)

### Option 1 — Build the robot container (RECOMMENDED)

Design is fully locked in `project_robot_container_design.md`. Six
questions answered, scope estimated at ~one focused session.

**Two layers to build:**

```
mycorp/luajit_base:1.0           (existing, project_luajit_base_design.md)
  └── mycorp/robot_base:1.0      (new — ChainTree supervisor + ENV
                                  validation + render config + spawn
                                  helpers + upward_peer.lua stub)
       └── mycorp/lunar_rover-class:1.0  (new — libcomm.so + libphysics.so
                                  + robot_sim + Lua modules + tunables.bin
                                  + comm_manifest.bin + remote.json
                                  + config.template.json + class_processes.json)
```

**Runtime invocation:**

```bash
docker run \
  -e ROBOT_ID=rover_3 \
  -e ROBOT_CLASS=lunar_rover \
  -e DONGLE_INSTANCE=3 \
  -e MQTT_HOST=mqtt.fleet.local \
  mycorp/lunar_rover-class:1.0
```

ENV is the only per-instance injection. `tunables.bin` is class-invariant
(physics doesn't change per rover). robot_sim ships in the class image.
Process supervision uses ChainTree (`feedback_no_soft_faults`: any child
death → container exits, Docker restart policy handles re-launch).

**Phase 1 → Phase 2 path:**
- Phase 1 (build now): rover talks to existing MQTT mission planner.
  `upward_peer.lua` is a no-op stub.
- Phase 2 (later, after rover works in containers): bring up TBD
  `robot_controller` service. Rebuild `robot_base:1.1` with real
  `upward_peer.lua` (registration, KB-via-controller, exception sink to
  Postgres). Class image rebuilds atop. Backward compatible via ENV.

**Open the session by reading:** `project_robot_container_design.md` in
memory; that's the imperative design doc. Then start building
`robot_base/Dockerfile` + the supervisor.

### Option 2 — L6 (drive_base tool catalogue)

Add `DRV_CMD_BEGIN_GRIP/RELEASE/DOCK/CHARGE/TOOL_MOVE` to the catalogue
so mixed_* MQTT scenarios pass. ~1 session. Closes the Linux e2e story
on the protocol side. Doesn't block container work; can be done either
order.

### Option 3 — Track B (Pico hardware port)

Architecture is proven on Linux. Hardware needed: Raspberry Pi Pico (~$5)
+ Pi Debug Probe (~$12). Multi-session.

## Recent commit stack (origin/master)

```
907cd431  continue.md: end-of-session 2026-05-01 — L5 closed, L6 next
532c16ae  L5: GET_TELEMETRY polled path closes libcomm gap; paths_only e2e green
f6b9a578  L5.5: dongle_hal.lua skeleton + libcomm gap discovered
41247f23  L5.2 + L5.3: drive_base FFI and tunables blob loader
0babc8f5  drive_base: L5.1b — TELEMETRY_ON/OFF + master-seq attribution
d2c289ac  robot_sim: L4 — four-thread dongle decomposition
a376ebdf  libcomm: L3 — drive_base wired to libphysics
c0342bc2  libcomm: Track A + Track C + L1/L2
```

## Memory pointers

- `project_robot_container_design.md` — locked container design (read first
  if doing Option 1).
- `project_dongle_l1_l5_progress.md` — L1-L5 closure summary.
- `project_dongle_track_c.md` — Track C contract locks (Q1-Q5).
- `project_dongle_architecture.md` — locked architecture.
- `project_luajit_base_design.md` — base-image conventions.
- `feedback_container_build.md` — staging-copy build pattern.
- `feedback_no_soft_faults.md` — fail-stop child supervision.
- `feedback_chaintree_runtime_fixes.md` — gotchas reusing ChainTree.

## Working tree state

Clean. All commits pushed to origin/master. Next session opens
without surprises.
