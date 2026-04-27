# ros_planner_ii_mqtt_robot

A LuaJIT robot process that speaks the **Planner II** MQTT command protocol on
top of a 2-wheel diff-drive **C physics simulator**. Runs as a single process
(`luajit mqtt_robot_main.lua rover_1_config.json`); embeds a 200 Hz inner
control loop inside a `.so` and a 10 Hz ChainTree dispatcher in Lua.

This documentation describes:

- the layered architecture (planner → MQTT → controller → ChainTree workers
  → HAL → C plant),
- the C physics surface (`libphysics.so`),
- the worker model and per-virtual-node behaviour,
- the MQTT wire protocol and link lifecycle,
- the operational scaffolding (configs, build, tests, harness flap-recovery).

## Quick map

| Layer | File(s) | Owner |
|---|---|---|
| Mission tick / config | [mqtt_robot_main.lua](architecture/main-loop.md), `mqtt_robot_config.lua` | this directory |
| Continuous-motion dispatcher | [robot_controller.lua](architecture/controller.md) | this directory |
| ChainTree workers | [remote_user_functions.lua](workers/index.md) | this directory |
| HAL selector | [robot_hal.lua](architecture/hal.md) | this directory |
| FFI wrapper | [physics_ffi.lua](physics/ffi.md) | this directory |
| C plant | [physics_core.c](physics/core.md) → `libphysics.so` | this directory |
| MQTT link / RPC plumbing | `link_client`, `mqtt_pubsub` | `building_blocks/knowledge_base/mqtt` |
| ChainTree DSL → `remote.json` | `remote_dsl.lua`, `remote_mqtt_ct.lua` | this directory |

## Read in this order

1. [Architecture overview](architecture/index.md) — the layer cake, three
   time domains, where each behaviour lives.
2. [Continuous-motion design](architecture/continuous-motion.md) — why the C
   queue is the path of trust and Lua only gates non-motion VNs.
3. [Physics core](physics/core.md) — chassis, motor PID, pure-pursuit,
   Bezier spline, tools, stations.
4. [Workers](workers/index.md) — per-VN ChainTree behaviour.
5. [Protocol](protocol/index.md) — link lifecycle, RPC, stream events.
6. [Operations](operations/index.md) — build, run, test, debug.
