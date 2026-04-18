# robot_manager Container -- Continuation Plan

## Session 1 (2026-04-18) -- stage 0: shell, registration-test only

## Purpose

Third application container (after test_app and ros_mission_planner_ii)
to populate the DCS gateway tab UI and exercise node_control's
instantiation path. Both processes are shells; real fleet-management
logic gets filled in later, likely when ros_fleet_manager is
productionized.

## Processes

1. `manager` (`start_order: 10`) -- lua worker shell. Prints one
   heartbeat line per second to stderr; does no fleet management.
   Eventually becomes the robot fleet manager (per-robot heartbeat
   aggregation, mission assignment, zone/battery watch).
2. `manager_ui` (`start_order: 30`) -- openresty, internal port
   8080, external per topology. Shell. **Eventually:** per-robot
   tiles, mission status, exception view.

## Build graph

```
luajit-base:latest  <-  openresty-base:latest  <-  robot-manager:latest
```

## DSL entries (see nanodatacenter_dcs/provisioning/laptop/)

- `definitions.lua`: new `robot_manager` entry with port_spec
  `manager_ui` at internal 8080.
- `topology.lua`: instance `robot_manager_01` on cpu_01 with
  `ports = { manager_ui = 19006 }`.

## Smoke test

```bash
cd building_blocks/robot_manager/container
./docker_build.sh

# Re-run DSL construct + slice + restart DCS (see dcs_console/continue.md
# for the exact sequence).
```

Expected:
- `NODE_READ_OWN_CONFIG` lists `robot_manager_01`.
- Gateway sidebar shows it as a routable UI alongside the other
  containers.
- Opening the tab renders the manager_ui shell page.

## Next (much later)

Populate the manager worker when ros_fleet_manager is ready. The UI
wiring comes with that work.
