# ros_mission_planner_ii Container -- Continuation Plan

## Session 1 (2026-04-18) -- stage 0: shell, registration-test only

## Purpose

Second application container (after test_app) to populate the DCS
gateway tab UI and exercise node_control's instantiation path with a
third real image. The planner + planner_ui processes are both shells;
real logic gets filled in during the planner_ii port sessions that
follow the admin UI work.

## Processes

1. `planner` (`start_order: 10`) -- lua worker shell. Prints one
   heartbeat line per second to stderr; does no planning. Will
   become the actual mission planner (virtual node graph,
   controlled nodes, 3-phase exceptions per
   `project_planner_architecture.md`).
2. `planner_ui` (`start_order: 30`) -- openresty, internal port
   8080, external per topology. Shell. **Eventually:** mission
   graph viewer, transit paths, energy budgets, exceptions.

## Build graph

```
luajit-base:latest  <-  openresty-base:latest  <-  ros-mission-planner-ii:latest
```

## DSL entries (see nanodatacenter_dcs/provisioning/laptop/)

- `definitions.lua`: new `ros_mission_planner_ii` entry with
  port_spec `planner_ui` at internal 8080.
- `topology.lua`: instance `ros_mission_planner_ii_01` on cpu_01
  with `ports = { planner_ui = 19005 }`.

## Smoke test

```bash
cd building_blocks/ros_mission_planner_ii/container
./docker_build.sh

# Re-run DSL construct + slice + restart DCS (see dcs_console/continue.md
# for the exact sequence).
```

Expected:
- `NODE_READ_OWN_CONFIG` lists `ros_mission_planner_ii_01`.
- Gateway sidebar (`http://localhost:19003/`) shows it as a
  routable UI.
- Opening the tab renders the planner_ui shell page.

## Next (much later)

Populate the planner worker with the ros_planner_ii logic already
built in `building_blocks/ros_planner_ii/`. The UI wiring comes with
that work.
