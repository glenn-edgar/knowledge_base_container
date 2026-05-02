# Add a new robot class

See **[Adding a new class](../containers/adding_a_new_class.md)** in
the Containers section for the full step-by-step recipe.

This page exists in the Porting section as a pointer because "add a
robot class" IS one of the three port axes. The detailed recipe lives
with the container docs because the work is mostly Dockerfile +
data-file template-copy.

## TL;DR

1. Fork `containers/lunar_rover-class/` → `containers/<your_class>/`.
2. Write `<your_class>_robot.{c,h}` implementing the logical_robot
   vtable, drop into `libcomm/`.
3. Author per-class `physics_config.json`, `sim_map.json`,
   `comm_manifest.lua`, `remote_dsl.lua`, `capabilities.lua`,
   `class_processes.json`, `config.template.json`.
4. Set `ROBOT_CLASS_BAKED=<your_class>` in the new Dockerfile.
5. Build + smoke. The harness in `dongle_base` works unchanged.

## What you don't have to do

- Touch `dongle_base`. The protocol framework + master-side Lua are
  generic.
- Touch `robot_base`. The supervisor is class-agnostic.
- Reimplement test peers. `planner_test_peer` works against any class
  that speaks Planner II MQTT.
