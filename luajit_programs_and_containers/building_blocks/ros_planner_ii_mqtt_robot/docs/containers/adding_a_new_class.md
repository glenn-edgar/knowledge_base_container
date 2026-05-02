# Adding a new robot class

Template steps for adding `factory_arm-class`, `lidar_pod-class`, etc.
on top of `dongle_base`. Most of the work is class-specific physics +
manifest; the container scaffolding is mechanical.

## What you need to provide

| Artifact | Source | Notes |
|---|---|---|
| `physics_config.json` | hand-written | Chassis, motors, follower, tools, battery |
| `sim_map.json` | hand-written | Stations the class interacts with |
| `comm_manifest.lua` | hand-written | avro_dsl schema for class wire opcodes |
| `<class>_robot.{c,h}` | hand-written | logical_robot implementation |
| `remote_dsl.lua` | hand-written | Mission state machine (compiles to `remote.json`) |
| `capabilities.lua` | hand-written | Verbs the class advertises |
| `config.template.json` | hand-written | `${VAR}` rendered to runtime config |
| `class_processes.json` | hand-written | Tells supervisor which processes to spawn |
| `Makefile.container` | template-copy | Final-link rule (parameterize SRCS) |
| `Dockerfile` | template-copy | Two-stage build atop `mycorp/dongle_base:1.0` |
| `docker_build.sh` | template-copy | Staging-copy from source dir |

## Concrete recipe

1. **Fork the class dir.** Copy `containers/lunar_rover-class/` →
   `containers/factory_arm-class/`. Most files are template-shaped.

2. **Add the logical_robot impl.** Drop `factory_arm_robot.c` + `.h`
   into `libcomm/` (alongside `drive_base_robot.{c,h}`). Implement the
   `logical_robot_vtable_t` interface. Pattern: `init` allocates
   physics handle + applies tunables; `on_msg` switches on opcode and
   calls `phys_*` primitives; `tick` advances physics + emits events;
   `shutdown` releases.

3. **Author the wire catalogue.** Add per-class opcodes to
   `comm_manifest.lua` (avoid colliding with drive_base's `0x10xx`
   range; e.g. arms use `0x20xx`). Generate the `.bin` + `.h` + `_ffi.lua`
   trio.

4. **Mirror master-side.** Add a `class_ffi.lua` (master-side
   constants/builders/decoders) and an HAL adapter that
   `dongle_hal.lua` can dispatch to. For drive-base this is
   `drive_base_ffi.lua` + the dongle_hal tool helpers; arm classes
   would add an analog.

5. **Write workers.** Update `remote_user_functions.lua` (or fork it)
   with class-specific worker functions. Compile the new
   `remote.json` via `remote_dsl.lua`.

6. **Tune `class_processes.json`.** Tell the supervisor what to run.
   The drive-base shape (robot_sim + mqtt_robot) is the common
   pattern; non-mobile classes might drop robot_sim entirely.

7. **Set `ROBOT_CLASS_BAKED`.** In the new class image's Dockerfile.
   Supervisor will fail-stop runtime instances whose `ROBOT_CLASS`
   doesn't match.

8. **Build + smoke.**

   ```bash
   bash containers/factory_arm-class/docker_build.sh
   docker run -d --add-host=host.docker.internal:host-gateway \
       -e ROBOT_ID=arm_1 \
       -e ROBOT_CLASS=factory_arm \
       -e DONGLE_INSTANCE=1 \
       -e MQTT_HOST=host.docker.internal \
       --name arm_1 \
       mycorp/factory_arm-class:1.0
   ```

9. **Add e2e test.** Use the inherited `planner_test_peer` to write
   an arm-specific scenario; the harness lives at
   `/opt/dongle_base/test_harness/` inside the image.

## Pitfalls

- **Slot/station name resolution** — `dongle_hal.lua` loads
  `physics_config.tools` at startup to map names to numeric slots.
  Mission code calls `hal:begin_grip("gripper")`; the resolver looks
  up `gripper → 1`. Make sure your `physics_config.tools[].name`
  values match what your workers reference.
- **Manifest collision** — opcodes are uint16. Reserve a per-class
  range (`0x10xx` drive_base, `0x20xx` arm, …) and DOCUMENT it.
- **`main.c` is currently class-coupled** — `robot_sim/main.c`
  `#include`s `drive_base_robot.h`. For a non-drive class, fork
  `main.c` too (or wait for the vtable abstraction refactor).
- **Manifest binary placement** — `dongle_base` ships header definitions
  but NOT a manifest .bin. Each class generates its own .bin and ships
  it under `/opt/apps/<class>/comm_manifest.bin`.

## Future simplification: the vtable refactor

Today `main.c` is class-coupled because it includes the class header.
A cleaner factoring would expose:

```c
// dongle_base provides this declaration:
extern void class_register_logical_robots(dongle_ctx_t *ctx);

// each class implements it in its own file:
void class_register_logical_robots(dongle_ctx_t *ctx) {
    drive_base_register(ctx, &drive_base_tunables);
}
```

Then `main.c` lives in `dongle_base`, becomes class-agnostic, and
class images only ship the implementation file. Worth doing once a
second class actually exists.
