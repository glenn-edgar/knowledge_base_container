# Workers (`remote_user_functions.lua`)

Each Planner II "virtual node" maps to a worker KB inside the ChainTree
remote, registered through `M.registry`. Workers are thin: a one-shot
`*_INIT` and a periodic `*_MAIN` that runs every `CFL_TIMER_EVENT`.

## Worker → packet-type map

| `packet_type` | Worker KB | Class |
|---|---|---|
| `INIT_CHECK`      | `worker_init_check`     | self-test |
| `PATH_LINE`       | `worker_path_line`      | path |
| `PATH_SPLINE`     | `worker_path_spline`    | path |
| `PATH_ROTATE`     | `worker_path_rotate`    | path |
| `PATH_WALL`       | `worker_path_wall`      | unsupported stub (capabilities exclude) |
| `DELIVER_PART`    | `worker_deliver_part`   | arm cycle (extend → release → retract) |
| `PAINT_SAMPLE`    | `worker_paint_sample`   | arm cycle (extend → hold → retract) |
| `LOAD_SHIPPING`   | `worker_load_shipping`  | arm cycle (extend → grip → retract) |
| `PASS_GATE`       | `worker_pass_gate`      | dwell stub (1.5 s) |
| `INSPECTION_SCAN` | `worker_inspection_scan`| dwell stub (1.0 s) |
| `RECHARGE`        | `worker_recharge`       | dock + charge |
| `IDLE`            | `worker_idle`           | dwell (0.5 s) |
| `OPERATION`       | `worker_operation`      | dwell stub (2.0 s) |

## Worker contract

Every `*_MAIN` filters on `eid == CFL_TIMER_EVENT`. INIT/TERMINATE pass
through with `CFL_CONTINUE`.

Each tick, an active worker:

- Sets `bb.worker_alive = true` (watchdog ping)
- Reads what it needs from the HAL via `bb._hal`
- Returns `CFL_CONTINUE` to keep running, or `CFL_DISABLE` to complete
- Sets `bb.worker_success = true|false`, `bb.fault_reason = "..."`,
  `bb["<worker_kb>.bitmask"] = N` on completion
- Sets per-packet `bb.delta_x / delta_y / delta_heading / delta_arm_angle`

The controller's completion path emits the final `kb_done` from those
blackboard fields.

## Sub-pages

- [Path workers](paths.md) — the three motion workers + `path_wall` stub
- [Arm-cycle workers](arm-cycle.md) — deliver, paint, load_shipping
- [Dwell workers](dwell.md) — pass_gate, inspection_scan, idle, operation
- [Recharge worker](recharge.md) — dock + charge, energy snap-back
- [Init-check worker](init-check.md) — self-test
- [Bitmask conventions](bitmasks.md) — what each worker writes on success
