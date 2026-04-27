# Worker status bitmasks

Each worker writes a per-KB status bitmask to
`bb["<worker_kb>.bitmask"]` at completion. The main loop publishes this
to the robot's `status_pub` topic at 1 Hz so the planner has a coarse
state snapshot independent of stream events.

## Per-worker conventions (success path)

| Worker | Mask | Bits set |
|---|---|---|
| `worker_init_check` | `0x0F` | battery, motors, sensors, comms |
| `worker_path_line` / `worker_path_spline` / `worker_path_rotate` | `0x01` | seg_complete |
| `worker_deliver_part` / `worker_paint_sample` / `worker_load_shipping` | `0x07` | at_target, gripped, complete |
| `worker_pass_gate` | `0x0F` | (all four bits) |
| `worker_inspection_scan` | `0x01` | reading_ready |
| `worker_recharge` | `0x02` | charge_complete |
| `worker_idle` | `0x01` | parked |
| `worker_operation` | `0x01` | action_complete |

## Per-worker conventions (failure path)

| Worker | Mask | Meaning |
|---|---|---|
| `worker_path_*` | `0x04` | motor_fault (cross-track abort or rate fault) |
| `worker_path_wall` | `0x04` | unsupported, always fails |
| arm-cycle workers | `0x08` | arm_fault |
| `worker_recharge` | `0x04` | charger_fault |

## How the publish surfaces it

```
on tick % 10 == 0 (1 Hz):
    raw = bb[bb.active_worker .. ".bitmask"] or 0
    publish_bitmask(status_pub, site, robot_id, active_worker, raw, fields)
```

The `fields` table is empty today — the protocol allows a per-worker
field map to be added later, e.g. `{ batt_pct = 87, payload = 1 }`.
