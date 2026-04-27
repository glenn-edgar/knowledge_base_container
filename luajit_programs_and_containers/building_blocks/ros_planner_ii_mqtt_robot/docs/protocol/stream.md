# Stream events

Robot → planner, on `…/robots/<id>/stream_bus`. Three event types.

## `ack`

Sent immediately after the controller validates an incoming RPC packet
type:

```json
{ "type": "ack", "seq": 17, "test_id": 2017, "status": "ok" }
```

`status` is currently always `"ok"` for accepted packets. Unknown
packet types get a `kb_done` failure instead.

## `heartbeat`

Three phases per worker:

| `phase` | When |
|---|---|
| `initial`   | once, on worker activation |
| `periodic`  | every 10 ticks (1 s sim) while running |
| `final`     | once, on completion (just before `kb_done`) |

Shape:

```json
{
  "type": "heartbeat",
  "phase": "periodic",
  "test_id":  2017,
  "delta_x":   1.95, "delta_y":   0.02, "delta_z":  0,
  "delta_heading":   0.01,
  "delta_arm_angle": 0,
  "global_x":  1.95, "global_y":  0.02, "global_z": 0,
  "global_heading":   0.01,
  "global_arm_angle": 0,
  "watchdog_ticks":   8,
  "worker": "worker_path_line",
  "sim_t":  1.6
}
```

`delta_*` are per-packet (since worker activation), `global_*` are
since boot — the controller folds completed packet deltas into
`global_pos` after each `kb_done`.

## `kb_done`

Terminal event for a worker:

```json
{
  "type":             "kb_done",
  "test_id":           2017,
  "success":           true,
  "delta_x":           2.00, "delta_y": 0.00, "delta_z": 0,
  "delta_heading":     0,
  "delta_arm_angle":   0,
  "energy_remaining":  9750,
  "energy_max":       10000,
  "energy_measured":   200,     // joules consumed during this packet
  "sim_t":             4.2
}
```

On failure:

```json
{
  "type":         "kb_done",
  "test_id":      2017,
  "success":      false,
  "fault_reason": "path_fault" | "watchdog_timeout" | "tool_fault" | "charger_fault" | "...",
  ...
}
```

## Status bitmask publish (`status_pub`)

Separate topic, 1 Hz:

```json
{
  "active_kb":   "worker_path_spline",
  "raw_bitmask": 1,
  "fields":      {}
}
```

`raw_bitmask` is whatever the worker last wrote to
`bb["<worker_kb>.bitmask"]`. `fields` is reserved for per-worker
labelled fields (currently empty). See [bitmasks](../workers/bitmasks.md)
for what each worker writes.
