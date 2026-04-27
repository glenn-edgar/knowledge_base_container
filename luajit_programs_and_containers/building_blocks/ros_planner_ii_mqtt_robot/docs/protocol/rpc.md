# Command packets (RPC)

Planner → robot, on `…/robots/<id>/rpc`.

## Generic shape

```json
{
  "packet_type": <int>,
  "seq":         <int>,        // monotonically increasing per planner
  "test_id":     <int>,        // round-trip identifier, echoed on kb_done
  "params":      { ... },      // packet-specific, see below
  "energy":      <int>         // optional planner-side budget hint
}
```

## `packet_type` table

| ID  | Worker | Use |
|----:|---|---|
| 1   | `init_check`     | self-test |
| 2   | `path_spline`    | move along a Bezier with given headings |
| 3   | `path_line`      | move along a line |
| 4   | `path_wall`      | (unsupported in sim) |
| 5   | `path_rotate`    | rotate in place |
| 6   | `deliver_part`   | extend → release → retract |
| 7   | `paint_sample`   | extend → hold → retract |
| 8   | `load_shipping`  | extend → grip → retract |
| 9   | `pass_gate`      | dwell (1.5 s) |
| 10  | `inspection_scan`| dwell (1.0 s) |
| 11  | `idle`           | dwell (0.5 s) |
| 12  | `recharge`       | dock + charge |
| —   | `operation`      | dwell (2.0 s) |
| 255 | shutdown         | sets `bb.shutdown_requested = true` |

(IDs from `command_packets.lua` in the shared MQTT base.)

## Path packet `params`

```json
{
  "from_x": 0.0, "from_y": 0.0,
  "to_x":   2.0, "to_y":   0.0,
  "from_heading": 0.0,
  "to_heading":   0.0,
  "speed":  0.5
}
```

For `path_rotate`, `params` carries `from_heading`, `to_heading`,
`rate` (rad/s).

## Arm-cycle packet `params`

| Field | Type | Used by | Default |
|---|---|---|---|
| `arm_target` | deg | extend stage | 0 |
| `arm_speed`  | dps | move speed   | 60 |
| `arm_return` | deg | retract target | 0 |
| `hold_time`  | s   | paint hold     | 1.0 (random harness) / 3.0 (default) |
| `payload_type` | int | informational | — |

## Recharge packet

```json
{
  "packet_type": 12,
  "params": { "target_energy": 100000 }
}
```

`target_energy` is in joules. Falls back to `battery_capacity_j` if
omitted. The worker errors out if the robot isn't currently at a
charger station.

## Inspection scan packet

```json
{ "params": { "sensor_port": 1, "sensor_type": 2 } }
```

Recorded but not yet acted on — the worker is dwell-only today.

## ACK protocol

Every recognised packet gets an immediate `ack` published to
`stream_bus` (not the rpc topic) before any work happens:

```json
{ "type": "ack", "seq": 17, "test_id": 2017, "status": "ok" }
```

Unknown `packet_type` gets a synthetic `kb_done` with
`success = false` and `fault_reason = "unknown_packet_type"` instead
of an ack.
