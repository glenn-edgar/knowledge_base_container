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

## Lower-level: drive_base wire opcodes

The mission-level packet types above translate (via
[`robot_controller`](../architecture/controller.md) and
[workers](../workers/index.md)) into per-bus wire opcodes that
robot_sim's drive_base logical_robot dispatches. Opcodes are
class-specific; drive_base's catalogue lives in
`libcomm/drive_base_robot.h` and `drive_base_ffi.lua`.

| Opcode | Name | Payload | Response |
|---|---|---|---|
| `0x1001` | `PUSH_LINE`        | 28 B (7 floats) | ACK |
| `0x1002` | `PUSH_SPLINE`      | 28 B (7 floats) | ACK |
| `0x1003` | `PUSH_ROTATE`      | 12 B (3 floats) | ACK |
| `0x1010` | `STOP`             | empty | ACK |
| `0x1011` | `RESUME`           | empty | ACK |
| `0x1012` | `ABORT`            | empty | ACK |
| `0x1020` | `TELEMETRY_ON`     | empty | ACK |
| `0x1021` | `TELEMETRY_OFF`    | empty | ACK |
| `0x1031` | `GET_TELEMETRY`    | empty | 32 B pose+path+seq+depth+flags |
| `0x1032` | `GET_TOOL_STATUS`  | u8 slot | 28 B mirroring `phys_tool_status_t` |
| `0x1033` | `GET_STATION`      | u8 kind | 4 B i32 station_idx (-1 = none) |
| `0x1040` | `BEGIN_GRIP`       | u8 slot | ACK |
| `0x1041` | `BEGIN_RELEASE`    | u8 slot | ACK |
| `0x1042` | `BEGIN_DOCK`       | u8 slot | ACK |
| `0x1043` | `BEGIN_CHARGE`     | u8 slot, f32 target_j | ACK |
| `0x1044` | `TOOL_MOVE`        | u8 slot, f32 target, f32 speed | ACK |
| `0x1080` | `EVT_TELEMETRY`    | 32 B (event, telemetry-on only) | — |
| `0x1081` | `EVT_SEG_DONE`     | 8 B  (event) | — |
| `0x1082` | `EVT_FAULT`        | tbd | — |

**L6 catalogue (added 2026-05-02):** `GET_TOOL_STATUS`,
`GET_STATION`, `BEGIN_GRIP`, `BEGIN_RELEASE`, `BEGIN_DOCK`,
`BEGIN_CHARGE`, `TOOL_MOVE`. Mission-side workers
(`deliver_part / paint_sample / load_shipping / recharge`) call into
these via `dongle_hal:begin_grip("gripper")`,
`hal:read_tool_status(2)`, `hal:station_at_pose("charger")`, etc.
Slot name resolution (`"gripper" → 1`, `"charge_port" → 2`) loads
`physics_config.tools` at hal start.

A new robot class adding its own opcodes should:

- Reserve an opcode range that doesn't collide with drive_base
  (e.g. arms use `0x20xx`).
- Mirror the opcode + payload shape on master-side via a
  `<class>_ffi.lua` module.
- Document the new opcodes here or in a sibling `protocol/<class>.md`.
