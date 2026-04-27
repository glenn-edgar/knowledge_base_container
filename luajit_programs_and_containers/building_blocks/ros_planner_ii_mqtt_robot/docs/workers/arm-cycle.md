# Arm-cycle workers

`deliver_part`, `paint_sample`, `load_shipping` share an `arm_cycle_init`
+ `arm_cycle_main` helper. Each is a state machine over a fixed
sub-sequence of `extend / grip / release / hold / retract` ops.

## Sub-sequences

| Worker | Sequence |
|---|---|
| `deliver_part`  | `extend → release → retract` |
| `paint_sample`  | `extend → hold → retract`    |
| `load_shipping` | `extend → grip → retract`    |

## Init

```lua
local cmd = parse_cmd(bb)               -- bb.command_json → table
bb._sub             = 1
bb._arm_target      = math.rad(cmd.arm_target or 0)
bb._arm_speed       = math.rad(cmd.arm_speed  or 60)
bb._arm_return      = math.rad(cmd.arm_return or 0)
bb._sub_sequence    = { ... }           -- per-worker

-- Snapshot the arm angle for delta accounting
bb._tool_start = hal:read_tool_status(0).value

-- Start the first sub-op
hal:begin_tool_move(0, bb._arm_target, bb._arm_speed)   -- if "extend"
hal:begin_grip(1) / begin_release(1)                    -- as appropriate
```

## Main

Each tick:

1. Read the current sub-op's tool status (slot 0 for arm moves, slot 1
   for grip/release).
2. Update `bb.delta_arm_angle` from the arm tool's current value.
3. **Sub-op done?**  `(flags & TOOL_F.AT_TARGET) != 0`  →  advance.
4. **Hold sub-op:** poll `hal:sim_time() - bb._hold_start >= duration`;
   advance when expired.
5. **Tool fault** (`TOOL_F.FAULT` on the slot) → `bitmask = 0x08`,
   `worker_success = false`, `fault_reason = "tool_fault"`, DISABLE.
6. **End of sequence** → `bitmask = 0x07` (`at_target | gripped | complete`),
   `worker_success = true`, DISABLE.

## Command schema

The planner's RPC packet `params` for these workers:

| Field | Used by | Default |
|---|---|---|
| `arm_target` (deg) | extend → arm angle | 0 |
| `arm_speed`  (dps) | extend speed       | 60 |
| `arm_return` (deg) | retract target     | 0 |
| `hold_time`  (s)   | paint hold         | 3.0 (1.0 in random harness) |
| `payload_type`     | informational      | — |

## Payload coupling

Payload mass coupling is owned by the C plant, not the worker. When
`begin_grip` completes near a `load_dock` station, C snaps
`payload_mass = station.param1`. When `begin_release` completes while
attached, C drops it. The arm-cycle worker just reads `TOOL_F_GRASPED /
TOOL_F_RELEASED` flags to know to advance.
