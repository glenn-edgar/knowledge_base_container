# Recharge worker

Different from arm-cycle because it relies on a `passive_dock` tool +
charger station. Init verifies the robot is at a charger; main polls
the charger tool's flags until done.

## Init

```lua
local hal = bb._hal
local si  = hal:station_at_pose("charger")
if si < 0 then
    bb._recharge_fault = "not_at_charger"
else
    local target_j = cmd.target_energy
                  or hal:read_tool_status(2).battery_capacity_j
    local rc = hal:begin_charge("charge_port", target_j)
    if rc < 0 then bb._recharge_fault = "charge_begin_failed:" .. rc end
end
```

If we're not at a charger, the next `WKR_RECHARGE_MAIN` tick will fail
the worker before kicking off.

## Main loop

```lua
local ts = hal:read_tool_status(2)   -- charge_port slot

if bb._recharge_fault then
    bitmask = 0x04 (charger_fault); success=false; fault_reason=...
    DISABLE
end
if ts.flags & TOOL_F.AT_TARGET then
    bitmask = 0x02 (charge_complete); success=true
    DISABLE
end
if ts.flags & TOOL_F.FAULT then
    bitmask = 0x04; success=false; fault_reason="charger_fault"
    DISABLE
end
-- else CONTINUE
```

## Energy snap-back at completion

This is the one worker where energy is **not** computed by the
controller's measurement of `energy_used_total` delta. Instead, in
`robot_controller:_after_completion`:

```lua
if bb.current_packet_type == cmd_packets.TYPE_RECHARGE then
    self.energy.remaining = math.floor(self.hal:read_tool_status(2).battery_j)
end
```

The remaining-energy reading is snapped to the actual battery joule
reading. This is so the planner sees a clean refilled value instead of
a "negative drain" inferred from the energy counter.

## Bitmask conventions

| Bit | Meaning |
|---|---|
| `0x01` | (unused) |
| `0x02` | charge_complete |
| `0x04` | charger_fault |

`0x08` and above unused for this worker.
