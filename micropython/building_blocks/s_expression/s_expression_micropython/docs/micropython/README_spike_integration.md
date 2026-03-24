# SPIKE Prime Integration

## Target Platform

- LEGO SPIKE Prime hub (STM32F413, ARM Cortex-M4)
- MicroPython v1.19+ (LEGO firmware)
- ~256K heap available
- ~100K flash available for frozen user modules

## Memory Budget

For a typical FLL mission tree (3 runs, ~310 nodes, 5 safety guards per run, spline paths):

### Flash (frozen into firmware)

| Component | Bytecode (est) |
|-----------|---------------|
| `se_runtime_spike.py` (slim engine) | ~10K |
| Mission module data (~310 nodes) | ~62K |
| SPIKE user functions (~15 fns) | ~5K |
| **Total** | **~77K of 100K** |

### Heap (runtime)

| Component | Heap |
|-----------|------|
| `new_module()` function tables | ~4K |
| `new_instance()` node states (310 nodes) | ~25K |
| Blackboard (~20 fields) | ~2K |
| Instance overhead | ~1K |
| **Total** | **~32K of 256K** |
| Per tick | 0 |

## Mission Tree Pattern

FLL missions use a state machine driven by hub button presses:

```python
# Hub button handler sets run_select field
inst["blackboard"]["run_select"] = menu_id

# Engine ticks in main loop
result = se.tick_once(inst, se.SE_EVENT_TICK, None)
```

The DSL structure:

```lua
se_state_machine("run_select", function()
    se_case(0, function() se_nop() end)           -- idle
    se_case(1, function()                          -- run 1
        spike_multi_guard({...}, function()
            se_sequence(
                function() spike_drivebase_reset() end,
                function() spike_drivebase_straight(250) end,
                ...
            )
        end)
        spike_drivebase_stop()
        set_field("run_select", 0)
    end)
    se_case(2, function() ... end)                 -- run 2
    se_case("default", function() se_nop() end)
end)
```

## SPIKE User Functions

Each `spike_*` call in the DSL maps to a user function registered at startup. These are the hardware interface:

| DSL Function | Type | Hardware Action |
|---|---|---|
| `spike_drivebase_straight(mm)` | main | Drive forward/back N mm |
| `spike_drivebase_turn(deg)` | main | Turn N degrees |
| `spike_drivebase_stop()` | oneshot | Stop motors |
| `spike_drivebase_brake()` | oneshot | Emergency brake |
| `spike_drivebase_reset()` | oneshot | Reset odometry |
| `spike_imu_reset_heading()` | oneshot | Zero the gyro |
| `spike_motor_run_target(port, speed, angle)` | main | Run motor to angle |
| `spike_battery_low(threshold)` | pred | Battery below threshold? |
| `spike_comm_timeout(ms)` | pred | BLE comm lost? |
| `spike_tilt_exceeded(deg)` | pred | Hub tilted too far? |
| `spike_bump_detected()` | pred | Accelerometer spike? |
| `spike_emergency_stop()` | oneshot | Kill all motors + LED red |
| `spline_follow(config)` | main | Follow cubic Bezier path |

## Safety Guards

`spike_multi_guard` generates nested `trigger_on_change` nodes that monitor predicates every tick and fire recovery actions on rising edges. Guards run in parallel with the main sequence via `function_interface`.

Example: if battery drops below 6.2V, `spike_emergency_stop()` fires immediately, interrupting whatever sequence step is active.
