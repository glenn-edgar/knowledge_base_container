# Pump Station Test — 2-Level Scan Tree with User VFT (LuaJIT)

## Overview

This test demonstrates the Scan Tree LuaJIT runtime with a 2-level hierarchical
fault model using a user-defined virtual function (`VFT_motor_check`) for
custom motor health evaluation. The scenario models a pump station with power
monitoring and two groups of pumps.

This is a direct port of the C version — identical DSL source, identical
evaluation results.

## Architecture

```
Level 1: actuation             — pump system status
  ├── actuation_output [pumps_ok, has_power]
  ├── group_a/                 — pumps 0-1
  │   └── group_a_output [p0_healthy, p1_healthy, ga_ok]
  └── group_b/                 — pumps 2-3
      └── group_b_output [p2_healthy, p3_healthy, gb_ok]

Level 0: power                 — power source status
  └── power_output [power_ok]
```

## Raw I/O Buffers

| Buffer | Type | Size | Description |
|--------|------|------|-------------|
| pump_faults | bool | 4 | Pump fault signals |
| power_status | bool | 2 | Grid power, backup power |
| alarm_clear | bool | 4 | Operator alarm clear signals |
| motor_current | float | 4 | Motor current readings in amps |
| motor_thresholds | float | 4 | Overcurrent thresholds in amps |

## User VFT: motor_health_check

The `motor_health_check` function compares a pump's motor current against a
threshold. Returns 1 (healthy) if current is below threshold, 0 (fault) if
at or above.

### DSL Registration

```lua
local VFT_motor_check = vft.user_vft("motor_health_check", {
    {name = "current",   type = "float", count = 1},
    {name = "threshold", type = "float", count = 1},
})
```

### DSL Instantiation

```lua
dsl:instantiate_vft(VFT_motor_check,
    "group_a_output:0-1",       -- output: 1 bit at position 0
    "motor_current:0-1",        -- input 0: current for pump 0
    "motor_thresholds:0-1")     -- input 1: threshold for pump 0
```

### LuaJIT Implementation

```lua
-- user_functions.lua
local M = {}

function M.user_vft_motor_health_check(state, nid, h, inputs, n_inputs)
    local cur = h:buf_data(inputs[1].buf_id)[inputs[1].start]
    local thr = h:buf_data(inputs[2].buf_id)[inputs[2].start]
    return (cur < thr) and 1 or 0
end

return M
```

**Data access:** `h:buf_data(inputs[i].buf_id)` returns a 0-indexed array.
Index by `inputs[i].start`.

### Passing to Descriptor

```lua
local make_desc = require("pump_station")
local user_funcs = require("user_functions")
local desc = make_desc(user_funcs)
```

## Files

| File | Description |
|------|-------------|
| `pump_test.lua` | DSL source — defines the 2-level tree |
| `pump_station.lua` | Generated descriptor (from codegen_luajit.lua) |
| `user_functions.lua` | User VFT implementation |
| `main.lua` | Test application — 6-step scenario |

## Build

```bash
# From scan_tree_luajit/
./st_build.sh dsl_tests/pump_test/pump_test.lua dsl_tests/pump_test/

# Run test
cd dsl_tests/pump_test && luajit main.lua
```

## Test Scenario Results

### System Size

```
pump_station: 9 bufs, 9 nodes, 5 raw, 4 layer
```

### Step 0: Initial

All NOT_OP — no cycle has run.

```
power: [0]=NOT_OP
actuation: [0]=NOT_OP [1]=NOT_OP
group_a: [0]=NOT_OP [1]=NOT_OP [2]=NOT_OP
group_b: [0]=NOT_OP [1]=NOT_OP [2]=NOT_OP
```

### Step 1: Power ON, Thresholds = 100A

Grid power on. All thresholds 100A, currents 0A. Everything healthy.

```
power: [0]=ACTIVE
group_a: [0]=ACTIVE [1]=ACTIVE [2]=ACTIVE
group_b: [0]=ACTIVE [1]=ACTIVE [2]=ACTIVE
actuation: [0]=ACTIVE [1]=ACTIVE
```

### Step 2: Pump 0 Overcurrent (150A)

150A > 100A threshold → pump 0 faults. Group A still OK (pump 1 healthy, OR).

```
group_a: [0]=FAULT [1]=ACTIVE [2]=ACTIVE
actuation: [0]=ACTIVE [1]=ACTIVE
```

### Step 3: Current Normal (50A)

Immediate recovery — user VFT is stateless, re-evaluates fresh each cycle.

### Step 4: Power OFF

Power sources both 0. Power faults, actuation loses power prerequisite.
Equipment stays healthy — fault model separates equipment from infrastructure.

```
power: [0]=FAULT
actuation: [0]=ACTIVE [1]=FAULT
```

### Step 5: Power Back ON

Full recovery in a single cycle.

## Key Observations

1. **User VFT simplicity:** 4 lines of Lua. The engine handles buffer routing.
2. **Stateless evaluation:** Faults clear immediately when the condition resolves.
3. **Fault isolation:** OR aggregation at group level absorbs single pump faults.
4. **Separation of concerns:** Power and pump health in separate levels.
5. **C parity:** Output matches the C version byte-for-byte.
