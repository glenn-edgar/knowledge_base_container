# Scan Tree LuaJIT — Builtin VFT Reference

## Overview

Virtual Function Templates (VFTs) are the computational nodes of the scan tree.
Each VFT reads from input buffers and writes a single boolean (0 or 1) to an
output position in a layer buffer. The engine evaluates all VFTs bottom-up,
level by level, in declaration order within each level.

There are two classes of VFT:

- **System** — implemented in `st_builtins.lua`, referenced by the generated
  descriptor. Covers logic, comparison, state, and protection functions.
- **User** — implemented by the application in a Lua table, passed to the
  descriptor factory. Used for domain-specific evaluation logic.

All VFTs share the same Lua function signature:

```lua
function vft_func(state, node_id, handle, inputs, n_inputs)
    -- return 0 or 1
end
```

| Parameter | Purpose |
|-----------|---------|
| `state` | Per-node state table. `state[node_id]` is a persistent byte. Used by fuse, latch. |
| `node_id` | 1-based node index into the state table. |
| `handle` | Scan tree handle — access to buffer data via `handle:buf_data()`. |
| `inputs` | Array of input descriptors: `{buf_id, start, count, role}`. |
| `n_inputs` | Number of input descriptors. |
| **return** | 0 or 1 — written to the output position in the layer buffer. |

## Input Roles

Inputs are tagged with roles so VFTs can identify which input is which:

| Role | Constant | Value | Used by |
|------|----------|-------|---------|
| DEFAULT | `ROLE_DEFAULT` | 0 | and, or, not, copy, range_check (value) |
| SET | `ROLE_SET` | 1 | latch |
| CLEAR | `ROLE_CLEAR` | 2 | latch, fuse |
| THRESHOLD | `ROLE_THRESHOLD` | 3 | k_of_n |
| BITS | `ROLE_BITS` | 4 | k_of_n |
| A | `ROLE_A` | 5 | gt, ge, eq, lt, le, range_check (low) |
| B | `ROLE_B` | 6 | gt, ge, eq, lt, le, range_check (high) |
| INPUT | `ROLE_INPUT` | 7 | fuse (trip signal) |

## Logic VFTs

### VFT_and

Output = 1 if ALL input bits are 1.

| | |
|---|---|
| **Inputs** | 1+ bool bits (contiguous range) |
| **Output** | 1 bit |
| **State** | Not used |

```lua
dsl:instantiate_vft(vft.VFT_and, "output:0-1", "input_bits:0-4")
```

### VFT_or

Output = 1 if ANY input bit is 1.

| | |
|---|---|
| **Inputs** | 1+ bool bits (contiguous range) |
| **Output** | 1 bit |
| **State** | Not used |

```lua
dsl:instantiate_vft(vft.VFT_or, "output:0-1", "input_bits:0-3")
```

### VFT_not

Output = inverted input.

| | |
|---|---|
| **Inputs** | Exactly 1 bool bit |
| **Output** | 1 bit |
| **State** | Not used |

```lua
dsl:instantiate_vft(vft.VFT_not, "output:0-1", "input:0-1")
```

### VFT_copy

Output = input (pass-through). Used to route a single bit between buffers.

| | |
|---|---|
| **Inputs** | Exactly 1 bool bit |
| **Output** | 1 bit |
| **State** | Not used |

```lua
dsl:instantiate_vft(vft.VFT_copy, "output:0-1", "source:3-1")
```

## Comparison VFTs

All comparison VFTs read two values via roles A and B. Output is 1 bit.

### VFT_gt

Output = 1 if A > B.

```lua
dsl:instantiate_vft(vft.VFT_gt, "result:0-1", "current:0-1", "threshold:0-1")
```

### VFT_ge

Output = 1 if A >= B.

```lua
dsl:instantiate_vft(vft.VFT_ge, "result:0-1", "value:0-1", "minimum:0-1")
```

### VFT_eq

Output = 1 if A == B. (Exact equality — use with caution for floats.)

```lua
dsl:instantiate_vft(vft.VFT_eq, "result:0-1", "measured:0-1", "expected:0-1")
```

### VFT_lt

Output = 1 if A < B.

```lua
dsl:instantiate_vft(vft.VFT_lt, "result:0-1", "temperature:0-1", "max_temp:0-1")
```

### VFT_le

Output = 1 if A <= B.

```lua
dsl:instantiate_vft(vft.VFT_le, "result:0-1", "pressure:0-1", "limit:0-1")
```

### VFT_range_check

Output = 1 if low <= value <= high. Three inputs.

| Role | Parameter |
|------|-----------|
| DEFAULT | Value to check |
| A | Low bound |
| B | High bound |

```lua
dsl:instantiate_vft(vft.VFT_range_check,
    "in_range:0-1", "pressure:0-1", "pressure_low:0-1", "pressure_high:0-1")
```

## Voting VFT

### VFT_k_of_n

Output = 1 if at least K of N input bits are 1. K is read from a buffer
at runtime.

| Role | Parameter |
|------|-----------|
| THRESHOLD | K value (1 element) |
| BITS | N input bits (contiguous range, 2+) |

```lua
dsl:instantiate_vft(vft.VFT_k_of_n,
    "quorum_ok:0-1", "quorum_threshold:0-1", "voter_bits:0-5")
```

## Stateful VFTs

### VFT_latch

Set/reset latch. Clear takes priority over set.

| Role | Parameter |
|------|-----------|
| SET | Set signal (1 bit) |
| CLEAR | Clear signal (1 bit) |

**State:** 0 = reset, 1 = set.

**Behavior:**
- Clear high → state = 0 (regardless of set)
- Set high (clear low) → state = 1
- Both low → hold previous state

```lua
dsl:instantiate_vft(vft.VFT_latch,
    "alarm_latched:0-1", "alarm_trigger:0-1", "alarm_clear:0-1")
```

### VFT_fuse

One-shot trip with operator clear cycle. Fires an action callback immediately
when the trip condition is detected.

| Role | Parameter |
|------|-----------|
| INPUT | Trip signal (1 bit) |
| CLEAR | Clear signal (1 bit) |

**State machine (3 states):**

```
State 0 (intact):   input high → blow, fire callback, output=1 → state 1
State 1 (blown):    output=1, clear high → state 2
State 2 (clearing): output=1, clear low → re-arm, output=0 → state 0
```

The clear cycle requires a full low→high→low sequence.

**DSL:**
```lua
dsl:instantiate_vft(vft.VFT_fuse,
    "pump_fused:0-1", "overcurrent:0-1", "fuse_clear:0-1", "on_pump_fuse")
```

**Action callbacks** in LuaJIT are resolved at runtime. The generated
descriptor stores action names as strings in `desc.fuse_table`. The test
application converts these to callable functions before creating the Handle:

```lua
for nid, action_name in pairs(desc.fuse_table) do
    desc.fuse_table[nid] = function(user_handle)
        print("FUSE: " .. action_name)
    end
end
```

## User-Defined VFTs

### DSL Registration

```lua
local VFT_my_check = vft.user_vft("my_check", {
    {name = "measured", type = "float", count = 1},
    {name = "setpoint", type = "float", count = 1},
})
```

### DSL Instantiation

```lua
dsl:instantiate_vft(VFT_my_check,
    "result:0-1", "sensor_reading:0-1", "target_value:0-1")
```

### Lua Implementation

```lua
-- user_functions.lua
local M = {}

function M.user_vft_my_check(state, nid, h, inputs, n_inputs)
    local measured = h:buf_data(inputs[1].buf_id)[inputs[1].start]
    local setpoint = h:buf_data(inputs[2].buf_id)[inputs[2].start]
    return (measured >= setpoint * 0.9 and measured <= setpoint * 1.1) and 1 or 0
end

return M
```

**Data access pattern:** `h:buf_data(inputs[i].buf_id)` returns a 0-indexed
array. Index by `inputs[i].start` to read the specific element.

### Passing User VFTs to the Descriptor

```lua
local make_desc = require("my_system")
local user_funcs = require("user_functions")
local desc = make_desc(user_funcs)
```

The factory function wires user VFT references into the node descriptors.

## DSL Parameter Format

All VFT instantiation parameters use `"buffer_name:start-count"`:

- **buffer_name** — name defined by `dsl:define_buffer()`
- **start** — zero-based element offset
- **count** — number of elements

Examples:
- `"power_status:0-2"` — 2 bits starting at position 0
- `"motor_current:3-1"` — 1 value at position 3
- `"output:0-1"` — single output bit at position 0
