# Pump Station Test — 2-Level Scan Tree with User VFT

## Overview

This test demonstrates the Scan Tree architecture with a 2-level hierarchical
fault model using a user-defined virtual function (`VFT_motor_check`) for
custom motor health evaluation. The scenario models a pump station with power
monitoring and two groups of pumps.

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
| pump_faults | bool | 4 | Pump fault signals (not used in current VFTs) |
| power_status | bool | 2 | Grid power, backup power |
| alarm_clear | bool | 4 | Operator alarm clear signals |
| motor_current | float | 4 | Motor current readings in amps |
| motor_thresholds | float | 4 | Overcurrent thresholds in amps |

## User VFT: motor_health_check

The `motor_health_check` function is a user-defined VFT that compares a pump's
motor current against a threshold. It returns 1 (healthy) if current is below
the threshold, 0 (fault) if at or above.

### DSL Registration (Lua)

```lua
local VFT_motor_check = vft.user_vft("motor_health_check", {
    {name = "current",   type = "float", count = 1},
    {name = "threshold", type = "float", count = 1},
})
```

`vft.user_vft()` takes two arguments:

1. **Function name** — becomes the C symbol `user_vft_motor_health_check`
   (prefixed with `user_vft_`).
2. **Input specification** — an array of input descriptors. Each entry has
   a `name`, `type`, and `count`. The codegen uses these to validate
   instantiation parameters and generate documentation in the
   `{name}_user_vft.h` header.

### DSL Instantiation (Lua)

```lua
dsl:instantiate_vft(VFT_motor_check,
    "group_a_output:0-1",       -- output: 1 bit at position 0
    "motor_current:0-1",        -- input 0: current for pump 0
    "motor_thresholds:0-1")     -- input 1: threshold for pump 0
```

The first argument after the VFT is always the output (`buffer:start-count`).
Subsequent arguments are inputs, matched in order to the input specification.

### C Implementation

```c
#include "scan_tree.h"

uint8_t user_vft_motor_health_check(
    uint8_t *state,
    const st_handle_t *h,
    const st_input_desc_t *inputs,
    uint32_t n_inputs)
{
    (void)state; (void)n_inputs;
    float cur = ((const float*)st_buf_data(h, inputs[0].buf_id))[inputs[0].start];
    float thr = ((const float*)st_buf_data(h, inputs[1].buf_id))[inputs[1].start];
    return (cur < thr) ? 1 : 0;
}
```

**Function signature:** All user VFTs have the same signature:

| Parameter | Purpose |
|-----------|---------|
| `state` | Pointer to a per-node `uint8_t` state byte, persistent across cycles. Used by stateful VFTs (fuse, latch). Not used here. |
| `h` | Handle to the scan tree — provides access to all buffer data. |
| `inputs` | Array of input descriptors. Each has `buf_id` (which buffer), `start` (element offset), `count` (number of elements), and `role`. |
| `n_inputs` | Number of entries in the inputs array. |
| **return** | `uint8_t` — 1 for true/active, 0 for false/fault. Written to the output position. |

**Data access pattern:** `st_buf_data(h, inputs[i].buf_id)` returns a `void*`
to the raw buffer's current data. Cast to the appropriate type and index by
`inputs[i].start` to read the specific element.

### Generated Header

The codegen produces `{name}_user_vft.h` with the prototype and documentation:

```c
/* motor_health_check
 * Inputs:
 *   current (float, count=1)
 *   threshold (float, count=1)
 * Access data: st_buf_data(h, inputs[i].buf_id) + inputs[i].start
 */
uint8_t user_vft_motor_health_check(
    uint8_t *state, const st_handle_t *h,
    const st_input_desc_t *inputs, uint32_t n_inputs);
```

The application implements this function in a separate `.c` file. The linker
resolves the symbol at build time.

## Test Scenario Results

### System Size

```
pump_station: 9 bufs, 9 nodes, 5 raw, 4 layer
```

9 buffers total: 5 raw I/O buffers (pump_faults, power_status, alarm_clear,
motor_current, motor_thresholds) and 4 layer buffers (power_output,
actuation_output, group_a_output, group_b_output). 9 VFT nodes evaluate
per cycle.

### Step 0: Initial

No cycle has run. All layer positions are NOT_OP (grey N).

```
power: [0]=NOT_OP
actuation: [0]=NOT_OP [1]=NOT_OP
group_a: [0]=NOT_OP [1]=NOT_OP [2]=NOT_OP
group_b: [0]=NOT_OP [1]=NOT_OP [2]=NOT_OP
```

The `print_states` output shows the cached `int8_t` state arrays. The
`st_display_tree` output shows the same information in hierarchical form
with Level 1 (actuation) at the top and Level 0 (power) at the bottom.

### Step 1: Power ON, Thresholds = 100A

Grid power set to 1. All motor thresholds set to 100A. Motor currents are
still 0A (initialized by `calloc`). First `st_cycle()` runs.

```
power: [0]=ACTIVE
group_a: [0]=ACTIVE [1]=ACTIVE [2]=ACTIVE
```

**Evaluation path (bottom-up):**

- Level 0: `VFT_or(power_status[0..1])` → grid=1 OR backup=0 → `power_output[0]` = 1 (ACTIVE)
- Level 1, group_a: `user_vft_motor_health_check(current=0, threshold=100)` → 0 < 100 → return 1 (healthy) for each pump. `VFT_or(group_a[0..1])` → 1 OR 1 → `group_a[2]` = 1 (ga_ok)
- Level 1, group_b: Same pattern, all healthy.
- Level 1: `VFT_copy(group_a[2])` → `actuation[0]` = 1 (pumps_ok). `VFT_copy(power[0])` → `actuation[1]` = 1 (has_power).

### Step 2: Pump 0 Overcurrent (150A)

`motor_current[0]` set to 150A. Threshold is 100A.

```
group_a: [0]=FAULT [1]=ACTIVE [2]=ACTIVE
actuation: [0]=ACTIVE [1]=ACTIVE
```

`user_vft_motor_health_check(current=150, threshold=100)` → 150 < 100 is
false → return 0 (FAULT). `group_a[0]` goes red F.

But `group_a[2]` (ga_ok) stays ACTIVE because it's computed by
`VFT_or(group_a[0], group_a[1])` → 0 OR 1 → 1. Pump 1 is still healthy,
so the group overall is OK. This propagates to `actuation[0]` = ACTIVE.

The fault is visible at the individual pump level but does not propagate
to the group or system level because of the OR aggregation — the station
can operate with at least one healthy pump per group.

### Step 3: Pump 0 Current Normal (50A)

`motor_current[0]` drops to 50A. 50 < 100 → return 1. `group_a[0]` recovers
to ACTIVE. Unlike a fuse, the user VFT has no memory — it re-evaluates fresh
each cycle. Recovery is immediate.

### Step 4: Power OFF

`power_status[0]` set to 0. Both grid and backup are now 0.

```
power: [0]=FAULT
actuation: [0]=ACTIVE [1]=FAULT
```

`VFT_or(power_status[0..1])` → 0 OR 0 → 0. `power_output[0]` = FAULT.
This propagates via `VFT_copy` to `actuation[1]` (has_power) = FAULT.

Note that `actuation[0]` (pumps_ok) remains ACTIVE — the pumps themselves
are healthy. The fault model correctly separates "equipment health" from
"infrastructure prerequisite." An operator can see that the pumps are fine
but power is the problem.

### Step 5: Power Back ON

`power_status[0]` set to 1. Grid power restored.

```
power: [0]=ACTIVE
actuation: [0]=ACTIVE [1]=ACTIVE
```

Full recovery. The scan tree is stateless for these VFTs — power comes back,
everything propagates to green in a single cycle.

## Key Observations

1. **User VFT simplicity:** The motor health check is 4 lines of C. The scan
   tree engine handles all the buffer routing, state tracking, and hierarchy.

2. **Stateless evaluation:** Without fuses or latches, faults clear immediately
   when the condition resolves. Each cycle is a fresh computation.

3. **Fault isolation:** The OR aggregation at the group level means a single
   pump fault doesn't take down the group. The display shows exactly where
   the fault is without requiring the operator to drill into logs.

4. **Separation of concerns:** Power status and pump health are evaluated in
   separate levels. The `has_power` pin at the actuation level combines them,
   making it clear whether a failure is equipment or infrastructure.

5. **Cached pointer access:** The test caches `ST_RAW_PTR` and
   `st_layer_states` pointers once at init. In the main loop, reading pump
   health is a direct array access — no function calls, no hash lookups.

