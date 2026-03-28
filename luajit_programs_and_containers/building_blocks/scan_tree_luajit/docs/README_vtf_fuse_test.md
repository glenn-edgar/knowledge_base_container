# VFT Fuse Test — 4-Level Water Treatment Plant (LuaJIT)

## Overview

This test demonstrates the Scan Tree LuaJIT runtime with a 4-level hierarchical
fault model using `VFT_fuse` for one-shot trip events requiring operator
acknowledgment. The scenario models a water treatment plant with power, safety,
equipment, process, and plant-level status aggregation.

This is a direct port of the C version — identical DSL source, identical
evaluation results.

### The Fuse Concept

A fuse is a scan tree node that executes an action immediately when a fault
condition is detected — without waiting for the covering behavior tree to
observe the fault and schedule a response. When the trip condition goes true,
the fuse fires its action callback during the same `evaluate()` call that
detected the fault.

## Architecture

```
Level 3: plant_status          — overall plant health
  └── plant_output [operational, full_capacity]

Level 2: process               — intake, treatment, distribution
  ├── process_output [intake_ready, treat_ready, dist_ready]
  ├── intake_ready_check/      — scratch: copies intake_ok + infra_ok, ANDs them
  ├── treat_ready_check/       — scratch: copies dosing_ok + infra_ok, ANDs them
  └── dist_ready_check/        — scratch: copies dist_ok + infra_ok, ANDs them

Level 1: equipment             — pumps, dosing, with fuses
  ├── equip_output [intake_ok, dosing_ok, dist_ok, has_infra]
  ├── intake_pumps/
  │   └── intake_output [p0_overcurrent, p1_overcurrent, p0_fused, p1_fused]
  ├── dosing/
  │   └── dosing_output [cl_high, cl_fused]
  ├── dist_pumps/
  │   └── dist_output [p0_overcurrent, p1_overcurrent, p0_fused, p1_fused]
  ├── intake_agg/              — OR of intake fuse bits
  └── dist_agg/                — OR of dist fuse bits

Level 0: infrastructure        — power + safety interlocks
  ├── infra_output [power_ok, safety_ok, infra_ok]
  └── safety_check/            — OR of safety alarm inputs
```

## Raw I/O Buffers

| Buffer | Type | Size | Description |
|--------|------|------|-------------|
| power_inputs | bool | 3 | Grid, generator, UPS status |
| safety_inputs | bool | 3 | E-stop, high water, gas detection |
| pump_current | float | 4 | Intake P0/P1, dist P0/P1 amps |
| pump_limits | float | 4 | Overcurrent thresholds per pump |
| chlorine_level | float | 1 | Current chlorine ppm |
| chlorine_max | float | 1 | Max allowable chlorine ppm |
| fuse_clear | bool | 5 | Operator clear signals (one per fuse) |

## Fuse Nodes

Five `VFT_fuse` instances, each with an action callback:

| Fuse | Input | Clear | Callback |
|------|-------|-------|----------|
| Intake pump 0 | overcurrent detected | fuse_clear[0] | `on_intake_p0_fuse` |
| Intake pump 1 | overcurrent detected | fuse_clear[1] | `on_intake_p1_fuse` |
| Chlorine dosing | chlorine over-limit | fuse_clear[2] | `on_chlorine_fuse` |
| Dist pump 0 | overcurrent detected | fuse_clear[3] | `on_dist_p0_fuse` |
| Dist pump 1 | overcurrent detected | fuse_clear[4] | `on_dist_p1_fuse` |

### Fuse Action Callbacks in LuaJIT

The generated descriptor stores fuse action names as strings. The test
application converts them to callable functions before creating the Handle:

```lua
for nid, action_name in pairs(desc.fuse_table) do
    desc.fuse_table[nid] = function(user_handle)
        io.write(string.format("  ** FUSE ACTION: %s **\n", action_name))
    end
end
```

## Files

| File | Description |
|------|-------------|
| `vtf_fuse_test_dsl.lua` | DSL source — defines the 4-level tree |
| `vtf_fuse_test.lua` | Generated descriptor (from codegen_luajit.lua) |
| `main.lua` | Test application — 11-step scenario |

## Build

```bash
# From scan_tree_luajit/
./st_build.sh dsl_tests/vtf_fuse_test/vtf_fuse_test_dsl.lua dsl_tests/vtf_fuse_test/

# Run test
cd dsl_tests/vtf_fuse_test && luajit main.lua
```

## Test Scenario Results

### Step 0: Initial

All NOT_OP. 20 buffers, 32 nodes.

### Step 1: Normal Startup

Power on, safety clear, pumps normal, chlorine normal. All systems green
except infrastructure shows one NOT_OP for the safety scratch (correct —
NOR gate chain produces expected states).

### Step 2: Intake Pump 0 Overcurrent (80A > 50A)

Fuse fires: `** FUSE ACTION: on_intake_p0_fuse **`

Intake fuse blown propagates: intake NOT ok → process intake failed → plant
not at full capacity but still operational (2/3 processes work).

### Step 3: Current Drops — Fuse Persists

Overcurrent clears but fuse stays blown. One-shot behavior — requires
operator clear cycle.

### Step 4: Operator Asserts Clear

`fuse_clear[0]` goes high. Fuse transitions to clearing state. Output still 1.

### Step 5: Operator Releases Clear — Fuse Re-arms

`fuse_clear[0]` goes low. Full low→high→low cycle complete. Fuse re-arms.
Full recovery.

### Step 6: Double Fault — Chlorine + Dist Pump 1

Two fuses fire simultaneously. Treatment and distribution fail. Plant still
operational via intake.

### Step 7: Emergency Stop

Safety interlock triggers. Infrastructure fails. All processes down.
Fuses remain blown independently of safety.

### Step 8: E-stop Released + Assert Clears

Safety recovers. Fuse clears asserted — fuses in clearing state.

### Step 9: Release Clears — Full Recovery

All fuses re-arm. Full green.

### Step 10: Total Power Loss

All power sources off. Infrastructure fails. Equipment healthy but no power
prerequisite. Restoring any single power source would recover.

## Key Observations

1. **Fuse persistence:** Blown state survives condition clearing. Models real
   circuit breakers.
2. **Two-phase clear:** Low→high→low prevents accidental re-arming.
3. **Independent fault domains:** Safety and equipment faults are orthogonal.
4. **Graceful degradation:** OR for operational, AND for full capacity.
5. **C parity:** State outputs match the C version exactly.
