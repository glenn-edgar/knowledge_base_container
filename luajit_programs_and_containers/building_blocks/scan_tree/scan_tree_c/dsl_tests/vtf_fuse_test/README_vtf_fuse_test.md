# VFT Fuse Test — 4-Level Water Treatment Plant

## Overview

This test demonstrates the Scan Tree architecture with a 4-level hierarchical
fault model using `VFT_fuse` for one-shot trip events requiring operator
acknowledgment. The scenario models a water treatment plant with power, safety,
equipment, process, and plant-level status aggregation.

### The Fuse Concept

A fuse is a scan tree node that executes an action immediately when a fault
condition is detected — without waiting for the covering behavior tree to
observe the fault and schedule a response. In a conventional behavior tree
architecture, a fault must propagate up through the tree, be observed by a
decision node, and then trigger an action on the next tick. This introduces
latency proportional to tree depth.

The fuse bypasses this delay. When the trip condition goes true, the fuse
fires its action callback during the same `st_evaluate()` call that detected
the fault. The behavior tree still sees the fault propagate upward through
the normal layer mechanism — but the critical action (emergency shutdown,
actuator disable, alarm notification) has already been taken.

This models real-world protection systems where the safety response must be
faster than the control loop. A physical circuit breaker doesn't wait for the
PLC to notice the overcurrent — it trips immediately. The fuse VFT brings
that same pattern into the scan tree: immediate action at the detection point,
with the full hierarchical fault model running in parallel for situational
awareness and operator display.

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

### Fuse State Machine

Each fuse has three internal states stored in the per-node state byte:

- **0 (intact):** Armed. If input goes high → blow fuse, fire callback, output = 1, transition to state 1.
- **1 (blown):** Output = 1. Waiting for clear signal to go high → transition to state 2.
- **2 (clearing):** Output = 1. Waiting for clear signal to go low → re-arm, output = 0, transition to state 0.

The clear cycle requires a full low→high→low sequence. This prevents accidental re-arming from a stuck clear signal.

## Files

| File | Description |
|------|-------------|
| `vtf_fuse_test.lua` | LuaJIT DSL source — defines the 4-level tree |
| `vtf_fuse_test.h` | Generated const data tables (20 buffers, 32 nodes) |
| `vtf_fuse_test_fuse_actions.h` | Generated fuse callback prototypes |
| `vtf_fuse_test.c` | Test application — 11-step scenario |
| `Makefile` | Build rules |

## Build

```bash
make
./vtf_fuse_test
```

To regenerate from DSL:

```bash
make generate
make
```

## Test Scenario Results

### Step 0: Initial

All positions show N (NOT_OP). No evaluation cycle has run.

```
plant_status.plant_output [N N]
process.process_output [N N N]
equipment.equip_output [N N N N]
infrastructure.infra_output [N N N]
```

### Step 1: Normal Startup

Power on (grid + generator + UPS), no safety alarms, all pump currents normal
(10-12A vs 50A limits), chlorine at 2 ppm (limit 5 ppm).

```
plant_status.plant_output [T T]              ← operational, full capacity
equipment.intake_pumps.intake_output [F F F F] ← no overcurrent, no fuses blown
infrastructure.infra_output [T T T]          ← power ok, safety ok, infra ok
```

All equipment sub-layer `F` values are correct: they represent overcurrent
detection and fuse-blown states. `F` means "no fault detected." The NOT gates
in the aggregation layer invert these into `equip_output [T T T T]`.

### Step 2: Intake Pump 0 Overcurrent (80A > 50A)

The `VFT_gt` node detects overcurrent → `intake_output[0]` = T. The `VFT_fuse`
trips → `intake_output[2]` = T, callback fires: `** FUSE: Intake pump 0 tripped! **`

```
plant_status.plant_output [T F]              ← operational (2/3 processes), not full
process.process_output [F T T]               ← intake failed, treat+dist ok
equipment.equip_output [F T T T]             ← intake NOT ok
equipment.intake_pumps.intake_output [T F T F] ← p0 overcurrent + p0 fuse blown
```

Fault propagation: fuse blown → intake_scratch T → NOT → equip_output[0] F →
intake_rdy_scratch[0] F → process_output[0] F → plant full_capacity F.

### Step 3: Current Drops — Fuse Persists

Pump 0 current returns to 10A. Overcurrent clears (`intake_output[0]` = F).
But the fuse stays blown (`intake_output[2]` = T) — this is the one-shot
behavior. The fuse state byte remains 1 (blown) regardless of input.

```
equipment.intake_pumps.intake_output [F F T F] ← overcurrent gone, fuse STILL blown
equipment.equip_output [F T T T]               ← intake still NOT ok
```

### Step 4: Operator Asserts Clear

`fuse_clear[0]` goes high. Fuse state transitions from 1 (blown) to 2 (clearing).
Output remains 1 — the fuse stays blown until the clear signal cycles back low.

```
equipment.intake_pumps.intake_output [F F T F] ← no change yet
```

### Step 5: Operator Releases Clear — Fuse Re-arms

`fuse_clear[0]` goes low, completing the low→high→low cycle. Fuse state
transitions from 2 (clearing) to 0 (intact). Output goes to 0. Full recovery.

```
plant_status.plant_output [T T]              ← full capacity restored
equipment.intake_pumps.intake_output [F F F F] ← all clear
```

### Step 6: Double Fault — Chlorine + Dist Pump 1

Chlorine spikes to 8 ppm (>5 ppm limit) AND dist pump 1 hits 70A (>50A limit).
Two fuse callbacks fire simultaneously:

```
** FUSE: Chlorine dosing high alarm! **
** FUSE: Distribution pump 1 tripped! **
```

```
plant_status.plant_output [T F]              ← operational (intake works), not full
process.process_output [T F F]               ← treat+dist failed
equipment.equip_output [T F F T]             ← dosing NOT ok, dist NOT ok
equipment.dosing.dosing_output [T T]         ← chlorine high + fuse blown
equipment.dist_pumps.dist_output [F T F T]   ← pump 1 overcurrent + fuse blown
```

### Step 7: Emergency Stop

E-stop pressed. Safety scratch goes T → NOT → safety_ok F → infra_ok F.
Infrastructure failure propagates to all process stages.

```
plant_status.plant_output [F F]              ← nothing operational
infrastructure.infra_output [T F F]          ← power ok, safety FAIL, infra FAIL
equipment.equip_output [T F F F]             ← has_infra = F
```

Note: fuses at equipment level remain blown. E-stop does not clear fuses —
they are independent of the safety interlock.

### Step 8: E-stop Released + Assert Clears

Safety clear, chlorine back to 2 ppm, dist pump back to 9A. Clear signals
asserted for chlorine and dist fuses. Fuses transition to state 2 (clearing)
but output still 1 — waiting for clear low.

```
infrastructure.infra_output [T T T]          ← infra recovered
equipment.dosing.dosing_output [F T]         ← chlorine normal, fuse still blown
equipment.dist_pumps.dist_output [F F F T]   ← current normal, fuse still blown
```

### Step 9: Release Clears — Full Recovery

Clear signals go low. Both fuses re-arm. Full propagation to green.

```
plant_status.plant_output [T T]              ← full capacity restored
process.process_output [T T T]               ← all processes ready
equipment.equip_output [T T T T]             ← all equipment ok
```

### Step 10: Total Power Loss

All three power sources off. Infrastructure fails, cascades to all processes.

```
plant_status.plant_output [F F]              ← nothing operational
infrastructure.infra_output [F T F]          ← power FAIL, safety ok, infra FAIL
equipment.equip_output [T T T F]             ← equipment ok but has_infra = F
```

Equipment remains healthy (no fuses blown) — the failure is purely
infrastructure. Restoring any single power source would recover the plant.

## Key Observations

1. **Fuse persistence:** Fuses hold their blown state even after the triggering
   condition clears. This models real-world circuit breakers and alarm latches.

2. **Two-phase clear:** The low→high→low cycle prevents accidental re-arming.
   An operator must deliberately assert and release the clear signal.

3. **Independent fault domains:** Safety interlocks (e-stop) and equipment
   faults are orthogonal. An e-stop doesn't clear fuses, and clearing fuses
   doesn't bypass safety.

4. **Graceful degradation:** The plant remains operational with partial faults.
   `plant_output[0]` (operational) uses OR — any single working process keeps
   the plant running. `plant_output[1]` (full_capacity) uses AND — all
   processes must be ready.

5. **Bottom-up evaluation:** The engine evaluates Level 0 first (infrastructure),
   then Level 1 (equipment), Level 2 (process), Level 3 (plant). Each level
   sees the settled state of the level below it. No iterative convergence needed.

6. **Callback side effects:** Fuse action callbacks fire exactly once per trip
   event. They execute during `st_evaluate()` and receive the application's
   `user_handle` pointer for logging, actuator control, or SCADA notification.


