```markdown
# Geological-Style Visualization of System Health and Failure Progression

This section extends the geological metaphor into a **complete end-to-end narrative** showing how a complex, fault-tolerant system evolves from **fully healthy operation** through **progressive faults** to **total system failure**.  
The visualization is driven entirely by runtime state (bitmasks, supervisors, behavior trees) and projected deterministically as geological structures.

---

## 0. The Cross-Section Template

The system is visualized as a vertical cross-section of functional layers:

- **CONTROL** – Behavior Trees, sequencing, autonomy logic  
- **SUPERVISION** – Supervisors, voting, restart / containment domains  
- **DIAGNOSTICS** – FMEA detectors, health monitors  
- **ACTUATION** – Pumps, valves, motors, physical effectors  
- **POWER** – Grid, buses, batteries, backup sources  

### Geological primitives used

- **Fold (buckle)** – stress detected but contained (warning / pending fault)  
- **Thrust fault (slip plane)** – latched fault or supervisor-declared failure  
- **Detachment horizon** – fault-containment boundary  
- **Imbricate stack** – multiple redundant elements failing (k-of-n)  
- **Duplex wedge** – nested supervisors trapping faults  
- **Triangle zone** – mode-dependent propagation (maintenance / startup)

---

## 1. Stage A — Completely Healthy (Flat Strata)

**Bitmasks**
- `FAULT = 0`, `ALARM = 0`, `INHIBIT = 0`
- `STATE = NOMINAL`
- All supervisors report **OK**

**Geological view**
```

CONTROL     ─────────────────────────────────
SUPERVISION ─────────────────────────────────
DIAGNOSTICS ─────────────────────────────────
ACTUATION   ─────────────────────────────────
POWER       ─────────────────────────────────

```

**Interpretation**
- No stress, no slip
- Redundancy unused but fully available

---

## 2. Stage B — Early Warning (Local Stress, No Slip)

Example: sensor drift, rising motor current, transient anomaly.

**Bitmasks**
- Leaf event asserted: `BE_x = 1`
- No derived failures (`FAULT_TOP = 0`)

**Geological view**
```

CONTROL     ─────────────────────────────────
SUPERVISION ─────────────────────────────────
DIAGNOSTICS ────────────∧────────────────────   (warning fold)
ACTUATION   ─────────────────────────────────
POWER       ─────────────────────────────────

```

**Interpretation**
- Stress accumulating
- Fault filtered or pending (debounce / persistence)

---

## 3. Stage C — Component Failure, Containment Holds

Example: one pump fails, redundancy absorbs it.

**Bitmasks**
- `PUMP_2_FAIL = 1` (latched)
- `FAIL_3_OF_4_PUMPS = 0`

**Geological view**
```

CONTROL     ─────────────────────────────────
SUPERVISION ─────────────══ DETACHMENT ═══════
DIAGNOSTICS ─────────────────────────────────
ACTUATION   ───────────────╲─────────────────  (single slip plane)
POWER       ─────────────────────────────────

```

**Interpretation**
- Failure exists but is contained
- System is degraded yet stable

---

## 4. Stage D — Redundancy Consumption (Imbrication)

Example: two of four redundant elements fail.

**Bitmasks**
- `PUMP_2_FAIL = 1`, `PUMP_3_FAIL = 1`
- `popcount(PUMP_FAIL) = 2` (near threshold)

**Geological view**
```

CONTROL     ─────────────────────────────────
SUPERVISION ─────────────══ DETACHMENT ═══════
DIAGNOSTICS ─────────────────────────────────
ACTUATION   ───────────╲──╲──────────────────  (imbricate stack)
POWER       ─────────────────────────────────

```

**Interpretation**
- Redundancy margin visibly shrinking
- One more slice may trigger escalation

---

## 5. Stage E — Containment Leakage (Duplex Stress)

Example: local recovery fails; escalation to higher supervisor.

**Bitmasks**
- `TRAIN_A_PUMP_GROUP_DEGRADED = 1`
- `STATE = DEGRADED`

**Geological view**
```

CONTROL     ─────────────────────────────────
SUPERVISION ────────╱▔▔╲──────╱▔▔╲────────────  (duplex wedges)
DIAGNOSTICS ─────────────────────────────────
ACTUATION   ───────────╲──╲──╱────────────────
POWER       ─────────────────────────────────

```

**Interpretation**
- Nested supervisors under stress
- Recovery attempts ongoing; containment weakening

---

## 6. Stage F — Common-Mode Failure (Through-Going Fault)

Example: loss of power bus or shared infrastructure.

**Bitmasks**
- `GRID_LOSS = 1` or `BUS_FAILURE = 1`
- `FAIL_ALL_PUMPS_POWER = 1`

**Geological view**
```

CONTROL     ────────────────╲────────────────
SUPERVISION ────────────────╲────────────────
DIAGNOSTICS ────────────────╲────────────────
ACTUATION   ────────────────╲────────────────
POWER       ────────────────╲________________

```

**Interpretation**
- Deep, systemic fault
- Redundancy largely bypassed

---

## 7. Stage G — Voting Threshold Exceeded

Example: third redundant element fails (3-of-4).

**Bitmasks**
- `popcount(PUMP_FAIL) >= 3`
- `FAIL_3_OF_4_PUMPS = 1`

**Geological view**
```

CONTROL     ───────────∧∧∧────────────────────
SUPERVISION ───────────╲╲╲════════════════════
DIAGNOSTICS ───────────╲╲╲────────────────────
ACTUATION   ───────────╲╲╲____________________
POWER       ─────────────────────────────────

```

**Interpretation**
- Containment boundary overwhelmed
- Designed safety threshold crossed

---

## 8. Stage H — Total Failure (Top Event True)

**Bitmasks**
- `FAULT_TOP = 1`
- `STATE = SHUTDOWN`
- Safety inhibits asserted

**Geological view**
```

CONTROL     ───────╲****╲****╲________________
SUPERVISION ───────╲****╲****╲________________
DIAGNOSTICS ───────╲****╲****╲________________
ACTUATION   ───────╲****╲****╲________________
POWER       ───────╲****╲****╲________________

```

**Interpretation**
- Fully connected failure surface
- System can no longer perform its function

---

## 9. Deterministic Generation Rules

The visualization is computed, not hand-drawn:

- **Fold intensity** ← fault pending score / trend
- **Slip plane** ← latched fault or supervisor failure
- **Imbrication count** ← `popcount()` of redundant failures
- **Detachment holds** ← failures < threshold and no common-mode
- **Through-going plane** ← common-mode or power loss
- **Collapse** ← `FAULT_TOP = 1`

---

## 10. Operator Insight at a Glance

- Flat strata → healthy  
- Small fold → early warning  
- Contained thrust → isolated failure  
- Imbricate stack → redundancy consumption  
- Duplex wedges → restart domains stressed  
- Deep fault → systemic risk  
- Buckled upper layers → threshold exceeded  
- Full collapse → top event

---

### Key Takeaway

This geological projection transforms abstract fault logic into an **intuitive, spatial narrative**: operators can *see* how close the system is to the next catastrophic slip, just as a geologist reads stress and failure in layered rock.
```
