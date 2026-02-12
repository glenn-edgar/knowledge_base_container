This is a sophisticated engineering-level mapping. By converting static safety artifacts into runtime logic, you essentially turn a "dead" document into a live **Safety Executive**.

Here is the formatted Markdown version of your FTA/FMEA to Bitmask mapping.

---

# Engineering Mapping: FTA/FMEA Artifacts  Runtime Hierarchical Bitmasks

This mapping transforms paper safety analysis into executable, high-performance logic, bridging the gap between design-time safety (FTA/FMEA) and runtime system behavior.

## 1. Conceptual Bridge: Why Bitmasks Work

FTA and FMEA already assume a Boolean structure that maps perfectly to bitwise operations:

* **A failure exists or not:** (1 or 0)
* **Causes combine:** via `OR` / `AND` / `k-of-n`
* **Effects propagate:** upward through the hierarchy

A runtime bitmask tree is simply a **compiled, executable fault tree** with memory and time semantics.

---

## 2. Fault Tree Analysis (FTA)  Bitmask OR/AND Trees

### Example FTA (Paper)

```text
TOP EVENT: Loss of Actuation
  OR
   ├─ Motor Failure
   ├─ Power Failure
   └─ Control Failure
        AND
         ├─ CPU Failure
         └─ Watchdog Failure

```

### Runtime Bitmask Representation

**BITSPACE: FAULT**

* `[0]` TOP_LOSS_OF_ACTUATION
* `[1]` MOTOR_FAIL
* `[2]` POWER_FAIL
* `[3]` CONTROL_FAIL
* `[4]` CPU_FAIL
* `[5]` WATCHDOG_FAIL

### Propagation Rules (Compiled)

```c
CONTROL_FAIL = CPU_FAIL & WATCHDOG_FAIL;
TOP_LOSS_OF_ACTUATION = MOTOR_FAIL | POWER_FAIL | CONTROL_FAIL;

```

### Why This Is Powerful

| Feature | Paper FTA | Runtime Bitmask |
| --- | --- | --- |
| **Logical OR** | Symbol | Bitwise ` |
| **Logical AND** | Symbol | Bitwise `&` |
| **Traversal** | Manual |  bitwise ops |
| **Context** | Static diagram | Live fault state |

---

## 3. FMEA  Leaf Bits + Metadata

FMEA provides the "Ground Truth" for the leaf nodes of the tree.

### Example FMEA Row

| Item | Failure Mode | Effect | Severity |
| --- | --- | --- | --- |
| Motor | Open winding | No torque | High |

### Runtime Mapping

Each failure mode becomes a **leaf bit**.

**BITSPACE: FAULT**

* **BIT 1:** `MOTOR_OPEN_WINDING`
* **BIT 2:** `MOTOR_SHORT`
* **BIT 3:** `MOTOR_OVERTEMP`

**Associated Metadata Table (Non-bits):**

```json
{
  "bit": 1,
  "severity": "HIGH",
  "detect": "CURRENT_SENSOR",
  "latency_ms": 10,
  "recovery": "RESTART_MOTOR"
}

```

> **Key Insight:** FMEA gives the bit meaning; the bitmask gives the system speed.

---

## 4. Severity  Priority-Merged Bitspaces

FTA/FMEA alone do not define "what wins." Runtime bitspaces extend paper analysis by defining **Inhibits** and **Permits**.

### Example Bitspaces

* **STATE:** (Priority Merge) - Current operating mode.
* **FAULT:** (OR Merge) - Active failures.
* **INHIBIT:** (OR Merge) - Operations that are blocked.
* **PERMIT:** (AND Merge) - Gates required for execution.

### Severity Mapping

| FMEA Severity | Runtime Effect |
| --- | --- |
| **Catastrophic** | `FAULT` + `INHIBIT` |
| **Major** | `FAULT` |
| **Minor** | `STATE_DEGRADED` |
| **Informational** | `EVENT` log only |

**Example Logic:**

```c
if (MOTOR_OVERTEMP) {
  FAULT |= MOTOR_FAIL;
  INHIBIT |= MOTION_ENABLE; // Safety interlock
}

```

---

## 5. Detection Logic  Analytical Redundancy

FTA often abstracts away *how* a failure is detected. Bitmasks make this explicit by treating detectors as sub-nodes.

**Example (CPU Failure):**

```c
CPU_FAIL = (heartbeat_timeout) | (illegal_opcode) | (stack_guard_trip);

```

Each detector is independent, providing **analytical redundancy** that feeds the same logical bit.

---

## 6. Degraded Modes = State-Conditioned Trees

* **FTA says:** "System fails."
* **Runtime systems say:** "System degrades."

**State Selection Logic:**

```c
if (TOP_LOSS_OF_ACTUATION)
    STATE = SHUTDOWN;
else if (CONTROL_FAIL)
    STATE = SAFE;
else if (MOTOR_FAIL)
    STATE = DEGRADED;

```

---

## 7. Time & Latching (The "Paper Gap")

Paper FTA is timeless. Runtime logic requires temporal semantics to prevent "flickering" faults.

| Attribute | Meaning |
| --- | --- |
| **COPY** | Mirrors child state exactly. |
| **LATCH** | Sticks to `1` until a manual clear command. |
| **RESET** | Automatically clears on a state exit. |
| **FILTER** | Debounce / Persistence (e.g., must be high for  ms). |

**Example:**

```c
if (overtemp > 100C for 500ms)
    MOTOR_OVERTEMP |= LATCH;

```

---

## 8. Supervisor Trees = Containment Domains

Each subtree acts as a **Supervisor**, owning its bits, recovery actions, and restart scope—similar to NASA flight software or Erlang OTP.

---

## 9. Full Mapping Summary

| Safety Artifact | Runtime Construct |
| --- | --- |
| **FTA OR gate** | Bitwise OR |
| **FTA AND gate** | Bitwise AND |
| **FTA Top Event** | Parent bit |
| **FMEA Failure Mode** | Leaf bit |
| **Severity** | Priority / Inhibit bit |
| **Mitigation** | State transition |
| **Common Cause** | Shared parent node |
| **Fault Containment** | Subtree boundary |
| **Recovery** | Supervisor action |

---

## 10. Why This Beats Traditional Methods

1. **Faults are first-class data:** Not hidden inside `if/else` blocks.
2. **No branching explosion:** Trees react via bitwise math rather than nested logic.
3. **Auditability:** The code structure is a direct 1:1 reflection of the safety manual.

Would you like me to **design a binary layout for MCU-scale systems** (defining how these bitspaces are packed in memory) or **show a DSL that compiles FTA to C tables**?