By integrating **Behavior Trees (BTs)** with **FTA/FMEA**, you transform a static safety document into a dynamic, reactive "Safety Executive." In this model, the BT serves as the host for both the logic propagation (the "thinking") and the recovery actions (the "doing").

---

# Pattern: Compiling FTA + FMEA into Behavior Trees

## 1. Core Architecture: The Coupled Tree Model

To keep the system deterministic and traceable, split the runtime execution into two logical trees that share a **Blackboard** containing your bitmask spaces (`FAULT`, `STATE`, `INHIBIT`, `PERMIT`).

* **Tree A: Fault Logic Tree** (The "Truth Maintenance" layer)
* Runs first every tick.
* Computes derived faults exactly like FTA gates.


* **Tree B: Response/Supervision Tree** (The "Policy" layer)
* Runs second.
* Handles recovery, degradation, and safe-state transitions based on Tree A's output.



---

## 2. Mapping FTA Gates to BT Control Nodes

You can implement FTA logic within a BT using custom **Compute Nodes** (which write to the bitmask) or **Control Flow Nodes** (which steer execution).

| FTA Gate | BT Node Strategy | Bitmask Logic |
| --- | --- | --- |
| **OR Gate** | **Selector** or **ComputeOR** | `P = A |
| **AND Gate** | **Sequence** or **ComputeAND** | `P = A & B` |
| **k-of-n Gate** | **VoteKofN** Decorator | `popcount(bits) >= k` |
| **Inhibit Gate** | **Condition Guard** | `OUT = E & CONDITION` |
| **Priority AND** | **SequenceWithTime** | `A & B & (tA < tB)` |

---

## 3. Mapping FMEA into Leaf Nodes (Basic Events)

In this architecture, **FMEA is the catalog** that defines your leaf nodes. Every failure mode in your FMEA becomes a diagnostic leaf in the BT.

* **Leaf Bit:** `BE_START_FAIL`
* **BT Node:** `DetectPumpStartFailure`
* **Metadata:** (Severity, Detection Latency, Recovery Action)

The BT doesn't just "see" a failure; it executes the specific diagnostic logic (e.g., checking current sensors vs. command state) to assert the bit defined by the FMEA.

---

## 4. Supervisor Trees as "Recovery Subtrees"

A Supervisor Tree ensures that failures are contained and recovery is attempted before escalating. In a BT, this is represented by a **Fallback (Selector)** pattern:

### The "Supervisor Wrapper" Pattern

1. **Condition:** Is the subsystem healthy? (Return `SUCCESS` if bit is 0).
2. **Recovery:** If unhealthy, execute `LocalRecoveryAction` (e.g., Reset/Restart).
3. **Escalate:** If recovery fails, set `ESC_BIT` and return `FAILURE` to the parent.

---

## 5. Concrete Example: Containment Spray as a BT

Based on the nuclear spray recirculation diagram, the tree would look like this:

### A. Fault Logic Tree (The "Compiler" output)

This tree purely updates the blackboard bitmask every tick.

```text
FaultLogicTree (Sequence)
  ComputeOR(out=FAIL_ALL_PUMPS_PWR, bits=[GRID_LOSS, BUS_FAIL])
  VoteKofN(k=3, bits=[P1, P2, P3, P4], out=FAIL_3_OF_4_PUMPS)
  ComputeOR(out=FAULT_TOP, bits=[FAIL_3_OF_4_PUMPS, FAIL_ALL_PUMPS_PWR, ...])

```

### B. Response/Supervision Tree (The "Executive")

This tree reacts to the `FAULT_TOP` bit computed above.

```text
ContainmentSpraySupervisor (Fallback)
  - Condition: FAULT_TOP == 0 (All Good)
  
  - DegradeManager (Sequence):
      - Reconfigure: Use remaining pumps to maintain flow
      - Condition: FLOW_ADEQUATE == 1
  
  - RecoveryManager (Sequence):
      - Action: RetryStart(Pumps)
      - Action: ResetTrips
  
  - Escalation (Sequence):
      - SetState: SHUTDOWN
      - Action: Inhibit(SYSTEM_ENABLE)
      - Action: RaiseAlarm

```

---

## 6. Comparison: Why This Integration Beats Traditional Logic

| Feature | Traditional State Machines | FTA-Driven Behavior Trees |
| --- | --- | --- |
| **Logic Density** | Massive `if/else` chains | O(1) Bitwise operations |
| **Traceability** | Code doesn't look like the FTA | BT nodes map 1:1 to FTA symbols |
| **Recovery** | Hardcoded per state | Hierarchical "Supervisor" wrappers |
| **Containment** | Global variables | Subtree-local bitmask domains |

---

## 7. Minimal Custom Node Set

To implement this, you only need to build these reusable nodes once:

1. `ComputeOR / ComputeAND`: Generic n-input logic.
2. `VoteKofN`: Threshold logic.
3. `Inhibit`: Conditional gating.
4. `Latching/Filter`: Temporal semantics (e.g., "Fault must persist for 500ms").

**What is your primary runtime target?**
I can provide a **BehaviorTree.CPP XML sketch** or a **ChainTree DSL equivalent** for the containment spray diagram—just let me know which layer you want to see.