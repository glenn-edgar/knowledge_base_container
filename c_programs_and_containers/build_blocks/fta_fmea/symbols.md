This document provides a technical translation of standard **Fault Tree Analysis (FTA)** symbols into the executable logic of **Runtime Bitmasks**. This bridge allows safety requirements to move directly from the design phase into high-performance, embedded execution logic.

---

## 1. Event Symbols (State & Data)

Event symbols represent the "nouns" of the safety system—the things that happen or exist.

### Top Event (TE) — Rectangle

* **Meaning:** The undesired system-level failure; the root of the tree.
* **FTA Role:** What you are trying to prevent, quantify, or mitigate.
* **Runtime Bitmask:** `TOP_EVENT = expression_of_children;`
* **Bitmask View:** A **derived parent bit**. It is never set directly; it is always computed.

### Intermediate Event — Rectangle

* **Meaning:** A failure caused by other events via logic gates.
* **FTA Role:** Structural grouping for readability and logic reuse.
* **Runtime Bitmask:** `INTERMEDIATE = A | B;` (or `A & B`)
* **Bitmask View:** A **computed node** that can be reused by multiple parents.

### Basic Event (BE) — Circle

* **Meaning:** Primitive failure with no further decomposition.
* **FTA Role:** The leaf node, usually derived from an FMEA row.
* **Runtime Bitmask:** `BASIC_EVENT |= DETECTED;`
* **Bitmask View:** A **leaf bit** set by hardware, software diagnostics, or sensors.

### Undeveloped Event — Diamond

* **Meaning:** A failure that exists but is not modeled (due to lack of data or relevance).
* **FTA Role:** Explicitly marks uncertainty.
* **Runtime Bitmask:** `UNDEVELOPED_EVENT |= ASSUMED;`
* **Bitmask View:** Usually forced to `FALSE` (optimistic) or `TRUE` (conservative) depending on safety posture.

### Conditional Event — Oval

* **Meaning:** A condition that must be true for a gate to activate (not a failure itself).
* **FTA Role:** Enables **Inhibit** and **Priority AND** gates.
* **Runtime Bitmask:** `CONDITION = (MODE == MAINTENANCE);`
* **Bitmask View:** A **mode or state bit** used as a gating signal.

### House Event (External Event) — House Shape

* **Meaning:** An external, assumed, or constant condition (not random).
* **FTA Role:** Defines configuration assumptions or environmental constraints.
* **Runtime Bitmask:** `HOUSE_EVENT = CONFIG_ENABLE;`
* **Bitmask View:** A **static or slowly changing control bit**.

### Transfer IN / Transfer OUT — Triangles

* **Meaning:** Logical continuation of a fault tree on another page.
* **FTA Role:** Hierarchy management and modularization.
* **Runtime Bitmask:** `TRANSFER_IN = REMOTE_SUBTREE_ROOT;`
* **Bitmask View:** A **symbolic link** referencing another bitmask subtree.

---

## 2. Gate Symbols (Logic & Propagation)

Gate symbols are the "verbs"—the logic that defines how failures combine and propagate upward.

### OR Gate

* **Meaning:** Output occurs if any input occurs.
* **Runtime Bitmask:** `OUT = A | B | C;`
* **Interpretation:** Represents a **single-point failure** and typical alarm aggregation.

### AND Gate

* **Meaning:** Output occurs only if all inputs occur.
* **Runtime Bitmask:** `OUT = A & B;`
* **Interpretation:** Represents **redundancy**; independent failures are required to reach the next level.

### Priority AND (PAND) Gate

* **Meaning:** Inputs must occur in a specific temporal order.
* **Runtime Bitmask:** `OUT = A & B & (timestamp_A < timestamp_B);`
* **Interpretation:** Sequence-dependent failures, common in startup/shutdown hazards.

### XOR Gate

* **Meaning:** Output occurs if exactly one input occurs.
* **Runtime Bitmask:** `OUT = (A ^ B);`
* **Interpretation:** Mutually exclusive failures or **mode conflicts**.

### k/N Gate (Voting Gate)

* **Meaning:** Failure occurs if  out of  inputs fail.
* **Runtime Bitmask:** `OUT = (popcount(INPUTS) >= k);`
* **Interpretation:** Used in **Triple Modular Redundancy (TMR)** or sensor voting.

### Inhibit Gate

* **Meaning:** Event occurs only if a specific condition is met.
* **Runtime Bitmask:** `OUT = EVENT & CONDITION;`
* **Interpretation:** **Mode-dependent hazards** (e.g., "Motion allowed" only if "Guard is closed").

---

## 3. One-to-One Mapping Summary

| FTA Symbol | Meaning | Runtime Bitmask |
| --- | --- | --- |
| **Top Event** | System failure | Computed root bit |
| **Intermediate** | Derived fault | `OR` / `AND` expression |
| **Basic Event** | Primitive failure | Leaf bit |
| **Conditional** | Enabling state | Mode/State bit |
| **House Event** | Assumption | Static config bit |
| **OR Gate** | Any fail | ` |
| **AND Gate** | All fail | `&` |
| **Priority AND** | Ordered fail | `&` + Time Logic |
| **XOR** | Exclusive fail | `^` |
| **k/N** | Voting | `popcount()` |
| **Inhibit** | Gated fail | `&` |

---

## 4. Why Bitmasks Are the Superior Execution Model

While paper FTA is a static diagnostic tool, bitmasks turn it into a **living safety executive**. Bitmasks add the critical dimensions that paper cannot express:

* **Time & Latching:** Ensuring a fault stays active until handled.
* **State Awareness:** Changing logic based on the machine's current mode.
* **Fault Containment:** Isolating logic subtrees to prevent cascade failures.

**What should we drill into next?**
Would you like me to show a **direct FTA  C struct  MCU binary layout**, or should we look at how to map **FMEA severity** directly into these inhibit and state transitions?