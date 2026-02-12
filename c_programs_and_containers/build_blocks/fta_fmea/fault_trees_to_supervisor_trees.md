This integrated narrative describes the transformation of a high-consequence safety diagram into an executable supervisor hierarchy. It bridges the gap between static **Fault Tree Analysis (FTA)** and dynamic **Runtime Bitmasks**.

---

# Integrated Description: FTA of Containment Spray Recirculation Flow

## Transformation into Supervisor Trees and Runtime Bitmasks

### 1. System-Level Failure Definition (Top Event)

The fault tree analyzes the system-level failure: **Insufficient fluid flow to containment spray recirculation nozzles.**

This **Top Event (TE)** represents the loss of a critical safety function. In runtime terms, this is never a directly asserted fault; it is a computed condition derived from lower-level failures.

* **Runtime Abstraction:** `FAULT_TOP = computed_from_children;`
* **Supervisor Abstraction:** * **SUPERVISOR:** Containment Spray System
* **FAILURE CONDITION:** `safety_flow < required_flow`



---

### 2. High-Level Fault Logic (OR Structure)

The Top Event occurs if any of the following high-level conditions are met:

* 3 out of 4 pump systems fail in the first 24 hours.
* 3 out of 4 recirculation lines fail (Train A or B).
* All 4 pumps fail due to lack of power.
* Maintenance condition is active or a common-mode failure occurs.

**Runtime Bitmask:**

```c
FAULT_TOP = 
    FAIL_3_OF_4_PUMPS | 
    FAIL_3_OF_4_LINES_A | 
    FAIL_3_OF_4_LINES_B | 
    FAIL_ALL_PUMPS_POWER | 
    MAINTENANCE_MODE | 
    COMMON_MODE_FAIL;

```

**Supervisor Interpretation:**
At runtime, this becomes failure escalation logic at the root supervisor:

> **ESCALATE IF:** `any(child_supervisor reports FAILURE)`

---

### 3. Train-Based Decomposition (Fault Containment Domains)

The system is explicitly split into **Train A** and **Train B**, designed for functional redundancy. FTA treats these as parallel branches; supervisor trees treat them as **fault-containment regions**.

**Supervisor Structure:**

* **SUPERVISOR:** Containment Spray System
* **[+] SUPERVISOR:** Train A
* **[+] SUPERVISOR:** Train B



Failures inside Train A do not propagate to Train B unless voting thresholds are exceeded. This containment is implicit in FTA but strictly enforced in supervisor trees.

---

### 4. k-out-of-n Logic  Supervisory Voting Policies

The system uses **k-of-n** logic for pump and line failures, tolerating up to one failure.

* **Runtime Bitmask:** `FAIL_3_OF_4_PUMPS = (popcount(PUMP_FAIL_BITS) >= 3);`
* **Supervisor Policy:** * **SUPERVISOR:** Pump Group
* **POLICY:** `tolerated_failures = 1`. If `failed_children >= 3`, report **FAILURE**.



This replaces a static gate with live counting and **time qualification** (e.g., "within first 24 hours").

---

### 5. Pump Failure Logic (Leaf-to-Parent Mapping)

Each pump system fails if it cannot start, cannot continue running, or cannot deliver fluid. These are decomposed into **Basic Events (BE1–BE17)**.

* **FTA Leaf Meaning:** Primitive failure with an associated probability.
* **Runtime Bitmask:** `PUMP_A1_FAIL = FAIL_START | FAIL_RUN | FAIL_DELIVER;`
* **Supervisor Interpretation:**
* **SUPERVISOR:** Pump A1
* **ACTION:** Attempt restart; if restart fails  escalate.



---

### 6. Inside vs. Outside Containment (Hierarchical Isolation)

Each pump path is split into **Inside** and **Outside** containment delivery.

**Supervisory Advantage:**
A fault inside containment does not immediately invalidate the outside containment infrastructure. Repair or reconfiguration can be localized.

---

### 7. Power Loss as a Common-Mode Supervisor

"All 4 pumps fail due to lack of power" is a **Common-Cause Failure (CCF)**.

* **Supervisor Trees:** Model this as a parent supervisor above both trains.
* **Runtime Bitmask:** `FAIL_ALL_PUMPS_POWER = GRID_LOSS | BUS_FAILURE;`

When this supervisor fails, lower pump supervisors are bypassed, and redundancy collapses legitimately rather than silently.

---

### 8. Maintenance and Common-Mode Mode Supervisors

* **Maintenance:** FTA treats this as a "house event." Supervisor trees treat it as a **Mode Change**.
* **Runtime Gating:** `if (MODE == MAINTENANCE) relax_redundancy_requirements();`

---

### 9. Full Conceptual Transformation Summary

| FTA Element | Runtime Bitmask | Supervisor Tree |
| --- | --- | --- |
| **Top Event** | Computed root bit | Root supervisor |
| **OR gate** | Bitwise ` | ` |
| **AND / k-of-n** | `&` / `popcount()` | Voting policy |
| **Basic Event** | Leaf bit | Leaf supervisor |
| **Train A/B** | Bitmask partition | Fault domain |
| **Common Mode** | Shared parent bit | Parent supervisor |
| **Maintenance** | Condition bit | Mode supervisor |

---

### 10. Key Insight: Why Integration Matters

**FTA describes how the system can fail; Supervisor Trees define how the system behaves while failing.** By integrating them, FTA logic becomes executable, and bitmasks become first-class fault data.

**What should we drill into next?**
Would you like me to show the **exact C structs/binary layouts** for MCU use, or map this directly to **Erlang/OTP supervisor** patterns?