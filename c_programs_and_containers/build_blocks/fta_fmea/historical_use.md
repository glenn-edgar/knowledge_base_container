Yes — **there’s a strong historical lineage for almost every concept we discussed**, just usually **split across different communities** (system safety, avionics/space autonomy, telecom/cloud). What’s relatively *new* is **joining them into one coherent runtime stack** (FTA/FMEA → bitmasks → supervisors → semantic projection like your “geology”).

## Historical use

### 1) FTA as “top-down causality trees”

* **Fault Tree Analysis (FTA)** was developed at **Bell Labs in 1962** for the U.S. Air Force **Minuteman** missile program and then expanded by Boeing/others in the 1960s. ([Wikipedia][1])
  **What matches your approach:** hierarchical OR/AND logic for system hazards, including redundancy modeling.

**But historically:** FTA was mostly used as an *analysis artifact* (design/certification), not compiled into a runtime fault-state fabric.

---

### 2) FMEA/FMECA as “bottom-up leaf catalog”

* **FMEA/FMECA** procedures trace back to U.S. military procedure documents (e.g., **MIL-P-1629**) and were widely used by NASA contractors and programs (Apollo and beyond). ([Wikipedia][2])
  **What matches:** leaf failures → effects → severity/criticality (your “basic event bits + metadata”).

**But historically:** FMEA results often ended up in checklists, test plans, or “fault protection requirements,” not in a unified executable bitmask model.

---

### 3) Supervisor trees and containment domains (“let it crash”)

* The idea of **structuring fault tolerance as process trees with restart responsibility** is strongly associated with **Erlang/OTP**, designed for systems that must “run forever,” with supervision hierarchies. ([lfe.io][3])
  **What matches:** your “duplex boundaries / restart domains / escalation policies.”

**But historically:** this lived in telecom/software reliability, not in classical FTA/FMEA safety engineering workflows.

---

### 4) PLEXIL and “intent execution under constraints”

NASA’s **PLEXIL** work sits in the “executive” category: deterministic plan execution, clear node outcomes vs node states, and integration with a functional layer. ([NASA Technical Reports Server][4])
**What matches:** your view that plan execution should consume **semantic state** (health envelopes, permissions), not raw faults.

**But historically:** PLEXIL typically relies on *separate* fault protection/monitoring subsystems and plan guards—less on a unified, compiled fault fabric.

---

## How current practice compares (2020s-ish)

### A) Safety-critical industries (aviation/nuclear/space)

**Common today**

* FTA/FMEA/FMECA used heavily for **design assurance** and certification evidence.
* Runtime often uses:

  * **FDIR** (Fault Detection, Isolation, and Recovery) style logic
  * Health monitors + mode logic
  * Redundancy management (voting, cross-strapping)

**Gap vs your concept**

* Runtime health is frequently *distributed* across components and mode managers.
* The FTA rarely becomes a **compiled, live hierarchy** of fault bits with explicit containment semantics.

---

### B) Cloud/distributed systems

**Common today**

* “Virtual operator” tooling: SRE dashboards, tracing, dependency graphs, automated remediation.
* Chaos testing to validate assumptions.

**Gap vs your concept**

* They have great observability, but weak **formal causality** (FTA-grade) and weak **explicit containment math** (k-of-n, PAND, inhibit) in a single fabric.

---

### C) The “new convergence”

What you’re proposing is essentially a convergence of:

* **Safety engineering** (FTA/FMEA)
* **Runtime fault containment** (supervisors)
* **Operator cognition** (semantic projection / geology)
* **Plan execution** (PLEXIL / BTs)

That convergence is **emerging** in pieces (health envelopes, autonomy stacks, integrated FDIR + planning), but it’s still uncommon to see it **fully unified** and **compiled** the way you’re describing.

---

## One clean way to state the comparison

### Historically

* **FTA/FMEA:** excellent at *explaining and proving* safety logic
* **Supervisors:** excellent at *surviving* faults
* **PLEXIL/Executives:** excellent at *executing intent* under constraints
* **Operator views:** excellent at *human comprehension* (but often detached from runtime structure)

### Current best practice

* Uses all of these, but typically as **separate layers** with hand-authored glue.

### Your integrated practice

* Treats the safety model as **compilable runtime structure**:

  * FMEA → leaf bits (+ metadata)
  * FTA → derived bits (OR/AND/k-of-n/PAND/inhibit)
  * Supervisors → containment + restart + escalation
  * Geological projection → semantic state for humans *and* virtual operators
  * PLEXIL/BT → intent policies guarded by those semantic variables

---

If you want, I can write a short “historical evolution” section for your report with a timeline (1949 → 1962 → 1980s → 2000s → today), and then a “what’s missing” section that motivates your unified approach.

[1]: https://en.wikipedia.org/wiki/Fault_tree_analysis?utm_source=chatgpt.com "Fault tree analysis"
[2]: https://en.wikipedia.org/wiki/Failure_mode_and_effects_analysis?utm_source=chatgpt.com "Failure mode and effects analysis"
[3]: https://lfe.io/papers/%5B2007%5D%20Armstrong%20-%20HOPL%20III%20A%20History%20of%20Erlang.pdf?utm_source=chatgpt.com "A History of Erlang"
[4]: https://ntrs.nasa.gov/api/citations/20060019246/downloads/20060019246.pdf?utm_source=chatgpt.com "Plan Execution Interchange Language (PLEXIL)"
