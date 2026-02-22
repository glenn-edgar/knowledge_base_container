# Scan Tree

## Hierarchical Bitmap-Driven Behavior Tree Execution — Top Level Design

---

## 1. Purpose

Scan Tree is a language-independent control system applicable to large I/O environments typical of SCADA systems. It generates hierarchical three-state bitmaps from raw I/O data, evaluates behavior trees using bit expressions over those bitmaps, and produces actions through a two-phase tick-then-act cycle.

---

## 2. Core Pipeline

1. **Data Acquisition** — The system ingests data from various buffers (representing I/O points, sensor readings, status registers, etc.).
2. **Hierarchical Bitmap Generation** — From the buffered data, the system generates hierarchical derived bitmaps arranged in layers that represent the system's functional structure.
3. **Behavior Tree Evaluation** — Behavior trees use bit expressions operating on these hierarchical bitmaps to control their execution flow (gate conditions, branch selection, etc.).
4. **Post-Tick Action Phase** — After a behavior tree tick completes, actions are produced. This phase is analogous to the PLEXIL invariant calculation phase — evaluating conditions and determining resulting actions as a distinct step following the tick.

---

## 3. Three-State Model

A single unified three-state model is used throughout the system for both derived bitmaps and feature status:

- **Active (Enable)** — The element or feature is operational and running.
- **Failure (Fault)** — The element or feature has encountered a fault condition.
- **Not Operational (Not Active)** — Prerequisites required for operation have not been achieved. This is distinct from failure — the element simply cannot run because its dependencies are not met.

---

## 4. Hierarchical Bitmap Layer Structure

The bitmaps are organized in layers, where each layer can be composed of sub-layers:

- Layers represent basic functional categories — operational requirements such as power, pressure, temperature, etc.
- The hierarchy descends from these broad functional categories down to specific functional areas.
- Sub-layers within a layer accommodate redundancy. Where redundant elements exist, individual elements can be in a fault state while the parent layer remains active because the redundancy satisfies the operational requirement.
- This layered redundancy model is what gives rise to the three-state logic — a feature is Active if sufficient redundant elements are healthy, in Failure if redundancy is exhausted, or Not Operational if prerequisites from higher layers have not been achieved.

The recommended approach for organizing bitmap layers is a geological-style cross-section model, where functional layers (Control, Supervision, Diagnostics, Actuation, Power) are arranged as strata. Fault progression is represented as deformation of these strata — folds for warnings, slip planes for latched faults, imbricate stacks for redundancy consumption, and so on. This model provides structure, causality, containment awareness, margin-to-failure visibility, and directional degradation tracking. It is suitable for both human operators and virtual operators/AI. See Appendix A for a full description.

---

## 5. System Entities

- **Raw Buffers** — Input data from I/O and other sources.
- **Derived Bitmaps** — Three-state bitmaps generated from raw buffers and other derived bitmaps.
- **Virtual Function Templates** — Abstract function definitions that serve as markers in the tree structure. Virtual functions have multiple inputs and a single boolean output. Side effects are contained within the function implementation.
- **Instantiated Virtual Functions** — Virtual function templates with their inputs and outputs wired to specific buffers.

Virtual functions act as markers, making the system language-independent. The actual implementation can be provided in C, Go, Python, or any other language. The Scan Tree DSL defines structure and relationships; the runtime language fills in the behavior. An instantiated virtual function is the result of binding a template's inputs and outputs to concrete buffers.

---

## 6. Wiring Rules

- **Rule A:** All buffers, except for the top-level desired buffer, must be wired to one or more virtual functions.
- **Rule B:** All buffer inputs, except for raw buffer inputs, must come from virtual functions.

These two rules together enforce a complete and consistent dataflow graph — raw buffers are the only unconstrained sources, the top-level desired buffer is the only unconstrained sink, and all intermediate data must flow through virtual functions.

---

## 7. DSL and Entity Identification

- The system is defined using a stack-based DSL, typically implemented in Python or LuaJIT.
- The stack-based structure of the DSL inherently ensures that the tree is balanced — push/pop semantics enforce proper nesting.
- Entity identification uses the ltree hierarchical path concept (e.g., `plant.area3.pump7`) to generate unique IDs for all entities. This is a naming/addressing convention borrowed from PostgreSQL ltree semantics — it does not imply the use of a PostgreSQL database.

---

## 8. Scan Rate

The Scan Tree does not operate on a fixed scan cycle. The computational graph is evaluated when the caller invokes the graph engine. The scan rate is therefore determined by the caller, not by the Scan Tree itself.

---

## 9. Initial Conditions

On cold start, all bit buffers are initialized to the **Not Operational** state. This is the correct initial assumption — no feature has achieved its prerequisites, and no faults have been detected. The first invocation of the graph engine evaluates the entire graph against the initial raw buffer state, establishing the baseline from which subsequent change-driven evaluation proceeds.

---

## 10. Change-Driven Evaluation

In operation, raw buffer changes are typically very small — only a few bits change at a time. The system exploits this by calculating only the portions of the graph affected by changes:

- Changed bits in raw buffers are detected via double buffering.
- Only virtual functions whose inputs have changed are triggered.
- If a virtual function's output does not change as a result of the new inputs, no update is propagated and the calculation path stops at that point.
- Only changed fields propagate through the graph.

Computational cost is proportional to the amount of change in the system, not the total size of the I/O space — critical for scaling to large SCADA-class I/O counts where the vast majority of points are stable at any given moment.

---

## 11. Construction Process Outputs

The Scan Tree DSL construction process produces the following artifacts in the target language:

1. **Three-state bit buffers** — Defined in the target system (Active/Fault/Not Active).
2. **Read access to buffers** — Interfaces for reading the state of all bit buffers.
3. **Double-buffered raw buffers** — Raw buffers implemented as double buffers in the target language, enabling identification of changed fields between scans.
4. **Runtime virtual functions** — Target-language implementations of the virtual functions defined by the templates.
5. **Runtime engine** — An engine that executes the computational graph, driving change-driven evaluation across the wired dataflow.

---

## 12. Operator Interface Philosophy

- The system does not generate operator alarms.
- Instead, it presents a view of which features are active and which features have faults.
- The operator sees system status as a feature state map rather than reacting to alarm events.
- The recommended visualization approach is the geological-style cross-section model described in Appendix A, which serves both human operators and virtual operators/AI.

---

## 13. System Boundaries

- The system does not interact with the historian.
- The geological visualization model (Appendix A) is an interpretation and planning layer — it does not replace fault logic, supervisors, or actuator control, and is not a sole safety mechanism.

---

## 14. Fault Handling

Scan Tree engine faults are handled like any C fault — there is no special recovery mechanism within the engine. If a virtual function or the engine itself encounters an error, standard C error handling applies (return codes, error flags, etc.). The Scan Tree is a computational engine, not a fault-tolerant runtime; fault tolerance is the responsibility of the system architecture surrounding it.

---

## 15. Open Concerns

1. **Bit expression definition.** Behavior trees use "bit expressions" to control execution, but the top-level design does not characterize what a bit expression is. Are these boolean combinations of bitmap positions? Predicates over three-state values? A formal or informal definition is needed.

2. **Top-level desired buffer.** Rule A references the "top-level desired buffer" as the sole unconstrained sink, but this entity is not defined elsewhere. Its role as the root output of the entire computational graph should be explicitly stated and its semantics clarified.

3. **Action space.** The post-tick action phase is described as analogous to PLEXIL's invariant calculation, but the design does not characterize what kinds of actions are produced or where they are delivered. Since this is the system's output to the outside world, even a top-level description of the action types and destinations would be valuable.

4. **Graph cycle prevention.** The wiring rules ensure completeness, but the design does not explicitly state whether cycles in the dataflow graph are permitted or prohibited. For change-driven propagation to terminate, cycles must either be forbidden or handled with a convergence mechanism.

5. **Virtual function side effect boundaries.** Side effects are stated to be within the function, but the design does not specify what side effects are permissible. Can a virtual function write to external hardware? Communicate over a network? Or are side effects limited to internal state updates? Clarifying the boundary would help define portability and testability expectations.

6. **Double buffer swap timing.** The design specifies double-buffered raw buffers for change detection but does not state when the buffer swap occurs relative to the graph evaluation cycle. Whether the swap is atomic, caller-managed, or engine-managed affects correctness guarantees.

7. **Derived bitmap dependency ordering.** Derived bitmaps can depend on other derived bitmaps. The design does not state how evaluation order is determined. A topological sort of the dataflow graph is implied but not specified.

8. **Scalability bounds.** The design states applicability to SCADA-class I/O counts but does not characterize expected scale (thousands, tens of thousands, hundreds of thousands of I/O points) or any known resource constraints (memory footprint per buffer, maximum graph depth).

9. **DSL validation.** The stack-based DSL ensures balanced trees, and the wiring rules ensure graph completeness, but the design does not describe what other validations the construction process performs — type checking, reachability analysis, dead node detection, etc.

10. **Multi-instance coordination.** The design describes a single Scan Tree instance. It does not address whether multiple Scan Tree instances can coexist, how they would share or partition the I/O space, or whether cross-instance dependencies are possible.

---

## Appendix A: Geological-Style Visualization Model

This appendix describes the recommended approach for organizing and visualizing hierarchical bitmap layers as a geological cross-section. This model is a derived semantic layer suitable for human operators and virtual operators/AI. It sits between supervisors/containment logic and operator reasoning/policy selection. It is for interpretation, planning, and intent — not for control loops.

### A.1 Cross-Section Template

The system is visualized as a vertical cross-section of functional layers:

- **Control** — Behavior trees, sequencing, autonomy logic.
- **Supervision** — Supervisors, voting, restart/containment domains.
- **Diagnostics** — Health monitors, FMEA detectors.
- **Actuation** — Pumps, valves, motors, physical effectors.
- **Power** — Grid, buses, batteries, backup sources.

### A.2 Geological Primitives

- **Fold (buckle)** — Stress detected but contained (warning/pending fault).
- **Thrust fault (slip plane)** — Latched fault or supervisor-declared failure.
- **Detachment horizon** — Fault-containment boundary.
- **Imbricate stack** — Multiple redundant elements failing (k-of-n).
- **Duplex wedge** — Nested supervisors trapping faults.
- **Triangle zone** — Mode-dependent propagation (maintenance/startup).

### A.3 Failure Progression Stages

The visualization maps directly to system state, progressing through recognizable stages:

- **Stage A — Flat strata:** Fully healthy. No faults, no stress, redundancy fully available.
- **Stage B — Local fold:** Early warning. Leaf event asserted but no derived failures.
- **Stage C — Contained thrust:** Component failure with containment holding. System degraded yet stable.
- **Stage D — Imbricate stack:** Redundancy consumption. Multiple redundant elements failing, margin shrinking.
- **Stage E — Duplex wedges:** Containment leakage. Local recovery fails, escalation to higher supervisors.
- **Stage F — Through-going fault:** Common-mode failure. Loss of shared infrastructure cuts across all layers.
- **Stage G — Buckled upper layers:** Voting threshold exceeded. Designed safety threshold crossed.
- **Stage H — Full collapse:** Top event true. System can no longer perform its function.

### A.4 Deterministic Generation Rules

The visualization is computed, not hand-drawn:

- Fold intensity ← fault pending score/trend.
- Slip plane ← latched fault or supervisor failure.
- Imbrication count ← popcount of redundant failures.
- Detachment holds ← failures below threshold and no common-mode.
- Through-going plane ← common-mode or power loss.
- Collapse ← top fault event true.

### A.5 Virtual Operator / AI Consumption

The geological model is not merely a human UI metaphor. It functions as a compressed, physically meaningful state abstraction that is well-suited for virtual operators and supervisory AI. It provides:

- **State compression** — A low-dimensional projection of high-dimensional fault space. Mathematically similar to hybrid automata mode surfaces, energy landscape models, and constraint satisfaction with soft limits.
- **Causal directionality** — Fault progression as trajectory (stages A→H), not isolated events. Gives direction of degradation, speed, likely next transitions, and remaining control authority.
- **Margin-to-failure awareness** — Imbricate stacks encode redundancy consumption, proximity to voting thresholds, and fragility vs. robustness.
- **Containment as conditional independence** — Detachment horizons are formal conditional independence boundaries. Subsystems below a holding detachment can be reasoned about independently and recovery can be localized.
- **Restart/replan domains** — Duplex wedges map to restart domains, replanning scopes, and safety envelopes.
- **Prediction and counterfactuals** — Because the model is deterministic and compositional, a virtual operator can simulate future faults, test interventions, and evaluate counterfactuals.

### A.6 Layer Mapping for Virtual Operator Reasoning

| Geological Layer | Virtual Operator Abstraction |
|---|---|
| Power | Resource availability |
| Actuation | Effectors |
| Diagnostics | Observability confidence |
| Supervision | Policy constraints |
| Control | Intent / plan execution |

This lets the operator reason top-down or bottom-up without switching models.

### A.7 Placement in AI Stack

The geological model sits in the following position:

Sensors/Telemetry → Diagnostics (FMEA leaf bits) → Fault Logic (FTA → bitmasks) → Supervisors (containment/recovery) → **Geological State Projection** → Virtual Operator Reasoning → Policy/Action Selection.

The geological model is the state abstraction layer, not the controller. It should not replace fault logic, replace supervisors, drive actuators directly, or be used as a sole safety mechanism.

### A.8 Advantages Over Conventional Dashboards

Traditional monitoring provides alarms, trends, and status lights. The geological model provides structure, causality, containment, margin, and direction — exactly what virtual operators lack today.