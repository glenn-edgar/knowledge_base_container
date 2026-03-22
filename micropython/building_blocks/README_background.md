# The Philosophical Foundations of PlanSys2

> *PlanSys2 is not just a software framework. It is the convergence point of three distinct intellectual traditions — each carrying its own assumptions, its own unresolved tensions, and its own implicit philosophy of what an "intelligent agent" fundamentally is.*

---

## Table of Contents

1. [Overview: Three Traditions, One Framework](#1-overview-three-traditions-one-framework)
2. [Layer 1 — The Logical Foundation: STRIPS and the Situation Calculus (1969–1971)](#2-layer-1--the-logical-foundation-strips-and-the-situation-calculus-19691971)
   - 2.1 [McCarthy and Hayes: The Situation Calculus](#21-mccarthy-and-hayes-the-situation-calculus)
   - 2.2 [The Frame Problem](#22-the-frame-problem)
   - 2.3 [STRIPS: The Pragmatic Cut](#23-strips-the-pragmatic-cut)
   - 2.4 [The Closed World Assumption and Its Costs](#24-the-closed-world-assumption-and-its-costs)
   - 2.5 [PDDL as STRIPS's Descendant](#25-pddl-as-stripss-descendant)
3. [Layer 2 — The Cognitive Foundation: BDI and Practical Reasoning (1987–1995)](#3-layer-2--the-cognitive-foundation-bdi-and-practical-reasoning-19871995)
   - 3.1 [Bratman's Philosophy of Intention](#31-bratmans-philosophy-of-intention)
   - 3.2 [Rao and Georgeff: Formalizing BDI](#32-rao-and-georgeff-formalizing-bdi)
   - 3.3 [The Deliberation vs. Commitment Tension](#33-the-deliberation-vs-commitment-tension)
   - 3.4 [BDI in PlanSys2](#34-bdi-in-plansys2)
4. [Layer 3 — The Architectural Tension: Deliberative vs. Reactive](#4-layer-3--the-architectural-tension-deliberative-vs-reactive)
   - 4.1 [The Deliberative Tradition](#41-the-deliberative-tradition)
   - 4.2 [The Reactive Critique](#42-the-reactive-critique)
   - 4.3 [Behavior Trees as Compromise](#43-behavior-trees-as-compromise)
   - 4.4 [PlanSys2 as Integration Attempt](#44-plansys2-as-integration-attempt)
5. [The Deeper Problem: Symbol Grounding](#5-the-deeper-problem-symbol-grounding)
   - 5.1 [What the Planner Cannot Know](#51-what-the-planner-cannot-know)
   - 5.2 [Where the Symbol Meets the World](#52-where-the-symbol-meets-the-world)
   - 5.3 [The Unverified Reconnection](#53-the-unverified-reconnection)
6. [The Frame Problem's Philosophical Legacy](#6-the-frame-problems-philosophical-legacy)
   - 6.1 [The Yale Shooting Problem](#61-the-yale-shooting-problem)
   - 6.2 [Non-Monotonic Reasoning and Its Limits](#62-non-monotonic-reasoning-and-its-limits)
   - 6.3 [The Commonsense Law of Inertia](#63-the-commonsense-law-of-inertia)
7. [What PlanSys2 Inherits from Each Tradition](#7-what-plansys2-inherits-from-each-tradition)
8. [The Gap That Remains](#8-the-gap-that-remains)
9. [The Unified DSL as Philosophical Response](#9-the-unified-dsl-as-philosophical-response)
10. [Further Reading](#10-further-reading)

---

## 1. Overview: Three Traditions, One Framework

PlanSys2 sits at the intersection of three intellectual genealogies that developed largely independently and were never formally reconciled:

**Genealogy 1 — Symbolic AI / Automated Planning (SRI, 1966–1971)**
The tradition of representing the world as a set of logical predicates and treating planning as theorem proving. Rooted in the Shakey robot project and crystallized in STRIPS (1971). This tradition gave us PDDL, the Domain Expert, the Problem Expert, and the Planner node.

**Genealogy 2 — Philosophy of Mind / Intentional Agency (Bratman 1987, Rao & Georgeff 1991–1995)**
The tradition of modeling agents using the folk-psychological concepts of Belief, Desire, and Intention, formalized into modal temporal logic. This tradition gave us the conceptual architecture underlying PlanSys2's four-node design and the Sense-Plan-Act loop.

**Genealogy 3 — Behavior-Based Robotics / Reactive Systems (Brooks 1986, BT.CPP)**
The tradition of rejecting explicit world models in favor of reactive, hierarchically organized behavior. This tradition gave us the Executor's behavior tree transformation — the execution engine that actually touches hardware.

PlanSys2 integrates all three with C++ and ROS2 middleware. The glue works. But the integration is architectural, not representational — each tradition retains its own language, its own data structures, and its own implicit assumptions about what the world is and how an agent relates to it.

Understanding *why* PlanSys2 is built the way it is requires tracing each genealogy to its philosophical origin.

---

## 2. Layer 1 — The Logical Foundation: STRIPS and the Situation Calculus (1969–1971)

### 2.1 McCarthy and Hayes: The Situation Calculus

The story begins with two papers from Stanford Research Institute that together defined the field of AI planning.

John McCarthy and Patrick J. Hayes, in their 1969 paper *"Some Philosophical Problems from the Standpoint of Artificial Intelligence,"* introduced the **situation calculus**: a first-order predicate logic formalism for reasoning about how the world changes over time in response to actions.

The ontology is simple:
- **Situations** — complete snapshots of the world at a point in time. The initial situation is `S0`. An action applied to a situation produces a new situation via the `do(action, situation)` function.
- **Fluents** — properties of the world that can be true or false and that may change from situation to situation. `at(robot, room1, S0)` means the robot is in room1 at situation S0.
- **Actions** — transformations from one situation to another. `do(move(robot, room1, room2), S0)` produces situation `S1`.

This was a profound move: time and change encoded in pure first-order logic, potentially amenable to mechanical theorem proving. The appeal was immense. If you could represent what the world is, what actions do, and what you want, then a theorem prover could find the plan — automatically, rigorously, provably.

The immediate obstacle was that this appeal was not realized.

### 2.2 The Frame Problem

McCarthy and Hayes identified the obstacle themselves. They named it the **frame problem**.

The problem is this: formal logic describes *what changes* when an action is performed. It says nothing about *what does not change*. But our commonsense understanding of actions is saturated with non-effects. When you move a box, its color doesn't change. When you turn on a light, the positions of the furniture don't change. When a robot navigates from room A to room B, the temperature of room A doesn't change.

In classical logic, the only way to know that something didn't change is to explicitly state that it didn't change. For every action and every fluent that the action does *not* affect, you need a **frame axiom**:

```
Color(x, c) holds after Move(x, p) if Color(x, c) held beforehand
Position(x, p) holds after Paint(x, c) if Position(x, p) held beforehand
```

The combinatorial explosion is immediate. With 1,000 fluents and 100 actions, the vast majority of action-fluent combinations are non-effects. You need roughly 99,000 frame axioms to formally specify what everybody already knows intuitively. The situation calculus, as McCarthy and Hayes formulated it, was computationally intractable for any realistic domain.

The frame problem is more than a technical nuisance. In the words of the Stanford Encyclopedia of Philosophy, it touches on the holistic nature of intelligence and is considered central to the development of cognitive science. Human reasoning manages frame inference effortlessly — we assume, without deliberation, that the world is mostly stable and only update what we have reason to update. No formal logic system built before the 1990s could replicate this.

The deeper philosophical question the frame problem raises: is the world a collection of independent facts that can be independently updated? Or is the world a holistic whole where any change potentially ramifies everywhere? Classical logic treats it as the former. Reality is closer to the latter. Every act of driving a robot changes air molecules, floor stress, thermal profiles, acoustic signatures. The symbolic world model works only because we agree, by convention, to ignore all of it.

### 2.3 STRIPS: The Pragmatic Cut

Fikes and Nilsson's 1971 paper introduced STRIPS (STanford Research Institute Problem Solver) as a direct response to the frame problem's intractability. Their solution was not to solve the philosophical problem but to sidestep it with a pragmatic restriction.

STRIPS represents the world as a set of ground, function-free, positive first-order predicate calculus literals — a flat list of things that are currently true. Actions are described by three components:

- **Preconditions** — a conjunction of literals that must be in the current world model for the action to apply
- **Add list** — literals added to the world model when the action executes
- **Delete list** — literals removed from the world model when the action executes

The elegant simplification: *anything not on the add list or delete list is unchanged, by assumption.* This is not a logical theorem — it is a convention. The frame problem is not solved; it is dissolved by restricting the representation. STRIPS operates on what is called the **closed world assumption**.

The architecture of STRIPS was equally significant. It combined **means-ends analysis** (GPS-style: identify the "difference" between current state and goal, find an operator that reduces that difference) with a **resolution theorem prover** (to verify whether preconditions hold in the current world model). Planning became: find a sequence of operators that transforms the initial world model into one where the goal formula is provable.

This was demonstrated on the Shakey robot — a physical mobile platform at SRI that could recognize rooms and boxes, generate plans using STRIPS, and execute those plans in the real world. For the first time, a physical robot could be given a symbolic goal ("get box B to room C") and reason about how to achieve it.

STRIPS was immediately influential and immediately criticized. The means-ends analysis was sound in closed, simple domains. In complex domains, it produced the **Sussman Anomaly** — situations where achieving one subgoal necessarily undoes another, forcing iterative interleaving that pure means-ends analysis cannot handle. The anomaly revealed that planning is more complex than sequential subgoal satisfaction; it requires reasoning about the *interactions* between goals.

### 2.4 The Closed World Assumption and Its Costs

The closed world assumption deserves philosophical attention because it is the silent axiom that makes all of STRIPS-descended planning tractable — and it is never explicitly stated in PDDL domain files.

The closed world assumption asserts: **anything not known to be true is false**. If `(at robot room2)` is not in the predicate state, the robot is not in room2. There is no uncertainty, no partial information, no "unknown." The world model is always complete and consistent.

This is a dramatic ontological commitment. It requires:

1. **Complete observability** — you must always know the full world state
2. **Perfect action models** — the add/delete lists must perfectly capture every effect of every action
3. **No exogenous change** — nothing in the world changes except through the actions you model
4. **Deterministic actions** — executing an action always produces exactly the effects listed

None of these hold in the physical world. Sensors fail, are noisy, or don't cover the full state. Action effects are probabilistic. External agents and events modify the world without the planner's knowledge. This is why the behavior tree execution layer exists at all — it is the mechanism for handling the gap between the planner's idealized world model and the actual physical reality in which the robot operates.

### 2.5 PDDL as STRIPS's Descendant

PDDL (Planning Domain Definition Language) is STRIPS's syntactic evolution after thirty years of academic committee work. The core machinery is identical:

```lisp
(:action pick
  :parameters (?obj ?room ?gripper)
  :precondition (and (at ?obj ?room) (at-robby ?room) (free ?gripper))
  :effect (and (carry ?obj ?gripper)
               (not (at ?obj ?room))
               (not (free ?gripper))))
```

The `precondition` is STRIPS's precondition list. The positive conjuncts in `effect` are the add list. The `(not ...)` conjuncts in `effect` are the delete list. Decades of academic syntax sits on top of a 1971 data structure.

PDDL 2.1 added numeric fluents (real-valued state variables, not just Boolean), durative actions (actions that take time), continuous effects (fluents that change continuously during action execution), and plan metrics (optimization objectives). These extensions made PDDL substantially more expressive — but also substantially harder to plan over. PlanSys2 currently supports PDDL 2.1 using POPF and Fast Downward as solvers.

The key point: **every predicate in your PlanSys2 domain is a direct descendant of STRIPS's closed world model from 1971.** When you write `(cargo-delivered)` as an effect, you are writing an add-list entry. When you write `(not (cargo-loaded))`, you are writing a delete-list entry. The philosophical commitments of the closed world assumption come with it, whether or not they are acknowledged.

---

## 3. Layer 2 — The Cognitive Foundation: BDI and Practical Reasoning (1987–1995)

### 3.1 Bratman's Philosophy of Intention

STRIPS treats an agent as a theorem prover with a goal formula. It has no model of *why* the agent has that goal, no account of what it means for an agent to *commit* to a plan, and no mechanism for deciding when to abandon a plan and form a new one.

Michael Bratman's 1987 book *Intention, Plans, and Practical Reason* filled this gap by importing the structure of human practical reasoning into AI. Bratman's central question was: how do human agents plan for the future, and what is the role of intention in that planning?

His answer drew on **folk psychology** — the common-sense conceptual vocabulary we use to explain and predict human behavior: beliefs, desires, and intentions.

- **Beliefs** are the agent's representation of what the world is like. They are not necessarily accurate — an agent can have false beliefs — but they are the agent's working model of reality.
- **Desires** are what the agent wants to be true of the world. They can be mutually inconsistent — you can desire both to eat cake and to lose weight.
- **Intentions** are different from desires in a critical way: they carry **commitment**. An intention is a desire that the agent has resolved to pursue. Intentions are stable across time. You don't reconsider your intention to fly to Boston every five minutes while driving to the airport — that would be computationally paralyzing. Intentions constrain further deliberation by excluding incompatible options from consideration.

The causal sequence in Bratman's model: beliefs and desires jointly generate *options*, the agent deliberates among options to form an intention, and the intention causes action. But it is the *intention* — not the desire — that directly causes action. Desires motivate; intentions commit.

Bratman further distinguished between intentions and plans: plans are *partially ordered hierarchical structures* that provide the means for executing intentions. Plans can be elaborated incrementally — you don't need to work out every detail before committing. This hierarchical, partially-specified structure of plans is directly reflected in the hierarchical task decomposition used by PlanSys2's plan executor.

### 3.2 Rao and Georgeff: Formalizing BDI

Anand Rao and Michael Georgeff at the Australian AI Institute (1991–1995) translated Bratman's philosophical theory into a formal computational architecture.

Their contribution was **BDI logic**: a multimodal temporal logic with possible-world semantics, in which beliefs, desires, and intentions each correspond to an accessibility relation over a space of possible worlds. The agent's beliefs are the set of worlds consistent with what it currently knows. Its desires are the worlds it would like to reach. Its intentions are the worlds it is committed to reaching.

This formal semantics gave BDI architecture a rigorous mathematical foundation — one that could be used to *prove* properties of agent behavior, not just implement it.

The computational instantiation was the **Procedural Reasoning System (PRS)**, one of the earliest implemented BDI agents, which was used for fault diagnosis on the Space Shuttle and for factory process control. PRS demonstrated that BDI was not just a philosophical framework but an engineering approach that worked on real problems.

The BDI framework can be understood across three levels simultaneously:
- **Philosophical level** — Bratman's folk-psychological vocabulary, explaining behavior in terms of mental attitudes
- **Logical level** — Rao and Georgeff's modal temporal logic, providing formal semantics
- **Implementation level** — the computational architecture, consisting of a belief store, desire store, intention stack, and deliberation engine

PlanSys2 operates primarily at the implementation level, but the conceptual structure of its four nodes directly mirrors the BDI logical architecture.

### 3.3 The Deliberation vs. Commitment Tension

The central tension in BDI architecture is between **deliberation** and **commitment**.

A fully deliberative agent reconsiders all its intentions on every cycle: examines its current beliefs, regenerates all possible plans, selects the globally optimal intention. Computationally perfect. Temporally impossible. A robot that re-plans from scratch every 10 milliseconds will never take an action.

A fully committed agent never reconsiders: once an intention is formed, it executes the plan to completion regardless of what happens in the world. Fast and predictable. But brittle. If the world changes significantly — a door that was open is now closed, a box that was in room A is now in room C — the committed agent will blindly pursue a plan that can no longer succeed.

Real agency lives in the tension. The key question is: **under what conditions should an agent reconsider a commitment?** Bratman's answer: an agent should reconsider when either (1) the plan is no longer feasible — the preconditions for execution can no longer be satisfied — or (2) the intention is no longer desirable — the goal that motivated it is no longer worth pursuing.

This is the theoretical basis for PlanSys2's replanning trigger: the Executor detects when a BT action node fails (plan is no longer feasible at this step) and signals the Problem Expert to update state and the Planner to regenerate a plan. The Sense-Plan-Act loop is the BDI deliberation cycle made concrete.

The cost of replanning is not just computational. Every replan is an admission that the world model was wrong. Every replan discards accumulated execution context — where the robot physically is, what physical state the actuators are in, what partial progress has been made. PlanSys2's replan-from-scratch approach discards all of this in exchange for a fresh globally optimal plan. This is philosophically clean but operationally expensive.

### 3.4 BDI in PlanSys2

The mapping from BDI concepts to PlanSys2 components is direct:

| BDI Concept | PlanSys2 Component | Role |
|---|---|---|
| **Beliefs** | Problem Expert | Live predicate state — what the system currently knows to be true |
| **Desires** | Goal declarations | What state the system wants to achieve |
| **Intentions** | Currently executing plan + BT | The committed course of action |
| **Deliberation** | Planner (POPF/Fast Downward) | Selecting the optimal action sequence from current beliefs and desires |
| **Means-ends reasoning** | Executor (plan → BT transformation) | Identifying how to achieve the sequenced intentions through concrete actions |
| **Belief revision** | Predicate state updates after each action | Updating the world model to reflect what actually happened |
| **Plan library** | BT XML files per action | The set of available execution strategies |

The Sense-Plan-Act loop — sense the world (update Problem Expert predicates), plan (run the PDDL planner), act (execute the BT) — is the BDI deliberation-execution cycle operating at timescale dictated by the planner's runtime.

One critical BDI property that PlanSys2 *lacks*: **explicit goal reasoning**. In a full BDI architecture, the agent can reason about whether its desires are still worth pursuing — whether the goal is still achievable, consistent, or motivationally relevant. PlanSys2's goal is set by the calling application and treated as fixed throughout execution. There is no mechanism for the planning system itself to reconsider whether the goal is still appropriate given changes in the world. This is an intentional simplification — it keeps the system tractable — but it is a philosophical departure from Bratman's model, in which agents are supposed to be capable of goal revision under changed circumstances.

---

## 4. Layer 3 — The Architectural Tension: Deliberative vs. Reactive

### 4.1 The Deliberative Tradition

STRIPS and BDI are both **deliberative** architectures. The defining characteristic of deliberation: the agent builds an *explicit symbolic model* of the world, reasons over that model to select actions, and then executes those actions in the hope that the world conforms to the model.

Deliberation gives you:
- Goal-directed behavior — the agent can pursue complex, long-horizon objectives
- Explainability — you can inspect the world model and the plan and understand why the agent is doing what it's doing
- Optimality — given a correct world model, the planner can find the globally optimal action sequence

Deliberation costs you:
- Model accuracy — the world model must accurately reflect the real world, which requires accurate sensing and accurate action models
- Computational time — planning can be expensive, and the world changes while planning is in progress
- Brittleness — if the world deviates from the model, the plan may be entirely invalid

The deliberative agent's relationship to time is strained. Planning happens in the agent's "mind" — in computation, separate from the physical world. While the planner is running, the world continues to change. The plan that emerges is a plan for the world as it was when planning began, not the world as it is when execution begins.

### 4.2 The Reactive Critique

Rodney Brooks, in his 1986 paper *"A Robust Layered Control System for a Mobile Robot"* and his 1990 paper *"Elephants Don't Play Chess,"* delivered a sharp challenge to the deliberative tradition.

Brooks's argument: intelligent behavior in the real world does not require symbolic world models. Insects navigate complex environments, avoid obstacles, and accomplish biological goals without any explicit representation of their environment. The world itself is the model — the agent needs only the ability to perceive and react appropriately.

His subsumption architecture organized behavior as layers of stimulus-response rules, each layer able to override lower layers. No planning, no world model, no goal representation. Just fast reactive loops at multiple levels of abstraction.

The reactive critique landed hard. Deliberative robots in the 1980s were notoriously slow and brittle. Shakey needed minutes to plan a simple navigation task. Brooks's reactive robots moved quickly and handled unexpected obstacles in real time. For many robotics problems, fast and approximately right beats slow and globally optimal.

The deep philosophical claim underlying the reactive approach: **representation is a liability, not an asset.** Every symbolic representation introduces a gap between the representation and reality. The more elaborate the representation, the more ways it can be wrong. An agent that uses the world directly as its model — that acts on raw sensor data rather than symbolic abstractions — never suffers from a stale or inaccurate world model.

### 4.3 Behavior Trees as Compromise

Behavior trees emerged from game AI in the late 2000s as a practical compromise between deliberative and reactive extremes.

A behavior tree is a hierarchical composition of action and condition nodes, organized by control flow nodes (sequence, fallback, parallel). The tick-based execution model is reactive — the tree is re-evaluated on every cycle, and the robot's current behavior is determined entirely by the current tree structure and the results of leaf node checks. There is no persistent "current plan" that accumulates commitment. Every tick re-evaluates from the root.

Yet behavior trees are typically *designed* using deliberative thinking. The developer constructs the tree based on their understanding of what the robot needs to do and in what order. The deliberation happens at design time, not at runtime. The runtime execution is entirely reactive.

This is the genius and the limitation of behavior trees. They are easy to implement and modify. They handle unexpected conditions gracefully because every tick re-evaluates conditions. They compose cleanly and are easy to reason about locally. But they encode deliberation as a *static artifact* — the designed tree structure — rather than as a *dynamic process* — the planner's output. A behavior tree cannot adapt its high-level strategy to changed world circumstances without human redesign. The planner can.

BehaviorTree.CPP deliberately rejected scripting languages in favor of pure C++, treating tree *structure* definition (via XML) as the only runtime-flexible element. All action logic is compiled C++. This made BT.CPP fast and robust but opaque — the behavior logic is distributed across dozens of C++ files, not in the tree structure where it can be inspected.

### 4.4 PlanSys2 as Integration Attempt

PlanSys2's architectural decision — translate PDDL plans into behavior trees for execution — is an attempt to combine deliberative planning's goal-directedness with behavior trees' reactive robustness.

The PlanSys2 executor:
1. Receives a flat action sequence from the planner
2. Analyzes the sequence for independent execution flows (actions whose preconditions and effects don't interact)
3. Builds a BT that runs independent flows in parallel and dependent flows in sequence
4. Executes the BT using BT.CPP, where each action node communicates with an action performer via the `/action_hub` topic

Each PDDL action is implemented as a BT subtree (a BT XML file) backed by C++ BT node plugins. The PDDL action arguments are passed to the BT via blackboard values (`arg0`, `arg1`, etc.).

The integration works mechanically. But it is integration by *translation*, not by *unification*. The PDDL representation and the BT representation remain separate. The developer must maintain both and ensure they remain consistent. There is no formal connection between `(effect cargo-delivered)` in the PDDL and the force sensor check in the BT.

The philosophical irony: PlanSys2 uses deliberative planning to determine *what* to do and reactive behavior trees to determine *how* to do it — but the boundary between "what" and "how" is drawn by the developer, manually, and is never checked.

---

## 5. The Deeper Problem: Symbol Grounding

### 5.1 What the Planner Cannot Know

The PDDL planner operates entirely within the symbolic world model. It manipulates predicates according to the add/delete rules of each action. It has no access to the physical world — no sensors, no actuators, no concept of physical space, force, time, or failure.

This is correct by design. The planner's strength is precisely its abstraction from physical detail. By operating on a simplified symbolic model, the planner can search efficiently over long time horizons and find action sequences that no purely reactive system could discover.

But this abstraction carries a profound assumption that is never made explicit: **the symbolic predicates correctly represent the physically relevant aspects of the world.** `(robot-at warehouse)` is a symbol. The planner treats it as a Boolean that certain actions set true and others set false. The planner has no concept of what it means for a robot to *physically be at* a warehouse, what sensor confirms this, what tolerance is acceptable, or what failures are possible.

This is the **symbol grounding problem**, articulated by Stevan Harnad in 1990. Symbols in a formal system are defined purely by their relationships to other symbols. The system is self-referential. But symbols in real AI applications must ultimately connect to non-symbolic physical reality — to pixels, to sensor voltages, to actuator positions. That connection is not provided by the formal system. It must be supplied externally.

In PlanSys2, symbol grounding is supplied by the C++ BT node plugins. The `(robot-at warehouse)` predicate is grounded by a BT `Condition` node that calls the AMCL localization service and checks whether the robot's pose is within a threshold of the warehouse's coordinates. This grounding is written by the developer and lives entirely outside the PDDL representation.

### 5.2 Where the Symbol Meets the World

The exact point at which a PDDL symbol makes contact with physical reality is the `verify` check at the end of an action's execution. In PlanSys2's architecture, this is the BT node that confirms an action's effect was actually achieved before the action is reported as successful.

But PlanSys2 does not enforce — or even encourage — that such verification exists. A BT action node can report SUCCESS without confirming any physical state. The PDDL planner will then proceed as though the effect predicates were asserted, building subsequent plan steps on a foundation that may be false.

This is the bug class that lives in the gap between the PDDL representation and the BT execution. The planner assumes `(cargo-delivered)` is true because the BT action node reported SUCCESS. The sensor that would confirm actual delivery may never have been checked. The predicate state diverges from physical reality. Subsequent plans are built on a lie.

### 5.3 The Unverified Reconnection

The verification gap is philosophically interesting because it is not a *bug in the logic*. The PDDL domain is internally consistent. The BT implementations are individually correct. The failure is in the *interface* between the two — in the implicit assumption that a BT SUCCESS corresponds to the predicate effects listed in the PDDL action.

This interface assumption is the residue of the symbol grounding problem. PDDL says: this action has effect `(cargo-delivered)`. BT says: this action called `deliver_cargo` succeeded. The mapping between "effect `(cargo-delivered)`" and "called `deliver_cargo` with SUCCESS" is made by the developer in launch files and YAML configuration. It is a naming convention, not a formal proof.

In a language where planning semantics and execution semantics are unified — where the `effect` clause and the `verify` clause live in the same node definition — this gap is structurally visible and mechanically checkable. In PlanSys2's architecture, it is invisible.

---

## 6. The Frame Problem's Philosophical Legacy

### 6.1 The Yale Shooting Problem

The frame problem was not "solved" by STRIPS's closed world assumption — it was deferred. When researchers in the 1980s returned to the situation calculus seeking a proper logical solution, they discovered the deferral had costs.

McCarthy proposed **circumscription** as a solution: minimize the number of changes that occur when an action is performed. The commonsense inference "if you load a gun and fire it, the victim dies" should follow without explicitly listing all the things that *don't* change.

Hanks and McDermott (1987) demonstrated with the **Yale Shooting Problem** that circumscription produces anomalies. The scenario: a gun is unloaded, then loaded, then some time passes, then it is fired at Fred. Formalizing this in temporal logic and applying circumscription to conclude the outcome yields *two equally valid solutions*: (1) Fred dies (as expected) or (2) the gun mysteriously unloads itself during the wait and Fred survives. Circumscription minimizes changes, but there are two different ways to minimize — and the logic cannot distinguish between them without additional, non-trivial axioms.

The Yale Shooting Problem demonstrated that non-monotonic reasoning — any logic that defaults to stability and allows exceptions — faces fundamental ordering problems in temporal domains. The "obvious" solution (Fred dies) is only obvious to humans because we implicitly order our reasoning: we apply the shooting law before considering whether the gun might have spontaneously unloaded. Logic has no such implicit ordering.

### 6.2 Non-Monotonic Reasoning and Its Limits

The frame problem drove thirty years of research into non-monotonic reasoning formalisms:

- **Circumscription** (McCarthy 1980/1986) — minimize what changes
- **Default logic** (Reiter 1980) — assume defaults unless contradicted
- **Autoepistemic logic** (Moore 1985) — reason about your own ignorance
- **Successor state axioms** (Reiter 1991) — describe when fluents change, deriving persistence as a consequence
- **Fluent calculus** (Thielscher 1998/2001) — represent states as compositional terms, solve the frame problem by construction

Each formalism successfully handled cases the previous ones failed on. By the early 2000s, the frame problem "as originally formulated" was considered solved — in the limited sense that formal solutions exist for closed, finite, deterministic domains.

But the philosophical problem it exposed did not disappear. The problem was that classical deductive logic — the foundation of symbolic AI — is monotonic: adding facts can only entail more conclusions, never retract previous ones. The real world is non-monotonic: new information routinely forces retraction of previous conclusions. No medical patient is "healthy" forever; no robot is always "at location A." Logic must be extended, in non-trivial ways, to handle this basic feature of temporal reality.

PDDL sidesteps this by making the world representation explicitly *imperative* rather than *declarative*: the programmer specifies exactly which predicates change (the add/delete lists), and everything else is assumed stable. This works. It does not generalize to domains where you don't know in advance what might change.

### 6.3 The Commonsense Law of Inertia

The principle that underlies all successful solutions to the frame problem is the **commonsense law of inertia**: fluents persist across time unless there is a specific reason for them to change. This is what humans implicitly apply when reasoning about the world.

The difficulty is that this principle is **defeasible** — it can be overridden by more specific information. A fluent that normally persists can change unexpectedly. An action that normally succeeds can fail. A robot that was "at location A" five minutes ago may no longer be at location A if someone moved it. The law of inertia is a *default*, not a theorem.

Managing defeasible defaults in formal reasoning requires non-classical logic. This is computationally expensive and difficult to reason about correctly. STRIPS and PDDL avoid the problem by assuming the developer knows exactly what changes and what doesn't — i.e., by treating the inertia law as exact rather than defeasible. The cost is that PDDL cannot represent uncertainty, cannot represent partial information, and cannot handle unexpected state changes except by detecting plan failure and replanning from scratch.

Behavior trees, by contrast, operate entirely on sensor data and do not maintain persistent world state across ticks. They handle unexpected state changes gracefully because they have no persistent state to become inconsistent. Their execution model is inherently non-monotonic: what was true on the last tick may not be true on this tick, and the tree adapts automatically.

This is the philosophical basis for why behavior trees are the right execution layer: they implement a form of reactive defeasible reasoning that PDDL's monotonic closed-world model cannot provide.

---

## 7. What PlanSys2 Inherits from Each Tradition

**From STRIPS/PDDL:**
- The Boolean predicate model of world state
- The closed world assumption
- The add/delete/precondition operator structure
- The goal-directed planning search
- The symbol grounding problem (inherited without acknowledgment)
- The assumption of complete observability and deterministic action effects

**From BDI:**
- The Sense-Plan-Act deliberation cycle
- The separation of belief state (Problem Expert) from goal state (goals) from intention state (current plan)
- The concept of plan commitment and replanning triggers
- The conceptual framework for multi-agent coordination via shared belief state
- The absence of explicit goal revision (intentionally simplified away)

**From behavior-based robotics:**
- The reactive, tick-based execution model
- The handling of unexpected conditions through tree fallback structures
- The distribution of "how to do X" logic across decoupled node plugins
- The rejection of persistent, plan-level world state during execution
- The implicit, developer-supplied symbol grounding

---

## 8. The Gap That Remains

The three traditions in PlanSys2 are not merely different tools. They carry different — and in some respects contradictory — ontological commitments.

**STRIPS/PDDL assumes:** the world is fully knowable as a complete set of Boolean facts; actions are deterministic; the planner has God's-eye-view of the world state.

**BDI assumes:** the agent's beliefs are necessarily partial and possibly incorrect; commitment to a plan is a practical resource-saving heuristic, not a metaphysical claim; the agent must be able to revise beliefs and goals dynamically.

**Reactive/BT assumes:** the world is only accessible through real-time sensor data; there is no useful persistent world representation; intelligent behavior emerges from fast reactive loops, not from symbolic deliberation.

These three assumptions cannot all be true simultaneously. PlanSys2 papers over the contradiction by using each tradition for what it's good at and connecting them at the interface. PDDL for strategic sequencing. BDI for conceptual framing. BT for tactical execution. The interface between them — the mapping from PDDL effects to BT verification — is where the philosophical contradictions surface as engineering bugs.

**The gap, stated precisely:** PDDL says an action has an effect. BT says an action succeeded. *That these two statements mean the same thing* is an assumption made by the developer and enforced by nothing.

---

## 9. The Unified DSL as Philosophical Response

The unified S-expression planning DSL (described in [README_unified_sexpr_planning_agent.md](README_unified_sexpr_planning_agent.md)) is, in its deepest form, a response to the philosophical problems outlined here.

**On symbol grounding:** By co-locating the `effect` clause and the `verify` clause in the same node definition, the DSL makes the symbol grounding obligation *structurally visible*. The `--validate` compiler target mechanically checks that every `effect` predicate has a corresponding `verify` — that every claim the planner makes about an action's outcome is confirmed by a physical sensor check in the execution block. This does not solve the symbol grounding problem philosophically, but it converts a hidden assumption into an explicit, checkable constraint.

**On the frame problem:** The DSL inherits STRIPS's closed-world assumption for the planning layer — it is pragmatically unavoidable for tractable planning. But the `verify` mechanism in the execution layer provides a form of defeasible checking that PDDL lacks: if the physical world does not match the expected predicate state after execution, the failure is detected and can trigger replanning. The DSL thus implements a version of the "interleaved planning and execution" approach where each action's effect is empirically confirmed before being asserted into the predicate state.

**On deliberative vs. reactive integration:** Rather than maintaining two separate representations (PDDL for deliberation, BT XML for reaction) that must be manually synchronized, the DSL maintains one representation with two views. The compiler extracts the planning view (`--pddl`) and the execution view (`--bt-xml`, `--micropython`, `--lua53`) from the same source. The representations cannot diverge because they are derived from a common authoritative definition.

**On the philosophical contradiction:** The DSL does not resolve the deep contradiction between PDDL's omniscient-world-model assumption and the physical world's intractability. No formal language can. But by making the boundary between planning representation and physical verification explicit — by forcing the developer to specify both the symbolic effect *and* the physical confirmation in the same expression — it makes the contradiction visible at compile time rather than at runtime.

The goal is not philosophical completeness. It is engineering honesty: a representation that accurately reflects the assumptions being made and provides tools for checking whether those assumptions hold in practice.

---

## 10. Further Reading

### Foundational Papers

- **Fikes, R. E. and Nilsson, N. J.** (1971). *STRIPS: A New Approach to the Application of Theorem Proving to Problem Solving.* Artificial Intelligence, 2, 189–208.
- **McCarthy, J. and Hayes, P. J.** (1969). *Some Philosophical Problems from the Standpoint of Artificial Intelligence.* Machine Intelligence 4, Edinburgh University Press, pp. 463–502.
- **Bratman, M. E.** (1987). *Intention, Plans, and Practical Reason.* Harvard University Press.
- **Rao, A. S. and Georgeff, M. P.** (1991). *Modeling Rational Agents within a BDI Architecture.* Proceedings of the 2nd International Conference on Principles of Knowledge Representation and Reasoning, pp. 473–484.
- **Brooks, R. A.** (1986). *A Robust Layered Control System for a Mobile Robot.* IEEE Journal on Robotics and Automation, 2(1), 14–23.
- **Hanks, S. and McDermott, D.** (1987). *Nonmonotonic Logic and Temporal Projection.* Artificial Intelligence, 33(3), 379–412.

### The Frame Problem (Philosophy)

- **Stanford Encyclopedia of Philosophy** — *The Frame Problem:* https://plato.stanford.edu/entries/frame-problem/
- **McCarthy, J.** (1986). *Applications of Circumscription to Formalizing Common Sense Knowledge.* Artificial Intelligence, 26(3), 89–116.

### PlanSys2 and Its Foundations

- **Martín, F., Ginés, J., Matellán, V. and Rodríguez, F. J.** (2021). *PlanSys2: A Planning System Framework for ROS2.* IEEE/RSJ IROS 2021. https://doi.org/10.1109/IROS51168.2021.9636544
- **Martín, F. et al.** (2021). *Optimized Execution of PDDL Plans using Behavior Trees.* AAMAS 2021. https://arxiv.org/pdf/2101.01964
- **PlanSys2 Documentation:** https://plansys2.github.io
- **PlanSys2 GitHub:** https://github.com/PlanSys2/ros2_planning_system

### BDI Architecture

- **Georgeff, M., Pell, B., Pollack, M., Tambe, M. and Wooldridge, M.** (1999). *The Belief-Desire-Intention Model of Agency.* ATAL 1998, LNCS 1555.
- **Wooldridge, M.** (2000). *Reasoning about Rational Agents.* MIT Press.

### Symbol Grounding

- **Harnad, S.** (1990). *The Symbol Grounding Problem.* Physica D: Nonlinear Phenomena, 42(1–3), pp. 335–346.

### Related Architectures

- **González-Santamarta, M. Á. et al.** (2023). *MERLIN2: MachinEd Ros 2 pLanINg.* ScienceDirect. https://doi.org/10.1016/j.simpa.2023.100556
- **Dal Moro, D. et al.** (2022). *Developing BDI-based Robotic Systems with ROS2.* PAAMS 2022.

---

## Repository

ChainTree public repositories:
- **glenn-edgar/knowledge_base_container** — ChainTree, s_compile.lua, and related projects (MIT license)
- **glenn-edgar/knowledge_base** — ltree knowledge base project (MIT license)

The unified planning DSL described in the companion document extends `s_compile.lua` with `--pddl`, `--bt-xml`, `--validate`, and `--monitor` compiler targets, directly addressing the gaps identified in this philosophical analysis.

