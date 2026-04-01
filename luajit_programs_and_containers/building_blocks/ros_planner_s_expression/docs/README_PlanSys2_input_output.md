# PDDL Planner: Inputs, Outputs, and the Full Pipeline
## A Complete Technical Reference for PlanSys2 and Domain-Independent Planning

---

## Table of Contents

1. [System Overview: What the Planner Actually Does](#1-system-overview-what-the-planner-actually-does)
2. [Input 1: The Domain File](#2-input-1-the-domain-file)
   - 2.1 [File Structure and Skeleton](#21-file-structure-and-skeleton)
   - 2.2 [Requirements Declaration](#22-requirements-declaration)
   - 2.3 [Types Hierarchy](#23-types-hierarchy)
   - 2.4 [Constants](#24-constants)
   - 2.5 [Predicates](#25-predicates)
   - 2.6 [Functions (Numeric Fluents)](#26-functions-numeric-fluents)
   - 2.7 [Actions (Instantaneous)](#27-actions-instantaneous)
   - 2.8 [Durative Actions (Temporal)](#28-durative-actions-temporal)
3. [Input 2: The Problem File](#3-input-2-the-problem-file)
   - 3.1 [File Structure and Skeleton](#31-file-structure-and-skeleton)
   - 3.2 [Objects](#32-objects)
   - 3.3 [Initial State](#33-initial-state)
   - 3.4 [Goal Specification](#34-goal-specification)
   - 3.5 [Metric (Optimization)](#35-metric-optimization)
4. [How PlanSys2 Generates the Inputs](#4-how-plansys2-generates-the-inputs)
   - 4.1 [Domain Expert: Static Model](#41-domain-expert-static-model)
   - 4.2 [Problem Expert: Live Knowledge Base](#42-problem-expert-live-knowledge-base)
   - 4.3 [The ROS2 Service API](#43-the-ros2-service-api)
   - 4.4 [File Generation Before Planning](#44-file-generation-before-planning)
5. [The Internal Translation Pipeline](#5-the-internal-translation-pipeline)
   - 5.1 [PDDL Parsing and Normalization](#51-pddl-parsing-and-normalization)
   - 5.2 [Grounding: Schemas to Ground Operators](#52-grounding-schemas-to-ground-operators)
   - 5.3 [SAS+ Translation](#53-sas-translation)
   - 5.4 [Mutex Group Inference](#54-mutex-group-inference)
   - 5.5 [Invariant Synthesis](#55-invariant-synthesis)
   - 5.6 [The output.sas File Format](#56-the-outputsas-file-format)
6. [Planner Search Algorithms](#6-planner-search-algorithms)
   - 6.1 [Forward State Space Search](#61-forward-state-space-search)
   - 6.2 [Heuristic Functions](#62-heuristic-functions)
   - 6.3 [POPF: Partial Order Planning Forward](#63-popf-partial-order-planning-forward)
   - 6.4 [Fast Downward / TFD](#64-fast-downward--tfd)
7. [Output: The Plan File](#7-output-the-plan-file)
   - 7.1 [Sequential Plan Format (STRIPS)](#71-sequential-plan-format-strips)
   - 7.2 [Temporal Plan Format (Durative Actions)](#72-temporal-plan-format-durative-actions)
   - 7.3 [Plan File Location in PlanSys2](#73-plan-file-location-in-plansys2)
   - 7.4 [Plan Validation with VAL](#74-plan-validation-with-val)
8. [How PlanSys2 Processes the Output](#8-how-plansys2-processes-the-output)
   - 8.1 [Plan Parsing](#81-plan-parsing)
   - 8.2 [Plan Graph Construction](#82-plan-graph-construction)
   - 8.3 [Parallel Execution Flow Detection](#83-parallel-execution-flow-detection)
   - 8.4 [Behavior Tree Construction from Plan Graph](#84-behavior-tree-construction-from-plan-graph)
9. [Execution: From Plan to Hardware](#9-execution-from-plan-to-hardware)
   - 9.1 [The Action Auction Protocol](#91-the-action-auction-protocol)
   - 9.2 [ActionExecutorClient API](#92-actionexecutorclient-api)
   - 9.3 [BT Node Implementation Pattern](#93-bt-node-implementation-pattern)
   - 9.4 [Argument Passing via Blackboard](#94-argument-passing-via-blackboard)
   - 9.5 [Lifecycle Node Integration](#95-lifecycle-node-integration)
10. [The Replanning Loop](#10-the-replanning-loop)
    - 10.1 [Failure Detection](#101-failure-detection)
    - 10.2 [State Update and Replan Trigger](#102-state-update-and-replan-trigger)
    - 10.3 [Multi-Robot Coordination](#103-multi-robot-coordination)
11. [Full Worked Example: Robot with Battery](#11-full-worked-example-robot-with-battery)
    - 11.1 [Domain File](#111-domain-file)
    - 11.2 [Problem File](#112-problem-file)
    - 11.3 [Generated Plan](#113-generated-plan)
    - 11.4 [Plan Graph](#114-plan-graph)
    - 11.5 [BT Construction](#115-bt-construction)
    - 11.6 [Action Implementation](#116-action-implementation)
12. [Common Mistakes and Debugging](#12-common-mistakes-and-debugging)
    - 12.1 [Domain File Errors](#121-domain-file-errors)
    - 12.2 [Problem File Errors](#122-problem-file-errors)
    - 12.3 [Plan Failure Modes](#123-plan-failure-modes)
    - 12.4 [Debugging Tools](#124-debugging-tools)
13. [PDDL Version Compatibility](#13-pddl-version-compatibility)
14. [Connection to Unified DSL](#14-connection-to-unified-dsl)

---

## 1. System Overview: What the Planner Actually Does

The planner is a **state-space search engine**. It takes a description of the world (states and actions) and finds a path through the state space from the initial state to a state satisfying the goal.

```
  ┌─────────────────────────────────────────────────────────┐
  │                    PLANNER PIPELINE                     │
  │                                                         │
  │  domain.pddl  ──┐                                       │
  │                 ├──► PARSER ──► GROUNDER ──► SAS+       │
  │  problem.pddl ──┘              TRANSLATOR    TASK       │
  │                                                ▼        │
  │                                           SEARCH        │
  │                                           ENGINE        │
  │                                          (h*, BFS,      │
  │                                          A*, LAMA)      │
  │                                                ▼        │
  │                                          sas_plan       │
  │                                         (plan file)     │
  └─────────────────────────────────────────────────────────┘
```

**Two inputs, one output:**
- **Input 1** — `domain.pddl`: The action vocabulary. Defines *what* can happen in this world — types of objects, boolean facts (predicates), numeric quantities (functions), and parametric actions with preconditions and effects. This file is static and does not change during a robot's operation.
- **Input 2** — `problem.pddl`: The planning instance. Defines *what is currently true* (the initial state), *what things exist* (objects), and *what we want to achieve* (the goal). This file changes every time the world state changes and a new plan is needed.
- **Output** — `sas_plan` (or `plan.pddl`): A sequence of grounded actions with timestamps (for temporal planning) or step numbers (for classical planning).

In PlanSys2, the inputs are dynamically generated from live ROS2 data structures. The Domain Expert holds the domain; the Problem Expert holds the current instance. When the Planner node is triggered, it assembles the two files, invokes POPF or TFD as a subprocess, reads the output, and passes the plan to the Executor.

---

## 2. Input 1: The Domain File

### 2.1 File Structure and Skeleton

A domain file defines the *universal* aspects of the problem — everything that is true regardless of which specific instance we are planning for. Several problem files may reference the same domain file.

```lisp
(define (domain <domain-name>)

  ;; 1. Requirements — declare which PDDL features are used
  (:requirements :strips :typing :adl :durative-actions :numeric-fluents)

  ;; 2. Types — object type hierarchy
  (:types
    robot location - object
    room dock - location)

  ;; 3. Constants — objects present in every problem
  (:constants
    charging_bay - dock)

  ;; 4. Predicates — boolean facts about the world
  (:predicates
    (robot_at ?r - robot ?l - location)
    (connected ?a - location ?b - location)
    (battery_full ?r - robot)
    (carrying ?r - robot ?o - object))

  ;; 5. Functions — numeric quantities
  (:functions
    (battery_level ?r - robot)
    (distance ?a - location ?b - location))

  ;; 6. Actions
  (:action move ...)
  (:durative-action navigate ...))
```

**Syntax rules:**
- Names use alphanumeric characters, hyphens (`-`), and underscores (`_`). Case-insensitive.
- Variables are prefixed with `?` (e.g., `?robot`, `?from`).
- Comments begin with `;` and run to end of line.
- Keywords are prefixed with `:` (e.g., `:requirements`, `:precondition`).

### 2.2 Requirements Declaration

Requirements declare which language features the domain uses. Most planners parse this to verify they support the required features (though many ignore it in practice).

| Requirement | Meaning | Planners |
|---|---|---|
| `:strips` | Basic STRIPS: positive preconditions, deterministic effects | All |
| `:typing` | Object type hierarchy | Most |
| `:equality` | Allow `=` predicate in preconditions | Most |
| `:negative-preconditions` | `(not ...)` in action preconditions | Most |
| `:disjunctive-preconditions` | `(or ...)` in preconditions | Some |
| `:adl` | Full ADL: `and`, `or`, `not`, `forall`, `exists`, conditional effects | Many |
| `:numeric-fluents` | Numeric state variables (PDDL 2.1) | POPF, TFD, FD |
| `:durative-actions` | Actions with duration (PDDL 2.1) | POPF, TFD |
| `:action-costs` | Optimize plan cost | Many |

**PlanSys2 uses PDDL 2.1**, which requires at minimum `:strips :typing :durative-actions :numeric-fluents` for temporal planning. Simple domains can use just `:strips :typing`.

### 2.3 Types Hierarchy

Types are object categories organized in an inheritance hierarchy. The root type is always `object`.

```lisp
(:types
  ;; Syntax: subtype - supertype
  robot - object
  location - object
  room corridor dock - location   ;; room, corridor, dock all inherit from location
  package cargo - object          ;; package and cargo both extend object
)
```

Types allow actions to declare constraints on their parameters:
```lisp
(:action pick
  :parameters (?r - robot ?p - package ?l - room)
  ;; r must be a robot, p must be a package, l must be a room specifically
  ;; (not any location — the action only makes sense in rooms)
  ...)
```

**Why types matter:** During grounding, the planner instantiates action schemas by substituting all possible combinations of objects of the correct types. Types dramatically reduce the number of ground operators the planner must consider.

### 2.4 Constants

Constants are objects that exist in every problem instance of this domain. They appear in the domain file rather than the problem file. Rare in practice — usually you'd just declare the object in each problem file.

```lisp
(:constants
  north_star - dock  ;; the docking bay is always present
)
```

### 2.5 Predicates

Predicates are the boolean facts — the atoms of the world state. Each predicate is a template that takes zero or more typed arguments.

```lisp
(:predicates
  (robot_at ?r - robot ?l - location)          ;; robot r is at location l
  (connected ?a - location ?b - location)      ;; there is a path from a to b
  (battery_full ?r - robot)                    ;; robot r has full battery
  (battery_low ?r - robot)                     ;; robot r has low battery
  (gripper_empty ?r - robot)                   ;; robot r's gripper holds nothing
  (carrying ?r - robot ?p - package)           ;; robot r carries package p
  (at_rest)                                    ;; zero-argument predicate — a global flag
)
```

**Static vs. dynamic predicates:** A predicate is *static* if no action ever adds or deletes it. Static predicates function as fixed relational data — like a map of which locations are connected. Planners can optimize by treating static predicates as lookup tables rather than search state variables.

**The closed world assumption:** Any predicate not listed in `(:init ...)` is assumed false. There is no "unknown." This is the fundamental ontological commitment of classical planning.

### 2.6 Functions (Numeric Fluents)

Functions are numeric state variables — real or integer quantities that actions can increase, decrease, or assign.

```lisp
(:functions
  (battery_level ?r - robot)          ;; current charge level, 0–100
  (distance ?a - location ?b - location)  ;; static distance map
  (total-cost)                        ;; cumulative plan cost (for :action-costs)
)
```

Functions are used in:
- Action preconditions: `(>= (battery_level ?r) 20)`
- Action effects: `(decrease (battery_level ?r) 15)`
- Plan metric: `(:metric minimize (total-cost))`
- Duration expressions: `(:duration (= ?duration (/ (distance ?from ?to) 10)))`

### 2.7 Actions (Instantaneous)

Instantaneous STRIPS actions execute atomically — they do not take time. Their effects are applied immediately when the action is applied.

```lisp
(:action move
  ;; Parameters: typed variables this action ranges over
  :parameters (?r - robot ?from - location ?to - location)

  ;; Precondition: what must be true in the current state
  ;; for this action to be applicable
  :precondition (and
    (robot_at ?r ?from)
    (connected ?from ?to)
    (not (= ?from ?to)))          ;; can't move from a place to itself

  ;; Effect: what changes in the state when this action executes
  ;; Positive terms = added, (not ...) terms = deleted
  :effect (and
    (robot_at ?r ?to)             ;; robot is now at destination
    (not (robot_at ?r ?from))))   ;; robot is no longer at source
```

**Effect variants:**

*Conditional effect* — only applies when condition holds at execution time:
```lisp
:effect (and
  (robot_at ?r ?to)
  (not (robot_at ?r ?from))
  (when (battery_low ?r)          ;; if battery is already low...
    (not (battery_low ?r))        ;; ...this would fail — example of conditional
    (stranded ?r)))               ;; ...robot gets stranded
```

*Universal effect* — applies to all matching objects:
```lisp
:effect (forall (?p - package)
  (when (carrying ?r ?p)
    (at ?p ?to)))                 ;; all carried packages move with the robot
```

*Numeric effect:*
```lisp
:effect (and
  (robot_at ?r ?to)
  (not (robot_at ?r ?from))
  (decrease (battery_level ?r) (distance ?from ?to)))
```

### 2.8 Durative Actions (Temporal)

Durative actions have a duration and can express conditions that must hold at the start, throughout, or at the end of execution. This is PDDL 2.1.

```lisp
(:durative-action navigate
  :parameters (?r - robot ?from - room ?to - room)

  ;; Duration: how long this action takes
  ;; Can be a constant, a function call, or a variable
  :duration (= ?duration (distance ?from ?to))

  ;; Condition: what must be true WHEN
  ;; (at start ...) — true when action begins
  ;; (over all ...) — true throughout the action's execution
  ;; (at end ...) — true when action completes
  :condition (and
    (at start (robot_at ?r ?from))
    (at start (>= (battery_level ?r) 10))
    (over all (connected ?from ?to))
    (over all (not (path_blocked ?from ?to))))

  ;; Effect: what changes WHEN
  ;; (at start ...) — applied when action begins
  ;; (at end ...) — applied when action completes
  :effect (and
    (at start (not (robot_at ?r ?from)))   ;; robot leaves source immediately
    (at end   (robot_at ?r ?to))           ;; robot arrives at end
    (at end   (decrease (battery_level ?r)
                        (* ?duration 0.5)))))
```

**The temporal planning challenge:** Multiple durative actions can overlap. The planner must ensure that `(over all ...)` conditions remain satisfied throughout each action's execution, even when other actions are running concurrently and modifying state.

---

## 3. Input 2: The Problem File

### 3.1 File Structure and Skeleton

The problem file is the *instance* — it describes the specific situation right now. It changes whenever the world state changes and a new plan is needed. In PlanSys2, this file is regenerated by the Problem Expert every time the Planner node is called.

```lisp
(define (problem <problem-name>)

  ;; Must match the domain name exactly
  (:domain <domain-name>)

  ;; Objects present in this specific problem
  (:objects ...)

  ;; What is true at the start of planning
  (:init ...)

  ;; What we want to be true at the end
  (:goal ...)

  ;; Optional: what to optimize
  (:metric minimize (total-cost)))
```

### 3.2 Objects

Objects are the typed entities that exist in this specific problem instance. Every object must have a type declared in the domain's `:types` section.

```lisp
(:objects
  ;; Syntax: object-name(s) - type
  leia r2d2 - robot
  entrance kitchen bedroom bathroom corridor - room
  charging_station - dock
  parcel_1 parcel_2 parcel_3 - package
)
```

Object names are ground constants — they have no `?` prefix. During grounding, the planner substitutes these names for the action parameters of the matching type.

**Combinatorial explosion from objects:** If you have 3 robots and 5 locations, the `move` action has `3 × 5 × 5 = 75` ground instantiations. With 10 robots and 20 locations, it has `10 × 20 × 20 = 4000`. Type restrictions limit which combinations are valid, but object counts directly drive grounding complexity.

### 3.3 Initial State

The initial state lists every predicate that is true at the start of planning. Everything not listed is assumed false (closed world assumption). For numeric fluents, the initial values are assigned with `(= (function arg) value)`.

```lisp
(:init
  ;; Robot positions
  (robot_at leia entrance)
  (robot_at r2d2 charging_station)

  ;; Battery levels
  (= (battery_level leia) 85)
  (= (battery_level r2d2) 100)
  (battery_full r2d2)

  ;; Map topology (static predicates)
  (connected entrance corridor)
  (connected corridor kitchen)
  (connected corridor bedroom)
  (connected corridor bathroom)
  (connected charging_station corridor)
  ;; Note: connected is NOT symmetric by default —
  ;; add both directions if the map is undirected:
  (connected corridor entrance)
  (connected kitchen corridor)
  ;; ...

  ;; Distances (numeric, static)
  (= (distance entrance corridor)  5.0)
  (= (distance corridor kitchen)   8.0)
  (= (distance corridor bedroom)   6.0)

  ;; Package locations
  (at parcel_1 entrance)
  (at parcel_2 kitchen)
  (gripper_empty leia)
  (gripper_empty r2d2)
)
```

**Key points:**
- Only positive facts are listed (no `(not ...)` in `:init`)
- Numeric assignments use `(= (function args) value)`
- Static predicates (map topology, distances) are listed here — they never change
- If you forget a predicate, the planner assumes it is false, which can silently make actions inapplicable

### 3.4 Goal Specification

The goal is a logical formula expressing the desired final state. The planner finds a plan that transforms the initial state into any state satisfying the goal formula.

```lisp
;; Simple conjunction goal — all conditions must be true
(:goal (and
  (robot_at leia kitchen)
  (at parcel_1 bedroom)
  (at parcel_2 bedroom)))

;; Disjunctive goal — at least one condition must be true (requires :disjunctive-preconditions)
(:goal (or
  (robot_at leia kitchen)
  (robot_at leia bedroom)))

;; Quantified goal — for all packages, they should be delivered (requires :adl)
(:goal (forall (?p - package)
  (delivered ?p)))

;; Negative goal — something must not be true
(:goal (and
  (at parcel_1 destination)
  (not (battery_low leia))))

;; Mixed goal
(:goal (and
  (forall (?p - package) (at ?p warehouse))
  (robot_at leia charging_station)
  (>= (battery_level leia) 50)))     ;; numeric goal requires :numeric-fluents
```

**Goal reachability:** If the goal is not reachable from the initial state (no sequence of actions can satisfy it), the planner will either run forever (exploring the entire state space) or return with an "unsolvable" message. The `--validate` target in the unified DSL checks goal reachability statically before invoking the planner.

### 3.5 Metric (Optimization)

Without a metric, the planner finds *any* valid plan. With a metric, it finds the *optimal* plan according to the specified criterion.

```lisp
;; Minimize total plan cost (requires :action-costs)
(:metric minimize (total-cost))

;; Minimize total plan execution time (temporal planning)
(:metric minimize (total-time))

;; Minimize a custom expression
(:metric minimize
  (+ (total-cost) (* 2 (battery_used robot1))))

;; Maximize a value
(:metric maximize (items_delivered))
```

Not all planners support all metric types. POPF supports `minimize (total-time)` and `minimize (total-cost)`. Fast Downward supports `minimize (total-cost)` in its optimal configuration.

---

## 4. How PlanSys2 Generates the Inputs

### 4.1 Domain Expert: Static Model

The Domain Expert node reads PDDL domain files at startup and holds the domain in memory as parsed data structures. It is static — the domain does not change during runtime.

```
launch file parameter:
  model_file: /path/to/domain.pddl:/path/to/extra_actions.pddl

Multiple files are merged into one domain. This enables modular domains
where each package contributes its own PDDL action definitions.
```

Services exposed by Domain Expert:
```
domain_expert/get_domain         → returns domain as PDDL string
domain_expert/get_domain_types   → returns type list
domain_expert/get_domain_actions → returns action name list
domain_expert/get_domain_predicates → returns predicate list
domain_expert/get_domain_functions  → returns function list
```

The domain is also published as a ROS2 latched topic:
```
domain_expert/domain [std_msgs/String]  (transient_local QoS)
```

### 4.2 Problem Expert: Live Knowledge Base

The Problem Expert is PlanSys2's runtime knowledge base. It maintains the current world state as three collections:
- **Instances** — the objects currently in the world
- **Predicates** — grounded boolean facts currently true
- **Functions** — current numeric values
- **Goals** — the current planning objective

These are populated and modified by your application code via ROS2 services:

```
problem_expert/add_problem_instance    → add a typed object
problem_expert/remove_problem_instance → remove an object
problem_expert/add_problem_predicate   → assert a predicate true
problem_expert/remove_problem_predicate → retract a predicate
problem_expert/add_problem_function    → set a numeric value
problem_expert/update_problem_function → update a numeric value
problem_expert/set_problem_goal        → set the planning goal
problem_expert/get_problem             → get current problem as PDDL string
```

Your mission controller code updates the Problem Expert as the robot's sensors report state changes. Every ROS2 node in your system is a potential contributor to the knowledge base.

### 4.3 The ROS2 Service API

PlanSys2 provides a C++ client library (`plansys2::PlannerClient`, `plansys2::ProblemExpertClient`) that wraps the raw service calls:

```cpp
#include "plansys2_client/PlannerClient.hpp"
#include "plansys2_client/ProblemExpertClient.hpp"

auto problem_client = std::make_shared<plansys2::ProblemExpertClient>();
auto planner_client = std::make_shared<plansys2::PlannerClient>();

// Add objects to the world
problem_client->addInstance(plansys2::Instance{"leia", "robot"});
problem_client->addInstance(plansys2::Instance{"kitchen", "room"});

// Assert predicates (current world state)
problem_client->addPredicate(plansys2::Predicate("(robot_at leia entrance)"));
problem_client->addPredicate(plansys2::Predicate("(connected entrance corridor)"));
problem_client->addPredicate(plansys2::Predicate("(connected corridor kitchen)"));

// Set numeric values
problem_client->addFunction(plansys2::Function("(= (battery_level leia) 85)"));

// Set the goal
problem_client->setGoal(plansys2::Goal("(and (robot_at leia kitchen))"));

// Request a plan
auto plan = planner_client->getPlan(
  domain_client->getDomain(),
  problem_client->getProblem());

if (plan.has_value()) {
  // plan.value() is a plansys2_msgs::msg::Plan
  for (auto& item : plan.value().items) {
    std::cout << item.time << " " << item.action << " " << item.duration << std::endl;
  }
}
```

You can also update state after an action executes:

```cpp
// After "move leia entrance corridor" succeeds:
problem_client->removePredicate(plansys2::Predicate("(robot_at leia entrance)"));
problem_client->addPredicate(plansys2::Predicate("(robot_at leia corridor)"));
problem_client->updateFunction(plansys2::Function(
  "(= (battery_level leia) " + std::to_string(new_level) + ")"));
```

### 4.4 File Generation Before Planning

When the Planner node receives a plan request, it:

1. **Retrieves the domain string** from the Domain Expert (cached, rarely re-fetched)
2. **Retrieves the problem string** from the Problem Expert (generated fresh every call)
3. **Writes two files to disk:**
   ```
   <output_dir>/<namespace>/domain.pddl
   <output_dir>/<namespace>/problem.pddl
   ```
4. **Invokes the PDDL solver as a subprocess** on these files
5. **Reads the plan output file:**
   ```
   <output_dir>/<namespace>/plan.pddl
   ```
6. **Parses and returns** the plan as a `plansys2_msgs::msg::Plan`

The use of files on disk (rather than stdin/stdout pipes) means the PDDL files are human-readable and inspectable during debugging:
```bash
# After triggering a plan, inspect the generated inputs:
cat /tmp/my_robot_ns/domain.pddl
cat /tmp/my_robot_ns/problem.pddl

# Run the planner directly for debugging:
ros2 run popf popf /tmp/my_robot_ns/domain.pddl /tmp/my_robot_ns/problem.pddl

# Or with Fast Downward:
./fast-downward.py /tmp/my_robot_ns/domain.pddl /tmp/my_robot_ns/problem.pddl \
  --search "astar(lmcut())"
```

---

## 5. The Internal Translation Pipeline

### 5.1 PDDL Parsing and Normalization

The first phase parses the two PDDL files and normalizes the AST:

- Comments stripped
- Case normalization (PDDL is case-insensitive)
- Type inheritance resolved: objects of subtype `room` implicitly satisfy parameters of type `location`
- Syntactic sugar compiled away: `:adl` features (quantifiers, conditionals) rewritten as simpler equivalents where possible
- Action schemas validated: every parameter used in preconditions and effects must appear in `:parameters`

**Typing compilation:** Types are compiled into static predicates during normalization. The declaration `(:types robot - object)` generates a static predicate `(robot ?x)`. The constraint `?r - robot` on an action parameter becomes the precondition `(robot ?r)`. This unifies the type system with the predicate system, simplifying the subsequent grounding phase.

### 5.2 Grounding: Schemas to Ground Operators

Action schemas use typed variables (`?r - robot`, `?from - location`). Before searching, the planner instantiates every schema with every valid combination of objects.

For the domain with objects `{leia, r2d2}` as robots and `{entrance, corridor, kitchen}` as rooms:

The schema:
```lisp
(:action move :parameters (?r - robot ?from - room ?to - room) ...)
```

Produces ground operators:
```
move(leia, entrance, corridor)
move(leia, entrance, kitchen)
move(leia, corridor, entrance)
move(leia, corridor, kitchen)
move(leia, kitchen, entrance)
move(leia, kitchen, corridor)
move(r2d2, entrance, corridor)
... (12 total for 2 robots × 3 rooms × 3 rooms, minus self-moves if precondition excludes them)
```

The `(not (= ?from ?to))` precondition eliminates `move(leia, entrance, entrance)` etc., so valid ground operators = 2 × 3 × 2 = 12.

**Grounding is expensive.** In large domains with many objects and actions, the number of ground operators can be in the millions. Fast Downward's translator includes reachability analysis to prune operators that can never be applicable given the initial state, dramatically reducing the ground operator set.

### 5.3 SAS+ Translation

Fast Downward does not search over the PDDL representation directly. It translates the ground PDDL task into **SAS+** (a finite-domain representation) before searching.

The core transformation: groups of mutually exclusive predicates are collapsed into a single multi-valued variable.

**Example:** The predicates `(robot_at leia entrance)`, `(robot_at leia corridor)`, `(robot_at leia kitchen)` are mutually exclusive — leia can only be in one place at a time. In PDDL, these are three separate Boolean variables. In SAS+, they become one variable:

```
var_robot_at_leia ∈ {entrance, corridor, kitchen, <none>}
```

The `<none>` value represents "leia is not at any of these locations" — needed when the robot is in transit (for durative actions) or if the domain doesn't require leia to always be somewhere.

**Why this matters:**
- Boolean state: each of 3 predicates is independently true or false = 2³ = 8 possible combinations, but only 4 are valid (at most one true)
- SAS+ state: 1 variable with 4 values — directly encodes only the valid states
- For N locations, Boolean = 2^N possible (mostly invalid) states; SAS+ = N+1 values

This reduction is exponential in the number of mutex groups detected, making search over SAS+ tasks dramatically faster than search over the raw Boolean state.

### 5.4 Mutex Group Inference

The translator automatically detects mutex groups — sets of predicates where at most one can be true simultaneously. Detection is based on two sources:

**Structural mutexes:** Predicates whose arguments form a functional dependency. If every action that sets `(robot_at ?r ?to)` also deletes `(robot_at ?r ?from)` for some `?from`, then for any fixed robot `?r`, at most one `(robot_at ?r ?)` can be true — this is a mutex group.

**Invariant synthesis:** A more powerful (but expensive) analysis that searches for invariants of the form "at most one of these facts is true in any reachable state." The translator iteratively generates and verifies candidate invariants using regression.

The resulting mutex groups are stored in the `output.sas` file and used by:
- The search engine (to prune states where multiple values in a mutex group are set)
- Landmark generation (landmarks that are mutex force a sequencing constraint)
- Pattern database heuristics (variables in the same mutex group can share a pattern)

### 5.5 Invariant Synthesis

Beyond simple mutex groups, the translator performs invariant synthesis — finding general constraints that hold in all reachable states. The most common invariant is the counting invariant: "at most k objects satisfy predicate P" for some small k.

For the robot location problem, the invariant "exactly one `(robot_at leia ?)` is true" is a counting invariant with k=1. For a package delivery domain, "each package is either in a room or in a vehicle, never both" is a disjunctive invariant.

These invariants allow the translator to identify when PDDL Boolean predicates can be collapsed into SAS+ finite-domain variables, producing smaller and more efficiently searchable state representations.

### 5.6 The output.sas File Format

Fast Downward's translator produces a structured text file with sections:

```
begin_version
3
end_version

begin_metric
0              ;; 0 = minimize plan length, 1 = minimize plan cost
end_metric

begin_variables
7              ;; number of variables
begin_variable
var0           ;; variable name
-1             ;; axiom layer (-1 = not a derived predicate)
4              ;; domain size (number of values)
Atom carry(ball1, left)    ;; value 0
Atom carry(ball2, left)    ;; value 1
Atom carry(ball3, left)    ;; value 2
NegatedAtom carry(ball4, left)  ;; value 3 = none of the above
end_variable
...
end_variables

begin_mutex_group
4              ;; 4 facts in this mutex group
1 4            ;; var1, value 4
0 4            ;; var0, value 4
2 0            ;; var2, value 0
2 1            ;; var2, value 1
end_mutex_group
...

begin_state
3 1 0 0 0 0 1  ;; initial values of var0..var6
end_state

begin_goal
2              ;; 2 goal conditions
0 3            ;; var0 = value 3
1 3            ;; var1 = value 3
end_goal

begin_operator
move rooma roomb    ;; ground action name
0                   ;; number of prevail conditions
1                   ;; number of effects
0 6 1 0             ;; effect: 0 conditions, var6, precondition value 1, new value 0
1                   ;; action cost
end_operator
...
```

This format is consumed directly by Fast Downward's C++ search engine. POPF and TFD use their own internal formats derived from their respective parsers but operating on similar representations.

---

## 6. Planner Search Algorithms

### 6.1 Forward State Space Search

All planners supported by PlanSys2 use **forward state space search** (also called progression): starting from the initial state, repeatedly apply applicable ground operators to generate successor states, until a state satisfying the goal is found.

The search maintains a **frontier** (states to explore) and a **closed set** (states already explored, to prevent cycles). The key decisions are: what order to expand states (the search strategy) and how to estimate the distance to the goal (the heuristic).

### 6.2 Heuristic Functions

The quality of a heuristic determines how efficiently the planner finds plans. All heuristics used in practice are **admissible relaxations** — they solve a simplified version of the problem that underestimates the true plan cost.

**Delete relaxation (h^+):** Ignore all delete effects. In the relaxed problem, facts once achieved are never undone. This makes the relaxed problem solvable greedily (Dijkstra-like). The actual cost in the relaxed problem is a lower bound on the real problem's cost.

**FF heuristic (h^FF):** The Fast Forward heuristic. Solve the delete-relaxed problem optimally using a relaxed planning graph, count the number of actions in the relaxed plan. Fast to compute. Not admissible (may overestimate) but very effective in practice. Used in POPF and TFD.

**LM-cut heuristic:** Finds "landmarks" — actions that must appear in any plan — and computes a cut through the landmark set. Admissible and powerful. Used in Fast Downward optimal mode.

**Causal graph heuristic (h^CG):** Decomposes the problem along the causal graph (which variables affect which other variables). Solves independent sub-problems in isolation and sums the costs. Fast Downward's original contribution. Effective on problems with hierarchical structure.

**Landmark heuristic (h^L):** Identifies propositions that must be true at some point in every plan (landmarks) and estimates cost from the number of unachieved landmarks. Combined with h^FF in LAMA, the dominant configuration for satisficing planning.

### 6.3 POPF: Partial Order Planning Forward

POPF (Partial Order Planning Forward) is PlanSys2's default solver. It is specifically designed for **temporal planning** — PDDL 2.1 problems with durative actions.

Key properties:
- Searches in a forward state space over partial orders of actions
- Actions are ordered only where necessary (temporal precedence constraints)
- Supports continuous numeric effects and conditions
- Handles concurrent actions efficiently — it does not require serializing independent actions
- Uses the FF heuristic adapted for temporal domains

POPF's plan output includes timestamps:
```
; Cost: 15.002
; Time 0.01
0.000: (navigate leia entrance corridor)  [5.000]
0.000: (navigate r2d2 corridor kitchen)   [8.000]
5.001: (navigate leia corridor kitchen)   [8.000]
8.001: (pick r2d2 parcel1 kitchen)        [2.000]
```

The format is: `<timestamp>: (<action> <arguments>)  [<duration>]`

### 6.4 Fast Downward / TFD

**Fast Downward (FD)** is the dominant classical (non-temporal) planner. It won the classical track of IPC 2004 and remains highly competitive. Fast Downward configurations include:

```bash
# Satisficing (find any valid plan, fast)
./fast-downward.py domain.pddl problem.pddl \
  --search "lazy_greedy([ff()], preferred=[ff()])"

# Optimal (find shortest plan, slower)
./fast-downward.py domain.pddl problem.pddl \
  --search "astar(lmcut())"

# LAMA (satisficing with quality improvement)
./fast-downward.py --alias lama-first domain.pddl problem.pddl
```

**Temporal Fast Downward (TFD)** is an extension of FD supporting durative actions. Used as an alternative to POPF in PlanSys2 for temporal domains.

---

## 7. Output: The Plan File

### 7.1 Sequential Plan Format (STRIPS)

For classical (non-temporal) planning with instantaneous actions, the plan is a numbered sequence of grounded actions:

```
; Cost: 5
; Time 0.00

(pick ball1 rooma left)
(pick ball2 rooma right)
(move rooma roomb)
(drop ball1 roomb left)
(drop ball2 roomb right)
; cost = 5 (unit cost)
```

Each line is a grounded action: the action name followed by its bound arguments, all lowercased, space-separated, wrapped in parentheses. The `;` lines are comments. The plan must be executed in strict sequential order — step N completes before step N+1 begins.

Fast Downward writes this to `sas_plan` by default. The file can also contain multiple solutions if the planner found multiple plans of increasing quality.

### 7.2 Temporal Plan Format (Durative Actions)

For temporal planning with POPF or TFD, the output includes timestamps and durations:

```
; Cost: 25.002
; Time 0.12

0.000: (navigate leia entrance corridor)  [5.000]
0.000: (navigate r2d2 corridor kitchen)   [8.000]
5.001: (pick leia parcel1 corridor)       [1.000]
6.002: (navigate leia corridor kitchen)   [8.000]
8.001: (drop r2d2 parcel2 kitchen)        [1.000]
14.003: (drop leia parcel1 kitchen)       [1.000]
```

Format per line:
```
<start_time>: (<action_name> <arg1> <arg2> ...) [<duration>]
```

- `start_time` — when the action begins, in seconds from plan start (float, 3 decimal places)
- `action_name` — lowercased action name
- `arg1...argN` — grounded arguments (object names)
- `duration` — how long the action takes, in seconds (float)

Actions with the same timestamp execute concurrently. The 0.001 second offsets (e.g., `5.001` instead of `5.000`) are epsilon separations inserted by POPF to maintain happens-before ordering when the logical dependency requires it but the actual times are equal.

### 7.3 Plan File Location in PlanSys2

PlanSys2 stores intermediate files:
```
<output_dir>/<namespace>/domain.pddl    → generated domain (from Domain Expert)
<output_dir>/<namespace>/problem.pddl   → generated problem (from Problem Expert)
<output_dir>/<namespace>/plan.pddl      → planner output (the plan)
```

Default `output_dir` is `/tmp`. For a namespaced deployment:
```
/tmp/robot1/domain.pddl
/tmp/robot1/problem.pddl
/tmp/robot1/plan.pddl
```

This allows multiple robots on the same machine to have independent planning contexts without file collisions.

You can run the planner directly on these files for debugging:
```bash
# POPF (temporal)
ros2 run popf popf /tmp/robot1/domain.pddl /tmp/robot1/problem.pddl

# Fast Downward (classical)
./fast-downward.py /tmp/robot1/domain.pddl /tmp/robot1/problem.pddl \
  --search "lazy_greedy([ff()], preferred=[ff()])"
```

### 7.4 Plan Validation with VAL

VAL (Plan Validator) takes a domain file, problem file, and plan file and verifies that the plan is correct — that all preconditions are satisfied at the time each action executes, and that the final state satisfies the goal.

```bash
# Validate a classical plan
Validate domain.pddl problem.pddl sas_plan

# Validate a temporal plan
Validate -t 0.001 domain.pddl problem.pddl plan.pddl
```

VAL output:
```
Checking plan: plan.pddl
Plan valid
Final value: 25.002
```

Or, on failure:
```
Checking plan: plan.pddl
Plan failed because of unsatisfied precondition at time 8.001
Action: (drop r2d2 parcel2 kitchen)
Unsatisfied precondition: (robot_at r2d2 kitchen)
```

VAL is indispensable for debugging. If the planner returns a plan that fails during execution, VAL will usually pinpoint the inconsistency between the PDDL model and the actual world state.

---

## 8. How PlanSys2 Processes the Output

### 8.1 Plan Parsing

PlanSys2's Planner node reads the plan file and parses it into a `plansys2_msgs::msg::Plan` message:

```
plansys2_msgs/msg/Plan:
  std_msgs/Header header
  plansys2_msgs/PlanItem[] items

plansys2_msgs/msg/PlanItem:
  float64 time      # start time (seconds)
  string action     # "(action_name arg1 arg2 ...)"
  float64 duration  # duration (seconds), 0 for instantaneous
```

For the temporal plan:
```
0.000: (navigate leia entrance corridor)  [5.000]
8.001: (drop r2d2 parcel2 kitchen)        [1.000]
```

Becomes:
```
items[0]:  time=0.000  action="(navigate leia entrance corridor)"  duration=5.000
items[1]:  time=8.001  action="(drop r2d2 parcel2 kitchen)"        duration=1.000
```

The action string is kept as a PDDL-style parenthesized expression. Individual arguments are extracted by splitting on spaces after stripping the outer parentheses: `["drop", "r2d2", "parcel2", "kitchen"]`.

### 8.2 Plan Graph Construction

The Executor analyzes the plan to determine causal dependencies between actions. This is the key optimization PlanSys2 contributes beyond simply running actions in sequence.

The plan graph is constructed by:

1. For each pair of actions A and B where B starts after A:
   - Check if any precondition of B is in the effect list of A
   - If so, A → B is a dependency edge (B depends on A)

2. For durative actions, also check:
   - If an `(over all ...)` condition of B overlaps with an action A that deletes it → dependency
   - If an `(at start ...)` effect of A conflicts with an `(over all ...)` condition of B → temporal constraint

The plan graph for a multi-robot plan might look like:
```
              navigate(r1, A→B)
             /
  start ─────
             \
              navigate(r2, C→D) ──► pick(r2, pkg1, D) ──► transport(r2, pkg1, D→E) ──► end
```

Two independent `navigate` actions have no dependency edge between them — they run in parallel. The `pick` and `transport` actions of r2 are sequentially dependent.

### 8.3 Parallel Execution Flow Detection

From the dependency graph, the Executor identifies independent execution flows — subsets of actions that can execute concurrently because they share no causal dependencies.

For the multi-robot plan above, two flows are identified:
- **Flow 1:** `navigate(r1, A→B)` — r1's independent movement
- **Flow 2:** `navigate(r2, C→D)` → `pick(r2, ...)` → `transport(r2, ...)` — r2's dependent sequence

These flows are executed in parallel, respecting the within-flow sequencing.

The paper from PlanSys2 notes the plan output for a 3-robot scenario:
```
(move rb1 ...):0    (move rb2 ...):0    (move rb3 ...):0
(transport rb1 ...):5   (transport rb2 ...):5   (transport rb3 ...):5
```

Three independent execution flows — one per robot — all start simultaneously at time 0, transition to their respective transport actions at time 5.

### 8.4 Behavior Tree Construction from Plan Graph

The Executor converts the plan graph into a BehaviorTree.CPP behavior tree at runtime:

```
plan graph →  BT construction  →  runtime execution

 A → B → C        Sequence(A_bt, B_bt, C_bt)
 A   D             Parallel(
 B   E    →          Sequence(A_bt, B_bt),
 ↓   ↓               Sequence(D_bt, E_bt))
 C   F             becomes wrong — need to respect exact dependencies
```

The actual BT structure uses the dependency graph directly:

Each action in the plan becomes a **BT Action node** that:
1. Checks if all predecessor actions have completed (via blackboard flags)
2. Sends an action request to the action performer via `/action_hub`
3. Monitors progress feedback
4. Returns SUCCESS when the performer reports completion, FAILURE on error

The root BT node is a `Parallel` node containing one node per identified execution flow. Within each flow, nodes are organized in a `Sequence`.

The BT is built programmatically in C++ — it is not a static XML file. The XML is generated from the plan graph at runtime and could in principle be inspected.

---

## 9. Execution: From Plan to Hardware

### 9.1 The Action Auction Protocol

PlanSys2's action execution uses a **bidding protocol** over the `/action_hub` topic, enabling multi-robot plans where multiple robots can implement the same action.

The protocol:

```
Executor BT node (ActionPerformerClient):
  1. Publishes action request to /action_hub:
     { action: "move", arguments: ["leia", "entrance", "corridor"],
       status: WAITING_BID }

Action Performer (robot node):
  2. Receives request on /action_hub
  3. Evaluates eligibility: can I execute (move leia entrance corridor)?
     - Does leia match this robot's identity?
     - Are my current state constraints satisfied?
  4. If eligible, publishes bid:
     { action: "move", arguments: [...],
       status: BIDDING }

Executor BT node:
  5. Receives bid, selects winner (first bid, or lowest cost)
  6. Confirms execution:
     { action: "move", ..., status: EXECUTING }

Action Performer:
  7. Executes the action
  8. Periodically publishes progress:
     { action: "move", ..., status: RUNNING, completion: 0.45 }

  9. On completion:
     { action: "move", ..., status: SUCCESS }
     (or FAILURE)

Executor BT node:
  10. Returns NodeStatus::SUCCESS (or FAILURE) to the BT
```

The ActionPerformerClient BT node and the ActionPerformer lifecycle node communicate exclusively through `/action_hub`. This decouples the planner's execution logic from the specific robot hardware that carries out each action — multiple robots can compete for the same action transparently.

### 9.2 ActionExecutorClient API

Every action implementation inherits from `plansys2::ActionExecutorClient`:

```cpp
#include "plansys2_executor/ActionExecutorClient.hpp"

class MoveAction : public plansys2::ActionExecutorClient
{
public:
  MoveAction()
  : ActionExecutorClient("move_action", 250ms)  // 250ms tick rate
  {}

protected:
  // Called every 250ms while action is executing
  void do_work() override
  {
    // get_arguments() returns the grounded action arguments
    // Index 0 = first argument (after action name)
    auto robot_name = get_arguments()[0];   // e.g., "leia"
    auto from_loc   = get_arguments()[1];   // e.g., "entrance"
    auto to_loc     = get_arguments()[2];   // e.g., "corridor"

    // Do some work...
    float progress = compute_progress();

    // Report progress (0.0 to 1.0)
    send_feedback(progress, "Navigating to " + to_loc);

    if (navigation_complete()) {
      // Report success
      finish(true, 1.0, "Navigation complete");
    } else if (navigation_failed()) {
      // Report failure
      finish(false, progress, "Navigation failed: obstacle detected");
    }
    // Otherwise: return without calling finish() to continue execution
  }
};
```

The argument order corresponds exactly to the PDDL action parameter order:
```lisp
(:action move :parameters (?robot - robot ?from - room ?to - room) ...)
```

So `get_arguments()[0]` = bound value of `?robot`, `[1]` = `?from`, `[2]` = `?to`.

### 9.3 BT Node Implementation Pattern

For more complex actions, PlanSys2 supports implementing the action as a full Behavior Tree using BehaviorTree.CPP. The BT XML file specifies the tree structure; C++ plugins implement the individual BT nodes.

Action launch configuration:
```python
move_cmd = Node(
  package='plansys2_bt_actions',
  executable='bt_action_node',
  name='move',
  parameters=[
    params_yaml_file,
    {'action_name': 'move',              # must match PDDL action name
     'bt_xml_file': path_to_bt_xml}
  ])
```

The BT XML for the move action:
```xml
<root main_tree_to_execute="MainTree">
  <BehaviorTree ID="MainTree">
    <Sequence name="root_sequence">
      <CheckBattery      robot="{arg0}" min_level="10"/>
      <Nav2NavigateToPose  goal="{arg2}"/>
      <VerifyPosition    location="{arg2}" tolerance="0.1"/>
    </Sequence>
  </BehaviorTree>
</root>
```

### 9.4 Argument Passing via Blackboard

Within the BT, PDDL action arguments are accessible through the blackboard using the special keys `arg0`, `arg1`, `arg2`, etc. These are automatically populated by PlanSys2's BT action node wrapper when the action is activated.

```
PDDL action:  (move r2d2 corridor kitchen)
              ──────┬─── ────┬──── ──┬────
                   arg0     arg1   arg2

In BT XML:  {arg0} = "r2d2"
            {arg1} = "corridor"
            {arg2} = "kitchen"
```

Custom BT node accessing arguments:
```cpp
class Nav2NavigateToPose : public BT::StatefulActionNode
{
public:
  // Declare the blackboard input
  static BT::PortsList providedPorts() {
    return { BT::InputPort<std::string>("goal") };  // reads from {arg2}
  }

  BT::NodeStatus onStart() override {
    // Read the goal location from the blackboard
    std::string goal_name;
    getInput("goal", goal_name);  // reads whatever was in {arg2}

    // Look up the actual coordinates from a map
    auto goal_pose = location_map_[goal_name];

    // Send navigation goal
    nav2_client_->sendGoal(goal_pose);
    return BT::NodeStatus::RUNNING;
  }
  ...
};
```

### 9.5 Lifecycle Node Integration

Every `ActionExecutorClient` is a **managed lifecycle node** (`rclcpp_lifecycle::LifecycleNode`). PlanSys2 activates and deactivates action nodes in coordination with the execution plan:

```
State transitions:
  UNCONFIGURED → configured  → inactive → active → inactive
                                                   (executing)

The Executor activates an action node when the BT node ticks it
and deactivates it when finish() is called (success or failure).
```

An action node can also cascade-activate dependent nodes. For example, a sensor processing node needed only during a specific action can be cascade-activated while the action runs and automatically deactivated when it completes — avoiding unnecessary CPU load from continuously-running sensor pipelines.

---

## 10. The Replanning Loop

### 10.1 Failure Detection

An action fails when its `ActionExecutorClient` calls `finish(false, ...)` or its BT returns `FAILURE`. The failure propagates up the BT tree through the Sequence control nodes, causing the entire plan execution to fail.

PlanSys2 does not automatically replan on failure — that responsibility belongs to the application's mission controller. The executor's `run` service returns with a failure indication, and the calling code decides whether to replan, retry, or abort.

### 10.2 State Update and Replan Trigger

A robust mission controller following the Sense-Plan-Act pattern:

```cpp
void mission_loop()
{
  while (!goal_achieved()) {

    // 1. SENSE: Update the Problem Expert from sensors
    update_predicate_state_from_sensors();

    // 2. PLAN: Request a new plan
    auto plan = planner_client_->getPlan(
      domain_client_->getDomain(),
      problem_client_->getProblem());

    if (!plan.has_value()) {
      RCLCPP_ERROR(get_logger(), "No plan found — goal unreachable");
      return;
    }

    // 3. ACT: Execute the plan
    executor_client_->start_plan_execution(plan.value());

    // Monitor execution
    while (executor_client_->is_executing()) {

      // Update world state from sensors during execution
      update_predicate_state_from_sensors();

      // Check if execution is still valid
      if (plan_is_no_longer_valid()) {
        executor_client_->cancel_plan_execution();
        break;  // Replan
      }

      sleep(100ms);
    }

    auto result = executor_client_->get_result();
    if (result.success) {
      // Update predicates to reflect what the plan achieved
      apply_plan_effects();
    }
    // Loop back: sense the updated state and replan if goal not yet achieved
  }
}
```

### 10.3 Multi-Robot Coordination

In a multi-robot deployment, each robot runs its own PlanSys2 instance with its own Problem Expert. Shared world knowledge is maintained by publishing predicate updates to a shared ROS2 topic or service that all robots subscribe to.

When one robot's action changes a predicate that another robot depends on, that robot's Problem Expert must be updated before it can generate valid plans. This is the developer's responsibility — PlanSys2 does not automatically synchronize predicate state between robots.

The action auction protocol handles the *execution* side of multi-robot coordination: multiple robots can bid on the same action, and the winning robot executes it. But the *knowledge* side — ensuring all robots agree on the current world state — is external to PlanSys2.

---

## 11. Full Worked Example: Robot with Battery

### 11.1 Domain File

```lisp
(define (domain simple-robot)
  (:requirements :strips :typing :adl :durative-actions :numeric-fluents)

  (:types robot room)

  (:predicates
    (robot_at ?r - robot ?room - room)
    (connected ?from - room ?to - room)
    (battery_full ?r - robot)
    (battery_low ?r - robot)
    (charging_point_at ?room - room))

  (:functions
    (battery_level ?r - robot))

  (:durative-action move
    :parameters (?r - robot ?from - room ?to - room)
    :duration (= ?duration 5)
    :condition (and
      (at start (robot_at ?r ?from))
      (at start (connected ?from ?to))
      (at start (> (battery_level ?r) 0))
      (over all (connected ?from ?to)))
    :effect (and
      (at start (not (robot_at ?r ?from)))
      (at end   (robot_at ?r ?to))
      (at end   (decrease (battery_level ?r) 10))
      (at end   (when (< (battery_level ?r) 30)
                  (battery_low ?r)))))

  (:durative-action charge
    :parameters (?r - robot ?room - room)
    :duration (= ?duration 10)
    :condition (and
      (at start (robot_at ?r ?room))
      (at start (charging_point_at ?room))
      (over all (robot_at ?r ?room)))
    :effect (and
      (at end (assign (battery_level ?r) 100))
      (at end (battery_full ?r))
      (at end (not (battery_low ?r)))))

  (:durative-action ask_charge
    :parameters (?r - robot ?from - room ?to - room)
    :duration (= ?duration 5)
    :condition (and
      (at start (robot_at ?r ?from))
      (at start (battery_low ?r))
      (at start (charging_point_at ?to))
      (at start (connected ?from ?to))
      (over all (connected ?from ?to)))
    :effect (and
      (at start (not (robot_at ?r ?from)))
      (at end   (robot_at ?r ?to)))))
```

### 11.2 Problem File

```lisp
(define (problem simple-robot-problem)
  (:domain simple-robot)

  (:objects
    leia  - robot
    entrance kitchen bedroom bathroom corridor chargingroom - room)

  (:init
    ;; Robot starting position and battery
    (robot_at leia entrance)
    (= (battery_level leia) 50)

    ;; Map topology
    (connected entrance corridor)
    (connected corridor entrance)
    (connected corridor kitchen)
    (connected kitchen corridor)
    (connected corridor bedroom)
    (connected bedroom corridor)
    (connected corridor bathroom)
    (connected bathroom corridor)
    (connected corridor chargingroom)
    (connected chargingroom corridor)

    ;; Charging infrastructure
    (charging_point_at chargingroom))

  (:goal (and
    (robot_at leia kitchen)
    (battery_full leia))))
```

### 11.3 Generated Plan

POPF output for this problem:
```
; Cost: 30.0
; Time 0.01

0.000: (move leia entrance corridor)     [5.000]
5.001: (move leia corridor chargingroom) [5.000]
10.002: (charge leia chargingroom)       [10.000]
20.003: (move leia chargingroom corridor) [5.000]
25.004: (move leia corridor kitchen)      [5.000]
```

The planner determined that leia must charge before reaching the kitchen — going directly would reduce battery from 50 to 30 (two 10-unit moves), and the goal requires `(battery_full leia)`, which requires charging. The optimal sequence is: move to charging room, charge, then move to kitchen.

### 11.4 Plan Graph

Dependency analysis:

```
move(leia, entrance→corridor)
  ↓ (produces robot_at leia corridor)
move(leia, corridor→chargingroom)
  ↓ (produces robot_at leia chargingroom)
charge(leia, chargingroom)
  ↓ (produces robot_at leia chargingroom + battery_full)
move(leia, chargingroom→corridor)
  ↓ (produces robot_at leia corridor)
move(leia, corridor→kitchen)
  ↓ (produces robot_at leia kitchen)
```

Single sequential execution flow — no parallelism possible (one robot). All five actions form one chain.

### 11.5 BT Construction

```
Sequence
├── BTActionNode("move leia entrance corridor",    t=0.000, dur=5.000)
├── BTActionNode("move leia corridor chargingroom", t=5.001, dur=5.000)
├── BTActionNode("charge leia chargingroom",        t=10.002, dur=10.000)
├── BTActionNode("move leia chargingroom corridor", t=20.003, dur=5.000)
└── BTActionNode("move leia corridor kitchen",      t=25.004, dur=5.000)
```

Each `BTActionNode` sends its action to `/action_hub` and waits for an ActionPerformer to bid and execute it.

### 11.6 Action Implementation

```cpp
class MoveAction : public plansys2::ActionExecutorClient
{
public:
  MoveAction() : ActionExecutorClient("move_action_node", 250ms) {}

private:
  std::shared_ptr<nav2_msgs::action::NavigateToPose::Goal> nav_goal_;
  rclcpp_action::Client<nav2_msgs::action::NavigateToPose>::SharedPtr nav_client_;

  void do_work() override
  {
    if (first_tick_) {
      auto robot = get_arguments()[0];   // "leia"
      auto from  = get_arguments()[1];   // "entrance"
      auto to    = get_arguments()[2];   // "corridor"

      // Look up destination pose
      auto pose = room_poses_[to];

      // Send navigation goal
      nav_goal_ = std::make_shared<...>();
      nav_goal_->pose.pose.position = pose;
      nav_client_->async_send_goal(*nav_goal_, ...);
      first_tick_ = false;
    }

    if (nav_complete_) {
      // Update Problem Expert — apply effects of this action
      // (In full PlanSys2, this is done by the Executor automatically
      //  based on the PDDL effects. In manual mode, you update here.)
      finish(true, 1.0, "Arrived at " + get_arguments()[2]);
    } else if (nav_failed_) {
      finish(false, progress_, "Navigation failed");
    } else {
      send_feedback(progress_, "Navigating...");
    }
  }
};
```

---

## 12. Common Mistakes and Debugging

### 12.1 Domain File Errors

**Missing type declaration:**
```lisp
;; WRONG — using type 'vehicle' without declaring it
(:predicates (at_vehicle ?v - vehicle ?l - location))

;; FIX — add to :types
(:types vehicle - object)
```

**Asymmetric connectivity:**
```lisp
;; WRONG — move from A to B works, but not B to A
(:init (connected roomA roomB))

;; FIX — add both directions for undirected edges
(:init (connected roomA roomB) (connected roomB roomA))
```

**Missing negative precondition requirement:**
```lisp
;; WRONG — using (not ...) in precondition without :negative-preconditions
(:requirements :strips :typing)
(:action pick
  :precondition (not (gripper_full ?r))  ;; ERROR if :negative-preconditions not declared

;; FIX
(:requirements :strips :typing :negative-preconditions)
```

**Action with no effects (useless):**
```lisp
;; WARNING — action will be generated but can never make progress toward goal
(:action observe
  :parameters (?r - robot)
  :precondition (robot_active ?r)
  :effect ())  ;; changes nothing
```

### 12.2 Problem File Errors

**Goal predicate undefined in domain:**
```lisp
;; WRONG — predicate 'delivered' not declared in domain
(:goal (delivered parcel1))

;; FIX — add to domain's :predicates
(:predicates ... (delivered ?p - package))
```

**Goal unreachable — no action produces the goal predicate:**
```lisp
(:goal (battery_charged leia))
;; But no action ever adds (battery_charged ?r)...
;; Planner will exhaust state space and return "unsolvable"
```

**Forgetting to list static predicates in :init:**
```lisp
;; Problem: planner can't find any path because no connections exist
(:init
  (robot_at leia entrance)
  ;; FORGOT: (connected entrance corridor) etc.
)
;; The planner will correctly conclude: no plan exists
;; But the real reason is missing init facts, not a truly unsolvable problem
```

**Numeric fluent not initialized:**
```lisp
;; domain has: (> (battery_level ?r) 0) as precondition
;; problem has: no assignment for battery_level
;; Result: battery_level has undefined initial value, precondition may fail
(:init
  ;; MISSING: (= (battery_level leia) 85)
  (robot_at leia entrance))
```

### 12.3 Plan Failure Modes

**Predicate mismatch between plan and reality:**

The plan says `(robot_at leia corridor)` after the move action. The actual robot drifted and is in an unmapped position. The AMCL localization reports the pose, but there is no code updating the Problem Expert's `(robot_at ...)` predicate after each action. The next plan step's precondition `(robot_at leia corridor)` is not updated in the knowledge base, but the planner assumed it was. The plan proceeds as if the robot is in the corridor when it isn't.

**Fix:** After each action completes, explicitly update the Problem Expert:
```cpp
// After move(leia, entrance, corridor) succeeds:
problem_client->removePredicate(Predicate("(robot_at leia entrance)"));
problem_client->addPredicate(Predicate("(robot_at leia corridor)"));
```

**Plan is found but immediately fails:**

The most common cause: the initial state in the problem file doesn't match the actual world state. VAL can diagnose this:
```bash
Validate domain.pddl problem.pddl plan.pddl
# Will report which precondition was unsatisfied and at which step
```

**Planner returns no plan:**

Causes:
1. Goal is unreachable — check that each goal predicate is achievable via some action chain from the initial state
2. Preconditions form a deadlock — action A requires fact F produced by action B, which requires F to already be true
3. Numeric constraints create infeasibility — battery runs out before reaching goal, no recharge possible
4. Type mismatch — action requires a specific subtype but only the supertype is available in :objects

### 12.4 Debugging Tools

```bash
# Check domain syntax
ros2 run plansys2_terminal plansys2_terminal  # then: get domain

# Run planner directly with verbose output
popf -v domain.pddl problem.pddl

# Validate a plan
Validate domain.pddl problem.pddl plan.pddl

# Fast Downward with plan validation
./fast-downward.py --validate domain.pddl problem.pddl \
  --search "lazy_greedy([ff()])"

# Inspect SAS+ translation (shows which predicates became mutex groups)
./fast-downward.py --translate domain.pddl problem.pddl
cat output.sas  # human-readable SAS+ representation

# PlanSys2 terminal commands
get domain               # show loaded domain
get problem              # show current problem state
get problem instances    # list current objects
get problem predicates   # list current true predicates
get plan                 # generate and display plan without executing
run                      # execute the plan
```

---

## 13. PDDL Version Compatibility

| Feature | PDDL Version | POPF | TFD | Fast Downward |
|---|---|---|---|---|
| STRIPS (basic) | 1.0 | ✓ | ✓ | ✓ |
| Typing | 1.0 | ✓ | ✓ | ✓ |
| Negative preconditions | 1.0 | ✓ | ✓ | ✓ |
| Disjunctive preconditions | 1.0 | ✓ | ✓ | ✓ |
| Conditional effects | 1.0 | ✓ | ✓ | ✓ |
| Quantified preconditions | 1.0 | ✓ | ✓ | ✓ |
| ADL (full) | 1.0 | ✓ | ✓ | ✓ |
| Numeric fluents | 2.1 | ✓ | ✓ | ✓ |
| Durative actions | 2.1 | ✓ | ✓ | Limited |
| Continuous effects | 2.1 | ✓ | ✓ | ✗ |
| Timed initial literals | 2.2 | Partial | Partial | ✗ |
| Plan preferences | 3.0 | ✗ | ✗ | ✗ |
| Modal preconditions | 3.1 | ✗ | ✗ | ✗ |

**PlanSys2 officially supports PDDL 2.1.** For simple classical (non-temporal) problems, any planner with `:strips :typing` support will work.

---

## 14. Connection to Unified DSL

The unified S-expression planning DSL described in the companion READMEs generates both the domain and problem files as compilation targets.

**`--pddl` target output:**

From the unified DSL:
```lisp
(action navigate (?robot ?from ?to)
  (require (robot_at ?robot ?from) (connected ?from ?to))
  (effect  (robot_at ?robot ?to) (not (robot_at ?robot ?from)))
  (execute ...))
```

The `--pddl` target strips the `execute` block and emits:

```lisp
;; domain.pddl
(:durative-action navigate
  :parameters (?robot - robot ?from - room ?to - room)
  :duration (= ?duration (distance ?from ?to))
  :condition (and
    (at start (robot_at ?robot ?from))
    (at start (connected ?from ?to))
    (over all (connected ?from ?to)))
  :effect (and
    (at start (not (robot_at ?robot ?from)))
    (at end   (robot_at ?robot ?to))))
```

The `(require ...)` clause maps to `:precondition` / `:condition`.
The `(effect ...)` clause maps to `:effect`.
Parameters with type annotations (`?robot - robot`) are inferred from the `(objects ...)` declaration.

**The `(init ...)` and `(goal ...)` blocks in `define-mission` map directly** to the problem file's `:init` and `:goal` sections.

**Correspondence table:**

| Unified DSL Form | PDDL Equivalent |
|---|---|
| `(require p1 p2)` | `:precondition (and p1 p2)` |
| `(require (path-clear ?a ?b))` | `:condition (over all (path-clear ?a ?b))` |
| `(effect p1)` | positive literal in `:effect` |
| `(effect (not p1))` | `(not p1)` in `:effect` |
| `(init p1 p2 ...)` | `:init (p1 p2 ...)` in problem file |
| `(goal p1 p2 ...)` | `:goal (and p1 p2 ...)` in problem file |
| `(objects ...)` | `:objects ...` in problem file |
| `(define-mission name ...)` | `(define (problem name) (:domain ...) ...)` |

The unified DSL thus produces syntactically correct PDDL that any PDDL 2.1-compatible planner can consume — POPF, TFD, Fast Downward, OPTIC, or any future planner. The planning layer is not tied to PlanSys2; it is tied to the standard.

---

## References

- **PDDL Reference:** https://planning.wiki/ref/pddl
- **PDDL Tutorial:** https://fareskalaboud.github.io/LearnPDDL/
- **PlanSys2 Documentation:** https://plansys2.github.io
- **PlanSys2 Design:** https://plansys2.github.io/design/index.html
- **Fast Downward:** https://www.fast-downward.org
- **Fast Downward Translator Format:** https://www.fast-downward.org/latest/documentation/translator-output-format/
- **POPF Paper:** Coles et al. (2010). Forward-Chaining Partial-Order Planning. ICAPS 2010.
- **Plan Validator VAL:** https://github.com/KCL-Planning/VAL

