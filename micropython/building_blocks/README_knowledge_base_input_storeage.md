# PDDL Planner Data in the SQLite3 ltree Knowledge Base
## Schema Design, Path Conventions, SQL Operations, and LuaJIT Integration

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [ltree Path Conventions for PDDL Data](#2-ltree-path-conventions-for-pddl-data)
   - 2.1 [Domain Subtree (Static)](#21-domain-subtree-static)
   - 2.2 [Problem Subtree (Live State)](#22-problem-subtree-live-state)
   - 2.3 [Plan Subtree (Output)](#23-plan-subtree-output)
   - 2.4 [Execution Subtree (Runtime Monitor)](#24-execution-subtree-runtime-monitor)
3. [Schema Definition](#3-schema-definition)
   - 3.1 [Core Knowledge Base Table](#31-core-knowledge-base-table)
   - 3.2 [Predicate State Index Table](#32-predicate-state-index-table)
   - 3.3 [Indexes](#33-indexes)
4. [Domain Construction: Writing the Domain to the KB](#4-domain-construction-writing-the-domain-to-the-kb)
   - 4.1 [Types Hierarchy](#41-types-hierarchy)
   - 4.2 [Predicate Declarations](#42-predicate-declarations)
   - 4.3 [Action Schema Storage](#43-action-schema-storage)
   - 4.4 [Function Declarations](#44-function-declarations)
5. [Problem Management: Live World State](#5-problem-management-live-world-state)
   - 5.1 [Adding and Removing Objects](#51-adding-and-removing-objects)
   - 5.2 [The Predicate State: Closed World in ltree](#52-the-predicate-state-closed-world-in-ltree)
   - 5.3 [Numeric Functions](#53-numeric-functions)
   - 5.4 [Goal Management](#54-goal-management)
6. [PDDL Text Generation from the KB](#6-pddl-text-generation-from-the-kb)
   - 6.1 [Generating domain.pddl](#61-generating-domainpddl)
   - 6.2 [Generating problem.pddl](#62-generating-problempddl)
7. [Plan Storage and Retrieval](#7-plan-storage-and-retrieval)
   - 7.1 [Storing the Plan Output](#71-storing-the-plan-output)
   - 7.2 [Reading the Plan for Execution](#72-reading-the-plan-for-execution)
   - 7.3 [Plan History](#73-plan-history)
8. [Execution Monitor: State Updates](#8-execution-monitor-state-updates)
   - 8.1 [Applying Action Effects](#81-applying-action-effects)
   - 8.2 [Replanning Triggers](#82-replanning-triggers)
9. [LuaJIT FFI Implementation](#9-luajit-ffi-implementation)
   - 9.1 [KB Handle and Core Operations](#91-kb-handle-and-core-operations)
   - 9.2 [Domain Loader](#92-domain-loader)
   - 9.3 [Problem Expert Operations](#93-problem-expert-operations)
   - 9.4 [Plan Storage and Retrieval](#94-plan-storage-and-retrieval)
   - 9.5 [PDDL Text Assembler](#95-pddl-text-assembler)
10. [Python Construction Layer](#10-python-construction-layer)
11. [Worked Example: Robot with Battery](#11-worked-example-robot-with-battery)
    - 11.1 [Load Domain into KB](#111-load-domain-into-kb)
    - 11.2 [Set Up Problem Instance](#112-set-up-problem-instance)
    - 11.3 [Assemble PDDL, Plan, Store Result](#113-assemble-pddl-plan-store-result)
    - 11.4 [Monitor Execution and Update State](#114-monitor-execution-and-update-state)
12. [ChainTree S-Expression Integration](#12-chaintree-s-expression-integration)
13. [Query Reference](#13-query-reference)

---

## 1. Design Philosophy

The ltree knowledge base stores PDDL planner data using the same hierarchical path structure already used by ChainTree nodes. Every PDDL concept maps to a path:

- **Existence encodes truth.** A predicate `(robot_at leia corridor)` is true if and only if a row exists at path `problem.state.robot_at.leia.corridor`. Delete the row to retract the predicate. This directly implements the closed-world assumption in the KB's natural idiom.

- **Path structure encodes type.** The path prefix determines what kind of node it is: `planner.domain.*` is static domain data, `planner.problem.state.*` is live world state, `planner.plan.current.*` is the active plan. Subtree queries with `LIKE 'planner.problem.state.%'` retrieve all true predicates efficiently.

- **The same Construct_KB and KnowledgeBaseManager infrastructure.** Domain loading is a one-time construction operation using the existing `Construct_KB` class. Problem management (add/remove predicates, update functions) uses `KnowledgeBaseManager` CRUD operations. No new infrastructure needed.

- **PDDL text is derived, not stored.** The KB stores structured data. The domain and problem PDDL text files are generated on demand by assembling KB rows into PDDL syntax. This means the KB is always the authoritative source of truth, and the PDDL files are just a serialization format for the external solver.

---

## 2. ltree Path Conventions for PDDL Data

All PDDL-related data lives under the root `planner`. This keeps it isolated from ChainTree behavior nodes and other KB subtrees.

### 2.1 Domain Subtree (Static)

The domain subtree is written once at startup and never modified during operation.

```
planner.domain
planner.domain.meta                          -- domain metadata (name, requirements)

planner.domain.types                         -- type hierarchy
planner.domain.types.<type_name>             -- type declaration
  └─ data.json = {"parent": "<parent_type>", "is_root": false}

planner.domain.predicates                    -- predicate declarations
planner.domain.predicates.<pred_name>        -- predicate
  └─ data.json = {"arity": 2, "static": false}
planner.domain.predicates.<pred_name>.p<n>   -- parameter n
  └─ data.json = {"type": "<type_name>", "var": "?r"}

planner.domain.functions                     -- numeric function declarations
planner.domain.functions.<func_name>         -- function
  └─ data.json = {"arity": 1}
planner.domain.functions.<func_name>.p<n>    -- parameter n
  └─ data.json = {"type": "<type_name>"}

planner.domain.actions                       -- action schemas
planner.domain.actions.<action_name>         -- action
  └─ data.json = {"temporal": false, "cost": 1}
planner.domain.actions.<action_name>.p<n>    -- parameter n (named for ordering)
  └─ data.json = {"var": "?robot", "type": "robot"}
planner.domain.actions.<action_name>.pre.<n> -- precondition clause n
  └─ data.json = {"expr": "(robot_at ?robot ?from)", "negated": false, "when": "at_start"}
planner.domain.actions.<action_name>.eff.<n> -- effect clause n
  └─ data.json = {"expr": "(robot_at ?robot ?to)", "negated": false, "when": "at_end"}
planner.domain.actions.<action_name>.dur     -- duration (temporal only)
  └─ data.json = {"expr": "(= ?duration 5)", "fixed": 5.0}
```

### 2.2 Problem Subtree (Live State)

The problem subtree changes continuously as the robot operates. This is the hot path — predicate state updates happen after every action.

```
planner.problem
planner.problem.meta                         -- problem metadata (name, domain ref)

planner.problem.objects                      -- typed object instances
planner.problem.objects.<obj_name>           -- object declaration
  └─ data.json = {"type": "robot"}

planner.problem.state                        -- CLOSED WORLD STATE
planner.problem.state.<pred>                 -- predicate root
planner.problem.state.<pred>.<arg0>          -- 1-arg predicate: TRUE if this row exists
planner.problem.state.<pred>.<arg0>.<arg1>   -- 2-arg predicate: TRUE if this row exists
planner.problem.state.<pred>.<arg0>.<arg1>.<arg2>  -- 3-arg predicate

-- Examples:
planner.problem.state.robot_at.leia.corridor
planner.problem.state.battery_full.r2d2
planner.problem.state.connected.entrance.corridor
planner.problem.state.gripper_empty.leia

planner.problem.functions                    -- numeric values
planner.problem.functions.<func_name>        -- function root
planner.problem.functions.<func_name>.<arg0> -- 1-arg numeric value
  └─ data.json = {"value": 85.0}
planner.problem.functions.<func_name>.<arg0>.<arg1>  -- 2-arg numeric value
  └─ data.json = {"value": 12.5}

-- Examples:
planner.problem.functions.battery_level.leia       -- = 85.0
planner.problem.functions.distance.entrance.corridor  -- = 5.0

planner.problem.goal                         -- goal conjuncts
planner.problem.goal.g<nnn>                  -- goal clause (zero-padded for ordering)
  └─ data.json = {"expr": "(robot_at leia kitchen)", "negated": false}
```

### 2.3 Plan Subtree (Output)

The plan subtree holds the current plan and archived historical plans.

```
planner.plan
planner.plan.current                         -- active plan
planner.plan.current.meta
  └─ data.json = {"status": "executing", "cost": 25.0, "generated_at": "<timestamp>"}
planner.plan.current.step.<nnn>              -- plan step (zero-padded: s000, s001...)
  └─ data.json = {"time": 0.0, "action": "navigate", "args": ["leia","entrance","corridor"],
                  "duration": 5.0, "status": "pending"}

-- Examples:
planner.plan.current.step.s000  → {"time":0.0, "action":"move", "args":["leia","entrance","corridor"], ...}
planner.plan.current.step.s001  → {"time":5.001, "action":"charge", "args":["leia","chargingroom"], ...}

planner.plan.history.<run_id>               -- archived plan runs
planner.plan.history.<run_id>.meta
planner.plan.history.<run_id>.step.<nnn>
```

### 2.4 Execution Subtree (Runtime Monitor)

Tracks which plan steps have been executed and whether effects were confirmed.

```
planner.exec
planner.exec.current.step.<nnn>             -- execution state per step
  └─ data.json = {"status": "success|failure|running|pending",
                  "started_at": "<ts>", "completed_at": "<ts>",
                  "effects_verified": true}
planner.exec.replan_requested               -- flag: trigger replanning
  └─ data.json = {"reason": "action_failed", "failed_step": "s003"}
```

---

## 3. Schema Definition

### 3.1 Core Knowledge Base Table

The existing `knowledge_base` table schema, extended minimally for PDDL use:

```sql
-- The existing KB table — no changes needed
CREATE TABLE IF NOT EXISTS knowledge_base (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    label    TEXT    NOT NULL,          -- last path component (e.g., "robot_at", "leia")
    name     TEXT    NOT NULL,          -- human-readable name
    properties TEXT  DEFAULT '{}',     -- JSON: type metadata, parameter info
    data     TEXT    DEFAULT '{}',     -- JSON: actual value/content
    path     TEXT    NOT NULL UNIQUE    -- full ltree path (TEXT, ltree-indexed)
);
```

The `label` column holds the last path component. For `planner.problem.state.robot_at.leia.corridor`, the label is `corridor`. The `data` JSON holds the predicate's content (usually just `{}` — existence encodes truth) or numeric values.

**Mapping PDDL concepts to columns:**

| PDDL Concept | `label` | `name` | `properties` JSON | `data` JSON |
|---|---|---|---|---|
| Type declaration | `robot` | `"Type: robot"` | `{"kind":"type"}` | `{"parent":"object","is_root":false}` |
| Predicate declaration | `robot_at` | `"robot_at(?r,?l)"` | `{"kind":"predicate","arity":2}` | `{"static":false}` |
| Ground predicate (true) | `corridor` (last arg) | `"(robot_at leia corridor)"` | `{"kind":"state_fact"}` | `{}` |
| Object instance | `leia` | `"leia"` | `{"kind":"object"}` | `{"type":"robot"}` |
| Numeric function | `leia` (arg) | `"battery_level(leia)"` | `{"kind":"function_value"}` | `{"value":85.0}` |
| Action schema | `move` | `"move(?r,?from,?to)"` | `{"kind":"action","temporal":false}` | `{"cost":1}` |
| Plan step | `s000` | `"step 0"` | `{"kind":"plan_step"}` | `{"time":0.0,"action":"move","args":["leia","entrance","corridor"],"duration":5.0,"status":"pending"}` |
| Goal clause | `g000` | `"goal 0"` | `{"kind":"goal"}` | `{"expr":"(robot_at leia kitchen)","negated":false}` |

### 3.2 Predicate State Index Table

For high-frequency predicate existence checks (the hot path during BT execution), a denormalized index is maintained:

```sql
-- Fast predicate lookup: is (robot_at leia corridor) true?
-- Maintained in sync with knowledge_base rows under planner.problem.state.*
CREATE TABLE IF NOT EXISTS predicate_state (
    path      TEXT PRIMARY KEY,        -- full ltree path (same as knowledge_base.path)
    pred_name TEXT NOT NULL,           -- e.g., "robot_at"
    arg0      TEXT,                    -- first argument
    arg1      TEXT,                    -- second argument
    arg2      TEXT,                    -- third argument (NULL for arity < 3)
    asserted_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_predstate_pred  ON predicate_state(pred_name);
CREATE INDEX IF NOT EXISTS idx_predstate_arg0  ON predicate_state(pred_name, arg0);
CREATE INDEX IF NOT EXISTS idx_predstate_full  ON predicate_state(pred_name, arg0, arg1);
```

**Sync trigger** — keeps `predicate_state` in sync automatically:

```sql
-- Insert trigger
CREATE TRIGGER IF NOT EXISTS sync_state_insert
AFTER INSERT ON knowledge_base
WHEN NEW.path LIKE 'planner.problem.state.%'
BEGIN
    INSERT OR REPLACE INTO predicate_state (path, pred_name, arg0, arg1, arg2)
    VALUES (
        NEW.path,
        -- Extract predicate name: 3rd component after planner.problem.state
        -- path format: planner.problem.state.<pred>.<arg0>.<arg1>...
        CASE WHEN instr(substr(NEW.path, length('planner.problem.state.')+1), '.') = 0
             THEN substr(NEW.path, length('planner.problem.state.')+1)
             ELSE substr(NEW.path, length('planner.problem.state.')+1,
                         instr(substr(NEW.path, length('planner.problem.state.')+1),'.')-1)
        END,
        -- arg0: 4th component
        NULL, NULL, NULL  -- populated by application layer
    );
END;

-- Delete trigger
CREATE TRIGGER IF NOT EXISTS sync_state_delete
AFTER DELETE ON knowledge_base
WHEN OLD.path LIKE 'planner.problem.state.%'
BEGIN
    DELETE FROM predicate_state WHERE path = OLD.path;
END;
```

In practice, the application layer populates `pred_name`, `arg0`, `arg1`, `arg2` explicitly rather than relying on string parsing in SQL triggers.

### 3.3 Indexes

```sql
-- Primary path lookup (used for all existence checks)
CREATE UNIQUE INDEX IF NOT EXISTS idx_kb_path     ON knowledge_base(path);

-- Subtree scan (used for domain/problem assembly)
-- SQLite doesn't have native ltree index — use LIKE prefix matching
-- For large KBs, a separate prefix index helps:
CREATE INDEX IF NOT EXISTS idx_kb_path_prefix
  ON knowledge_base(path)
  WHERE path LIKE 'planner.%';

-- Label lookup (action/predicate name queries)
CREATE INDEX IF NOT EXISTS idx_kb_label ON knowledge_base(label);

-- Type filter (kind-based queries)
CREATE INDEX IF NOT EXISTS idx_kb_kind
  ON knowledge_base(json_extract(properties, '$.kind'));
```

---

## 4. Domain Construction: Writing the Domain to the KB

The domain is written once at startup using the `Construct_KB` class. It is read during every planning cycle to generate `domain.pddl`.

### 4.1 Types Hierarchy

```sql
-- Domain root
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('domain', 'Planning Domain', '{"kind":"domain_root"}',
        '{"name":"simple-robot","requirements":[":strips",":typing",":durative-actions",":numeric-fluents"]}',
        'planner.domain');

-- Types root
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('types', 'Type Hierarchy', '{"kind":"types_root"}', '{}',
        'planner.domain.types');

-- Type: object (root type)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('object', 'object', '{"kind":"type"}',
        '{"parent":null,"is_root":true}',
        'planner.domain.types.object');

-- Type: robot (subtype of object)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('robot', 'robot', '{"kind":"type"}',
        '{"parent":"object","is_root":false}',
        'planner.domain.types.robot');

-- Type: location (subtype of object)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('location', 'location', '{"kind":"type"}',
        '{"parent":"object","is_root":false}',
        'planner.domain.types.location');

-- Type: room (subtype of location)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('room', 'room', '{"kind":"type"}',
        '{"parent":"location","is_root":false}',
        'planner.domain.types.room');
```

**Query the type hierarchy:**

```sql
-- Get all types
SELECT label, json_extract(data,'$.parent') AS parent
FROM knowledge_base
WHERE path LIKE 'planner.domain.types.%'
  AND json_extract(properties,'$.kind') = 'type'
ORDER BY path;

-- Get all subtypes of 'location' (direct children only)
SELECT label FROM knowledge_base
WHERE path LIKE 'planner.domain.types.%'
  AND json_extract(data,'$.parent') = 'location';
```

### 4.2 Predicate Declarations

```sql
-- Predicate: robot_at(?r - robot, ?l - location)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('robot_at', 'robot_at', '{"kind":"predicate","arity":2}',
        '{"static":false}',
        'planner.domain.predicates.robot_at');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p0', 'param 0', '{"kind":"pred_param"}',
        '{"var":"?r","type":"robot","position":0}',
        'planner.domain.predicates.robot_at.p0');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p1', 'param 1', '{"kind":"pred_param"}',
        '{"var":"?l","type":"location","position":1}',
        'planner.domain.predicates.robot_at.p1');

-- Predicate: battery_full(?r - robot)  — zero-arg after bound
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('battery_full', 'battery_full', '{"kind":"predicate","arity":1}',
        '{"static":false}',
        'planner.domain.predicates.battery_full');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p0', 'param 0', '{"kind":"pred_param"}',
        '{"var":"?r","type":"robot","position":0}',
        'planner.domain.predicates.battery_full.p0');

-- Static predicate: connected(?a - room, ?b - room)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('connected', 'connected', '{"kind":"predicate","arity":2}',
        '{"static":true}',          -- static: never modified by actions
        'planner.domain.predicates.connected');
```

**Query predicates for PDDL generation:**

```sql
-- Get all predicate declarations with parameters
SELECT
  p.label AS pred_name,
  json_extract(p.data,'$.static') AS is_static,
  json_group_array(
    json_object(
      'var',  json_extract(pp.data,'$.var'),
      'type', json_extract(pp.data,'$.type')
    )
  ) AS params
FROM knowledge_base p
LEFT JOIN knowledge_base pp
  ON pp.path LIKE p.path || '.p%'
  AND json_extract(pp.properties,'$.kind') = 'pred_param'
WHERE json_extract(p.properties,'$.kind') = 'predicate'
  AND p.path LIKE 'planner.domain.predicates.%'
  AND p.path NOT LIKE '%.p%'   -- exclude param rows
GROUP BY p.path
ORDER BY p.path;
```

### 4.3 Action Schema Storage

```sql
-- Action: move (durative)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('move', 'move', '{"kind":"action"}',
        '{"temporal":true,"cost":1}',
        'planner.domain.actions.move');

-- Parameters
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p0','param 0','{"kind":"action_param"}',
        '{"var":"?r","type":"robot","position":0}',
        'planner.domain.actions.move.p0');
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p1','param 1','{"kind":"action_param"}',
        '{"var":"?from","type":"room","position":1}',
        'planner.domain.actions.move.p1');
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p2','param 2','{"kind":"action_param"}',
        '{"var":"?to","type":"room","position":2}',
        'planner.domain.actions.move.p2');

-- Duration
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('duration','duration','{"kind":"duration"}',
        '{"expr":"(= ?duration 5)","fixed_value":5.0}',
        'planner.domain.actions.move.duration');

-- Preconditions
-- at start: (robot_at ?r ?from)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('c0','precond 0','{"kind":"precondition"}',
        '{"expr":"(robot_at ?r ?from)","negated":false,"when":"at_start","pos":0}',
        'planner.domain.actions.move.pre.c0');
-- at start: (connected ?from ?to)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('c1','precond 1','{"kind":"precondition"}',
        '{"expr":"(connected ?from ?to)","negated":false,"when":"at_start","pos":1}',
        'planner.domain.actions.move.pre.c1');
-- over all: (connected ?from ?to)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('c2','precond 2','{"kind":"precondition"}',
        '{"expr":"(connected ?from ?to)","negated":false,"when":"over_all","pos":2}',
        'planner.domain.actions.move.pre.c2');

-- Effects
-- at start: (not (robot_at ?r ?from))
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('e0','effect 0','{"kind":"effect"}',
        '{"expr":"(robot_at ?r ?from)","negated":true,"when":"at_start","pos":0}',
        'planner.domain.actions.move.eff.e0');
-- at end: (robot_at ?r ?to)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('e1','effect 1','{"kind":"effect"}',
        '{"expr":"(robot_at ?r ?to)","negated":false,"when":"at_end","pos":1}',
        'planner.domain.actions.move.eff.e1');
-- at end: numeric effect
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('e2','effect 2','{"kind":"effect"}',
        '{"expr":"(decrease (battery_level ?r) 10)","negated":false,"when":"at_end","numeric":true,"pos":2}',
        'planner.domain.actions.move.eff.e2');
```

### 4.4 Function Declarations

```sql
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('battery_level','battery_level','{"kind":"function","arity":1}',
        '{}',
        'planner.domain.functions.battery_level');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('p0','param 0','{"kind":"func_param"}',
        '{"var":"?r","type":"robot","position":0}',
        'planner.domain.functions.battery_level.p0');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('distance','distance','{"kind":"function","arity":2}',
        '{"static":true}',         -- distance never changes
        'planner.domain.functions.distance');
```

---

## 5. Problem Management: Live World State

### 5.1 Adding and Removing Objects

```sql
-- Add object: leia is a robot
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('leia', 'leia', '{"kind":"object"}',
        '{"type":"robot"}',
        'planner.problem.objects.leia');

-- Add object: corridor is a room
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('corridor', 'corridor', '{"kind":"object"}',
        '{"type":"room"}',
        'planner.problem.objects.corridor');

-- Remove object (and all its state facts — cascade via trigger or application)
DELETE FROM knowledge_base WHERE path = 'planner.problem.objects.leia';
-- Also clean up any predicates involving this object:
DELETE FROM knowledge_base
WHERE path LIKE 'planner.problem.state.%.leia%'
   OR path LIKE 'planner.problem.state.%.%.leia%';

-- Query all objects of a specific type
SELECT label
FROM knowledge_base
WHERE path LIKE 'planner.problem.objects.%'
  AND json_extract(data,'$.type') = 'robot';
```

### 5.2 The Predicate State: Closed World in ltree

The most important design decision: **predicate truth = row existence.** No value column needed for boolean predicates — the path existing means "true," the path absent means "false."

```sql
-- ASSERT: (robot_at leia corridor) = TRUE
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('corridor', '(robot_at leia corridor)', '{"kind":"state_fact"}',
        '{}',
        'planner.problem.state.robot_at.leia.corridor');
INSERT INTO predicate_state (path, pred_name, arg0, arg1)
VALUES ('planner.problem.state.robot_at.leia.corridor', 'robot_at', 'leia', 'corridor');

-- RETRACT: (robot_at leia corridor) = FALSE  (robot moved)
DELETE FROM knowledge_base
WHERE path = 'planner.problem.state.robot_at.leia.corridor';
-- predicate_state cleaned up by trigger

-- CHECK: is (robot_at leia corridor) true?
SELECT EXISTS(
  SELECT 1 FROM predicate_state
  WHERE pred_name = 'robot_at' AND arg0 = 'leia' AND arg1 = 'corridor'
) AS is_true;

-- CHECK: where is leia?  (find what robot_at leia X is true for)
SELECT arg1 AS location
FROM predicate_state
WHERE pred_name = 'robot_at' AND arg0 = 'leia';

-- GET ALL true predicates (the full current world state)
SELECT pred_name, arg0, arg1, arg2
FROM predicate_state
ORDER BY pred_name, arg0, arg1;

-- GET all true predicates of a specific type
SELECT arg0, arg1
FROM predicate_state
WHERE pred_name = 'connected'
ORDER BY arg0, arg1;

-- Atomic state transition: robot moves from entrance to corridor
-- (single transaction to maintain consistency)
BEGIN;
DELETE FROM knowledge_base
  WHERE path = 'planner.problem.state.robot_at.leia.entrance';
INSERT INTO knowledge_base (label, name, properties, data, path)
  VALUES ('corridor','(robot_at leia corridor)','{"kind":"state_fact"}','{}',
          'planner.problem.state.robot_at.leia.corridor');
INSERT INTO predicate_state (path, pred_name, arg0, arg1)
  VALUES ('planner.problem.state.robot_at.leia.corridor','robot_at','leia','corridor');
COMMIT;
```

**Bulk state load** from sensor data or after replanning:

```sql
-- Efficiently replace all robot_at predicates with new sensor data
BEGIN;

-- Remove all existing robot_at facts
DELETE FROM knowledge_base
WHERE path LIKE 'planner.problem.state.robot_at.%';

-- Insert current known positions
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES
  ('kitchen', '(robot_at leia kitchen)', '{"kind":"state_fact"}', '{}',
   'planner.problem.state.robot_at.leia.kitchen'),
  ('corridor', '(robot_at r2d2 corridor)', '{"kind":"state_fact"}', '{}',
   'planner.problem.state.robot_at.r2d2.corridor');

-- Rebuild predicate_state index
DELETE FROM predicate_state WHERE pred_name = 'robot_at';
INSERT INTO predicate_state (path, pred_name, arg0, arg1)
SELECT path,
       'robot_at',
       -- arg0: 4th path component (after planner.problem.state.robot_at.)
       substr(path, length('planner.problem.state.robot_at.')+1,
              instr(substr(path, length('planner.problem.state.robot_at.')+1),'.') - 1),
       -- arg1: 5th path component
       substr(path, length('planner.problem.state.robot_at.')+1 +
              instr(substr(path, length('planner.problem.state.robot_at.')+1),'.'))
FROM knowledge_base
WHERE path LIKE 'planner.problem.state.robot_at.%';

COMMIT;
```

### 5.3 Numeric Functions

```sql
-- SET battery_level(leia) = 85
INSERT OR REPLACE INTO knowledge_base (label, name, properties, data, path)
VALUES ('leia', 'battery_level(leia)', '{"kind":"function_value"}',
        '{"value":85.0}',
        'planner.problem.functions.battery_level.leia');

-- GET battery_level(leia)
SELECT json_extract(data,'$.value') AS battery_level
FROM knowledge_base
WHERE path = 'planner.problem.functions.battery_level.leia';

-- DECREASE battery_level(leia) by 10
UPDATE knowledge_base
SET data = json_set(data, '$.value',
                   json_extract(data,'$.value') - 10.0)
WHERE path = 'planner.problem.functions.battery_level.leia';

-- ASSIGN battery_level(leia) = 100  (after charging)
UPDATE knowledge_base
SET data = '{"value":100.0}'
WHERE path = 'planner.problem.functions.battery_level.leia';

-- GET all numeric values (for problem.pddl :init)
SELECT
  -- reconstruct "= (func_name arg0) value"
  substr(path, length('planner.problem.functions.')+1) AS func_path,
  json_extract(data,'$.value') AS value
FROM knowledge_base
WHERE path LIKE 'planner.problem.functions.%'
  AND json_extract(properties,'$.kind') = 'function_value'
ORDER BY path;
```

### 5.4 Goal Management

```sql
-- SET goal: (and (robot_at leia kitchen) (battery_full leia))
-- First clear existing goal
DELETE FROM knowledge_base WHERE path LIKE 'planner.problem.goal.%';

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('g000', 'goal 0', '{"kind":"goal"}',
        '{"expr":"(robot_at leia kitchen)","negated":false,"order":0}',
        'planner.problem.goal.g000');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('g001', 'goal 1', '{"kind":"goal"}',
        '{"expr":"(battery_full leia)","negated":false,"order":1}',
        'planner.problem.goal.g001');

-- GET current goal
SELECT json_extract(data,'$.expr') AS goal_clause,
       json_extract(data,'$.negated') AS negated
FROM knowledge_base
WHERE path LIKE 'planner.problem.goal.%'
ORDER BY json_extract(data,'$.order');

-- CHECK if goal is currently satisfied (query against predicate_state)
-- For (robot_at leia kitchen):
SELECT EXISTS(
  SELECT 1 FROM predicate_state
  WHERE pred_name='robot_at' AND arg0='leia' AND arg1='kitchen'
) AS goal_0_satisfied;
```

---

## 6. PDDL Text Generation from the KB

This is the bridge between the structured KB and the external PDDL solver. Both `domain.pddl` and `problem.pddl` are assembled from KB queries at plan time.

### 6.1 Generating domain.pddl

The domain assembler runs these queries and formats the results:

```sql
-- 1. Get domain name and requirements
SELECT json_extract(data,'$.name') AS domain_name,
       json_extract(data,'$.requirements') AS requirements
FROM knowledge_base
WHERE path = 'planner.domain';

-- 2. Get type hierarchy for (:types ...) block
-- Group by parent type for PDDL syntax: "subtype1 subtype2 - parenttype"
SELECT
  json_extract(data,'$.parent') AS parent_type,
  group_concat(label, ' ')       AS subtypes
FROM knowledge_base
WHERE path LIKE 'planner.domain.types.%'
  AND path NOT LIKE 'planner.domain.types.%.%'   -- direct children only
  AND json_extract(data,'$.parent') IS NOT NULL
GROUP BY parent_type;

-- 3. Get predicate declarations for (:predicates ...) block
SELECT
  p.label AS pred_name,
  group_concat(
    json_extract(pp.data,'$.var') || ' - ' || json_extract(pp.data,'$.type'),
    ' '
  ) AS param_str
FROM knowledge_base p
LEFT JOIN knowledge_base pp
  ON pp.path LIKE p.path || '.p%'
WHERE json_extract(p.properties,'$.kind') = 'predicate'
  AND p.path LIKE 'planner.domain.predicates.%'
  AND length(p.path) - length(replace(p.path,'.','')) = 3   -- depth = 4 levels
GROUP BY p.path
ORDER BY p.label;

-- 4. Get function declarations for (:functions ...) block
SELECT
  f.label AS func_name,
  group_concat(
    json_extract(fp.data,'$.var') || ' - ' || json_extract(fp.data,'$.type'),
    ' '
  ) AS param_str
FROM knowledge_base f
LEFT JOIN knowledge_base fp
  ON fp.path LIKE f.path || '.p%'
WHERE json_extract(f.properties,'$.kind') = 'function'
  AND f.path LIKE 'planner.domain.functions.%'
  AND length(f.path) - length(replace(f.path,'.','')) = 3
GROUP BY f.path;

-- 5. Get action schemas
-- For each action, get params, preconditions, effects separately
SELECT label AS action_name,
       json_extract(data,'$.temporal') AS is_temporal
FROM knowledge_base
WHERE json_extract(properties,'$.kind') = 'action'
  AND path LIKE 'planner.domain.actions.%'
ORDER BY label;

-- 5a. Params for action 'move'
SELECT json_extract(data,'$.var')  AS var,
       json_extract(data,'$.type') AS type,
       json_extract(data,'$.position') AS pos
FROM knowledge_base
WHERE path LIKE 'planner.domain.actions.move.p%'
ORDER BY json_extract(data,'$.position');

-- 5b. Preconditions for action 'move'
SELECT json_extract(data,'$.expr')     AS expr,
       json_extract(data,'$.negated')  AS negated,
       json_extract(data,'$.when')     AS temporal_qual
FROM knowledge_base
WHERE path LIKE 'planner.domain.actions.move.pre.%'
ORDER BY json_extract(data,'$.pos');

-- 5c. Effects for action 'move'
SELECT json_extract(data,'$.expr')    AS expr,
       json_extract(data,'$.negated') AS negated,
       json_extract(data,'$.when')    AS temporal_qual
FROM knowledge_base
WHERE path LIKE 'planner.domain.actions.move.eff.%'
ORDER BY json_extract(data,'$.pos');
```

**Lua assembler function sketch:**

```lua
-- assemble_domain_pddl(db) → string
local function assemble_domain_pddl(db)
  local lines = {}
  local meta = kb_get(db, 'planner.domain')
  local name = meta.name
  local reqs = table.concat(meta.requirements, ' ')

  table.insert(lines, string.format('(define (domain %s)', name))
  table.insert(lines, string.format('  (:requirements %s)', reqs))

  -- Types block
  local types = kb_query_children(db, 'planner.domain.types', 'type')
  local type_by_parent = {}
  for _, t in ipairs(types) do
    local p = t.parent or 'object'
    type_by_parent[p] = type_by_parent[p] or {}
    table.insert(type_by_parent[p], t.label)
  end
  table.insert(lines, '  (:types')
  for parent, subtypes in pairs(type_by_parent) do
    table.insert(lines, string.format('    %s - %s',
      table.concat(subtypes, ' '), parent))
  end
  table.insert(lines, '  )')

  -- Predicates, functions, actions assembled similarly...
  -- (full implementation in planner_kb.lua)

  table.insert(lines, ')')
  return table.concat(lines, '\n')
end
```

### 6.2 Generating problem.pddl

```sql
-- 1. Get all objects grouped by type for (:objects ...) block
SELECT json_extract(data,'$.type') AS obj_type,
       group_concat(label, ' ')    AS object_names
FROM knowledge_base
WHERE path LIKE 'planner.problem.objects.%'
  AND json_extract(properties,'$.kind') = 'object'
GROUP BY obj_type;

-- 2. Get ALL true predicates for (:init ...) block
-- Static predicates (map topology, distances) and dynamic state
SELECT pred_name, arg0, arg1, arg2
FROM predicate_state
ORDER BY pred_name, arg0, arg1;

-- 3. Get all numeric values for (:init ...) block
SELECT
  substr(path, length('planner.problem.functions.')+1) AS func_spec,
  json_extract(data,'$.value') AS val
FROM knowledge_base
WHERE path LIKE 'planner.problem.functions.%'
  AND json_extract(properties,'$.kind') = 'function_value';

-- 4. Get goal for (:goal ...) block
SELECT json_extract(data,'$.expr')    AS expr,
       json_extract(data,'$.negated') AS negated
FROM knowledge_base
WHERE path LIKE 'planner.problem.goal.%'
ORDER BY json_extract(data,'$.order');
```

**Assembly example output:**

```lisp
(define (problem simple-robot-problem)
  (:domain simple-robot)
  (:objects
    leia r2d2 - robot
    entrance corridor kitchen chargingroom - room)
  (:init
    (robot_at leia entrance)
    (robot_at r2d2 corridor)
    (connected entrance corridor)
    (connected corridor entrance)
    (connected corridor kitchen)
    (connected corridor chargingroom)
    (connected chargingroom corridor)
    (charging_point_at chargingroom)
    (= (battery_level leia) 85.0)
    (= (battery_level r2d2) 100.0))
  (:goal (and
    (robot_at leia kitchen)
    (battery_full leia))))
```

---

## 7. Plan Storage and Retrieval

### 7.1 Storing the Plan Output

After the external solver returns `plan.pddl`, parse and store each step:

```sql
-- Clear current plan
DELETE FROM knowledge_base WHERE path LIKE 'planner.plan.current.%';

-- Store plan metadata
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('current', 'Current Plan', '{"kind":"plan_meta"}',
        '{"status":"ready","cost":30.0,"generated_at":"2024-03-01T14:30:22Z",
          "solver":"popf","step_count":5}',
        'planner.plan.current');

-- Store each step (zero-padded step index for natural sort)
-- Step 0: (move leia entrance corridor) at t=0.0, dur=5.0
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('s000', 'step 0', '{"kind":"plan_step"}',
        '{"time":0.0,"action":"move","args":["leia","entrance","corridor"],
          "duration":5.0,"status":"pending","index":0}',
        'planner.plan.current.step.s000');

-- Step 1
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('s001', 'step 1', '{"kind":"plan_step"}',
        '{"time":5.001,"action":"move","args":["leia","corridor","chargingroom"],
          "duration":5.0,"status":"pending","index":1}',
        'planner.plan.current.step.s001');

-- Step 2
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('s002', 'step 2', '{"kind":"plan_step"}',
        '{"time":10.002,"action":"charge","args":["leia","chargingroom"],
          "duration":10.0,"status":"pending","index":2}',
        'planner.plan.current.step.s002');
```

**Parser helper** (Lua, reading POPF output):

```lua
-- parse_plan_file(path) → array of step tables
local function parse_plan_file(filepath)
  local steps = {}
  local f = io.open(filepath, 'r')
  if not f then return nil, "cannot open " .. filepath end
  local idx = 0
  for line in f:lines() do
    -- Skip comment lines and blank lines
    if not line:match('^%s*;') and line:match('%S') then
      -- POPF format: "0.000: (move leia entrance corridor)  [5.000]"
      local time, action_str, dur = line:match(
        '^%s*([%d%.]+):%s+%((.-)%)%s+%[([%d%.]+)%]')
      if time then
        local tokens = {}
        for w in action_str:gmatch('%S+') do table.insert(tokens, w) end
        local action = tokens[1]
        local args = {}
        for i = 2, #tokens do table.insert(args, tokens[i]) end
        table.insert(steps, {
          index    = idx,
          time     = tonumber(time),
          action   = action,
          args     = args,
          duration = tonumber(dur),
          status   = 'pending'
        })
        idx = idx + 1
      end
    end
  end
  f:close()
  return steps
end
```

### 7.2 Reading the Plan for Execution

```sql
-- Get all pending steps in execution order
SELECT
  label AS step_id,
  json_extract(data,'$.index')    AS step_index,
  json_extract(data,'$.time')     AS start_time,
  json_extract(data,'$.action')   AS action,
  json_extract(data,'$.args')     AS args_json,
  json_extract(data,'$.duration') AS duration,
  json_extract(data,'$.status')   AS status
FROM knowledge_base
WHERE path LIKE 'planner.plan.current.step.%'
  AND json_extract(data,'$.status') = 'pending'
ORDER BY json_extract(data,'$.index');

-- Get a specific step
SELECT json_extract(data,'$.action') AS action,
       json_extract(data,'$.args')   AS args
FROM knowledge_base
WHERE path = 'planner.plan.current.step.s002';

-- Mark step as executing
UPDATE knowledge_base
SET data = json_set(data, '$.status', 'executing',
                         '$.started_at', datetime('now'))
WHERE path = 'planner.plan.current.step.s002';

-- Mark step as complete (success)
UPDATE knowledge_base
SET data = json_set(data, '$.status', 'success',
                         '$.completed_at', datetime('now'))
WHERE path = 'planner.plan.current.step.s002';

-- Mark step as failed
UPDATE knowledge_base
SET data = json_set(data, '$.status', 'failure',
                         '$.completed_at', datetime('now'),
                         '$.failure_reason', 'navigation_timeout')
WHERE path = 'planner.plan.current.step.s002';

-- Check if plan is complete (all steps success)
SELECT COUNT(*) = 0 AS plan_complete
FROM knowledge_base
WHERE path LIKE 'planner.plan.current.step.%'
  AND json_extract(data,'$.status') != 'success';

-- Check if any step failed
SELECT COUNT(*) > 0 AS plan_failed
FROM knowledge_base
WHERE path LIKE 'planner.plan.current.step.%'
  AND json_extract(data,'$.status') = 'failure';
```

### 7.3 Plan History

```sql
-- Archive current plan before replanning
-- Generate run_id from timestamp: run_20240301_143022
INSERT INTO knowledge_base (label, name, properties, data, path)
SELECT
  'run_' || strftime('%Y%m%d_%H%M%S','now') AS label,
  'Plan Run ' || datetime('now')             AS name,
  properties,
  data,
  replace(path,
          'planner.plan.current',
          'planner.plan.history.run_' || strftime('%Y%m%d_%H%M%S','now'))
FROM knowledge_base
WHERE path LIKE 'planner.plan.current.%';

-- Query plan history
SELECT
  label AS run_id,
  json_extract(data,'$.generated_at') AS generated,
  json_extract(data,'$.cost') AS cost,
  json_extract(data,'$.step_count') AS steps
FROM knowledge_base
WHERE path LIKE 'planner.plan.history.%.step' IS NULL
  AND json_extract(properties,'$.kind') = 'plan_meta'
ORDER BY json_extract(data,'$.generated_at') DESC;
```

---

## 8. Execution Monitor: State Updates

### 8.1 Applying Action Effects

After an action succeeds, apply its declared effects from the domain schema to update the problem state:

```lua
-- apply_action_effects(db, action_name, bound_args)
-- Looks up the action's effect list in the domain KB,
-- substitutes bound arguments, and updates predicate state.
local function apply_action_effects(db, action_name, bound_args)
  -- Get effect rows for this action
  local effects = kb_query_where(db,
    'planner.domain.actions.' .. action_name .. '.eff.%')

  for _, eff in ipairs(effects) do
    local data    = json.decode(eff.data)
    local negated = data.negated
    local when    = data.when     -- "at_end", "at_start"
    local expr    = data.expr     -- "(robot_at ?r ?to)"

    -- Only apply at_end effects after completion
    if when == 'at_end' or when == nil then
      -- Substitute ?var → bound_arg
      -- Parse expr: "(robot_at ?r ?to)" → pred="robot_at", vars=["?r","?to"]
      local pred, vars = parse_predicate_expr(expr)
      local bound = {}
      for i, var in ipairs(vars) do
        -- Look up param name in action's param list to find binding
        local arg_idx = get_param_index(db, action_name, var)
        table.insert(bound, bound_args[arg_idx + 1])
      end

      if not data.numeric then
        -- Boolean predicate effect
        local state_path = 'planner.problem.state.' .. pred
        for _, arg in ipairs(bound) do
          state_path = state_path .. '.' .. arg
        end

        if negated then
          -- Retract
          kb_delete(db, state_path)
        else
          -- Assert
          kb_insert(db, {
            path  = state_path,
            label = bound[#bound],
            name  = '(' .. pred .. ' ' .. table.concat(bound,' ') .. ')',
            properties = '{"kind":"state_fact"}',
            data  = '{}'
          })
        end
      else
        -- Numeric effect: parse and apply
        apply_numeric_effect(db, data.expr, bound_args)
      end
    end
  end
end
```

### 8.2 Replanning Triggers

```sql
-- Signal replan needed
INSERT OR REPLACE INTO knowledge_base (label, name, properties, data, path)
VALUES ('replan_requested', 'Replan Flag', '{"kind":"system_flag"}',
        '{"reason":"action_failed","failed_step":"s003",
          "failed_action":"move","timestamp":"' || datetime('now') || '"}',
        'planner.exec.replan_requested');

-- Check if replan is needed (non-blocking, polled by mission controller)
SELECT EXISTS(
  SELECT 1 FROM knowledge_base
  WHERE path = 'planner.exec.replan_requested'
) AS replan_needed;

-- Clear replan flag after replanning
DELETE FROM knowledge_base WHERE path = 'planner.exec.replan_requested';
```

The mission controller polling loop in Lua:

```lua
local function mission_loop(db, solver_path)
  while not goal_achieved(db) do

    -- Assemble PDDL files
    write_file('/tmp/domain.pddl', assemble_domain_pddl(db))
    write_file('/tmp/problem.pddl', assemble_problem_pddl(db))

    -- Run solver
    local ok = os.execute(solver_path ..
      ' /tmp/domain.pddl /tmp/problem.pddl > /tmp/plan.pddl 2>&1')
    if not ok then
      error('Planner failed — goal may be unreachable')
    end

    -- Store plan
    local steps = parse_plan_file('/tmp/plan.pddl')
    store_plan(db, steps)

    -- Execute plan
    for _, step in ipairs(steps) do
      execute_step(db, step)  -- calls action performer, waits for result

      -- Apply effects on success
      if step.status == 'success' then
        apply_action_effects(db, step.action, step.args)
      end

      -- Check for replan trigger
      if kb_exists(db, 'planner.exec.replan_requested') then
        kb_delete(db, 'planner.exec.replan_requested')
        break  -- Abort current plan, replan from updated state
      end
    end
  end
end
```

---

## 9. LuaJIT FFI Implementation

### 9.1 KB Handle and Core Operations

```lua
-- planner_kb.lua
-- LuaJIT FFI wrapper for planner operations on the SQLite3 KB

local ffi  = require('ffi')
local json = require('dkjson')  -- or cjson

-- Reuse existing KB FFI handle from kb_data_structures.lua
-- (the SQLite3 connection is already open)

local function kb_path_exists(db, path)
  local sql = 'SELECT 1 FROM knowledge_base WHERE path = ? LIMIT 1'
  local stmt = db:prepare(sql)
  stmt:bind(1, path)
  local row = stmt:step()
  stmt:finalize()
  return row ~= nil
end

local function kb_get_data(db, path)
  local sql = 'SELECT data FROM knowledge_base WHERE path = ?'
  local stmt = db:prepare(sql)
  stmt:bind(1, path)
  local row = stmt:step()
  stmt:finalize()
  if row then
    return json.decode(row[1])
  end
  return nil
end

local function kb_insert(db, label, name, properties, data, path)
  local sql = [[
    INSERT OR IGNORE INTO knowledge_base (label, name, properties, data, path)
    VALUES (?, ?, ?, ?, ?)
  ]]
  local stmt = db:prepare(sql)
  stmt:bind(1, label)
  stmt:bind(2, name)
  stmt:bind(3, json.encode(properties))
  stmt:bind(4, json.encode(data))
  stmt:bind(5, path)
  stmt:step()
  stmt:finalize()
end

local function kb_upsert(db, label, name, properties, data, path)
  local sql = [[
    INSERT OR REPLACE INTO knowledge_base (label, name, properties, data, path)
    VALUES (?, ?, ?, ?, ?)
  ]]
  local stmt = db:prepare(sql)
  stmt:bind(1, label)
  stmt:bind(2, name)
  stmt:bind(3, json.encode(properties))
  stmt:bind(4, json.encode(data))
  stmt:bind(5, path)
  stmt:step()
  stmt:finalize()
end

local function kb_delete(db, path)
  local sql = 'DELETE FROM knowledge_base WHERE path = ?'
  local stmt = db:prepare(sql)
  stmt:bind(1, path)
  stmt:step()
  stmt:finalize()
end

local function kb_delete_subtree(db, prefix)
  local sql = "DELETE FROM knowledge_base WHERE path LIKE ?"
  local stmt = db:prepare(sql)
  stmt:bind(1, prefix .. '.%')
  stmt:step()
  stmt:finalize()
  -- Also delete the root node itself
  kb_delete(db, prefix)
end

local function kb_query_subtree(db, prefix)
  local sql = [[
    SELECT label, name, properties, data, path
    FROM knowledge_base
    WHERE path LIKE ?
    ORDER BY path
  ]]
  local stmt = db:prepare(sql)
  stmt:bind(1, prefix .. '.%')
  local rows = {}
  while true do
    local row = stmt:step()
    if not row then break end
    table.insert(rows, {
      label = row[1], name = row[2],
      properties = json.decode(row[3] or '{}'),
      data = json.decode(row[4] or '{}'),
      path = row[5]
    })
  end
  stmt:finalize()
  return rows
end
```

### 9.2 Domain Loader

```lua
-- planner_kb_domain.lua
-- Load a parsed domain structure into the KB

local function load_domain(db, domain)
  -- domain = {
  --   name = "simple-robot",
  --   requirements = {":strips", ":typing", ":durative-actions"},
  --   types = { {name="robot", parent="object"}, ... },
  --   predicates = { {name="robot_at", params=[{var="?r",type="robot"},...], static=false}, ... },
  --   functions  = { {name="battery_level", params=[...], static=false}, ... },
  --   actions    = { {name="move", temporal=true, params=[...], pre=[...], eff=[...]}, ... }
  -- }

  -- Insert domain root
  kb_upsert(db, 'domain', 'Planning Domain',
    {kind='domain_root'},
    {name=domain.name, requirements=domain.requirements},
    'planner.domain')

  -- Types
  kb_upsert(db, 'types', 'Type Hierarchy', {kind='types_root'}, {}, 'planner.domain.types')
  for _, t in ipairs(domain.types) do
    kb_upsert(db, t.name, t.name,
      {kind='type'},
      {parent=t.parent, is_root=(t.parent==nil)},
      'planner.domain.types.' .. t.name)
  end

  -- Predicates
  kb_upsert(db, 'predicates', 'Predicates', {kind='predicates_root'}, {},
    'planner.domain.predicates')
  for _, pred in ipairs(domain.predicates) do
    local ppath = 'planner.domain.predicates.' .. pred.name
    kb_upsert(db, pred.name, pred.name,
      {kind='predicate', arity=#pred.params},
      {static=pred.static or false},
      ppath)
    for i, p in ipairs(pred.params) do
      kb_upsert(db, 'p'..(i-1), 'param '..i,
        {kind='pred_param'},
        {var=p.var, type=p.type, position=i-1},
        ppath..'.p'..(i-1))
    end
  end

  -- Functions
  kb_upsert(db, 'functions', 'Functions', {kind='functions_root'}, {},
    'planner.domain.functions')
  for _, fn in ipairs(domain.functions or {}) do
    local fpath = 'planner.domain.functions.' .. fn.name
    kb_upsert(db, fn.name, fn.name,
      {kind='function', arity=#fn.params},
      {static=fn.static or false},
      fpath)
    for i, p in ipairs(fn.params) do
      kb_upsert(db, 'p'..(i-1), 'param '..i,
        {kind='func_param'},
        {var=p.var, type=p.type, position=i-1},
        fpath..'.p'..(i-1))
    end
  end

  -- Actions
  kb_upsert(db, 'actions', 'Actions', {kind='actions_root'}, {},
    'planner.domain.actions')
  for _, action in ipairs(domain.actions) do
    local apath = 'planner.domain.actions.' .. action.name
    kb_upsert(db, action.name, action.name,
      {kind='action'},
      {temporal=action.temporal or false, cost=action.cost or 1},
      apath)

    -- Parameters
    for i, p in ipairs(action.params) do
      kb_upsert(db, 'p'..(i-1), 'param '..i,
        {kind='action_param'},
        {var=p.var, type=p.type, position=i-1},
        apath..'.p'..(i-1))
    end

    -- Duration (temporal only)
    if action.temporal and action.duration then
      kb_upsert(db, 'duration', 'duration',
        {kind='duration'},
        {expr=action.duration.expr, fixed_value=action.duration.value},
        apath..'.duration')
    end

    -- Preconditions
    for i, c in ipairs(action.preconditions) do
      local ckey = string.format('c%03d', i-1)
      kb_upsert(db, ckey, 'precond '..i,
        {kind='precondition'},
        {expr=c.expr, negated=c.negated or false,
         when=c.when or 'at_start', pos=i-1},
        apath..'.pre.'..ckey)
    end

    -- Effects
    for i, e in ipairs(action.effects) do
      local ekey = string.format('e%03d', i-1)
      kb_upsert(db, ekey, 'effect '..i,
        {kind='effect'},
        {expr=e.expr, negated=e.negated or false,
         when=e.when or 'at_end', numeric=e.numeric or false, pos=i-1},
        apath..'.eff.'..ekey)
    end
  end
end
```

### 9.3 Problem Expert Operations

```lua
-- planner_kb_problem.lua

local M = {}

-- Add typed object
function M.add_object(db, obj_name, obj_type)
  kb_upsert(db, obj_name, obj_name,
    {kind='object'},
    {type=obj_type},
    'planner.problem.objects.' .. obj_name)
end

function M.remove_object(db, obj_name)
  kb_delete(db, 'planner.problem.objects.' .. obj_name)
  -- Cascade: remove all state facts involving this object
  -- (scan predicate_state for arg0/arg1/arg2 = obj_name)
  local sql = [[
    DELETE FROM knowledge_base
    WHERE path LIKE 'planner.problem.state.%'
      AND (path LIKE '%.' || ? || '.%' OR path LIKE '%.' || ?)
  ]]
  local stmt = db:prepare(sql)
  stmt:bind(1, obj_name)
  stmt:bind(2, obj_name)
  stmt:step()
  stmt:finalize()
end

-- Assert predicate (variadic args)
function M.assert_predicate(db, pred_name, ...)
  local args = {...}
  local path = 'planner.problem.state.' .. pred_name
  for _, arg in ipairs(args) do
    path = path .. '.' .. arg
  end
  local display = '(' .. pred_name .. ' ' .. table.concat(args, ' ') .. ')'
  kb_upsert(db, args[#args] or pred_name, display,
    {kind='state_fact'}, {}, path)

  -- Update predicate_state index
  local sql = [[
    INSERT OR REPLACE INTO predicate_state (path, pred_name, arg0, arg1, arg2)
    VALUES (?, ?, ?, ?, ?)
  ]]
  local stmt = db:prepare(sql)
  stmt:bind(1, path)
  stmt:bind(2, pred_name)
  stmt:bind(3, args[1] or nil)
  stmt:bind(4, args[2] or nil)
  stmt:bind(5, args[3] or nil)
  stmt:step()
  stmt:finalize()
end

-- Retract predicate
function M.retract_predicate(db, pred_name, ...)
  local args = {...}
  local path = 'planner.problem.state.' .. pred_name
  for _, arg in ipairs(args) do
    path = path .. '.' .. arg
  end
  kb_delete(db, path)
  -- predicate_state cleaned by trigger
end

-- Check if predicate is true
function M.predicate_true(db, pred_name, ...)
  local args = {...}
  local sql
  if #args == 0 then
    sql = 'SELECT 1 FROM predicate_state WHERE pred_name=? LIMIT 1'
  elseif #args == 1 then
    sql = 'SELECT 1 FROM predicate_state WHERE pred_name=? AND arg0=? LIMIT 1'
  elseif #args == 2 then
    sql = 'SELECT 1 FROM predicate_state WHERE pred_name=? AND arg0=? AND arg1=? LIMIT 1'
  else
    sql = 'SELECT 1 FROM predicate_state WHERE pred_name=? AND arg0=? AND arg1=? AND arg2=? LIMIT 1'
  end
  local stmt = db:prepare(sql)
  stmt:bind(1, pred_name)
  for i, arg in ipairs(args) do stmt:bind(i+1, arg) end
  local row = stmt:step()
  stmt:finalize()
  return row ~= nil
end

-- Set numeric value
function M.set_function(db, func_name, value, ...)
  local args = {...}
  local path = 'planner.problem.functions.' .. func_name
  for _, arg in ipairs(args) do path = path .. '.' .. arg end
  local label = args[#args] or func_name
  local display = func_name .. '(' .. table.concat(args,',') .. ')'
  kb_upsert(db, label, display,
    {kind='function_value'}, {value=value}, path)
end

-- Get numeric value
function M.get_function(db, func_name, ...)
  local args = {...}
  local path = 'planner.problem.functions.' .. func_name
  for _, arg in ipairs(args) do path = path .. '.' .. arg end
  local d = kb_get_data(db, path)
  return d and d.value or nil
end

-- Update numeric value (in-place)
function M.update_function(db, func_name, new_value, ...)
  M.set_function(db, func_name, new_value, ...)
end

-- Set goal
function M.set_goal(db, goal_conjuncts)
  -- goal_conjuncts = {
  --   {expr="(robot_at leia kitchen)", negated=false},
  --   {expr="(battery_full leia)", negated=false}
  -- }
  kb_delete_subtree(db, 'planner.problem.goal')
  for i, g in ipairs(goal_conjuncts) do
    local gkey = string.format('g%03d', i-1)
    kb_upsert(db, gkey, 'goal ' .. i,
      {kind='goal'},
      {expr=g.expr, negated=g.negated or false, order=i-1},
      'planner.problem.goal.' .. gkey)
  end
end

return M
```

### 9.4 Plan Storage and Retrieval

```lua
-- planner_kb_plan.lua

local M = {}

function M.store_plan(db, steps, meta)
  -- Clear current plan
  kb_delete_subtree(db, 'planner.plan.current')

  -- Plan metadata
  kb_upsert(db, 'current', 'Current Plan',
    {kind='plan_meta'},
    {status='ready', cost=meta.cost or 0,
     generated_at=os.date('!%Y-%m-%dT%H:%M:%SZ'),
     solver=meta.solver or 'popf', step_count=#steps},
    'planner.plan.current')

  -- Steps
  for i, step in ipairs(steps) do
    local skey = string.format('s%03d', i-1)
    kb_upsert(db, skey, 'step ' .. i,
      {kind='plan_step'},
      {time=step.time, action=step.action, args=step.args,
       duration=step.duration, status='pending', index=i-1},
      'planner.plan.current.step.' .. skey)
  end
end

function M.get_pending_steps(db)
  local sql = [[
    SELECT data, path
    FROM knowledge_base
    WHERE path LIKE 'planner.plan.current.step.%'
      AND json_extract(data,'$.status') = 'pending'
    ORDER BY json_extract(data,'$.index')
  ]]
  local stmt = db:prepare(sql)
  local steps = {}
  while true do
    local row = stmt:step()
    if not row then break end
    local d = json.decode(row[1])
    d._path = row[2]
    table.insert(steps, d)
  end
  stmt:finalize()
  return steps
end

function M.update_step_status(db, step_path, status, extra)
  local d = kb_get_data(db, step_path) or {}
  d.status = status
  if extra then for k,v in pairs(extra) do d[k]=v end end
  local sql = 'UPDATE knowledge_base SET data=? WHERE path=?'
  local stmt = db:prepare(sql)
  stmt:bind(1, json.encode(d))
  stmt:bind(2, step_path)
  stmt:step()
  stmt:finalize()
end

function M.plan_complete(db)
  local sql = [[
    SELECT COUNT(*) FROM knowledge_base
    WHERE path LIKE 'planner.plan.current.step.%'
      AND json_extract(data,'$.status') != 'success'
  ]]
  local stmt = db:prepare(sql)
  local row = stmt:step()
  stmt:finalize()
  return row and row[1] == 0
end

function M.plan_has_failure(db)
  local sql = [[
    SELECT COUNT(*) FROM knowledge_base
    WHERE path LIKE 'planner.plan.current.step.%'
      AND json_extract(data,'$.status') = 'failure'
  ]]
  local stmt = db:prepare(sql)
  local row = stmt:step()
  stmt:finalize()
  return row and row[1] > 0
end

return M
```

### 9.5 PDDL Text Assembler

```lua
-- planner_pddl_assembler.lua
-- Generates domain.pddl and problem.pddl from KB data

local M = {}

function M.assemble_domain(db)
  local out = {}
  local meta = kb_get_data(db, 'planner.domain')
  local reqs = table.concat(meta.requirements, ' ')

  table.insert(out, '(define (domain ' .. meta.name .. ')')
  table.insert(out, '  (:requirements ' .. reqs .. ')')

  -- Types block
  local type_rows = kb_query_subtree(db, 'planner.domain.types')
  -- Group by parent
  local by_parent = {}
  for _, r in ipairs(type_rows) do
    if r.properties.kind == 'type' and r.data.parent then
      by_parent[r.data.parent] = by_parent[r.data.parent] or {}
      table.insert(by_parent[r.data.parent], r.label)
    end
  end
  table.insert(out, '  (:types')
  for parent, children in pairs(by_parent) do
    table.insert(out, '    ' .. table.concat(children,' ') .. ' - ' .. parent)
  end
  table.insert(out, '  )')

  -- Predicates block
  local pred_rows = kb_query_subtree(db, 'planner.domain.predicates')
  local preds = {}  -- pred_name → {params=[]}
  for _, r in ipairs(pred_rows) do
    if r.properties.kind == 'predicate' then
      preds[r.label] = {params={}}
    elseif r.properties.kind == 'pred_param' then
      local pred_name = r.path:match('planner%.domain%.predicates%.(.-)%.p%d+')
      if preds[pred_name] then
        table.insert(preds[pred_name].params, r.data)
      end
    end
  end
  table.insert(out, '  (:predicates')
  for name, p in pairs(preds) do
    local param_str = ''
    for _, param in ipairs(p.params) do
      param_str = param_str .. ' ' .. param.var .. ' - ' .. param.type
    end
    table.insert(out, '    (' .. name .. param_str .. ')')
  end
  table.insert(out, '  )')

  -- Functions block (if any)
  local func_rows = kb_query_subtree(db, 'planner.domain.functions')
  local funcs = {}
  for _, r in ipairs(func_rows) do
    if r.properties.kind == 'function' then
      funcs[r.label] = {params={}}
    elseif r.properties.kind == 'func_param' then
      local fn = r.path:match('planner%.domain%.functions%.(.-)%.p%d+')
      if funcs[fn] then table.insert(funcs[fn].params, r.data) end
    end
  end
  if next(funcs) then
    table.insert(out, '  (:functions')
    for name, f in pairs(funcs) do
      local ps = ''
      for _, p in ipairs(f.params) do
        ps = ps .. ' ' .. p.var .. ' - ' .. p.type
      end
      table.insert(out, '    (' .. name .. ps .. ')')
    end
    table.insert(out, '  )')
  end

  -- Actions block
  local action_names_rows = kb_query_subtree(db, 'planner.domain.actions')
  -- ... (full action assembly follows same pattern: params → pre → eff)
  -- Abbreviated here; full implementation in planner_pddl_assembler.lua

  table.insert(out, ')')
  return table.concat(out, '\n')
end

function M.assemble_problem(db)
  local out = {}
  local pmeta = kb_get_data(db, 'planner.domain')
  local prob_name = 'current_problem'

  table.insert(out, '(define (problem ' .. prob_name .. ')')
  table.insert(out, '  (:domain ' .. (pmeta and pmeta.name or 'domain') .. ')')

  -- Objects block
  local obj_rows = kb_query_subtree(db, 'planner.problem.objects')
  local by_type = {}
  for _, r in ipairs(obj_rows) do
    if r.properties.kind == 'object' then
      local t = r.data.type
      by_type[t] = by_type[t] or {}
      table.insert(by_type[t], r.label)
    end
  end
  table.insert(out, '  (:objects')
  for type_name, objs in pairs(by_type) do
    table.insert(out, '    ' .. table.concat(objs, ' ') .. ' - ' .. type_name)
  end
  table.insert(out, '  )')

  -- Init block
  table.insert(out, '  (:init')

  -- Boolean facts from predicate_state
  local sql = [[
    SELECT pred_name, arg0, arg1, arg2
    FROM predicate_state
    ORDER BY pred_name, arg0, arg1
  ]]
  local stmt = db:prepare(sql)
  while true do
    local row = stmt:step()
    if not row then break end
    local pred, a0, a1, a2 = row[1], row[2], row[3], row[4]
    local args = {pred}
    if a0 then table.insert(args, a0) end
    if a1 then table.insert(args, a1) end
    if a2 then table.insert(args, a2) end
    table.insert(out, '    (' .. table.concat(args, ' ') .. ')')
  end
  stmt:finalize()

  -- Numeric values
  local fn_rows = kb_query_subtree(db, 'planner.problem.functions')
  for _, r in ipairs(fn_rows) do
    if r.properties.kind == 'function_value' then
      -- path: planner.problem.functions.<func>.<arg>
      local func_path = r.path:gsub('planner%.problem%.functions%.', '')
      -- e.g. "battery_level.leia" → "(battery_level leia)"
      local func_name, args_str = func_path:match('^([^.]+)%.?(.*)$')
      local args = args_str:gsub('%.', ' ')
      local expr = '(= (' .. func_name
      if args ~= '' then expr = expr .. ' ' .. args end
      expr = expr .. ') ' .. tostring(r.data.value) .. ')'
      table.insert(out, '    ' .. expr)
    end
  end
  table.insert(out, '  )')

  -- Goal block
  local goal_rows = kb_query_subtree(db, 'planner.problem.goal')
  table.insert(out, '  (:goal (and')
  for _, r in ipairs(goal_rows) do
    if r.properties.kind == 'goal' then
      local expr = r.data.negated and '(not ' .. r.data.expr .. ')' or r.data.expr
      table.insert(out, '    ' .. expr)
    end
  end
  table.insert(out, '  ))')

  table.insert(out, ')')
  return table.concat(out, '\n')
end

return M
```

---

## 10. Python Construction Layer

Using the existing `Construct_KB` infrastructure for domain loading at startup:

```python
# planner_kb_loader.py
# Extends Construct_KB to load PDDL domain data

from construct_kb_sqlite import Construct_KB
import json

class PlannerKB(Construct_KB):
    """
    Extends Construct_KB to manage PDDL planning data.
    Uses the existing ltree KB infrastructure.
    """

    def __init__(self, db_path, ltree_extension_path=None):
        super().__init__(db_path, 'knowledge_base', ltree_extension_path)
        self._ensure_predicate_state_table()

    def _ensure_predicate_state_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS predicate_state (
                path        TEXT PRIMARY KEY,
                pred_name   TEXT NOT NULL,
                arg0        TEXT,
                arg1        TEXT,
                arg2        TEXT,
                asserted_at TEXT DEFAULT (datetime('now'))
            )
        ''')
        self.cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_ps_pred ON predicate_state(pred_name)')
        self.cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_ps_full ON predicate_state(pred_name,arg0,arg1)')
        self.conn.commit()

    # ── Domain Loading ────────────────────────────────────────────────────

    def load_domain_from_pddl(self, domain_name, pddl_text):
        """Parse and store a PDDL domain string into the KB."""
        # Use an external PDDL parser (pddl library or custom)
        import pddl
        domain = pddl.parse_domain(pddl_text)
        self.load_domain(domain_name, domain)

    def load_domain(self, domain_name, domain_struct):
        """Store a parsed domain structure into the KB."""
        self.push_path('planner')
        self.push_path('domain')
        self._insert_node('domain', domain_name, 'Planning Domain',
                          properties={'kind': 'domain_root'},
                          data={'name': domain_name,
                                'requirements': domain_struct.requirements})
        self._load_types(domain_struct.types)
        self._load_predicates(domain_struct.predicates)
        self._load_functions(domain_struct.functions)
        self._load_actions(domain_struct.actions)
        self.pop_path()  # domain
        self.pop_path()  # planner
        self.conn.commit()

    # ── Problem Management ────────────────────────────────────────────────

    def assert_predicate(self, pred_name, *args):
        """Assert (pred_name arg0 arg1 ...) = TRUE"""
        path = 'planner.problem.state.' + pred_name
        for arg in args:
            path += '.' + arg
        label = args[-1] if args else pred_name
        display = f"({pred_name} {' '.join(args)})"
        self.cursor.execute('''
            INSERT OR IGNORE INTO knowledge_base
            (label, name, properties, data, path)
            VALUES (?, ?, ?, ?, ?)
        ''', (label, display,
              json.dumps({'kind': 'state_fact'}),
              '{}', path))
        # Update predicate_state index
        self.cursor.execute('''
            INSERT OR REPLACE INTO predicate_state
            (path, pred_name, arg0, arg1, arg2)
            VALUES (?, ?, ?, ?, ?)
        ''', (path, pred_name,
              args[0] if len(args) > 0 else None,
              args[1] if len(args) > 1 else None,
              args[2] if len(args) > 2 else None))

    def retract_predicate(self, pred_name, *args):
        """Retract (pred_name arg0 arg1 ...) = FALSE"""
        path = 'planner.problem.state.' + pred_name
        for arg in args:
            path += '.' + arg
        self.cursor.execute(
            'DELETE FROM knowledge_base WHERE path=?', (path,))
        self.cursor.execute(
            'DELETE FROM predicate_state WHERE path=?', (path,))

    def predicate_true(self, pred_name, *args):
        """Check if predicate is currently true."""
        if len(args) == 0:
            self.cursor.execute(
                'SELECT 1 FROM predicate_state WHERE pred_name=? LIMIT 1',
                (pred_name,))
        elif len(args) == 1:
            self.cursor.execute(
                'SELECT 1 FROM predicate_state WHERE pred_name=? AND arg0=? LIMIT 1',
                (pred_name, args[0]))
        elif len(args) == 2:
            self.cursor.execute(
                'SELECT 1 FROM predicate_state WHERE pred_name=? AND arg0=? AND arg1=? LIMIT 1',
                (pred_name, args[0], args[1]))
        return self.cursor.fetchone() is not None

    def set_function(self, func_name, value, *args):
        """Set numeric function value."""
        path = 'planner.problem.functions.' + func_name
        for arg in args:
            path += '.' + arg
        label = args[-1] if args else func_name
        self.cursor.execute('''
            INSERT OR REPLACE INTO knowledge_base
            (label, name, properties, data, path)
            VALUES (?, ?, ?, ?, ?)
        ''', (label, f"{func_name}({','.join(args)})",
              json.dumps({'kind': 'function_value'}),
              json.dumps({'value': value}), path))

    def get_function(self, func_name, *args):
        """Get numeric function value."""
        path = 'planner.problem.functions.' + func_name
        for arg in args:
            path += '.' + arg
        self.cursor.execute(
            "SELECT json_extract(data,'$.value') FROM knowledge_base WHERE path=?",
            (path,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def set_goal(self, goal_exprs):
        """Set the planning goal. goal_exprs = [(expr, negated), ...]"""
        self.cursor.execute(
            "DELETE FROM knowledge_base WHERE path LIKE 'planner.problem.goal.%'")
        for i, (expr, negated) in enumerate(goal_exprs):
            gkey = f'g{i:03d}'
            path = f'planner.problem.goal.{gkey}'
            self.cursor.execute('''
                INSERT INTO knowledge_base
                (label, name, properties, data, path)
                VALUES (?, ?, ?, ?, ?)
            ''', (gkey, f'goal {i}',
                  json.dumps({'kind': 'goal'}),
                  json.dumps({'expr': expr, 'negated': negated, 'order': i}),
                  path))
```

---

## 11. Worked Example: Robot with Battery

### 11.1 Load Domain into KB

```python
kb = PlannerKB('/var/data/robot_kb.sqlite')

# Load domain from PDDL file
with open('simple_robot.pddl') as f:
    kb.load_domain_from_pddl('simple-robot', f.read())

kb.conn.commit()
print("Domain loaded.")
```

This writes the full domain hierarchy under `planner.domain.*`. It runs once at startup.

### 11.2 Set Up Problem Instance

```python
# Add objects
for name, type_ in [('leia','robot'), ('r2d2','robot'),
                     ('entrance','room'), ('corridor','room'),
                     ('kitchen','room'), ('chargingroom','room')]:
    kb.add_object(name, type_)  # → planner.problem.objects.<name>

# Assert initial predicates (world state at startup)
kb.assert_predicate('robot_at', 'leia', 'entrance')
kb.assert_predicate('robot_at', 'r2d2', 'corridor')
# Map topology (static — set once, never retracted)
for a, b in [('entrance','corridor'), ('corridor','entrance'),
             ('corridor','kitchen'), ('kitchen','corridor'),
             ('corridor','chargingroom'), ('chargingroom','corridor')]:
    kb.assert_predicate('connected', a, b)
kb.assert_predicate('charging_point_at', 'chargingroom')

# Initial numeric values
kb.set_function('battery_level', 50.0, 'leia')
kb.set_function('battery_level', 100.0, 'r2d2')

# Set goal
kb.set_goal([
    ('(robot_at leia kitchen)', False),
    ('(battery_full leia)', False)
])

kb.conn.commit()
```

### 11.3 Assemble PDDL, Plan, Store Result

```lua
-- mission_controller.lua
local assembler = require('planner_pddl_assembler')
local plan_kb   = require('planner_kb_plan')

-- Generate PDDL files
local domain_pddl = assembler.assemble_domain(db)
local problem_pddl = assembler.assemble_problem(db)

write_file('/tmp/domain.pddl', domain_pddl)
write_file('/tmp/problem.pddl', problem_pddl)

-- Run POPF
os.execute('popf /tmp/domain.pddl /tmp/problem.pddl > /tmp/plan.pddl 2>&1')

-- Parse and store plan
local steps = parse_plan_file('/tmp/plan.pddl')
-- steps = [
--   {index=0, time=0.0, action="move", args=["leia","entrance","corridor"], duration=5.0},
--   {index=1, time=5.001, action="move", args=["leia","corridor","chargingroom"], duration=5.0},
--   {index=2, time=10.002, action="charge", args=["leia","chargingroom"], duration=10.0},
--   {index=3, time=20.003, action="move", args=["leia","chargingroom","corridor"], duration=5.0},
--   {index=4, time=25.004, action="move", args=["leia","corridor","kitchen"], duration=5.0},
-- ]

plan_kb.store_plan(db, steps, {cost=30.0, solver='popf'})
-- Stored under planner.plan.current.step.s000 ... s004
```

### 11.4 Monitor Execution and Update State

```lua
-- After step s000 (move leia entrance corridor) succeeds:
local problem = require('planner_kb_problem')

problem.retract_predicate(db, 'robot_at', 'leia', 'entrance')
problem.assert_predicate(db,  'robot_at', 'leia', 'corridor')
problem.update_function(db,   'battery_level', 40.0, 'leia')

plan_kb.update_step_status(db, 'planner.plan.current.step.s000',
  'success', {completed_at=os.date('!%Y-%m-%dT%H:%M:%SZ')})

-- Verify: is (robot_at leia corridor) now true?
assert(problem.predicate_true(db, 'robot_at', 'leia', 'corridor'))
assert(not problem.predicate_true(db, 'robot_at', 'leia', 'entrance'))
```

---

## 12. ChainTree S-Expression Integration

The planning KB slots directly into the S-expression engine's `?boolean` function nodes and `@oneshot` nodes:

```lisp
; In the S-expression tree, a condition node checks predicate state:
; (check-preconditions robot_at leia kitchen)

; This binds to a Lua function:
; ?check_preconditions(pred_name, arg0, arg1) → bool
; which calls: problem.predicate_true(db, pred_name, arg0, arg1)
```

The `--monitor` compiler target for the unified DSL emits S-expression code:

```lisp
; Generated by s_compile.lua --monitor for action "move(leia, entrance, corridor)"
(pipeline
  @kb_retract_predicate "robot_at" "leia" "entrance"
  @kb_assert_predicate  "robot_at" "leia" "corridor"
  @kb_decrease_function "battery_level" "leia" 10.0
  ?kb_check_replan_needed)
```

These `@kb_*` and `?kb_*` functions are registered by `planner_kb.lua` as named functions in the S-engine's function table, making the planning KB a first-class citizen of the ChainTree runtime.

---

## 13. Query Reference

Quick reference for the most common operations:

```sql
-- Is predicate (robot_at leia corridor) true?
SELECT 1 FROM predicate_state WHERE pred_name='robot_at' AND arg0='leia' AND arg1='corridor';

-- Where is leia?
SELECT arg1 FROM predicate_state WHERE pred_name='robot_at' AND arg0='leia';

-- All true predicates
SELECT pred_name, arg0, arg1, arg2 FROM predicate_state ORDER BY pred_name;

-- Get battery level
SELECT json_extract(data,'$.value') FROM knowledge_base
WHERE path='planner.problem.functions.battery_level.leia';

-- All current objects
SELECT label, json_extract(data,'$.type') AS type FROM knowledge_base
WHERE path LIKE 'planner.problem.objects.%';

-- Current goal
SELECT json_extract(data,'$.expr') FROM knowledge_base
WHERE path LIKE 'planner.problem.goal.%' ORDER BY json_extract(data,'$.order');

-- Next pending plan step
SELECT json_extract(data,'$.action'), json_extract(data,'$.args')
FROM knowledge_base
WHERE path LIKE 'planner.plan.current.step.%'
  AND json_extract(data,'$.status')='pending'
ORDER BY json_extract(data,'$.index') LIMIT 1;

-- Is plan complete?
SELECT COUNT(*)=0 FROM knowledge_base
WHERE path LIKE 'planner.plan.current.step.%'
  AND json_extract(data,'$.status') != 'success';

-- All action names in domain
SELECT label FROM knowledge_base
WHERE json_extract(properties,'$.kind')='action'
  AND path LIKE 'planner.domain.actions.%';
```

