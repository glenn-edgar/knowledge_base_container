# PDDL/KB Planning System: Complete Implementation
## Filling the Gaps — Schema, Assembler, Effects, Validation, and Integration

---

## Table of Contents

1. [Gap 1 — Schema Initialization: The Complete SQL Script](#1-gap-1--schema-initialization)
2. [Gap 2 — Type Inheritance Resolution](#2-gap-2--type-inheritance-resolution)
3. [Gap 3 — Complete PDDL Text Assembler](#3-gap-3--complete-pddl-text-assembler)
4. [Gap 4 — Effect Application Engine](#4-gap-4--effect-application-engine)
5. [Gap 5 — Goal Satisfaction Checker](#5-gap-5--goal-satisfaction-checker)
6. [Gap 6 — KB Validation Before Planning](#6-gap-6--kb-validation-before-planning)
7. [Gap 7 — Replan Policy](#7-gap-7--replan-policy)
8. [Gap 8 — ChainTree S-Expression Function Registration](#8-gap-8--chaintree-s-expression-function-registration)
9. [Gap 9 — Unified DSL --kb Compiler Target](#9-gap-9--unified-dsl---kb-compiler-target)
10. [Gap 10 — Multi-Robot KB Coordination](#10-gap-10--multi-robot-kb-coordination)
11. [Complete Mission Controller: All Gaps Wired Together](#11-complete-mission-controller)

---

## 1. Gap 1 — Schema Initialization

The complete, runnable schema script. Run this once on a fresh database before any other operation.

```sql
-- schema_init.sql
-- Complete schema for the PDDL/ChainTree planning knowledge base.
-- Compatible with SQLite 3.37+ (JSON functions, UPSERT, RETURNING).
-- Run: sqlite3 /var/data/robot_kb.sqlite < schema_init.sql

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

-- ── CORE KNOWLEDGE BASE TABLE ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS knowledge_base (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    label      TEXT    NOT NULL,
    name       TEXT    NOT NULL,
    properties TEXT    NOT NULL DEFAULT '{}',
    data       TEXT    NOT NULL DEFAULT '{}',
    path       TEXT    NOT NULL UNIQUE,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_kb_path
    ON knowledge_base(path);
CREATE INDEX IF NOT EXISTS idx_kb_label
    ON knowledge_base(label);
CREATE INDEX IF NOT EXISTS idx_kb_kind
    ON knowledge_base(json_extract(properties,'$.kind'));
-- Prefix scan index for subtree queries
CREATE INDEX IF NOT EXISTS idx_kb_path_prefix
    ON knowledge_base(substr(path, 1, 40));

-- Auto-update updated_at
CREATE TRIGGER IF NOT EXISTS kb_updated_at
AFTER UPDATE ON knowledge_base
BEGIN
    UPDATE knowledge_base SET updated_at = datetime('now')
    WHERE id = NEW.id;
END;

-- ── PREDICATE STATE SHADOW TABLE ─────────────────────────────────────────
-- Fast O(1) lookup for "is predicate X true right now?"
-- Kept in sync with knowledge_base via triggers.
CREATE TABLE IF NOT EXISTS predicate_state (
    path        TEXT    NOT NULL PRIMARY KEY,
    pred_name   TEXT    NOT NULL,
    arg0        TEXT,
    arg1        TEXT,
    arg2        TEXT,
    asserted_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ps_pred
    ON predicate_state(pred_name);
CREATE INDEX IF NOT EXISTS idx_ps_pred_arg0
    ON predicate_state(pred_name, arg0);
CREATE INDEX IF NOT EXISTS idx_ps_pred_arg01
    ON predicate_state(pred_name, arg0, arg1);
CREATE INDEX IF NOT EXISTS idx_ps_pred_arg012
    ON predicate_state(pred_name, arg0, arg1, arg2);

-- Sync triggers: delete from predicate_state when state facts are removed
CREATE TRIGGER IF NOT EXISTS kb_state_fact_delete
AFTER DELETE ON knowledge_base
WHEN OLD.path LIKE 'planner.problem.state.%'
BEGIN
    DELETE FROM predicate_state WHERE path = OLD.path;
END;

-- ── TYPE INHERITANCE CACHE ────────────────────────────────────────────────
-- Pre-computed type closure: for every type T, which objects satisfy T
-- (directly or via inheritance)?  Rebuilt when domain or objects change.
CREATE TABLE IF NOT EXISTS type_closure (
    type_name   TEXT NOT NULL,
    object_name TEXT NOT NULL,
    PRIMARY KEY (type_name, object_name)
);

CREATE INDEX IF NOT EXISTS idx_tc_type
    ON type_closure(type_name);
CREATE INDEX IF NOT EXISTS idx_tc_object
    ON type_closure(object_name);

-- ── LINKS TABLE (ChainTree tree structure) ───────────────────────────────
-- Existing ChainTree adjacency list.  Planning nodes use the same table.
CREATE TABLE IF NOT EXISTS links_table (
    parent_ltree_name TEXT NOT NULL,
    child_ltree_name  TEXT NOT NULL PRIMARY KEY,
    link_order        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_links_parent
    ON links_table(parent_ltree_name);

-- ── PLAN EXECUTION LOG ───────────────────────────────────────────────────
-- Append-only execution history for diagnostics and replanning context.
CREATE TABLE IF NOT EXISTS execution_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (datetime('now')),
    run_id      TEXT    NOT NULL,
    step_index  INTEGER NOT NULL,
    action      TEXT    NOT NULL,
    args        TEXT    NOT NULL,   -- JSON array
    status      TEXT    NOT NULL,   -- pending/executing/success/failure
    duration_s  REAL,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_execlog_run
    ON execution_log(run_id, step_index);

-- ── REPLAN EVENT TABLE ───────────────────────────────────────────────────
-- Append-only record of every replanning event for diagnostics.
CREATE TABLE IF NOT EXISTS replan_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (datetime('now')),
    trigger     TEXT    NOT NULL,   -- action_failed/obstacle/door_locked/etc
    detail      TEXT,
    prior_run_id TEXT
);

-- ── SCHEMA VERSION ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_meta (key, value)
VALUES ('schema_version', '1'),
       ('created_at',     datetime('now')),
       ('description',    'PDDL/ChainTree planning KB');
```

**LuaJIT initialization helper:**

```lua
-- kb_schema.lua
local M = {}

function M.initialize(db, schema_path)
  -- schema_path: path to schema_init.sql, or nil to use embedded SQL
  local sql
  if schema_path then
    local f = io.open(schema_path, 'r')
    sql = f:read('*a')
    f:close()
  else
    sql = M.EMBEDDED_SCHEMA  -- copy schema SQL inline for embedded targets
  end
  local rc = db:exec(sql)
  assert(rc == 0, 'Schema init failed: ' .. (db:errmsg() or ''))
end

function M.is_initialized(db)
  local rc = false
  for _ in db:nrows(
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='knowledge_base'")
  do rc = true end
  return rc
end

function M.schema_version(db)
  for row in db:nrows(
    "SELECT value FROM schema_meta WHERE key='schema_version'")
  do return tonumber(row.value) end
  return nil
end

return M
```

---

## 2. Gap 2 — Type Inheritance Resolution

Type inheritance is the hidden complexity that makes grounding work correctly. If an action parameter is `?l - location` and `room` is a subtype of `location`, then objects of type `room` satisfy that parameter.

### 2.1 Type Hierarchy Queries

```sql
-- Get all ancestors of type 'room' (the full inheritance chain)
WITH RECURSIVE ancestors(type_name, parent) AS (
    SELECT label, json_extract(data,'$.parent')
    FROM knowledge_base
    WHERE path = 'planner.domain.types.room'

    UNION ALL

    SELECT k.label, json_extract(k.data,'$.parent')
    FROM knowledge_base k
    JOIN ancestors a ON k.label = a.parent
    WHERE a.parent IS NOT NULL
      AND k.path LIKE 'planner.domain.types.%'
)
SELECT type_name FROM ancestors;
-- Returns: room, location, object

-- Get all subtypes of 'location' (direct and transitive)
WITH RECURSIVE subtypes(type_name) AS (
    SELECT label FROM knowledge_base
    WHERE json_extract(data,'$.parent') = 'location'
      AND path LIKE 'planner.domain.types.%'

    UNION ALL

    SELECT k.label
    FROM knowledge_base k
    JOIN subtypes s ON json_extract(k.data,'$.parent') = s.type_name
    WHERE k.path LIKE 'planner.domain.types.%'
)
SELECT type_name FROM subtypes;
-- Returns: room, corridor, dock  (all subtypes of location)
```

### 2.2 Type Closure Cache

Recomputing the transitive type closure on every grounding query is expensive. Build it once when the domain and objects are loaded, and rebuild it when either changes.

```lua
-- type_resolver.lua

local M = {}

-- Build the full type_closure table.
-- For every (type_name, object_name) pair where object satisfies type
-- (directly or through inheritance), insert a row.
function M.rebuild_type_closure(db)
  db:exec('BEGIN')
  db:exec('DELETE FROM type_closure')

  -- Get all objects with their declared types
  local obj_sql = [[
    SELECT label AS obj_name,
           json_extract(data,'$.type') AS obj_type
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.objects.%'
      AND json_extract(properties,'$.kind') = 'object'
  ]]

  -- For each object, walk the type hierarchy upward and insert
  -- (ancestor_type, object_name) for every ancestor
  local ins = db:prepare(
    'INSERT OR IGNORE INTO type_closure (type_name, object_name) VALUES (?,?)')

  for row in db:nrows(obj_sql) do
    local obj  = row.obj_name
    local type = row.obj_type

    -- Insert the direct type
    ins:bind_values(type, obj)
    ins:step(); ins:reset()

    -- Walk ancestors
    local current = type
    while current do
      local parent = nil
      for prow in db:nrows(string.format(
        "SELECT json_extract(data,'$.parent') AS p FROM knowledge_base "..
        "WHERE path='planner.domain.types.%s'", current))
      do parent = prow.p end

      if parent then
        ins:bind_values(parent, obj)
        ins:step(); ins:reset()
      end
      current = parent
    end
  end

  ins:finalize()
  db:exec('COMMIT')
end

-- Get all objects satisfying a type (direct + inherited)
function M.objects_of_type(db, type_name)
  local result = {}
  local stmt = db:prepare(
    'SELECT object_name FROM type_closure WHERE type_name=? ORDER BY object_name')
  stmt:bind_values(type_name)
  while true do
    local row = stmt:step()
    if not row then break end
    table.insert(result, row[1])
  end
  stmt:finalize()
  return result
end

-- Check if an object satisfies a type
function M.object_is_type(db, obj_name, type_name)
  local stmt = db:prepare(
    'SELECT 1 FROM type_closure WHERE type_name=? AND object_name=? LIMIT 1')
  stmt:bind_values(type_name, obj_name)
  local row = stmt:step()
  stmt:finalize()
  return row ~= nil
end

-- Get the declared type of an object
function M.object_type(db, obj_name)
  for row in db:nrows(string.format([[
    SELECT json_extract(data,'$.type') AS t FROM knowledge_base
    WHERE path='planner.problem.objects.%s'
  ]], obj_name)) do
    return row.t
  end
  return nil
end

return M
```

### 2.3 Using the Type Closure

```lua
-- Before planning: rebuild closure if objects changed
type_resolver.rebuild_type_closure(db)

-- Query: which objects can fill a '?l - location' parameter?
local locations = type_resolver.objects_of_type(db, 'location')
-- Returns: entrance, corridor, kitchen, charging (all rooms + any docks)

-- Query: is 'kitchen' a valid room?
assert(type_resolver.object_is_type(db, 'kitchen', 'room'))
assert(type_resolver.object_is_type(db, 'kitchen', 'location'))  -- via inheritance
assert(type_resolver.object_is_type(db, 'kitchen', 'object'))    -- root type
assert(not type_resolver.object_is_type(db, 'kitchen', 'robot'))
```

---

## 3. Gap 3 — Complete PDDL Text Assembler

The full assembler, handling instantaneous and durative actions, numeric fluents, conditional effects, and quantified goals.

```lua
-- pddl_assembler.lua
-- Generates domain.pddl and problem.pddl from KB data.
-- All string building is explicit — no templates, no magic.

local json = require('dkjson')
local M    = {}

-- ── HELPERS ──────────────────────────────────────────────────────────────

local function q(db, sql, ...)  -- query returning rows
  local results = {}
  local stmt = db:prepare(sql)
  if select('#',...) > 0 then stmt:bind_values(...) end
  while true do
    local row = stmt:step()
    if type(row) ~= 'table' then break end
    table.insert(results, row)
  end
  stmt:finalize()
  return results
end

local function q1(db, sql, ...)  -- query returning first row
  local rows = q(db, sql, ...)
  return rows[1]
end

local function get_data(db, path)
  local row = q1(db,
    'SELECT data FROM knowledge_base WHERE path=?', path)
  return row and json.decode(row[1]) or nil
end

local function get_children(db, prefix, kind)
  local sql = [[
    SELECT label, data, path FROM knowledge_base
    WHERE path LIKE ? || '.%'
      AND path NOT LIKE ? || '.%.%'
  ]]
  if kind then
    sql = sql .. string.format(
      " AND json_extract(properties,'$.kind')='%s'", kind)
  end
  sql = sql .. ' ORDER BY path'
  return q(db, sql, prefix, prefix)
end

local function get_params(db, parent_path)
  -- Returns params in position order
  local rows = q(db, [[
    SELECT data FROM knowledge_base
    WHERE path LIKE ? || '.p%'
      AND json_extract(properties,'$.kind') IN ('action_param','pred_param','func_param')
    ORDER BY json_extract(data,'$.position')
  ]], parent_path)
  local params = {}
  for _, r in ipairs(rows) do
    table.insert(params, json.decode(r[1]))
  end
  return params
end

local function params_to_str(params)
  -- [{var="?r", type="robot"}, ...] → "?r - robot ?from - room ?to - room"
  local parts = {}
  for _, p in ipairs(params) do
    table.insert(parts, p.var .. ' - ' .. p.type)
  end
  return table.concat(parts, ' ')
end

local function effect_line(eff, is_temporal)
  -- eff = {expr, negated, when, numeric}
  local e = eff.negated and ('(not ' .. eff.expr .. ')') or eff.expr
  if is_temporal and eff.when then
    local w = eff.when == 'at_start' and 'at start'
           or eff.when == 'at_end'   and 'at end'
           or 'at end'
    e = '(' .. w .. ' ' .. e .. ')'
  end
  return e
end

local function precond_line(pre, is_temporal)
  local e = pre.negated and ('(not ' .. pre.expr .. ')') or pre.expr
  if is_temporal and pre.when then
    local w = pre.when == 'at_start' and 'at start'
           or pre.when == 'over_all'  and 'over all'
           or pre.when == 'at_end'   and 'at end'
           or 'at start'
    e = '(' .. w .. ' ' .. e .. ')'
  end
  return e
end

-- ── DOMAIN ASSEMBLER ─────────────────────────────────────────────────────

function M.assemble_domain(db)
  local out = {}
  local function emit(s) table.insert(out, s) end

  local meta = get_data(db, 'planner.domain')
  assert(meta, 'Domain not loaded in KB')

  emit('(define (domain ' .. meta.name .. ')')
  emit('  (:requirements ' .. table.concat(meta.requirements, ' ') .. ')')

  -- ── TYPES ──────────────────────────────────────────────────────────
  local type_rows = q(db, [[
    SELECT label, data FROM knowledge_base
    WHERE path LIKE 'planner.domain.types.%'
      AND path NOT LIKE 'planner.domain.types.%.%'
      AND json_extract(properties,'$.kind') = 'type'
    ORDER BY path
  ]])

  if #type_rows > 0 then
    -- Group by parent type: "subA subB - parent"
    local by_parent = {}
    local parent_order = {}
    for _, r in ipairs(type_rows) do
      local d = json.decode(r[2])
      if d.parent then
        if not by_parent[d.parent] then
          table.insert(parent_order, d.parent)
          by_parent[d.parent] = {}
        end
        table.insert(by_parent[d.parent], r[1])
      end
    end
    emit('  (:types')
    for _, parent in ipairs(parent_order) do
      emit('    ' .. table.concat(by_parent[parent], ' ') .. ' - ' .. parent)
    end
    emit('  )')
  end

  -- ── PREDICATES ─────────────────────────────────────────────────────
  local pred_rows = q(db, [[
    SELECT label, path FROM knowledge_base
    WHERE path LIKE 'planner.domain.predicates.%'
      AND path NOT LIKE 'planner.domain.predicates.%.%'
      AND json_extract(properties,'$.kind') = 'predicate'
    ORDER BY label
  ]])

  if #pred_rows > 0 then
    emit('  (:predicates')
    for _, r in ipairs(pred_rows) do
      local params = get_params(db, r[2])
      if #params > 0 then
        emit('    (' .. r[1] .. ' ' .. params_to_str(params) .. ')')
      else
        emit('    (' .. r[1] .. ')')
      end
    end
    emit('  )')
  end

  -- ── FUNCTIONS ──────────────────────────────────────────────────────
  local func_rows = q(db, [[
    SELECT label, path FROM knowledge_base
    WHERE path LIKE 'planner.domain.functions.%'
      AND path NOT LIKE 'planner.domain.functions.%.%'
      AND json_extract(properties,'$.kind') = 'function'
    ORDER BY label
  ]])

  if #func_rows > 0 then
    emit('  (:functions')
    for _, r in ipairs(func_rows) do
      local params = get_params(db, r[2])
      if #params > 0 then
        emit('    (' .. r[1] .. ' ' .. params_to_str(params) .. ')')
      else
        emit('    (' .. r[1] .. ')')
      end
    end
    emit('  )')
  end

  -- ── ACTIONS ────────────────────────────────────────────────────────
  local action_rows = q(db, [[
    SELECT label, data, path FROM knowledge_base
    WHERE path LIKE 'planner.domain.actions.%'
      AND path NOT LIKE 'planner.domain.actions.%.%'
      AND json_extract(properties,'$.kind') = 'action'
    ORDER BY label
  ]])

  for _, ar in ipairs(action_rows) do
    local aname    = ar[1]
    local adata    = json.decode(ar[2])
    local apath    = ar[3]
    local temporal = adata.temporal

    -- Parameters
    local params = get_params(db, apath)

    if temporal then
      emit('')
      emit('  (:durative-action ' .. aname)
      emit('    :parameters (' .. params_to_str(params) .. ')')

      -- Duration
      local dur = get_data(db, apath .. '.duration')
      if dur then
        emit('    :duration (' .. dur.expr .. ')')
      end

      -- Conditions (grouped by temporal qualifier)
      local pre_rows = q(db, [[
        SELECT data FROM knowledge_base
        WHERE path LIKE ? || '.pre.%'
        ORDER BY json_extract(data,'$.pos')
      ]], apath)

      if #pre_rows > 0 then
        emit('    :condition (and')
        for _, pr in ipairs(pre_rows) do
          local pre = json.decode(pr[1])
          emit('      ' .. precond_line(pre, true))
        end
        emit('    )')
      end

      -- Effects (grouped by temporal qualifier)
      local eff_rows = q(db, [[
        SELECT data FROM knowledge_base
        WHERE path LIKE ? || '.eff.%'
        ORDER BY json_extract(data,'$.pos')
      ]], apath)

      if #eff_rows > 0 then
        emit('    :effect (and')
        for _, er in ipairs(eff_rows) do
          local eff = json.decode(er[1])
          -- Handle conditional effect: when { condition, effect }
          if eff.condition then
            emit('      (when ' .. eff.condition)
            emit('        ' .. effect_line(eff, true) .. ')')
          else
            emit('      ' .. effect_line(eff, true))
          end
        end
        emit('    )')
      end

      emit('  )')

    else
      -- Instantaneous action
      emit('')
      emit('  (:action ' .. aname)
      emit('    :parameters (' .. params_to_str(params) .. ')')

      local pre_rows = q(db, [[
        SELECT data FROM knowledge_base
        WHERE path LIKE ? || '.pre.%'
        ORDER BY json_extract(data,'$.pos')
      ]], apath)

      if #pre_rows == 1 then
        local pre = json.decode(pre_rows[1][1])
        emit('    :precondition ' .. precond_line(pre, false))
      elseif #pre_rows > 1 then
        emit('    :precondition (and')
        for _, pr in ipairs(pre_rows) do
          local pre = json.decode(pr[1])
          emit('      ' .. precond_line(pre, false))
        end
        emit('    )')
      else
        emit('    :precondition ()')
      end

      local eff_rows = q(db, [[
        SELECT data FROM knowledge_base
        WHERE path LIKE ? || '.eff.%'
        ORDER BY json_extract(data,'$.pos')
      ]], apath)

      if #eff_rows == 1 then
        local eff = json.decode(eff_rows[1][1])
        emit('    :effect ' .. effect_line(eff, false))
      elseif #eff_rows > 1 then
        emit('    :effect (and')
        for _, er in ipairs(eff_rows) do
          local eff = json.decode(er[1])
          emit('      ' .. effect_line(eff, false))
        end
        emit('    )')
      end

      emit('  )')
    end
  end

  emit(')')
  return table.concat(out, '\n')
end

-- ── PROBLEM ASSEMBLER ─────────────────────────────────────────────────────

function M.assemble_problem(db)
  local out = {}
  local function emit(s) table.insert(out, s) end

  local domain_meta = get_data(db, 'planner.domain')
  assert(domain_meta, 'Domain not loaded in KB')

  emit('(define (problem current-problem)')
  emit('  (:domain ' .. domain_meta.name .. ')')

  -- ── OBJECTS ────────────────────────────────────────────────────────
  -- Group by type, resolve via type_closure direct type only
  local obj_rows = q(db, [[
    SELECT label, json_extract(data,'$.type') AS obj_type
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.objects.%'
      AND json_extract(properties,'$.kind') = 'object'
    ORDER BY json_extract(data,'$.type'), label
  ]])

  if #obj_rows > 0 then
    local by_type  = {}
    local type_ord = {}
    for _, r in ipairs(obj_rows) do
      local t = r[2]
      if not by_type[t] then
        table.insert(type_ord, t)
        by_type[t] = {}
      end
      table.insert(by_type[t], r[1])
    end
    emit('  (:objects')
    for _, t in ipairs(type_ord) do
      emit('    ' .. table.concat(by_type[t], ' ') .. ' - ' .. t)
    end
    emit('  )')
  end

  -- ── INIT ───────────────────────────────────────────────────────────
  emit('  (:init')

  -- Boolean predicates from predicate_state
  -- (includes both static map facts + dynamic world state)
  local bool_rows = q(db, [[
    SELECT pred_name, arg0, arg1, arg2
    FROM predicate_state
    ORDER BY pred_name, arg0, arg1, arg2
  ]])
  for _, r in ipairs(bool_rows) do
    local parts = {r[1]}
    if r[2] then table.insert(parts, r[2]) end
    if r[3] then table.insert(parts, r[3]) end
    if r[4] then table.insert(parts, r[4]) end
    emit('    (' .. table.concat(parts, ' ') .. ')')
  end

  -- Numeric function values
  local func_rows = q(db, [[
    SELECT path, label, json_extract(data,'$.value') AS val
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.functions.%'
      AND json_extract(properties,'$.kind') = 'function_value'
    ORDER BY path
  ]])
  for _, r in ipairs(func_rows) do
    -- path = planner.problem.functions.battery_level.leia
    -- → "(= (battery_level leia) 85.0)"
    local suffix = r[1]:sub(#'planner.problem.functions.' + 1)
    -- suffix = "battery_level.leia" → func_name="battery_level", args="leia"
    local dot = suffix:find('%.')
    local func_name, args_str
    if dot then
      func_name = suffix:sub(1, dot-1)
      args_str  = suffix:sub(dot+1):gsub('%.', ' ')
    else
      func_name = suffix
      args_str  = ''
    end
    local func_expr = args_str ~= ''
      and '(' .. func_name .. ' ' .. args_str .. ')'
      or  '(' .. func_name .. ')'
    emit(string.format('    (= %s %g)', func_expr, tonumber(r[3]) or 0))
  end

  emit('  )')

  -- ── GOAL ───────────────────────────────────────────────────────────
  local goal_rows = q(db, [[
    SELECT json_extract(data,'$.expr')    AS expr,
           json_extract(data,'$.negated') AS negated
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
      AND json_extract(properties,'$.kind') = 'goal'
    ORDER BY json_extract(data,'$.order')
  ]])

  if #goal_rows == 0 then
    error('No goal set in KB — cannot assemble problem.pddl')
  elseif #goal_rows == 1 then
    local g = goal_rows[1]
    local expr = (g[2] == 1 or g[2] == true)
      and '(not ' .. g[1] .. ')' or g[1]
    emit('  (:goal ' .. expr .. ')')
  else
    emit('  (:goal (and')
    for _, g in ipairs(goal_rows) do
      local expr = (g[2] == 1 or g[2] == true)
        and '(not ' .. g[1] .. ')' or g[1]
      emit('    ' .. expr)
    end
    emit('  ))')
  end

  -- ── METRIC (if defined) ────────────────────────────────────────────
  local metric = get_data(db, 'planner.problem.metric')
  if metric then
    emit('  (:metric ' .. metric.direction .. ' ' .. metric.expr .. ')')
  end

  emit(')')
  return table.concat(out, '\n')
end

return M
```

---

## 4. Gap 4 — Effect Application Engine

After an action succeeds, the engine reads the action's effect clauses from the domain KB, substitutes the bound arguments for the parameter variables, and applies each effect to the world state.

```lua
-- effect_engine.lua
-- Applies PDDL action effects to the KB world state after execution.

local json          = require('dkjson')
local problem       = require('planner_kb_problem')

local M = {}

-- Build a substitution table from parameter definitions and bound args.
-- params = [{var="?r", type="robot", position=0}, ...]
-- args   = {"leia", "entrance", "corridor"}
-- returns {"?r"="leia", "?from"="entrance", "?to"="corridor"}
local function make_subst(params, args)
  local subst = {}
  for _, p in ipairs(params) do
    subst[p.var] = args[p.position + 1]
  end
  return subst
end

-- Apply substitution to an expression string.
-- "(robot_at ?r ?to)" + {?r="leia", ?to="corridor"} → "(robot_at leia corridor)"
local function substitute(expr, subst)
  return expr:gsub('%?%a[%a%d_-]*', function(var)
    return subst[var] or var
  end)
end

-- Parse a predicate expression into name and arguments.
-- "(robot_at leia corridor)" → "robot_at", {"leia", "corridor"}
-- "(battery_full leia)"      → "battery_full", {"leia"}
-- "(path_clear)"             → "path_clear", {}
local function parse_pred_expr(expr)
  -- Remove outer parens
  local inner = expr:match('^%s*%((.-)%)%s*$')
  if not inner then return nil, {} end
  local tokens = {}
  for t in inner:gmatch('%S+') do table.insert(tokens, t) end
  local name = tokens[1]
  local args = {}
  for i = 2, #tokens do table.insert(args, tokens[i]) end
  return name, args
end

-- Parse a numeric effect expression and apply it.
-- Handles: (decrease (f args) amount)
--          (increase (f args) amount)
--          (assign   (f args) value)
--          (= (f args) value)         -- same as assign
local function apply_numeric_effect(db, expr, subst)
  local bound = substitute(expr, subst)
  -- Remove outer parens
  local inner = bound:match('^%s*%((.-)%)%s*$') or bound

  -- Parse: "decrease (battery_level leia) 10"
  local op = inner:match('^(%a+)')
  if not op then return end

  -- Extract function call: "(battery_level leia)"
  local func_expr = inner:match('%((.-)%)')
  -- Extract amount: last token after the function call
  local rest      = inner:gsub('%(' .. func_expr:gsub('[%(%)%.%+%-%*%?%[%]%%%^%$]','%%%0') .. '%)', '')
  local amount    = tonumber(rest:match('[%-%.%d]+$'))

  if not func_expr then return end

  -- Parse function call: "battery_level leia"
  local func_tokens = {}
  for t in func_expr:gmatch('%S+') do table.insert(func_tokens, t) end
  local func_name = func_tokens[1]
  local func_args = {}
  for i = 2, #func_tokens do table.insert(func_args, func_tokens[i]) end

  local path = 'planner.problem.functions.' .. func_name
  for _, a in ipairs(func_args) do path = path .. '.' .. a end

  -- Read current value
  local current = 0.0
  for row in db:nrows(
    "SELECT json_extract(data,'$.value') FROM knowledge_base WHERE path='" .. path .. "'")
  do current = tonumber(row[1]) or 0.0 end

  local new_val
  op = op:lower()
  if op == 'decrease' then
    new_val = current - (amount or 0)
  elseif op == 'increase' then
    new_val = current + (amount or 0)
  elseif op == 'assign' or op == '=' then
    new_val = amount or 0
  else
    return  -- unknown operator
  end

  -- Write back
  problem.set_function(db, func_name, new_val, table.unpack(func_args))
end

-- ── MAIN ENTRY POINT ─────────────────────────────────────────────────────

-- apply_effects(db, action_name, bound_args, temporal_when)
--   action_name  : "move"
--   bound_args   : {"leia", "entrance", "corridor"}
--   temporal_when: "at_start" | "at_end" | nil (instantaneous)
function M.apply_effects(db, action_name, bound_args, temporal_when)
  -- Get action parameters
  local param_rows = {}
  for row in db:nrows(string.format([[
    SELECT data FROM knowledge_base
    WHERE path LIKE 'planner.domain.actions.%s.p%%'
    ORDER BY json_extract(data,'$.position')
  ]], action_name)) do
    table.insert(param_rows, json.decode(row[1]))
  end
  local subst = make_subst(param_rows, bound_args)

  -- Get effect clauses
  local eff_sql = string.format([[
    SELECT data FROM knowledge_base
    WHERE path LIKE 'planner.domain.actions.%s.eff.%%'
    ORDER BY json_extract(data,'$.pos')
  ]], action_name)

  db:exec('BEGIN')

  for row in db:nrows(eff_sql) do
    local eff = json.decode(row[1])

    -- For temporal actions, only apply effects matching temporal_when
    if temporal_when and eff.when and eff.when ~= temporal_when then
      goto continue
    end

    -- Evaluate conditional effect: check 'when' condition against current state
    if eff.condition then
      local cond_bound = substitute(eff.condition, subst)
      local cname, cargs = parse_pred_expr(cond_bound)
      if cname then
        local cond_true = problem.predicate_true(db, cname,
          table.unpack(cargs))
        if not cond_true then goto continue end
      end
    end

    if eff.numeric then
      -- Numeric effect: decrease/increase/assign a function
      apply_numeric_effect(db, eff.expr, subst)
    else
      -- Boolean predicate effect
      local bound_expr = substitute(eff.expr, subst)
      local pred_name, pred_args = parse_pred_expr(bound_expr)

      if pred_name then
        if eff.negated then
          -- Delete predicate
          problem.retract_predicate(db, pred_name,
            table.unpack(pred_args))
        else
          -- Assert predicate
          problem.assert_predicate(db, pred_name,
            table.unpack(pred_args))
        end
      end
    end

    ::continue::
  end

  db:exec('COMMIT')
end

-- Convenience: apply at_start effects when action begins
function M.apply_at_start_effects(db, action_name, bound_args)
  M.apply_effects(db, action_name, bound_args, 'at_start')
end

-- Convenience: apply at_end effects when action completes
function M.apply_at_end_effects(db, action_name, bound_args)
  M.apply_effects(db, action_name, bound_args, 'at_end')
end

-- Convenience: apply all effects (instantaneous action)
function M.apply_all_effects(db, action_name, bound_args)
  M.apply_effects(db, action_name, bound_args, nil)
end

return M
```

---

## 5. Gap 5 — Goal Satisfaction Checker

Determines whether the current predicate state already satisfies the goal — without invoking the planner.

```lua
-- goal_checker.lua
-- Evaluates goal conjuncts against current predicate/function state.

local json    = require('dkjson')
local problem = require('planner_kb_problem')

local M = {}

-- Parse a goal expression into a checkable form.
-- Handles:
--   "(robot_at leia kitchen)"          → boolean predicate check
--   "(battery_full leia)"              → boolean predicate check
--   "(>= (battery_level leia) 80)"     → numeric comparison
--   "(not (battery_low leia))"         → negated predicate check
local function parse_goal_expr(expr)
  local inner = expr:match('^%s*%((.-)%)%s*$')
  if not inner then return nil end

  -- Negated
  if inner:match('^not%s') then
    local sub = inner:match('^not%s+(.+)$')
    local inner_parsed = parse_goal_expr('(' .. sub .. ')')
    if inner_parsed then
      inner_parsed.negated = true
      return inner_parsed
    end
    return nil
  end

  -- Numeric comparison: (>= (func args) value) or (<= ...)
  local cmp_op, func_call, cmp_val = inner:match(
    '^([<>]=?|=)%s+%((.-)%)%s+([%-%.%d]+)$')
  if cmp_op then
    local func_tokens = {}
    for t in func_call:gmatch('%S+') do table.insert(func_tokens, t) end
    return {
      kind     = 'numeric',
      op       = cmp_op,
      func     = func_tokens[1],
      args     = {table.unpack(func_tokens, 2)},
      value    = tonumber(cmp_val),
      negated  = false,
    }
  end

  -- Boolean predicate: (pred arg0 arg1 ...)
  local tokens = {}
  for t in inner:gmatch('%S+') do table.insert(tokens, t) end
  if #tokens >= 1 then
    return {
      kind    = 'predicate',
      pred    = tokens[1],
      args    = {table.unpack(tokens, 2)},
      negated = false,
    }
  end
  return nil
end

-- Evaluate a single parsed goal expression against current KB state.
local function eval_goal_expr(db, parsed)
  if parsed.kind == 'predicate' then
    local is_true = problem.predicate_true(db, parsed.pred,
      table.unpack(parsed.args))
    return parsed.negated and not is_true or is_true

  elseif parsed.kind == 'numeric' then
    local current = problem.get_function(db, parsed.func,
      table.unpack(parsed.args))
    if current == nil then return false end
    local target = parsed.value
    local op = parsed.op
    local result
    if     op == '>=' then result = current >= target
    elseif op == '<=' then result = current <= target
    elseif op == '>'  then result = current >  target
    elseif op == '<'  then result = current <  target
    elseif op == '='  then result = math.abs(current - target) < 0.001
    else result = false end
    return parsed.negated and not result or result
  end

  return false
end

-- Check if ALL goal conjuncts are currently satisfied.
-- Returns: true/false, plus a table of unsatisfied goal expressions.
function M.goal_achieved(db)
  local goal_rows = {}
  for row in db:nrows([[
    SELECT json_extract(data,'$.expr')    AS expr,
           json_extract(data,'$.negated') AS negated
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
      AND json_extract(properties,'$.kind') = 'goal'
    ORDER BY json_extract(data,'$.order')
  ]]) do
    table.insert(goal_rows, row)
  end

  if #goal_rows == 0 then
    return false, {'no goal defined'}
  end

  local unsatisfied = {}
  for _, g in ipairs(goal_rows) do
    local expr    = g.expr or g[1]
    local negated = (g.negated == 1 or g.negated == true)
    local full    = negated and ('(not ' .. expr .. ')') or expr

    local parsed = parse_goal_expr(full)
    if not parsed then
      table.insert(unsatisfied, full .. ' [unparseable]')
    elseif not eval_goal_expr(db, parsed) then
      table.insert(unsatisfied, full)
    end
  end

  return #unsatisfied == 0, unsatisfied
end

-- Check a single predicate goal directly (convenience)
function M.check_predicate_goal(db, pred_name, ...)
  return problem.predicate_true(db, pred_name, ...)
end

-- Report which goals are satisfied and which are not (debugging)
function M.goal_status(db)
  local status = {}
  for row in db:nrows([[
    SELECT json_extract(data,'$.expr')    AS expr,
           json_extract(data,'$.negated') AS negated,
           json_extract(data,'$.order')   AS ord
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
    ORDER BY json_extract(data,'$.order')
  ]]) do
    local expr    = row.expr or row[1]
    local negated = (row.negated == 1 or row.negated == true)
    local full    = negated and ('(not ' .. expr .. ')') or expr
    local parsed  = parse_goal_expr(full)
    local sat     = parsed and eval_goal_expr(db, parsed) or false
    table.insert(status, {expr=full, satisfied=sat, order=row.ord or row[3]})
  end
  return status
end

return M
```

---

## 6. Gap 6 — KB Validation Before Planning

Run this before assembling PDDL to catch problems that would cause the solver to fail with a cryptic error.

```lua
-- kb_validator.lua
-- Validates KB consistency before invoking the PDDL solver.
-- Returns: ok (bool), errors (string array), warnings (string array)

local json         = require('dkjson')
local type_resolver = require('type_resolver')

local M = {}

function M.validate(db)
  local errors   = {}
  local warnings = {}
  local function err(s) table.insert(errors,   '[ERROR] ' .. s) end
  local function wrn(s) table.insert(warnings, '[WARN]  ' .. s) end

  -- ── 1. DOMAIN LOADED ───────────────────────────────────────────────
  local domain_meta = nil
  for row in db:nrows(
    "SELECT data FROM knowledge_base WHERE path='planner.domain'")
  do domain_meta = json.decode(row[1]) end

  if not domain_meta then
    err('Domain not loaded. Call load_domain() before planning.')
    return false, errors, warnings
  end

  -- ── 2. AT LEAST ONE ACTION ─────────────────────────────────────────
  local action_count = 0
  for _ in db:nrows([[
    SELECT 1 FROM knowledge_base
    WHERE path LIKE 'planner.domain.actions.%'
      AND path NOT LIKE 'planner.domain.actions.%.%'
  ]]) do action_count = action_count + 1 end

  if action_count == 0 then
    err('Domain has no actions defined.')
  end

  -- ── 3. GOAL IS SET ─────────────────────────────────────────────────
  local goal_count = 0
  for _ in db:nrows([[
    SELECT 1 FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
      AND json_extract(properties,'$.kind') = 'goal'
  ]]) do goal_count = goal_count + 1 end

  if goal_count == 0 then
    err('No goal set. Call set_goal() before planning.')
  end

  -- ── 4. GOAL PREDICATES ARE DECLARED IN DOMAIN ──────────────────────
  for row in db:nrows([[
    SELECT json_extract(data,'$.expr') AS expr
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
      AND json_extract(properties,'$.kind') = 'goal'
  ]]) do
    local expr = row.expr or row[1]
    -- Extract predicate name from goal expression (e.g., "robot_at" from "(robot_at ...)")
    local pred_name = expr:match('%((%a[%a%d_-]*)')
    if pred_name then
      local declared = false
      for _ in db:nrows(string.format([[
        SELECT 1 FROM knowledge_base
        WHERE path='planner.domain.predicates.%s'
      ]], pred_name)) do declared = true end
      if not declared then
        err(string.format(
          "Goal predicate '%s' is not declared in domain.", pred_name))
      end
    end
  end

  -- ── 5. GOAL PREDICATES ARE ACHIEVABLE ──────────────────────────────
  -- At least one action must have each goal predicate in its effect list
  for row in db:nrows([[
    SELECT json_extract(data,'$.expr') AS expr
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.goal.%'
      AND json_extract(properties,'$.kind') = 'goal'
  ]]) do
    local expr = row.expr or row[1]
    local pred_name = expr:match('%((%a[%a%d_-]*)')
    if pred_name then
      local achievable = false
      for _ in db:nrows(string.format([[
        SELECT 1 FROM knowledge_base
        WHERE path LIKE 'planner.domain.actions.%%.eff.%%'
          AND json_extract(data,'$.negated') = 0
          AND json_extract(data,'$.expr') LIKE '(%s%%'
      ]], '(' .. pred_name)) do achievable = true end

      -- Also check if it starts in :init
      for _ in db:nrows(string.format(
        "SELECT 1 FROM predicate_state WHERE pred_name='%s' LIMIT 1",
        pred_name)) do achievable = true end

      if not achievable then
        err(string.format(
          "Goal predicate '%s' is never produced by any action and not in init.",
          pred_name))
      end
    end
  end

  -- ── 6. ALL OBJECTS IN PREDICATES ARE DECLARED ──────────────────────
  for row in db:nrows([[
    SELECT pred_name, arg0, arg1, arg2
    FROM predicate_state
    WHERE pred_name NOT IN ('connected','path_blocked','door_locked')
  ]]) do
    local args = {row.arg0, row.arg1, row.arg2}
    for _, arg in ipairs(args) do
      if arg then
        local declared = false
        for _ in db:nrows(string.format([[
          SELECT 1 FROM knowledge_base
          WHERE path='planner.problem.objects.%s'
        ]], arg)) do declared = true end
        if not declared then
          err(string.format(
            "Predicate (%s %s %s) references undeclared object '%s'.",
            row.pred_name or '', row.arg0 or '', row.arg1 or '', arg))
        end
      end
    end
  end

  -- ── 7. TYPE CONSISTENCY ────────────────────────────────────────────
  -- Objects in predicates should satisfy the parameter type
  for row in db:nrows([[
    SELECT p.label AS pred_name, pp.data AS param_data, p.path AS pred_path
    FROM knowledge_base p
    JOIN knowledge_base pp ON pp.path LIKE p.path || '.p%'
    WHERE p.path LIKE 'planner.domain.predicates.%'
      AND p.path NOT LIKE 'planner.domain.predicates.%.%'
      AND json_extract(pp.properties,'$.kind') = 'pred_param'
    ORDER BY p.label, json_extract(pp.data,'$.position')
  ]]) do
    local param = json.decode(row.param_data or '{}')
    local pos   = (param.position or 0)

    -- Check all asserted instances of this predicate
    for inst in db:nrows(string.format(
      "SELECT arg0, arg1, arg2 FROM predicate_state WHERE pred_name='%s'",
      row.pred_name)) do
      local args = {inst.arg0, inst.arg1, inst.arg2}
      local arg  = args[pos + 1]
      if arg and param.type then
        if not type_resolver.object_is_type(db, arg, param.type) then
          err(string.format(
            "Object '%s' in predicate (%s) does not satisfy type '%s'.",
            arg, row.pred_name, param.type))
        end
      end
    end
  end

  -- ── 8. MAP CONNECTIVITY ────────────────────────────────────────────
  -- After projection, check that at least some connected edges exist
  local conn_count = 0
  for _ in db:nrows(
    "SELECT 1 FROM predicate_state WHERE pred_name='connected' LIMIT 1")
  do conn_count = 1 end

  if conn_count == 0 then
    wrn('No (connected ...) predicates in state. Did you run project_map()?')
  end

  -- Check that every waypoint object has at least one outgoing edge
  for row in db:nrows([[
    SELECT object_name FROM type_closure
    WHERE type_name IN (SELECT label FROM knowledge_base
                        WHERE path LIKE 'planner.domain.types.%'
                          AND json_extract(data,'$.parent') IN ('location','room'))
  ]]) do
    local wp = row.object_name
    local has_edge = false
    for _ in db:nrows(string.format([[
      SELECT 1 FROM predicate_state
      WHERE pred_name='connected' AND arg0='%s' LIMIT 1
    ]], wp)) do has_edge = true end
    if not has_edge then
      wrn(string.format("Waypoint/location '%s' has no outgoing connected edges.", wp))
    end
  end

  -- ── 9. NUMERIC FUNCTIONS INITIALIZED ──────────────────────────────
  -- Warn if an action uses a function in its preconditions that has no value
  for row in db:nrows([[
    SELECT DISTINCT
      substr(json_extract(data,'$.expr'), 2,
             instr(json_extract(data,'$.expr'),' ')-2) AS func_name
    FROM knowledge_base
    WHERE path LIKE 'planner.domain.actions.%.pre.%'
      AND json_extract(data,'$.expr') LIKE '(>=%'
       OR json_extract(data,'$.expr') LIKE '(<=%'
       OR json_extract(data,'$.expr') LIKE '(>%'
       OR json_extract(data,'$.expr') LIKE '(<%'
  ]]) do
    -- Check that at least one object has a value for this function
    local func = row.func_name
    if func then
      local has_val = false
      for _ in db:nrows(string.format([[
        SELECT 1 FROM knowledge_base
        WHERE path LIKE 'planner.problem.functions.%s.%%' LIMIT 1
      ]], func)) do has_val = true end
      if not has_val then
        wrn(string.format(
          "Function '%s' used in precondition has no initialized values.", func))
      end
    end
  end

  -- ── 10. DISTANCE/CONNECTIVITY CONSISTENCY ─────────────────────────
  local inconsistent = 0
  for _ in db:nrows([[
    SELECT 1 FROM predicate_state ps
    WHERE ps.pred_name = 'connected'
      AND NOT EXISTS (
        SELECT 1 FROM knowledge_base kb
        WHERE kb.path = 'planner.problem.functions.distance.'
                     || ps.arg0 || '.' || ps.arg1
      )
  ]]) do inconsistent = inconsistent + 1 end

  if inconsistent > 0 then
    err(string.format(
      '%d connected edge(s) have no corresponding distance function value.',
      inconsistent))
  end

  return #errors == 0, errors, warnings
end

return M
```

---

## 7. Gap 7 — Replan Policy

The replan policy decides what happens when execution deviates from the plan. It is a state machine with four states.

```lua
-- replan_policy.lua
-- Defines when and how to replan, retry, or abort.

local M = {}

-- Policy configuration (tune per deployment)
M.config = {
  max_retries_per_action = 2,     -- retry same action this many times before replanning
  max_replans_per_goal   = 5,     -- give up after this many replans on the same goal
  replan_on_path_blocked = true,  -- replan when obstacle blocks an edge
  replan_on_door_locked  = true,  -- replan when a door locks
  retry_delay_ms         = 500,   -- wait between retries (ms)
  replan_delay_ms        = 100,   -- wait before replanning (ms)
}

-- Policy state (per mission run)
local state = {
  retry_count    = {},    -- {step_key → count}
  replan_count   = 0,
  abort_reason   = nil,
}

function M.reset()
  state.retry_count  = {}
  state.replan_count = 0
  state.abort_reason = nil
end

-- ── DECISION FUNCTION ─────────────────────────────────────────────────────

-- Decide what to do after a step fails.
-- Returns one of: "retry", "replan", "abort"
-- plus a reason string.
function M.on_step_failure(db, step, failure_reason)
  local step_key = step.action .. ':' .. table.concat(step.args or {}, ',')

  -- Track retry count for this specific action+args combination
  state.retry_count[step_key] = (state.retry_count[step_key] or 0) + 1

  -- ── Rule 1: Hard abort conditions ──────────────────────────────────
  -- Some failures can never be recovered by replanning.
  if failure_reason:find('hardware_fault') then
    return 'abort', 'hardware fault — operator intervention required'
  end
  if failure_reason:find('emergency_stop') then
    return 'abort', 'emergency stop asserted'
  end

  -- ── Rule 2: Max replans exceeded ───────────────────────────────────
  if state.replan_count >= M.config.max_replans_per_goal then
    return 'abort', string.format(
      'max replans (%d) exceeded for current goal',
      M.config.max_replans_per_goal)
  end

  -- ── Rule 3: Retry before replanning ────────────────────────────────
  -- For transient failures (navigation timeout, communication drop),
  -- retry the same action before replanning.
  local transient = failure_reason:find('timeout')
                 or failure_reason:find('communication')
                 or failure_reason:find('busy')

  if transient and state.retry_count[step_key]
                    <= M.config.max_retries_per_action then
    return 'retry', string.format(
      'transient failure "%s" — retry %d/%d',
      failure_reason,
      state.retry_count[step_key],
      M.config.max_retries_per_action)
  end

  -- ── Rule 4: Replan for structural failures ──────────────────────────
  -- Navigation failures often mean the world changed — replan.
  state.replan_count = state.replan_count + 1
  state.retry_count[step_key] = 0  -- reset retry count for this action

  return 'replan', string.format(
    'action %s failed: %s (replan #%d)',
    step.action, failure_reason, state.replan_count)
end

-- Decide what to do when a passability event fires during execution.
-- Returns: "continue", "replan"
function M.on_passability_change(db, event_type, arg0, arg1)
  if event_type == 'path_blocked' then
    if not M.config.replan_on_path_blocked then
      return 'continue', 'passability change ignored by policy'
    end
    -- Only replan if the blocked edge is on the current plan path
    if M.blocked_edge_is_on_plan(db, arg0, arg1) then
      state.replan_count = state.replan_count + 1
      return 'replan', string.format(
        'path blocked: %s → %s is on active plan', arg0, arg1)
    end
    return 'continue', string.format(
      'path blocked: %s → %s not on active plan, continuing', arg0, arg1)
  end

  if event_type == 'door_locked' then
    if not M.config.replan_on_door_locked then
      return 'continue', 'door event ignored by policy'
    end
    state.replan_count = state.replan_count + 1
    return 'replan', 'door locked: ' .. arg0
  end

  return 'continue', 'unknown event type'
end

-- Check if the blocked edge appears in pending plan steps
function M.blocked_edge_is_on_plan(db, from_wp, to_wp)
  for row in db:nrows([[
    SELECT json_extract(data,'$.action') AS action,
           json_extract(data,'$.args')   AS args
    FROM knowledge_base
    WHERE path LIKE 'planner.plan.current.step.%'
      AND json_extract(data,'$.status') = 'pending'
  ]]) do
    local action = row.action
    local args   = json.decode(row.args or '[]')
    -- Assume move/navigate has from=args[2], to=args[3] (robot is args[1])
    if (action == 'move' or action == 'navigate') then
      if args[2] == from_wp and args[3] == to_wp then
        return true
      end
    end
  end
  return false
end

-- Log a replan event
function M.log_replan(db, trigger, detail, prior_run_id)
  db:exec(string.format([[
    INSERT INTO replan_log (trigger, detail, prior_run_id)
    VALUES ('%s', '%s', '%s')
  ]], trigger:gsub("'","''"),
      (detail or ''):gsub("'","''"),
      (prior_run_id or ''):gsub("'","''")))
end

return M
```

---

## 8. Gap 8 — ChainTree S-Expression Function Registration

The bridge between the S-engine's function dispatch table and the planning KB operations.

```lua
-- planner_sengine_bridge.lua
-- Registers planning KB operations as S-engine callable functions.
--
-- Usage in S-expression trees:
--   ?predicate_true "robot_at" "leia" "corridor"  → boolean
--   @kb_assert      "battery_full" "leia"          → oneshot (void)
--   @kb_retract     "robot_at" "leia" "entrance"   → oneshot (void)
--   !replan_if_needed                              → main (returns control code)

local problem  = require('planner_kb_problem')
local goal_chk = require('goal_checker')
local proj     = require('planner_projection')
local json     = require('dkjson')

local M = {}

-- The KB database handle, set once at startup.
-- All S-engine functions close over this.
local _db = nil

function M.set_db(db)
  _db = db
end

-- ── BOOLEAN FUNCTIONS (?name → bool) ─────────────────────────────────────
-- These are registered as boolean_fns in the S-engine dispatch table.

M.boolean_fns = {

  -- ?predicate_true "pred_name" ["arg0" ["arg1" ["arg2"]]]
  predicate_true = function(pred_name, arg0, arg1, arg2)
    if not _db then return false end
    return problem.predicate_true(_db, pred_name, arg0, arg1, arg2)
  end,

  -- ?goal_achieved → true when all goal conjuncts are satisfied
  goal_achieved = function()
    if not _db then return false end
    local ok, _ = goal_chk.goal_achieved(_db)
    return ok
  end,

  -- ?battery_ok "robot_name" "min_level"
  battery_ok = function(robot, min_level)
    if not _db then return false end
    local level = problem.get_function(_db, 'battery_level', robot)
    return level ~= nil and level >= (tonumber(min_level) or 20)
  end,

  -- ?replan_requested → true if a replan flag is set
  replan_requested = function()
    if not _db then return false end
    return problem.replan_requested(_db)
  end,

  -- ?plan_complete → all plan steps succeeded
  plan_complete = function()
    if not _db then return false end
    local n = 0
    for _ in _db:nrows([[
      SELECT 1 FROM knowledge_base
      WHERE path LIKE 'planner.plan.current.step.%'
        AND json_extract(data,'$.status') != 'success'
      LIMIT 1
    ]]) do n = 1 end
    return n == 0
  end,

  -- ?path_clear "from" "to"
  path_clear = function(from_wp, to_wp)
    if not _db then return false end
    return not problem.predicate_true(_db, 'path_blocked', from_wp, to_wp)
  end,

  -- ?connected "from" "to"
  connected = function(from_wp, to_wp)
    if not _db then return false end
    return problem.predicate_true(_db, 'connected', from_wp, to_wp)
  end,
}

-- ── ONESHOT FUNCTIONS (@name → void) ─────────────────────────────────────
-- These are registered as oneshot_fns in the S-engine dispatch table.

M.oneshot_fns = {

  -- @kb_assert "pred_name" ["arg0" ["arg1" ["arg2"]]]
  kb_assert = function(pred_name, arg0, arg1, arg2)
    if not _db then return end
    local args = {}
    if arg0 then table.insert(args, arg0) end
    if arg1 then table.insert(args, arg1) end
    if arg2 then table.insert(args, arg2) end
    problem.assert_predicate(_db, pred_name, table.unpack(args))
  end,

  -- @kb_retract "pred_name" ["arg0" ["arg1" ["arg2"]]]
  kb_retract = function(pred_name, arg0, arg1, arg2)
    if not _db then return end
    local args = {}
    if arg0 then table.insert(args, arg0) end
    if arg1 then table.insert(args, arg1) end
    if arg2 then table.insert(args, arg2) end
    problem.retract_predicate(_db, pred_name, table.unpack(args))
  end,

  -- @kb_set_function "func_name" value ["arg0" ["arg1"]]
  kb_set_function = function(func_name, value, arg0, arg1)
    if not _db then return end
    local args = {}
    if arg0 then table.insert(args, arg0) end
    if arg1 then table.insert(args, arg1) end
    problem.set_function(_db, func_name, tonumber(value), table.unpack(args))
  end,

  -- @kb_decrease_function "func_name" amount "arg0" ["arg1"]
  kb_decrease_function = function(func_name, amount, arg0, arg1)
    if not _db then return end
    local args = {}
    if arg0 then table.insert(args, arg0) end
    if arg1 then table.insert(args, arg1) end
    local current = problem.get_function(_db, func_name, table.unpack(args))
    if current then
      problem.set_function(_db, func_name,
        current - tonumber(amount), table.unpack(args))
    end
  end,

  -- @kb_project_map → re-run map projection (call after passability changes)
  kb_project_map = function()
    if not _db then return end
    proj.project_map(_db)
  end,

  -- @kb_set_replan_flag "reason"
  kb_set_replan_flag = function(reason)
    if not _db then return end
    problem.set_replan_flag(_db, reason, '')
  end,

  -- @kb_clear_replan_flag
  kb_clear_replan_flag = function()
    if not _db then return end
    problem.clear_replan_flag(_db)
  end,

  -- @kb_log "message"
  kb_log = function(message)
    -- log to execution_log if needed, or just print
    print('[KB] ' .. (message or ''))
  end,
}

-- ── MAIN FUNCTIONS (!name → CFL_CONTINUE/HALT/etc.) ──────────────────────
-- These are registered as main_fns in the S-engine dispatch table.
-- Return values should match the CFL control codes used by the S-engine.

local CFL_CONTINUE  = 0
local CFL_HALT      = 1
local CFL_TERMINATE = 2
local CFL_RESET     = 3

M.main_fns = {

  -- !check_and_replan → CFL_HALT if replan needed, CFL_CONTINUE otherwise
  check_and_replan = function()
    if not _db then return CFL_CONTINUE end
    if problem.replan_requested(_db) then
      return CFL_HALT
    end
    return CFL_CONTINUE
  end,

  -- !wait_for_goal → CFL_CONTINUE until goal achieved, then CFL_TERMINATE
  wait_for_goal = function()
    if not _db then return CFL_CONTINUE end
    local ok, _ = goal_chk.goal_achieved(_db)
    return ok and CFL_TERMINATE or CFL_CONTINUE
  end,
}

-- ── REGISTRATION ─────────────────────────────────────────────────────────

-- Register all functions into an existing S-engine function table.
-- s_engine_fns = {boolean_fns={}, oneshot_fns={}, main_fns={}}
function M.register_into(s_engine_fns)
  for name, fn in pairs(M.boolean_fns) do
    s_engine_fns.boolean_fns[name] = fn
  end
  for name, fn in pairs(M.oneshot_fns) do
    s_engine_fns.oneshot_fns[name] = fn
  end
  for name, fn in pairs(M.main_fns) do
    s_engine_fns.main_fns[name] = fn
  end
end

return M
```

**Usage in an S-expression tree:**

```lisp
; In a ChainTree behavior tree node:
; After move action succeeds — update KB state

(pipeline
  @kb_retract "robot_at" "leia" "entrance"
  @kb_assert  "robot_at" "leia" "corridor"
  @kb_decrease_function "battery_level" "10.0" "leia"
  ?goal_achieved)

; Check before executing a step: is the path clear?
(cond
  (?path_clear "corridor" "kitchen"
    (pipeline
      @kb_log "path clear, proceeding"
      !execute_move_action))
  (else
    (pipeline
      @kb_set_replan_flag "path_blocked_detected"
      !check_and_replan)))
```

---

## 9. Gap 9 — Unified DSL `--kb` Compiler Target

Rather than emitting PDDL text, the `--kb` target writes directly into the KB structure, bypassing the text assembler entirely.

```lua
-- s_compile_kb_target.lua
-- s_compile.lua extension: --kb output target
-- Writes domain structure directly into the ltree KB from the unified DSL AST.

local json = require('dkjson')
local M    = {}

-- Entry point called by s_compile.lua when --kb flag is given
-- ast = parsed unified DSL AST (define-mission form)
-- db  = open SQLite database handle
function M.emit_kb(ast, db)
  assert(ast[1] == 'define-mission',
    '--kb target requires a (define-mission ...) top-level form')

  local mission_name = ast[2]
  local body         = ast

  db:exec('BEGIN')

  -- ── DOMAIN METADATA ────────────────────────────────────────────────
  -- Infer requirements from what the mission uses
  local requirements = M.infer_requirements(ast)
  M.kb_upsert(db, 'domain', 'Planning Domain',
    {kind='domain_root'},
    {name=mission_name, requirements=requirements},
    'planner.domain')

  -- ── SCAN AST FOR ACTION NODES ──────────────────────────────────────
  -- Collect all predicates, types, and functions used across action defs
  local types_seen      = {}   -- {type_name → parent}
  local predicates_seen = {}   -- {pred_name → [param types]}
  local functions_seen  = {}   -- {func_name → [param types]}

  for _, node in ipairs(body) do
    if type(node) == 'table' and node[1] == 'action' then
      M.scan_action_node(node, types_seen, predicates_seen, functions_seen)
    end
  end

  -- ── EMIT TYPES ─────────────────────────────────────────────────────
  M.kb_upsert(db, 'types', 'Type Hierarchy', {kind='types_root'}, {},
    'planner.domain.types')
  M.kb_upsert(db, 'object', 'object', {kind='type'},
    {parent=nil, is_root=true}, 'planner.domain.types.object')

  for type_name, parent in pairs(types_seen) do
    M.kb_upsert(db, type_name, type_name, {kind='type'},
      {parent=parent or 'object', is_root=false},
      'planner.domain.types.' .. type_name)
  end

  -- ── EMIT PREDICATES ────────────────────────────────────────────────
  M.kb_upsert(db, 'predicates', 'Predicates', {kind='predicates_root'}, {},
    'planner.domain.predicates')

  for pred_name, params in pairs(predicates_seen) do
    local ppath = 'planner.domain.predicates.' .. pred_name
    M.kb_upsert(db, pred_name, pred_name,
      {kind='predicate', arity=#params}, {static=false}, ppath)
    for i, p in ipairs(params) do
      M.kb_upsert(db, 'p'..(i-1), 'param '..i,
        {kind='pred_param'},
        {var='?p'..i, type=p, position=i-1},
        ppath..'.p'..(i-1))
    end
  end

  -- ── EMIT FUNCTIONS ─────────────────────────────────────────────────
  if next(functions_seen) then
    M.kb_upsert(db, 'functions', 'Functions', {kind='functions_root'}, {},
      'planner.domain.functions')
    for func_name, params in pairs(functions_seen) do
      local fpath = 'planner.domain.functions.' .. func_name
      M.kb_upsert(db, func_name, func_name,
        {kind='function', arity=#params}, {}, fpath)
      for i, p in ipairs(params) do
        M.kb_upsert(db, 'p'..(i-1), 'param '..i,
          {kind='func_param'},
          {var='?p'..i, type=p, position=i-1},
          fpath..'.p'..(i-1))
      end
    end
  end

  -- ── EMIT ACTIONS ───────────────────────────────────────────────────
  M.kb_upsert(db, 'actions', 'Actions', {kind='actions_root'}, {},
    'planner.domain.actions')

  for _, node in ipairs(body) do
    if type(node) == 'table' and node[1] == 'action' then
      M.emit_action_to_kb(db, node)
    end
  end

  -- ── EMIT INIT (from (init ...) block) ──────────────────────────────
  for _, node in ipairs(body) do
    if type(node) == 'table' and node[1] == 'init' then
      M.emit_init_to_kb(db, node)
    end
  end

  -- ── EMIT OBJECTS (from (objects ...) block) ────────────────────────
  for _, node in ipairs(body) do
    if type(node) == 'table' and node[1] == 'objects' then
      M.emit_objects_to_kb(db, node)
    end
  end

  -- ── EMIT GOAL (from (goal ...) block) ─────────────────────────────
  for _, node in ipairs(body) do
    if type(node) == 'table' and node[1] == 'goal' then
      M.emit_goal_to_kb(db, node)
    end
  end

  db:exec('COMMIT')
  return true
end

-- ── EMIT ACTION ──────────────────────────────────────────────────────────

function M.emit_action_to_kb(db, node)
  -- node = (action name (params...) (require ...) (effect ...) (execute ...))
  local action_name = node[2]
  local params_node = node[3]  -- list of (?var - type) pairs or nil
  local apath = 'planner.domain.actions.' .. action_name

  -- Detect if temporal (has duration in require or explicit :duration)
  local temporal = M.has_temporal_annotations(node)

  M.kb_upsert(db, action_name, action_name,
    {kind='action'},
    {temporal=temporal, cost=1},
    apath)

  -- Parameters
  if params_node and type(params_node) == 'table' then
    -- params_node format: (("?robot" "robot") ("?from" "room") ...)
    for i, p in ipairs(params_node) do
      M.kb_upsert(db, 'p'..(i-1), 'param '..i,
        {kind='action_param'},
        {var=p[1], type=p[2], position=i-1},
        apath..'.p'..(i-1))
    end
  end

  -- Find require and effect sub-nodes
  for _, sub in ipairs(node) do
    if type(sub) == 'table' then
      if sub[1] == 'require' then
        M.emit_preconditions_to_kb(db, apath, sub, temporal)
      elseif sub[1] == 'effect' then
        M.emit_effects_to_kb(db, apath, sub, temporal)
      end
      -- (execute ...) block is stripped — not emitted to domain
    end
  end
end

function M.emit_preconditions_to_kb(db, apath, require_node, temporal)
  local i = 0
  for _, clause in ipairs(require_node) do
    if type(clause) == 'table' and clause[1] ~= 'require' then
      local ckey = string.format('c%03d', i)
      local when, expr, negated = M.parse_temporal_clause(clause, temporal)
      M.kb_upsert(db, ckey, 'precond '..i,
        {kind='precondition'},
        {expr=M.ast_to_pddl(expr), negated=negated,
         when=when, pos=i},
        apath..'.pre.'..ckey)
      i = i + 1
    end
  end
end

function M.emit_effects_to_kb(db, apath, effect_node, temporal)
  local i = 0
  for _, clause in ipairs(effect_node) do
    if type(clause) == 'table' and clause[1] ~= 'effect' then
      local ekey = string.format('e%03d', i)
      local when, expr, negated = M.parse_temporal_clause(clause, temporal)
      local is_numeric = M.is_numeric_expr(expr)
      M.kb_upsert(db, ekey, 'effect '..i,
        {kind='effect'},
        {expr=M.ast_to_pddl(expr), negated=negated,
         when=when, numeric=is_numeric, pos=i},
        apath..'.eff.'..ekey)
      i = i + 1
    end
  end
end

-- ── EMIT INIT ────────────────────────────────────────────────────────────

function M.emit_init_to_kb(db, init_node)
  local problem = require('planner_kb_problem')
  -- init_node = (init (robot_at home) (shark-in-cave) ...)
  for _, fact in ipairs(init_node) do
    if type(fact) == 'table' and fact[1] ~= 'init' then
      local pred_name = fact[1]
      local args = {}
      for i = 2, #fact do table.insert(args, tostring(fact[i])) end
      problem.assert_predicate(db, pred_name, table.unpack(args))
    end
  end
end

-- ── EMIT OBJECTS ─────────────────────────────────────────────────────────

function M.emit_objects_to_kb(db, objects_node)
  -- objects_node = (objects home nursery shark-cave reef-station habitat)
  -- Objects without explicit types default to 'location' in robot domains
  -- Typed format: (objects (home "room") (leia "robot") ...)
  for _, obj in ipairs(objects_node) do
    if type(obj) == 'table' then
      -- Typed: (obj_name "type")
      M.kb_upsert(db, obj[1], obj[1], {kind='object'},
        {type=obj[2] or 'object'},
        'planner.problem.objects.' .. obj[1])
    elseif type(obj) == 'string' and obj ~= 'objects' then
      M.kb_upsert(db, obj, obj, {kind='object'},
        {type='location'},   -- default type
        'planner.problem.objects.' .. obj)
    end
  end
end

-- ── EMIT GOAL ────────────────────────────────────────────────────────────

function M.emit_goal_to_kb(db, goal_node)
  -- Clear existing goal
  db:exec("DELETE FROM knowledge_base WHERE path LIKE 'planner.problem.goal.%'")
  local i = 0
  for _, clause in ipairs(goal_node) do
    if type(clause) == 'table' and clause[1] ~= 'goal' then
      local negated = clause[1] == 'not'
      local expr_node = negated and clause[2] or clause
      local gkey = string.format('g%03d', i)
      M.kb_upsert(db, gkey, 'goal '..i,
        {kind='goal'},
        {expr=M.ast_to_pddl(expr_node), negated=negated, order=i},
        'planner.problem.goal.'..gkey)
      i = i + 1
    end
  end
end

-- ── UTILITIES ────────────────────────────────────────────────────────────

function M.kb_upsert(db, label, name, props, data, path)
  local sql = [[
    INSERT OR REPLACE INTO knowledge_base
    (label, name, properties, data, path)
    VALUES (?, ?, ?, ?, ?)
  ]]
  local stmt = db:prepare(sql)
  stmt:bind_values(label, name, json.encode(props), json.encode(data), path)
  stmt:step()
  stmt:finalize()
end

function M.ast_to_pddl(node)
  -- Convert S-expression AST node back to PDDL string
  if type(node) == 'string'  then return node end
  if type(node) == 'number'  then return tostring(node) end
  if type(node) ~= 'table'   then return tostring(node) end
  local parts = {}
  for _, v in ipairs(node) do
    table.insert(parts, M.ast_to_pddl(v))
  end
  return '(' .. table.concat(parts, ' ') .. ')'
end

function M.is_numeric_expr(node)
  if type(node) ~= 'table' then return false end
  local op = node[1]
  return op == 'decrease' or op == 'increase'
      or op == 'assign'   or op == '='
end

function M.has_temporal_annotations(action_node)
  -- Check if any require/effect clause has at:start/over:all/at:end prefix
  for _, sub in ipairs(action_node) do
    if type(sub) == 'table' and
       (sub[1] == 'require' or sub[1] == 'effect') then
      for _, clause in ipairs(sub) do
        if type(clause) == 'table' then
          local first = clause[1]
          if first == 'at_start' or first == 'over_all'
          or first == 'at_end'   or first == 'at-start'
          or first == 'over-all' or first == 'at-end' then
            return true
          end
        end
      end
    end
  end
  return false
end

function M.parse_temporal_clause(clause, temporal)
  -- clause may be: (at_start (robot_at ?r ?from))
  --             or (robot_at ?r ?from)
  --             or (not (robot_at ?r ?from))
  if not temporal then
    local negated = clause[1] == 'not'
    local expr    = negated and clause[2] or clause
    return nil, expr, negated
  end

  local qualifiers = {at_start=true, over_all=true, at_end=true,
                      ['at-start']=true, ['over-all']=true, ['at-end']=true}

  if qualifiers[clause[1]] then
    local when = clause[1]:gsub('-','_')
    local inner = clause[2]
    local negated = type(inner)=='table' and inner[1]=='not'
    local expr    = negated and inner[2] or inner
    return when, expr, negated
  end

  local negated = clause[1] == 'not'
  local expr    = negated and clause[2] or clause
  return 'at_end', expr, negated  -- default temporal qualifier
end

function M.infer_requirements(ast)
  local reqs = {':strips', ':typing'}
  local has_temporal = false
  local has_numeric  = false

  local function scan(node)
    if type(node) ~= 'table' then return end
    if node[1] == 'action' then
      if M.has_temporal_annotations(node) then has_temporal = true end
    end
    for _, sub in ipairs(node) do
      if type(sub) == 'table' then
        if sub[1] == 'duration' then has_temporal = true end
        if sub[1] == 'decrease' or sub[1] == 'increase' then
          has_numeric = true
        end
        scan(sub)
      end
    end
  end
  scan(ast)

  if has_temporal then
    table.insert(reqs, ':durative-actions')
  end
  if has_numeric then
    table.insert(reqs, ':numeric-fluents')
  end
  return reqs
end

function M.scan_action_node(node, types_seen, predicates_seen, functions_seen)
  -- Scan action params to collect type names
  local params_node = node[3]
  if type(params_node) == 'table' then
    for _, p in ipairs(params_node) do
      if type(p) == 'table' and p[2] then
        types_seen[p[2]] = types_seen[p[2]] or 'object'
      end
    end
  end
  -- Scan require/effect to collect predicate and function names
  local function scan_clause(clause)
    if type(clause) ~= 'table' then return end
    local name = clause[1]
    if type(name) == 'string' and name:match('^[a-zA-Z]') then
      if name ~= 'not' and name ~= 'and' and name ~= 'or'
      and name ~= 'when' and name ~= 'forall' then
        -- Heuristic: if name is 'decrease'/'increase' it's a function effect
        if name == 'decrease' or name == 'increase' or name == 'assign' then
          local fn_call = clause[2]
          if type(fn_call) == 'table' then
            functions_seen[fn_call[1]] = functions_seen[fn_call[1]] or {}
          end
        else
          -- Treat as predicate
          predicates_seen[name] = predicates_seen[name] or {}
        end
      end
    end
    for _, sub in ipairs(clause) do scan_clause(sub) end
  end
  for _, sub in ipairs(node) do
    if type(sub) == 'table' and
       (sub[1] == 'require' or sub[1] == 'effect') then
      for _, clause in ipairs(sub) do scan_clause(clause) end
    end
  end
end

return M
```

---

## 10. Gap 10 — Multi-Robot KB Coordination

Each robot has its own in-memory KB. Shared world state is synchronized via NATS JetStream, which is already ChainTree's messaging infrastructure.

### 10.1 What Needs Coordination

| Data | Scope | Sync needed? |
|---|---|---|
| `planner.domain.*` | Static, identical on all robots | Load from same file — no sync |
| `planner.map.*` | Static, identical on all robots | Load from same file — no sync |
| `planner.problem.objects.*` | Shared world | Sync on change |
| `planner.problem.state.*` (dynamic predicates) | Shared world | Sync on change |
| `planner.problem.functions.*` | Shared world | Sync on change |
| `planner.problem.goal.*` | Per-robot or shared | Depends on architecture |
| `planner.plan.current.*` | Per-robot | No sync |
| `planner.exec.*` | Per-robot | No sync |

### 10.2 NATS-Based State Synchronization

```lua
-- planner_sync.lua
-- Synchronizes shared predicate state between robots via NATS.

local nats = require('nats_pubsub')  -- existing ChainTree NATS FFI module
local json = require('dkjson')
local problem = require('planner_kb_problem')

local M = {}

-- Subject convention:
--   planner.state.assert  → broadcast: "robot A asserted (pred arg0 arg1)"
--   planner.state.retract → broadcast: "robot A retracted (pred arg0 arg1)"
--   planner.state.function → broadcast: "robot A set (func args) = value"

local ASSERT_SUBJ   = 'planner.state.assert'
local RETRACT_SUBJ  = 'planner.state.retract'
local FUNCTION_SUBJ = 'planner.state.function'

-- ── PUBLISHER SIDE ────────────────────────────────────────────────────────

-- Called after assert_predicate — broadcasts to all robots
function M.publish_assert(nc, robot_id, pred_name, ...)
  local args = {...}
  local msg = json.encode({
    source    = robot_id,
    pred_name = pred_name,
    args      = args,
    ts        = os.time(),
  })
  nats.publish(nc, ASSERT_SUBJ, msg)
end

function M.publish_retract(nc, robot_id, pred_name, ...)
  local args = {...}
  local msg = json.encode({
    source    = robot_id,
    pred_name = pred_name,
    args      = args,
    ts        = os.time(),
  })
  nats.publish(nc, RETRACT_SUBJ, msg)
end

function M.publish_function(nc, robot_id, func_name, value, ...)
  local args = {...}
  local msg = json.encode({
    source    = robot_id,
    func_name = func_name,
    args      = args,
    value     = value,
    ts        = os.time(),
  })
  nats.publish(nc, FUNCTION_SUBJ, msg)
end

-- ── SUBSCRIBER SIDE ──────────────────────────────────────────────────────

-- Start background subscription — applies remote state changes to local KB.
-- robot_id: this robot's identifier (ignore own broadcasts)
function M.start_sync_subscriber(nc, db, robot_id)

  -- Assert subscriber
  nats.subscribe(nc, ASSERT_SUBJ, function(msg_data)
    local ok, msg = pcall(json.decode, msg_data)
    if not ok or msg.source == robot_id then return end  -- ignore own messages

    -- Apply remote assertion to local KB
    problem.assert_predicate(db, msg.pred_name,
      table.unpack(msg.args or {}))
  end)

  -- Retract subscriber
  nats.subscribe(nc, RETRACT_SUBJ, function(msg_data)
    local ok, msg = pcall(json.decode, msg_data)
    if not ok or msg.source == robot_id then return end

    problem.retract_predicate(db, msg.pred_name,
      table.unpack(msg.args or {}))
  end)

  -- Function subscriber
  nats.subscribe(nc, FUNCTION_SUBJ, function(msg_data)
    local ok, msg = pcall(json.decode, msg_data)
    if not ok or msg.source == robot_id then return end

    problem.set_function(db, msg.func_name, msg.value,
      table.unpack(msg.args or {}))
  end)
end

-- ── WRAPPED PROBLEM OPERATIONS WITH AUTO-PUBLISH ─────────────────────────

-- Use these wrappers instead of calling problem.* directly
-- when running in multi-robot mode.
function M.make_synced_problem(db, nc, robot_id)
  return {
    assert_predicate = function(pred_name, ...)
      problem.assert_predicate(db, pred_name, ...)
      M.publish_assert(nc, robot_id, pred_name, ...)
    end,
    retract_predicate = function(pred_name, ...)
      problem.retract_predicate(db, pred_name, ...)
      M.publish_retract(nc, robot_id, pred_name, ...)
    end,
    set_function = function(func_name, value, ...)
      problem.set_function(db, func_name, value, ...)
      M.publish_function(nc, robot_id, func_name, value, ...)
    end,
    -- Read-only operations delegate directly to problem module
    predicate_true  = function(...) return problem.predicate_true(db, ...) end,
    get_function    = function(...) return problem.get_function(db, ...) end,
    set_goal        = function(...) return problem.set_goal(db, ...) end,
    replan_requested = function() return problem.replan_requested(db) end,
    set_replan_flag  = function(...) return problem.set_replan_flag(db, ...) end,
    clear_replan_flag = function() return problem.clear_replan_flag(db) end,
  }
end

return M
```

### 10.3 State Snapshot on Join

When a new robot comes online or a robot recovers after a crash, it needs the current world state from peers:

```lua
-- Request full state snapshot from a known peer
local SNAPSHOT_REQ_SUBJ   = 'planner.state.snapshot.request'
local SNAPSHOT_REPLY_SUBJ = 'planner.state.snapshot.reply'

function M.request_state_snapshot(nc, db, robot_id, timeout_ms)
  -- Publish request
  local req = json.encode({source=robot_id, ts=os.time()})
  nats.publish(nc, SNAPSHOT_REQ_SUBJ, req)

  -- Wait for reply (first responder wins)
  local snapshot = nil
  local sub = nats.subscribe_once(nc, SNAPSHOT_REPLY_SUBJ,
    function(msg_data)
      local ok, msg = pcall(json.decode, msg_data)
      if ok and msg.source ~= robot_id then
        snapshot = msg
      end
    end, timeout_ms)

  if snapshot then
    M.apply_snapshot(db, snapshot)
    print(string.format('[sync] Applied state snapshot from %s (%d predicates)',
      snapshot.source, #(snapshot.predicates or {})))
  else
    print('[sync] No snapshot received — starting with local state')
  end
end

function M.serve_snapshots(nc, db, robot_id)
  -- Respond to snapshot requests from other robots
  nats.subscribe(nc, SNAPSHOT_REQ_SUBJ, function(msg_data)
    local ok, req = pcall(json.decode, msg_data)
    if not ok or req.source == robot_id then return end

    -- Build snapshot from current KB state
    local predicates = {}
    for row in db:nrows(
      'SELECT pred_name, arg0, arg1, arg2 FROM predicate_state ORDER BY pred_name')
    do
      table.insert(predicates, {
        pred=row.pred_name, a0=row.arg0, a1=row.arg1, a2=row.arg2})
    end

    local functions = {}
    for row in db:nrows([[
      SELECT path, json_extract(data,'$.value') AS val
      FROM knowledge_base
      WHERE path LIKE 'planner.problem.functions.%'
        AND json_extract(properties,'$.kind') = 'function_value'
    ]]) do
      table.insert(functions, {path=row.path, value=row.val})
    end

    local reply = json.encode({
      source=robot_id, ts=os.time(),
      predicates=predicates, functions=functions,
    })
    nats.publish(nc, SNAPSHOT_REPLY_SUBJ, reply)
  end)
end

function M.apply_snapshot(db, snapshot)
  db:exec('BEGIN')
  -- Clear and rebuild predicate state
  db:exec([[
    DELETE FROM knowledge_base
    WHERE path LIKE 'planner.problem.state.%'
      AND json_extract(properties,'$.kind') = 'state_fact'
  ]])
  db:exec('DELETE FROM predicate_state')

  for _, p in ipairs(snapshot.predicates or {}) do
    problem.assert_predicate(db, p.pred, p.a0, p.a1, p.a2)
  end
  for _, f in ipairs(snapshot.functions or {}) do
    -- path = planner.problem.functions.battery_level.leia
    local suffix = f.path:sub(#'planner.problem.functions.' + 1)
    local dot = suffix:find('%.')
    if dot then
      local func_name = suffix:sub(1, dot-1)
      local args_str  = suffix:sub(dot+1)
      local args = {}
      for a in args_str:gmatch('[^.]+') do table.insert(args, a) end
      problem.set_function(db, func_name, tonumber(f.value),
        table.unpack(args))
    end
  end
  db:exec('COMMIT')
end

return M
```

---

## 11. Complete Mission Controller

Everything wired together into the final mission loop.

```lua
-- mission_controller.lua
-- Complete mission controller using all modules.

local schema      = require('kb_schema')
local memdb       = require('planner_memdb')
local map_loader  = require('planner_map_loader')
local type_res    = require('type_resolver')
local projection  = require('planner_projection')
local validator   = require('kb_validator')
local assembler   = require('pddl_assembler')
local plan_kb     = require('planner_kb_plan')
local problem     = require('planner_kb_problem')
local effects     = require('effect_engine')
local goal_chk    = require('goal_checker')
local policy      = require('replan_policy')
local bridge      = require('planner_sengine_bridge')

local function write_file(path, content)
  local f = assert(io.open(path, 'w'))
  f:write(content)
  f:close()
end

local function parse_plan_file(path)
  local steps = {}
  local f = io.open(path, 'r')
  if not f then return nil end
  local idx = 0
  for line in f:lines() do
    if not line:match('^%s*;') and line:match('%S') then
      local time, action_str, dur = line:match(
        '^%s*([%d%.]+):%s+%((.-)%)%s+%[([%d%.]+)%]')
      if not time then
        -- Try instantaneous format: "(action args)"
        action_str = line:match('^%s*%((.-)%)%s*$')
        time, dur  = '0', '0'
      end
      if action_str then
        local tokens = {}
        for t in action_str:gmatch('%S+') do table.insert(tokens, t) end
        table.insert(steps, {
          index=idx, time=tonumber(time), action=tokens[1],
          args={table.unpack(tokens,2)}, duration=tonumber(dur),
          status='pending'
        })
        idx = idx + 1
      end
    end
  end
  f:close()
  return steps
end

-- ── STARTUP ──────────────────────────────────────────────────────────────

local function startup(disk_path, map_yaml, solver_path)
  -- 1. Initialize schema if new database
  local needs_init = not (io.open(disk_path, 'r'))
  local disk_db = require('lsqlite3').open(disk_path)
  if needs_init then
    schema.initialize(disk_db)
  end
  disk_db:close()

  -- 2. Load disk DB into memory
  local db = memdb.load_to_memory(disk_path)
  schema.initialize(db)  -- ensure schema exists in memory copy

  -- 3. Load map if not already present
  if not (schema.is_initialized(db) and
          db:first_value("SELECT 1 FROM knowledge_base WHERE path='planner.map' LIMIT 1")) then
    print('[startup] Loading map from ' .. map_yaml)
    map_loader.load_yaml(db, map_yaml)
  end

  -- 4. Register S-engine functions
  bridge.set_db(db)

  return db
end

-- ── PLANNING CYCLE ───────────────────────────────────────────────────────

local function planning_cycle(db, solver_path)
  -- 1. Rebuild type closure (objects may have changed)
  type_res.rebuild_type_closure(db)

  -- 2. Project map → PDDL predicates
  local edge_count = projection.project_map(db)
  print(string.format('[plan] %d connected edges projected', edge_count))

  -- 3. Validate before planning
  local ok, errors, warnings = validator.validate(db)
  for _, w in ipairs(warnings) do print(w) end
  if not ok then
    for _, e in ipairs(errors) do print(e) end
    return nil, errors
  end

  -- 4. Assemble PDDL
  local domain_pddl  = assembler.assemble_domain(db)
  local problem_pddl = assembler.assemble_problem(db)
  write_file('/tmp/planner_domain.pddl',  domain_pddl)
  write_file('/tmp/planner_problem.pddl', problem_pddl)

  -- 5. Run solver
  local rc = os.execute(solver_path ..
    ' /tmp/planner_domain.pddl /tmp/planner_problem.pddl'..
    ' > /tmp/planner_plan.pddl 2>/tmp/planner_solver.log')

  if rc ~= 0 then
    local log = io.open('/tmp/planner_solver.log')
    local msg = log and log:read('*a') or 'no log'
    if log then log:close() end
    print('[plan] Solver failed: ' .. msg)
    return nil, {'solver failed: ' .. msg}
  end

  -- 6. Parse and store plan
  local steps = parse_plan_file('/tmp/planner_plan.pddl')
  if not steps or #steps == 0 then
    return nil, {'empty plan — goal may already be satisfied or unreachable'}
  end

  plan_kb.store_plan(db, steps, {
    cost   = 0,
    solver = solver_path
  })

  print(string.format('[plan] Plan found: %d steps', #steps))
  return steps, nil
end

-- ── EXECUTION CYCLE ──────────────────────────────────────────────────────

-- execute_action is provided by the caller (depends on ROS/hardware interface)
-- Signature: execute_action(action_name, args, duration_s) → success, reason

local function execution_cycle(db, steps, execute_action_fn, disk_path)
  for _, step in ipairs(steps) do
    -- Check goal in case it was achieved by a side effect
    local done, _ = goal_chk.goal_achieved(db)
    if done then
      print('[exec] Goal achieved early — stopping execution')
      return 'goal_achieved'
    end

    print(string.format('[exec] Step %d: %s(%s)',
      step.index, step.action, table.concat(step.args or {}, ', ')))

    -- Mark executing
    plan_kb.update_step_status(db, step._path or
      ('planner.plan.current.step.s' .. string.format('%03d', step.index)),
      'executing', {started_at=os.date('!%Y-%m-%dT%H:%M:%SZ')})

    -- Apply at_start effects (temporal)
    effects.apply_at_start_effects(db, step.action, step.args)

    -- Execute
    local success, reason = execute_action_fn(
      step.action, step.args, step.duration)

    if success then
      -- Apply at_end effects
      effects.apply_at_end_effects(db, step.action, step.args)
      plan_kb.update_step_status(db, step._path or
        ('planner.plan.current.step.s' .. string.format('%03d', step.index)),
        'success', {completed_at=os.date('!%Y-%m-%dT%H:%M:%SZ')})

      -- Flush changed state to disk
      memdb.flush_state_changes(db, disk_path)

    else
      plan_kb.update_step_status(db, step._path or
        ('planner.plan.current.step.s' .. string.format('%03d', step.index)),
        'failure', {failure_reason=reason})

      -- Consult replan policy
      local decision, detail = policy.on_step_failure(db, step, reason)
      print(string.format('[exec] Policy decision: %s — %s', decision, detail))

      if decision == 'retry' then
        -- Re-execute the same step
        local s2, r2 = execute_action_fn(
          step.action, step.args, step.duration)
        if s2 then
          effects.apply_at_end_effects(db, step.action, step.args)
          plan_kb.update_step_status(db, step._path or
            ('planner.plan.current.step.s' .. string.format('%03d', step.index)),
            'success')
          memdb.flush_state_changes(db, disk_path)
        else
          return 'replan', 'retry failed: ' .. (r2 or '')
        end

      elseif decision == 'replan' then
        policy.log_replan(db, 'action_failed', detail, nil)
        return 'replan', detail

      elseif decision == 'abort' then
        return 'abort', detail
      end
    end

    -- Check for externally-triggered replan (obstacle sensor etc.)
    if problem.replan_requested(db) then
      local reason_str = problem.replan_reason(db)
      problem.clear_replan_flag(db)
      policy.log_replan(db, 'external', reason_str, nil)
      return 'replan', reason_str
    end

  end -- step loop

  return 'complete', nil
end

-- ── MAIN MISSION LOOP ─────────────────────────────────────────────────────

local function run_mission(disk_path, map_yaml, solver_path,
                           execute_action_fn)
  local db = startup(disk_path, map_yaml, solver_path)
  policy.reset()

  print('[mission] Starting mission loop')

  while true do
    -- Check if goal is already achieved
    local done, unsatisfied = goal_chk.goal_achieved(db)
    if done then
      print('[mission] Goal achieved.')
      memdb.commit_to_disk(db, disk_path)
      return true
    end
    print(string.format('[mission] %d goal clauses unsatisfied', #unsatisfied))

    -- Plan
    local steps, plan_errors = planning_cycle(db, solver_path)
    if not steps then
      print('[mission] Planning failed:')
      for _, e in ipairs(plan_errors or {}) do print('  ' .. e) end
      return false
    end

    -- Execute
    local outcome, detail = execution_cycle(
      db, steps, execute_action_fn, disk_path)

    if outcome == 'complete' then
      -- Verify goal is satisfied (execution completed but check explicitly)
      local satisfied, _ = goal_chk.goal_achieved(db)
      if satisfied then
        print('[mission] Mission complete.')
        memdb.commit_to_disk(db, disk_path)
        return true
      else
        print('[mission] Plan executed but goal not achieved — replanning')
        -- Fall through to replan
      end

    elseif outcome == 'goal_achieved' then
      print('[mission] Goal achieved during execution.')
      memdb.commit_to_disk(db, disk_path)
      return true

    elseif outcome == 'replan' then
      print('[mission] Replanning: ' .. (detail or ''))
      -- Loop back to planning_cycle

    elseif outcome == 'abort' then
      print('[mission] ABORT: ' .. (detail or ''))
      memdb.commit_to_disk(db, disk_path)
      return false
    end

  end -- mission loop
end

return {
  startup          = startup,
  planning_cycle   = planning_cycle,
  execution_cycle  = execution_cycle,
  run_mission      = run_mission,
}
```
