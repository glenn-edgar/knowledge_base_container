#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_phase1_catalog.lua
--
-- Host-side smoke for Phase 1 of the new ROS planner:
--   C1  action_catalog.lua subsystem  + catalogs/actions.lua
--   C2  infrastructure_registry.lua active-node validator
--   C3  robot_classes.lua subsystem   + catalogs/robot_classes.lua
--
-- Strategy: stub the KB API (with_header / add_info_node /
-- add_status_field) so each subsystem's install_site hook can run
-- without postgres. The sanity-checks fire BEFORE any KB call so the
-- mock just needs to not throw. with_header runs its body closure so
-- the nested emits get exercised too (and so any KB call inside an
-- error-rejected branch would still be reachable for proof).
--
-- Coverage:
--   happy paths
--     - action_catalog with the 3 fixture actions
--     - robot_classes with the 1 fixture class
--     - infrastructure_registry with no active-node defs (today's state)
--   mutation paths (the four kb_build cross-validation guarantees):
--     - action_catalog: invalid wire type in parameter_schema
--     - action_catalog: missing description
--     - infrastructure_registry: def.robot_virtual_action references
--       an action_id NOT in ctx.ACTIONS -> "unknown action_id" error
--     - robot_classes: capability references an action_id NOT in
--       ctx.ACTIONS -> "unknown action_id" error
--     - robot_classes: duplicate capability entry rejected
--
-- Live-cluster build_kb smoke (real pg, all 22 subsystems firing in
-- one transaction) is the user-driven step. See the Phase 1 acceptance
-- recipe at the end of this file.
--
-- Usage:   luajit construction/tests/test_planner_phase1_catalog.lua
-- Exit:    0 = all green; non-zero = at least one failure.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local SUBSYS_DIR = SCRIPT_DIR .. "../subsystems/"
local CATS_DIR   = SCRIPT_DIR .. "../catalogs/"

---------------------------------------------------------------------------
-- assertion helpers
---------------------------------------------------------------------------

local pass, fail = 0, 0

local function check(cond, msg)
  if cond then
    pass = pass + 1
    print("  ok  " .. msg)
  else
    fail = fail + 1
    print("  FAIL " .. msg)
  end
end

-- run install_site, expect SUCCESS; record what KB calls happened
local function run_ok(name, mod, ctx)
  local ok, err = pcall(mod.install_site, ctx)
  if ok then
    pass = pass + 1
    print(string.format("  ok  %s: install_site succeeded", name))
  else
    fail = fail + 1
    print(string.format("  FAIL %s: install_site errored: %s",
      name, tostring(err)))
  end
end

-- run install_site, expect FAILURE matching pattern
local function run_fail(name, mod, ctx, pattern)
  local ok, err = pcall(mod.install_site, ctx)
  if ok then
    fail = fail + 1
    print(string.format(
      "  FAIL %s: install_site should have errored (pattern=%q)",
      name, pattern))
  elseif not tostring(err):find(pattern, 1, true) then
    fail = fail + 1
    print(string.format(
      "  FAIL %s: error %q does not match pattern %q",
      name, tostring(err), pattern))
  else
    pass = pass + 1
    print(string.format(
      "  ok  %s: rejected as expected (%q)", name, pattern))
  end
end

---------------------------------------------------------------------------
-- mock KB
---------------------------------------------------------------------------

local function make_kb()
  local kb = { calls = {} }
  function kb:with_header(label, name, attrs, status, descr, body)
    table.insert(self.calls, { op = "with_header", label = label, name = name })
    if type(body) == "function" then body() end
  end
  function kb:add_info_node(label, name, attrs, payload, descr)
    table.insert(self.calls, { op = "add_info_node", label = label, name = name })
  end
  function kb:add_status_field(name, attrs, descr, value)
    table.insert(self.calls, { op = "add_status_field", name = name })
  end
  function kb:add_doc_class(spec)
    table.insert(self.calls, { op = "add_doc_class" })
  end
  return kb
end

local function load_subsystem(name)
  local chunk, err = loadfile(SUBSYS_DIR .. name .. ".lua")
  if not chunk then error("loadfile " .. name .. ": " .. tostring(err)) end
  return chunk()
end

local function load_catalog(name)
  local chunk, err = loadfile(CATS_DIR .. name .. ".lua")
  if not chunk then error("loadfile " .. name .. ": " .. tostring(err)) end
  return chunk()
end

---------------------------------------------------------------------------
-- fixture loaders
---------------------------------------------------------------------------

local action_catalog        = load_subsystem("action_catalog")
local robot_classes         = load_subsystem("robot_classes")
local infrastructure_registry = load_subsystem("infrastructure_registry")

local FIXTURE_ACTIONS = load_catalog("actions")
local FIXTURE_CLASSES = load_catalog("robot_classes")

---------------------------------------------------------------------------
-- tests
---------------------------------------------------------------------------

print("== fixture catalog shape ==")
check(type(FIXTURE_ACTIONS.recharge) == "table", "actions.recharge present")
check(type(FIXTURE_ACTIONS.dock_in) == "table",  "actions.dock_in present")
check(type(FIXTURE_ACTIONS.dock_out) == "table", "actions.dock_out present")
check(FIXTURE_ACTIONS.recharge.parameter_schema.target_soc == "float",
      "recharge.parameter_schema.target_soc = float")
check(type(FIXTURE_CLASSES.surface_hauler_v2) == "table",
      "robot_classes.surface_hauler_v2 present")

print()
print("== happy: action_catalog with fixture rows ==")
do
  local ctx = { kb = make_kb(), ACTIONS = FIXTURE_ACTIONS }
  run_ok("action_catalog", action_catalog, ctx)
  -- expect 1 outer with_header + 3 add_info_node
  local with_count, info_count = 0, 0
  for _, c in ipairs(ctx.kb.calls) do
    if c.op == "with_header"   then with_count = with_count + 1 end
    if c.op == "add_info_node" then info_count = info_count + 1 end
  end
  check(with_count == 1, "emitted exactly 1 actions.catalog header")
  check(info_count == 3, "emitted 3 action rows")
end

print()
print("== happy: robot_classes with fixture rows ==")
do
  local ctx = {
    kb            = make_kb(),
    ACTIONS       = FIXTURE_ACTIONS,
    ROBOT_CLASSES = FIXTURE_CLASSES,
  }
  run_ok("robot_classes", robot_classes, ctx)
end

print()
print("== happy: infrastructure_registry with NO active-node defs (today) ==")
do
  -- Reuse the real catalogs/definitions.lua so we exercise the real
  -- service_contract walk too. None of those defs declare
  -- robot_virtual_action today; result should be a clean install_site
  -- with the existing infra service rows + zero active_node_def rows.
  local DEFS = load_catalog("definitions")
  local ctx = { kb = make_kb(), DEFINITIONS = DEFS, ACTIONS = FIXTURE_ACTIONS }
  run_ok("infrastructure_registry (no docks)", infrastructure_registry, ctx)
  local active = 0
  for _, c in ipairs(ctx.kb.calls) do
    if c.op == "with_header" and c.label == "active_node_def" then
      active = active + 1
    end
  end
  check(active == 0, "no active_node_def rows emitted (no dock def yet)")
end

print()
print("== happy: infrastructure_registry WITH a fake dock def ==")
do
  local DEFS = {
    fake_dock_v1 = {
      kind                 = "infrastructure",
      runtime              = "docker",
      image                = "fake/dock:latest",
      robot_virtual_action = {
        recharge = {
          cmd_topic    = "dock/{dock_id}/recharge/cmd/{robot_id}",
          status_topic = "dock/{dock_id}/recharge/status/{robot_id}",
        },
        dock_in = {
          cmd_topic    = "dock/{dock_id}/dock_in/cmd/{robot_id}",
          status_topic = "dock/{dock_id}/dock_in/status/{robot_id}",
        },
      },
    },
  }
  local ctx = { kb = make_kb(), DEFINITIONS = DEFS, ACTIONS = FIXTURE_ACTIONS }
  run_ok("infrastructure_registry (1 dock def)", infrastructure_registry, ctx)
  local active, action_rows = 0, 0
  for _, c in ipairs(ctx.kb.calls) do
    if c.op == "with_header" and c.label == "active_node_def" then
      active = active + 1
      check(c.name == "fake_dock_v1", "active_node_def keyed by def_name")
    end
    if c.op == "add_info_node" and c.label == "action" then
      action_rows = action_rows + 1
    end
  end
  check(active == 1, "exactly 1 active_node_def row emitted")
  check(action_rows == 2, "2 action rows under fake_dock_v1 (recharge + dock_in)")
end

print()
print("== mutation: action_catalog rejects bad parameter_schema wire type ==")
do
  local ctx = {
    kb      = make_kb(),
    ACTIONS = {
      recharge = {
        description = "x",
        parameter_schema = { target_soc = "float64" }, -- not in {string,int,float,bool}
      },
    },
  }
  run_fail("bad parameter_schema type", action_catalog, ctx,
           "not in {string,int,float,bool}")
end

print()
print("== mutation: action_catalog rejects missing description ==")
do
  local ctx = {
    kb      = make_kb(),
    ACTIONS = { recharge = { parameter_schema = {} } },
  }
  run_fail("missing description", action_catalog, ctx,
           "description required")
end

print()
print("== mutation: infrastructure_registry rejects unknown action_id ==")
do
  local DEFS = {
    bad_dock = {
      kind                 = "infrastructure",
      runtime              = "docker",
      image                = "x",
      robot_virtual_action = {
        rechrage = { -- typo
          cmd_topic    = "x",
          status_topic = "x",
        },
      },
    },
  }
  local ctx = { kb = make_kb(), DEFINITIONS = DEFS, ACTIONS = FIXTURE_ACTIONS }
  run_fail("dock_dict typo", infrastructure_registry, ctx,
           "unknown action_id")
end

print()
print("== mutation: robot_classes rejects unknown action_id ==")
do
  local ctx = {
    kb            = make_kb(),
    ACTIONS       = FIXTURE_ACTIONS,
    ROBOT_CLASSES = {
      surface_hauler_v2 = {
        description  = "x",
        capabilities = { "recharge", "rechrage" }, -- second is a typo
      },
    },
  }
  run_fail("class capability typo", robot_classes, ctx,
           "unknown action_id")
end

print()
print("== mutation: robot_classes rejects duplicate capability ==")
do
  local ctx = {
    kb            = make_kb(),
    ACTIONS       = FIXTURE_ACTIONS,
    ROBOT_CLASSES = {
      surface_hauler_v2 = {
        description  = "x",
        capabilities = { "recharge", "recharge" },
      },
    },
  }
  run_fail("duplicate capability", robot_classes, ctx,
           "more than once")
end

---------------------------------------------------------------------------
-- summary
---------------------------------------------------------------------------

print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)

-- =============================================================================
-- Live-cluster acceptance recipe (user-driven; per
-- feedback_user_driven_testing.md):
--
--   POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
--       construction/build_kb.sh
--
-- Expected: build prints
--   "sanity passes: ok (cpu count = N)"
--   "=== DCS KB built ===" with subsystems: 22  (was 20 before Phase 1)
-- and exits 0. No service_contract / active_node_def errors.
--
-- After build_kb succeeds, query to confirm catalog rows landed:
--
--   psql ... -c "SELECT path FROM knowledge_base
--                WHERE path LIKE 'system.%.site.%.actions.catalog.%'
--                ORDER BY path;"
--
--   psql ... -c "SELECT path FROM knowledge_base
--                WHERE path LIKE 'system.%.site.%.robot_classes.catalog.%'
--                ORDER BY path;"
-- =============================================================================
