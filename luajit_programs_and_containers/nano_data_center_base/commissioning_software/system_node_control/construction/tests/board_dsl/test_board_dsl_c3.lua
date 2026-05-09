#!/usr/bin/env luajit
-- =============================================================================
-- test_board_dsl_c3.lua -- Phase 4 C3 KB-connected validating compile.
--
-- Strategy: stand up a mock KB conn that has the same surface as
-- DBI-postgres (:prepare/:execute/:fetch). Pre-load it with rows that
-- mimic Phase 1's action_catalog + robot_classes + active_node_def
-- emissions. Then drive b:build({kb_conn = mock, ...}) through the
-- four KB-driven checks plus capability import.
--
-- Coverage:
--   - import_capabilities resolves class -> action_ids
--   - import_capabilities for nonexistent class fails
--   - import_capabilities errors at offline build (no kb_conn)
--   - kb_ref existence: passing + failing
--   - action_id catalog lookup: passing + failing
--   - parameter_schema match: missing field, wrong type, extra field
--   - active-node action presence: passing + failing
--   - schema cache: same action_id used twice = single KB query
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
package.path = SCRIPT_DIR .. "../../scripts/board_dsl/?.lua;" .. package.path

local bd     = require("board_dsl")
local dkjson = require("dkjson")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end
local function expect_error(name, fn, want)
  local good, err = pcall(fn)
  if good then fail = fail + 1; print("  FAIL " .. name .. " -- expected error containing " .. want); return end
  local s = tostring(err)
  if not s:find(want, 1, true) then
    fail = fail + 1; print("  FAIL " .. name .. " -- err " .. s .. " missing " .. want); return
  end
  pass = pass + 1; print("  ok  " .. name)
end

------------------------------------------------------------------------
-- mock KB conn
------------------------------------------------------------------------
-- conn:prepare(sql) -> stmt
-- stmt:execute() -> true
-- stmt:fetch(true) -> row or nil
-- stmt:close() -> nil

local function make_kb(rows, query_log)
  -- rows: map of path (ltree string) -> data table (or nil for absent)
  -- query_log (optional): table where executed paths are appended
  local conn = {}
  function conn:prepare(sql)
    local path = sql:match("path = '(.-)'::ltree")
    local stmt = { sql = sql, path = path, _fetched = false }
    function stmt:execute()
      if query_log then query_log[#query_log + 1] = self.path end
      return true
    end
    function stmt:fetch(_named)
      if self._fetched then return nil end
      self._fetched = true
      local data = rows[self.path]
      if data == nil then return nil end
      -- Live pg returns data::text -> JSON string. The mock returns
      -- the same to exercise decode_json_data() path.
      return { data = dkjson.encode(data) }
    end
    function stmt:close() end
    return stmt
  end
  return conn
end

------------------------------------------------------------------------
print("== fixture KB rows ==")
------------------------------------------------------------------------

local SYS  = "moon_base"
local SITE = "alpha"

local function p_action(id) return string.format(
  "system.%s.site.%s.actions.catalog.action.%s", SYS, SITE, id) end
local function p_class(name) return string.format(
  "system.%s.site.%s.robot_classes.catalog.class.%s", SYS, SITE, name) end

local DOCK_REF = "system.moon_base.site.alpha.infrastructure.registry.active_node_def.dock_recharge_v1"

local fixture_rows = {
  [p_action("recharge")] = {
    description = "x",
    parameter_schema = { target_soc = "float" },
  },
  [p_action("dock_in")] = {
    description = "x", parameter_schema = {},
  },
  [p_action("dock_out")] = {
    description = "x", parameter_schema = {},
  },
  [p_class("surface_hauler_v2")] = {
    description = "x",
    capabilities = { "recharge", "dock_in", "dock_out" },
  },
  [DOCK_REF] = { kind = "active_node" },  -- the def header row
  [DOCK_REF .. ".action.recharge"] = { cmd_topic = "x", status_topic = "y" },
  [DOCK_REF .. ".action.dock_in"]  = { cmd_topic = "x", status_topic = "y" },
  [DOCK_REF .. ".action.dock_out"] = { cmd_topic = "x", status_topic = "y" },
}
ok("fixture has 8 rows", (function()
  local n = 0; for _ in pairs(fixture_rows) do n = n + 1 end; return n == 8
end)())

local REGION = { {x=0,y=0}, {x=10,y=0}, {x=10,y=10}, {x=0,y=10} }

local function fresh_with_dock(declared_caps)
  local b = bd.new{ name = "warehouse_a", region = REGION }
  if declared_caps then b:declare_capabilities(declared_caps) end
  b:add_node{ name = "lander_pad", x = 1, y = 1 }
  b:add_node{ name = "dock_3",     x = 5, y = 5, kb_ref = DOCK_REF }
  return b
end

------------------------------------------------------------------------
print()
print("== happy: all 4 KB checks pass with declared capabilities ==")
------------------------------------------------------------------------

do
  local b = fresh_with_dock{ "recharge" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = {
      bd.straight_line{ end_pos = {x=2, y=2} },
      bd.spline{ end_pos = {x=4, y=4}, end_heading = 0.5 },
      bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                   params = { target_soc = 0.85 } },
    },
  }
  local good, out = pcall(b.build, b,
    { kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE,
      planner_namespace = "test_planner" })
  ok("build with KB succeeded", good, good and "" or tostring(out))
  ok("emitted 2 leaves", good and #out.edges[1].path == 2)
end

------------------------------------------------------------------------
print()
print("== happy: import_capabilities resolves from KB ==")
------------------------------------------------------------------------

do
  local b = fresh_with_dock()
  b:import_capabilities("surface_hauler_v2")
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = {
      bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                   params = { target_soc = 0.85 } },
      bd.activate{ action_id = "dock_in", kb_ref = DOCK_REF },
    },
  }
  local good, out = pcall(b.build, b,
    { kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE,
      planner_namespace = "test_planner" })
  ok("build with import_capabilities succeeded", good,
     good and "" or tostring(out))
  -- the imported caps should now appear in the canonical capabilities list
  if good then
    local has_recharge, has_dock_in = false, false
    for _, c in ipairs(out.capabilities) do
      if c == "recharge" then has_recharge = true end
      if c == "dock_in"  then has_dock_in  = true end
    end
    ok("imported recharge", has_recharge)
    ok("imported dock_in",  has_dock_in)
  end
end

expect_error("import_capabilities offline (no kb_conn) errors", function()
  local b = fresh_with_dock()
  b:import_capabilities("surface_hauler_v2")
  b:build()
end, "requires KB at build time")

expect_error("import_capabilities for unknown class", function()
  local b = fresh_with_dock()
  b:import_capabilities("ghost_class")
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "class not found in KB")

------------------------------------------------------------------------
print()
print("== KB check 1: kb_ref existence ==")
------------------------------------------------------------------------

expect_error("node.kb_ref not in KB", function()
  local b = bd.new{ name = "t", region = REGION }
  b:declare_capabilities{ "recharge" }
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 5, y = 5,
              kb_ref = "system.moon_base.site.alpha.infrastructure.registry.active_node_def.ghost_dock" }
  b:add_edge{ from = "n1", to = "n2" }
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "kb_ref does not resolve")

------------------------------------------------------------------------
print()
print("== KB check 2: action_id in catalog ==")
------------------------------------------------------------------------

expect_error("activate.action_id not in catalog", function()
  local b = fresh_with_dock{ "fake_action" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "fake_action", kb_ref = DOCK_REF } },
  }
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "not found in action catalog")

------------------------------------------------------------------------
print()
print("== KB check 3: params match parameter_schema ==")
------------------------------------------------------------------------

expect_error("params missing required field", function()
  local b = fresh_with_dock{ "recharge" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                          params = {} } },
  }
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "missing required param")

expect_error("params wrong type", function()
  local b = fresh_with_dock{ "recharge" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                          params = { target_soc = "85%" } } },  -- string not float
  }
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "wrong type")

expect_error("params extra field", function()
  local b = fresh_with_dock{ "recharge" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                          params = { target_soc = 0.85, bonus = 1 } } },
  }
  b:build({ kb_conn = make_kb(fixture_rows), system_name = SYS, site_name = SITE, planner_namespace = "test_planner" })
end, "unknown param")

------------------------------------------------------------------------
print()
print("== KB check 4: action present in active-node robot_virtual_action ==")
------------------------------------------------------------------------

expect_error("action not advertised by active node", function()
  -- build a fixture that has the action_id in catalog but NOT under
  -- the dock def's robot_virtual_action.
  local trimmed = {}
  for k, v in pairs(fixture_rows) do
    if k ~= DOCK_REF .. ".action.dock_in" then trimmed[k] = v end
  end
  local b = fresh_with_dock{ "dock_in" }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "dock_in", kb_ref = DOCK_REF } },
  }
  b:build({ kb_conn = make_kb(trimmed), system_name = SYS, site_name = SITE,
            planner_namespace = "test_planner" })
end, "not advertised by active-node def")

------------------------------------------------------------------------
print()
print("== schema cache: action_id queried only once ==")
------------------------------------------------------------------------

do
  local b = fresh_with_dock{ "recharge" }
  -- Two activate leaves on different edges referencing the same action.
  b:add_node{ name = "n3", x = 7, y = 7, kb_ref = DOCK_REF }
  b:add_edge{
    from = "lander_pad", to = "dock_3",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                          params = { target_soc = 0.85 } } },
  }
  b:add_edge{
    from = "lander_pad", to = "n3",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_REF,
                          params = { target_soc = 0.5 } } },
  }
  local log = {}
  local good = pcall(b.build, b,
    { kb_conn = make_kb(fixture_rows, log), system_name = SYS, site_name = SITE,
      planner_namespace = "test_planner" })
  ok("multi-edge same-action build succeeded", good)
  -- Count queries that hit the action-catalog row.
  local catalog_path = p_action("recharge")
  local n = 0
  for _, p in ipairs(log) do if p == catalog_path then n = n + 1 end end
  ok(string.format("recharge catalog hit once (got %d)", n), n == 1)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
