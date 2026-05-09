#!/usr/bin/env luajit
-- =============================================================================
-- test_board_dsl_c5_phase7.lua -- Phase 7 C2 acceptance for the
-- board DSL validator's tenant-aware kb_ref check (REJECT catalog +
-- own-tenant / shared-infrastructure happy paths).
--
-- Per project_phase7_multitenant_design.md Q4+Q8: the validator
-- allows kb_ref into:
--   - own tenant subtree   (system.<sys>.site.<S>.planner.<own_ns>.*)
--   - infrastructure shared (system.<sys>.site.<S>.infrastructure.registry.*)
-- and rejects with three specific messages:
--   case 1: cross-tenant ref to planner.<other_ns>.*
--   case 2: orchestration infra ref to app_containers.* or cpu.*
--   case 3: unknown prefix
--
-- This test stubs the KB connection so it doesn't need pg or NATS.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
package.path = SCRIPT_DIR .. "../../scripts/board_dsl/?.lua;" .. package.path

local bd = require("board_dsl")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- KB stub: every kb_ref the test passes is treated as "exists" so the
-- validator doesn't bail on the existence check before the
-- tenant-prefix check fires. The stub also serves the action-catalog
-- + active-node-action rows so leaves with kb_refs to dock_v1 in
-- infrastructure.registry pass through.
------------------------------------------------------------------------

local SYS  = "moon_base"
local SITE = "alpha"
local OWN  = "tenant_a"
local OTHER = "tenant_b"

local DOCK_OWN_NS  = string.format(
  "system.%s.site.%s.planner.%s.fixtures.dock_in_own", SYS, SITE, OWN)
local DOCK_OTHER   = string.format(
  "system.%s.site.%s.planner.%s.fixtures.dock_in_other", SYS, SITE, OTHER)
local DOCK_INFRA   = string.format(
  "system.%s.site.%s.infrastructure.registry.active_node_def.dock_v1",
  SYS, SITE)
local APP_INFRA    = string.format(
  "system.%s.site.%s.app_containers.mission_planner_01.runtime.heartbeat",
  SYS, SITE)
local CPU_INFRA    = string.format(
  "system.%s.site.%s.cpu.cpu_02.container.mission_planner_01",
  SYS, SITE)
local UNKNOWN_REF  = "system.other_system.site.beta.boards.x"

-- Action catalog path -- per board_dsl's path_action_catalog():
-- system.<sys>.site.<S>.actions.catalog.action.<id>
local ACTION_RECHARGE = string.format(
  "system.%s.site.%s.actions.catalog.action.recharge", SYS, SITE)

-- KB stub mirroring test_board_dsl_c3's make_kb: parse the path out of
-- the SQL string (board_dsl embeds it via pg_escape, not parameterized).
local dkjson = require("dkjson")
local function make_kb(rows)
  local conn = {}
  function conn:prepare(sql)
    local path = sql:match("path = '(.-)'::ltree")
    local stmt = { sql = sql, path = path, _fetched = false }
    function stmt:execute() return true end
    function stmt:fetch(_named)
      if self._fetched then return nil end
      self._fetched = true
      local row_data = rows[self.path]
      if row_data == nil then return nil end
      return { data = type(row_data) == "string"
                       and row_data
                       or dkjson.encode(row_data) }
    end
    function stmt:close() end
    return stmt
  end
  return conn
end

-- Fixture rows: stored as Lua tables (make_kb encodes to JSON for
-- the data::text column). All test kb_refs resolve to "exists" so
-- the validator's tenant-prefix check fires (not the existence check
-- in front of it). Action catalog lives where board_dsl looks for
-- it (system.<sys>.site.<S>.actions.catalog.action.<id>).
local function fixture_rows()
  return {
    [DOCK_OWN_NS]  = {},
    [DOCK_OTHER]   = {},
    [DOCK_INFRA]   = {},
    [APP_INFRA]    = {},
    [CPU_INFRA]    = {},
    [UNKNOWN_REF]  = {},
    [ACTION_RECHARGE] = {
      description = "x",
      parameter_schema = { target_soc = "float" },
    },
    -- Active-node-def's recharge action row (Check 4 hook)
    [DOCK_INFRA .. ".action.recharge"] = {},
  }
end

-- A board factory: minimal valid board with one node having the given kb_ref.
local function board_with_kb_ref(ref)
  local b = bd.new{
    name   = "phase7_test",
    region = { { x=0, y=0 }, { x=10, y=0 }, { x=10, y=10 }, { x=0, y=10 } },
  }
  b:declare_capabilities{ "recharge" }
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 5, y = 5, kb_ref = ref }
  b:add_edge{ from = "n1", to = "n2" }
  return b
end

local function build_with(opts)
  local b = opts.board
  return pcall(b.build, b, {
    kb_conn           = make_kb(fixture_rows()),
    system_name       = SYS,
    site_name         = SITE,
    planner_namespace = OWN,
  })
end

------------------------------------------------------------------------
print("== build() requires planner_namespace in KB-validating mode ==")
------------------------------------------------------------------------

do
  local b = board_with_kb_ref(DOCK_INFRA)
  local good, err = pcall(b.build, b, {
    kb_conn = make_kb(fixture_rows()),
    system_name = SYS, site_name = SITE,
    -- planner_namespace deliberately omitted
  })
  ok("missing planner_namespace -> error",
     not good and tostring(err):find("planner_namespace"), tostring(err))
end

do
  -- offline mode (no kb) doesn't need planner_namespace
  local b = board_with_kb_ref(DOCK_INFRA)
  local good, err = pcall(b.build, b, {})
  ok("no-kb mode without planner_namespace works",
     good == true, tostring(err))
end

------------------------------------------------------------------------
print()
print("== happy path: own-tenant + shared-infrastructure refs allowed ==")
------------------------------------------------------------------------

do
  local good, err = build_with{ board = board_with_kb_ref(DOCK_OWN_NS) }
  ok("own-tenant kb_ref accepted",
     good == true, tostring(err))
end

do
  local good, err = build_with{ board = board_with_kb_ref(DOCK_INFRA) }
  ok("infrastructure.registry kb_ref accepted",
     good == true, tostring(err))
end

------------------------------------------------------------------------
print()
print("== REJECT case 1: cross-tenant kb_ref ==")
------------------------------------------------------------------------

do
  local good, err = build_with{ board = board_with_kb_ref(DOCK_OTHER) }
  err = tostring(err)
  ok("cross-tenant kb_ref rejected", not good)
  ok("error mentions 'cross-tenant'",
     err:find("cross%-tenant") ~= nil, err)
  ok("error names the other tenant",
     err:find(OTHER, 1, true) ~= nil, err)
  ok("error names the own tenant",
     err:find(OWN, 1, true) ~= nil, err)
  ok("error suggests infrastructure.registry as alternative",
     err:find("infrastructure%.registry") ~= nil, err)
end

------------------------------------------------------------------------
print()
print("== REJECT case 2: orchestration infrastructure (app_containers, cpu) ==")
------------------------------------------------------------------------

do
  local good, err = build_with{ board = board_with_kb_ref(APP_INFRA) }
  err = tostring(err)
  ok("app_containers.* kb_ref rejected", not good)
  ok("error mentions 'orchestration infrastructure'",
     err:find("orchestration infrastructure") ~= nil, err)
  ok("error lists allowed prefixes (planner.<own_ns>)",
     err:find("planner%." .. OWN) ~= nil, err)
  ok("error lists allowed prefixes (infrastructure.registry)",
     err:find("infrastructure%.registry") ~= nil, err)
end

do
  local good, err = build_with{ board = board_with_kb_ref(CPU_INFRA) }
  err = tostring(err)
  ok("cpu.* kb_ref rejected", not good)
  ok("error mentions 'orchestration infrastructure'",
     err:find("orchestration infrastructure") ~= nil, err)
end

------------------------------------------------------------------------
print()
print("== REJECT case 3: unknown prefix ==")
------------------------------------------------------------------------

do
  local good, err = build_with{ board = board_with_kb_ref(UNKNOWN_REF) }
  err = tostring(err)
  ok("unknown-prefix kb_ref rejected", not good)
  ok("error says 'does not match any allowed prefix'",
     err:find("does not match any allowed prefix") ~= nil, err)
  ok("error names the offending ref",
     err:find(UNKNOWN_REF, 1, true) ~= nil, err)
end

------------------------------------------------------------------------
print()
print("== same validation applies to activate{ kb_ref=... } leaves ==")
------------------------------------------------------------------------

do
  -- Build a board where the activate leaf's kb_ref is cross-tenant.
  local b = bd.new{
    name = "phase7_activate_test",
    region = { { x=0, y=0 }, { x=10, y=0 }, { x=10, y=10 }, { x=0, y=10 } },
  }
  b:declare_capabilities{ "recharge" }
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 5, y = 5 }
  b:add_edge{
    from = "n1", to = "n2",
    path = { bd.activate{ action_id = "recharge", kb_ref = DOCK_OTHER,
                          params = { target_soc = 0.85 } } },
  }
  local good, err = pcall(b.build, b, {
    kb_conn = make_kb(fixture_rows()), system_name = SYS, site_name = SITE,
    planner_namespace = OWN,
  })
  err = tostring(err)
  ok("activate leaf cross-tenant kb_ref rejected", not good)
  ok("error mentions 'cross-tenant'",
     err:find("cross%-tenant") ~= nil, err)
  ok("error mentions 'activate'",
     err:find("activate") ~= nil, err)
end

------------------------------------------------------------------------
print()
print("== boards.lua subsystem: per-tenant doc_class iteration ==")
------------------------------------------------------------------------

do
  -- Drive subsystems/boards.lua with a fake ctx + capture add_doc_class
  -- calls. Confirms that #PLANNERS=2 -> 2 doc_class registrations,
  -- one per tenant namespace, each with the per-tenant path.
  local boards_subsystem = dofile(
    SCRIPT_DIR .. "../../subsystems/boards.lua")

  local calls = {}
  local fake_kb = {
    add_doc_class = function(self, def) calls[#calls + 1] = def end,
  }
  local ctx = {
    kb           = fake_kb,
    SYSTEM_NAME  = "moon_base",
    SITE         = "alpha",
    PLANNERS     = {
      { name = "mission_planner_01", namespace = "mission_planner_01" },
      { name = "mission_planner_02", namespace = "tunnel_ops" },
    },
  }
  boards_subsystem.install_site(ctx)
  ok("install_site emits one doc_class per planner", #calls == 2,
     "got " .. #calls)
  ok("first doc_class namespace includes mission_planner_01",
     calls[1] and calls[1].namespace ==
     "system.moon_base.site.alpha.planner.mission_planner_01.boards",
     calls[1] and calls[1].namespace)
  ok("second doc_class namespace includes tunnel_ops",
     calls[2] and calls[2].namespace ==
     "system.moon_base.site.alpha.planner.tunnel_ops.boards",
     calls[2] and calls[2].namespace)
  ok("doc_class writer is commissioning_only",
     calls[1] and calls[1].writer == "commissioning_only")
  ok("doc_class content_type is JSON",
     calls[1] and calls[1].content_type == "application/json")
  ok("doc_class description mentions per-tenant",
     calls[1] and calls[1].description:find("planner") ~= nil)
end

do
  -- Empty PLANNERS -> no doc_class registration (graceful no-op).
  local boards_subsystem = dofile(
    SCRIPT_DIR .. "../../subsystems/boards.lua")
  local calls = {}
  local fake_kb = {
    add_doc_class = function(self, def) calls[#calls + 1] = def end,
  }
  local ctx = {
    kb = fake_kb, SYSTEM_NAME = "x", SITE = "y", PLANNERS = {},
  }
  boards_subsystem.install_site(ctx)
  ok("empty PLANNERS -> no doc_class calls", #calls == 0,
     "got " .. #calls)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
