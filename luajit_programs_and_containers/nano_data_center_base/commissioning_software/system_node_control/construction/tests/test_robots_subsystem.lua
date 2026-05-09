#!/usr/bin/env luajit
-- =============================================================================
-- test_robots_subsystem.lua -- robot-sim layer commit 1 acceptance.
--
-- Coverage:
--   robots.lua install_site:
--     - empty ROBOTS list -> no-op (no kb calls)
--     - one robot in one tenant -> emits planner.<ns>.robots.<id>
--     - two robots in same tenant -> both under same robots catalog
--     - robots in two tenants -> two planner.<ns>.robots subtrees
--     - sorted by robot_id within tenant; tenants sorted alphabetically
--     - empty capabilities array allowed
--   robots.lua sanity_check (each rejection):
--     - missing robot_id
--     - invalid robot_id chars (slash, space)
--     - duplicate robot_id (across tenants)
--     - planner_namespace not in PLANNERS
--     - capabilities not a table
--     - capability not a string / empty string
--     - duplicate capability within a robot
--     - unknown action_id (not in ctx.ACTIONS)
--   build_kb.lua structural sanity:
--     - ROBOTS enumeration loop present
--     - filters on def="robot_sim"
--     - asserts params.robot_id + params.planner_namespace required
--     - ctx.ROBOTS exposed
--     - "robots" appears in SUBSYSTEMS list (after boards)
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local SUBSYS_DIR = SCRIPT_DIR .. "../subsystems/"

local pass, fail = 0, 0

local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

------------------------------------------------------------------------
-- Mock KB (same shape as test_planner_phase1_catalog)
------------------------------------------------------------------------

local function make_kb()
  local kb = { calls = {} }
  function kb:with_header(label, name, attrs, status, descr, body)
    table.insert(self.calls, { op = "with_header", label = label, name = name })
    if type(body) == "function" then body() end
  end
  function kb:add_info_node(label, name, attrs, payload, descr)
    table.insert(self.calls, {
      op = "add_info_node", label = label, name = name, payload = payload,
    })
  end
  function kb:add_status_field(name, attrs, descr, value)
    table.insert(self.calls, { op = "add_status_field", name = name })
  end
  return kb
end

local function load_subsystem(name)
  local chunk, err = loadfile(SUBSYS_DIR .. name .. ".lua")
  if not chunk then error("loadfile " .. name .. ": " .. tostring(err)) end
  return chunk()
end

local robots_mod = load_subsystem("robots")

------------------------------------------------------------------------
-- Fixtures
------------------------------------------------------------------------

local FIXTURE_PLANNERS = {
  { name = "mission_planner_01", namespace = "mission_planner_01" },
  { name = "mission_planner_02", namespace = "tunnel_ops" },
}
local FIXTURE_ACTIONS = {
  recharge  = { description = "x", parameter_schema = {} },
  dock_in   = { description = "x", parameter_schema = {} },
  dock_out  = { description = "x", parameter_schema = {} },
}

local function make_ctx(robots_list, opts)
  opts = opts or {}
  return {
    kb       = make_kb(),
    PLANNERS = opts.planners or FIXTURE_PLANNERS,
    ACTIONS  = opts.actions  or FIXTURE_ACTIONS,
    ROBOTS   = robots_list,
  }
end

local function run_ok(name, ctx)
  local ok_call, err = pcall(robots_mod.install_site, ctx)
  if ok_call then
    pass = pass + 1; print("  ok  " .. name)
  else
    fail = fail + 1; print("  FAIL " .. name .. " -- " .. tostring(err))
  end
end

local function run_fail(name, ctx, pattern)
  local ok_call, err = pcall(robots_mod.install_site, ctx)
  if ok_call then
    fail = fail + 1
    print(string.format("  FAIL %s: should have errored (pattern=%q)",
      name, pattern))
  elseif not tostring(err):find(pattern, 1, true) then
    fail = fail + 1
    print(string.format("  FAIL %s: error %q did not match pattern %q",
      name, tostring(err), pattern))
  else
    pass = pass + 1
    print(string.format("  ok  %s: rejected (%q)", name, pattern))
  end
end

------------------------------------------------------------------------
print("== happy path: empty ROBOTS is a no-op ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({})
  run_ok("empty ROBOTS install_site succeeds", ctx)
  ok("no kb calls emitted", #ctx.kb.calls == 0,
     "got " .. #ctx.kb.calls .. " calls")
end

------------------------------------------------------------------------
print()
print("== happy path: one robot in one tenant ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({
    { container_name    = "robot_sim_rover_1",
      robot_id          = "rover_1",
      planner_namespace = "mission_planner_01",
      capabilities      = { "recharge", "dock_in" } },
  })
  run_ok("install_site succeeds", ctx)

  -- Expect: with_header(planner) -> with_header(robots) -> add_info_node(robot)
  local calls = ctx.kb.calls
  ok("emits 3 calls (planner header, robots header, robot leaf)",
     #calls == 3, "got " .. #calls)
  ok("calls[1] = with_header planner mission_planner_01",
     calls[1].op == "with_header" and calls[1].label == "planner"
     and calls[1].name == "mission_planner_01")
  ok("calls[2] = with_header robots catalog",
     calls[2].op == "with_header" and calls[2].label == "robots"
     and calls[2].name == "catalog")
  ok("calls[3] = add_info_node robot rover_1",
     calls[3].op == "add_info_node" and calls[3].label == "robot"
     and calls[3].name == "rover_1")
  ok("payload includes robot_id", calls[3].payload.robot_id == "rover_1")
  ok("payload includes container_name",
     calls[3].payload.container_name == "robot_sim_rover_1")
  ok("payload includes capabilities",
     #calls[3].payload.capabilities == 2 and
     calls[3].payload.capabilities[1] == "recharge")
end

------------------------------------------------------------------------
print()
print("== happy path: two robots in same tenant ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({
    { container_name = "robot_sim_b", robot_id = "rover_b",
      planner_namespace = "mission_planner_01",
      capabilities = { "recharge" } },
    { container_name = "robot_sim_a", robot_id = "rover_a",
      planner_namespace = "mission_planner_01",
      capabilities = { "dock_in" } },
  })
  run_ok("install_site succeeds", ctx)

  -- Expect: 1 planner header, 1 robots header, 2 robot leaves; sorted alpha
  local calls = ctx.kb.calls
  ok("4 total calls", #calls == 4, "got " .. #calls)
  ok("rover_a comes before rover_b (sorted)",
     calls[3].name == "rover_a" and calls[4].name == "rover_b")
end

------------------------------------------------------------------------
print()
print("== happy path: robots in two tenants ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({
    { container_name = "rs_t", robot_id = "tunnel_rover",
      planner_namespace = "tunnel_ops", capabilities = {} },
    { container_name = "rs_m", robot_id = "main_rover",
      planner_namespace = "mission_planner_01", capabilities = {} },
  })
  run_ok("install_site succeeds with multi-tenant", ctx)

  -- Expect 6 calls: planner(mp_01), robots, robot(main_rover),
  --                 planner(tunnel_ops), robots, robot(tunnel_rover)
  -- (tenant order alphabetical: mission_planner_01 < tunnel_ops)
  local calls = ctx.kb.calls
  ok("6 total calls", #calls == 6, "got " .. #calls)
  ok("first tenant header = mission_planner_01 (alpha sort)",
     calls[1].op == "with_header" and calls[1].label == "planner"
     and calls[1].name == "mission_planner_01")
  ok("first tenant emits main_rover",
     calls[3].op == "add_info_node" and calls[3].name == "main_rover")
  ok("second tenant header = tunnel_ops",
     calls[4].op == "with_header" and calls[4].label == "planner"
     and calls[4].name == "tunnel_ops")
  ok("second tenant emits tunnel_rover",
     calls[6].op == "add_info_node" and calls[6].name == "tunnel_rover")
end

------------------------------------------------------------------------
print()
print("== happy path: empty capabilities allowed ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "noop_robot",
      planner_namespace = "mission_planner_01", capabilities = {} },
  })
  run_ok("empty capabilities ok", ctx)
end

------------------------------------------------------------------------
print()
print("== sanity_check: rejection cases ==")
------------------------------------------------------------------------

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = nil,
      planner_namespace = "mission_planner_01", capabilities = {} },
  })
  run_fail("missing robot_id", ctx, "robot_id required")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "bad/id",
      planner_namespace = "mission_planner_01", capabilities = {} },
  })
  run_fail("invalid robot_id chars (slash)", ctx, "invalid characters")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "bad space",
      planner_namespace = "mission_planner_01", capabilities = {} },
  })
  run_fail("invalid robot_id chars (space)", ctx, "invalid characters")
end

do
  local ctx = make_ctx({
    { container_name = "rs1", robot_id = "rover_x",
      planner_namespace = "mission_planner_01", capabilities = {} },
    { container_name = "rs2", robot_id = "rover_x",
      planner_namespace = "tunnel_ops", capabilities = {} },
  })
  run_fail("duplicate robot_id across tenants", ctx, "must be unique")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "orphan",
      planner_namespace = "no_such_tenant", capabilities = {} },
  })
  run_fail("planner_namespace not in PLANNERS",
           ctx, "no orphan robots")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "r1",
      planner_namespace = "mission_planner_01",
      capabilities = "not a table" },
  })
  run_fail("capabilities not a table", ctx, "list of action_id strings")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "r1",
      planner_namespace = "mission_planner_01",
      capabilities = { "" } },
  })
  run_fail("empty capability string", ctx, "non-empty string")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "r1",
      planner_namespace = "mission_planner_01",
      capabilities = { "recharge", "recharge" } },
  })
  run_fail("duplicate capability", ctx, "more than once")
end

do
  local ctx = make_ctx({
    { container_name = "rs", robot_id = "r1",
      planner_namespace = "mission_planner_01",
      capabilities = { "fly_to_moon" } },  -- not in fixture ACTIONS
  })
  run_fail("unknown action_id", ctx, "unknown")
end

------------------------------------------------------------------------
print()
print("== build_kb.lua: ROBOTS enumeration + SUBSYSTEMS list ==")
------------------------------------------------------------------------

do
  local src = read_file(SCRIPT_DIR .. "../build_kb.lua")
  ok("build_kb.lua readable", src ~= nil)
  if src then
    -- "robots" subsystem registered after "boards"
    ok("\"robots\" subsystem registered in SUBSYSTEMS list",
       src:find('"robots"', 1, true) ~= nil)
    local boards_pos = src:find('"boards"', 1, true)
    local robots_pos = src:find('"robots"', 1, true)
    ok("\"robots\" comes after \"boards\" in SUBSYSTEMS",
       boards_pos and robots_pos and boards_pos < robots_pos)

    -- ROBOTS enumeration loop present
    ok("ROBOTS local table declared",
       src:find("local ROBOTS = {}", 1, true) ~= nil)
    ok("filters on def == \"robot_sim\"",
       src:find('inst.def == "robot_sim"', 1, true) ~= nil)
    ok("requires params.robot_id",
       src:find("missing params.robot_id", 1, true) ~= nil)
    ok("requires params.planner_namespace",
       src:find("missing params.planner_namespace", 1, true) ~= nil)

    -- ctx.ROBOTS exposed
    ok("ctx.ROBOTS exposed",
       src:find("ROBOTS                 = ROBOTS", 1, false) ~= nil)
  end
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
