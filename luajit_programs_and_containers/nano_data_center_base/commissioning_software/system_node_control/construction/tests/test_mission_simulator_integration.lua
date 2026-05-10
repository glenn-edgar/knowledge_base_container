#!/usr/bin/env luajit
-- =============================================================================
-- test_mission_simulator_integration.lua -- Phase 5 C5 prep.
--
-- End-to-end test of the planner-side dispatch chain WITHOUT the
-- action_server coroutine + pg + NATS overhead:
--
--   mission_builder.build(use_drive_v2=true)
--     -> kind-discriminated route (legacy bookends + drive_packet nav)
--     -> route walker (this test)
--     -> hub_runtime:send_drive_packet for each drive_packet entry
--     -> simulator (mock_mqtt_robot_lib) generates drive_ack + drive_done
--     -> hub_runtime:tick drives the per-packet state machine
--     -> drive_done observed; drive_clear; advance to next entry
--
-- Coverage:
--   - happy: 2-leg mission, init_check + drive_packet + drive_packet + idle
--     dispatch sequence (legacy bookends counted but not driven by this
--     test's walker; verified by route shape + count, exercised by
--     C3b's existing test elsewhere)
--   - per-packet completion: each drive_packet ends in drive_done; pose
--     deltas accumulate via hub_control
--   - packet_id sequencing: ids 1, 2 across legs do not collide; matches
--     mission_builder's monotonic counter
--   - failure injection: simulator rejects one packet's ack -> dispatch
--     halts at that packet, fault propagates
--   - rebuild() gap probe: a mission built with use_drive_v2=true,
--     then rebuilt via mission_builder.rebuild(), should ALSO produce
--     drive_packet entries -- but rebuild() today does NOT forward the
--     flag (synthetic mission_cmd has no use_drive_v2). Documents the
--     known C3b follow-up.
--
-- Required env at run:
--   liblua_cbor.so must be loadable. If the host doesn't have it on
--   LD_LIBRARY_PATH the test skips cleanly (exit 0) — gated below.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
local LUA_SHARE  = REPO_ROOT
    .. "nano_data_center_base/luajit/luajit_base/container/prebuilt_lua_share"
local SCRIPTS    = REPO_ROOT
    .. "nano_data_center_base/commissioning_software/system_node_control/construction/scripts"

package.path = PLANNER   .. "/lib/?.lua;"
            .. PLANNER   .. "/hub_dsl/protocol/?.lua;"
            .. PLANNER   .. "/hub_dsl/hub_functions/?.lua;"
            .. SCRIPTS   .. "/?.lua;"
            .. LUA_SHARE .. "/?.lua;"
            .. LUA_SHARE .. "/chain_tree/lua_dsl/luajit_pipeline/?.lua;"
            .. package.path

-- Gate: liblua_cbor.so must be on the OS linker search path. The test
-- was originally written assuming an operator-set LD_LIBRARY_PATH; on
-- bare hosts the dlopen at lua_cbor.lua:17 errors at require-time which
-- the regression sweep then mis-classifies as a test failure. Probe the
-- bare-name load up-front and skip cleanly if it doesn't resolve.
do
    local ok_ffi, ffi = pcall(require, "ffi")
    if not ok_ffi then
        print("=== test_mission_simulator_integration: SKIPPED ===")
        print("  reason: ffi unavailable")
        os.exit(0)
    end
    local ok_so, so_err = pcall(ffi.load, "lua_cbor")
    if not ok_so then
        print("=== test_mission_simulator_integration: SKIPPED ===")
        print("  reason: liblua_cbor.so not on linker search path")
        print("  detail: " .. tostring(so_err))
        print("  fix: LD_LIBRARY_PATH=<dir-containing-liblua_cbor.so> luajit " .. (arg[0] or "this test"))
        os.exit(0)
    end
end

local hub_runtime     = require("hub_runtime")
local rb              = require("route_builder")
local mission_builder = require("mission_builder")
local sim             = require("mock_mqtt_robot_lib")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- Stub bidirectional transport: same shape as 3a C1 test.
------------------------------------------------------------------------
local function make_stub_tx()
  local sent, recv_q = {}, {}
  return {
    sent     = sent,
    recv_q   = recv_q,
    send_rpc = function(self, bytes) sent[#sent + 1] = bytes end,
    recv_stream = function(self)
      if #recv_q == 0 then return nil end
      return table.remove(recv_q, 1)
    end,
    queue_recv = function(self, payload) recv_q[#recv_q + 1] = payload end,
    close = function() end,
  }
end

------------------------------------------------------------------------
local function make_hub_rt(tx)
  return hub_runtime.new({
    robot_id        = "test_robot",
    site            = "moonbase.alpha.surface_ops",
    system_name     = "ros_planner_ii",
    own_instance_id = "test_planner",
    transport       = tx,
    energy_max      = 10000,
    energy_infinite = true,
  })
end

------------------------------------------------------------------------
-- Stub planner: identical pattern to test_drive_v2_dispatch.lua.
-- Wraps build / build_v2 directly with a fixed graph + path lookup.
------------------------------------------------------------------------
local function make_stub_planner(graph, path_lookup)
  return {
    graph    = graph,
    vn_defs  = {
      init_check = { energy_cost = 1 },
      idle       = { energy_cost = 1 },
      operation  = { energy_cost = 5 },
    },
    is_transit       = function(self, _) return false end,
    get_node_type    = function(self, _) return nil end,
    get_node_params  = function(self, _) return {} end,
    plan = function(self, from, to, opts)
      local path = path_lookup[from .. ">" .. to]
      if not path then
        return nil, { error = "no path found", path = nil, cost = math.huge, segments = 0 }
      end
      local rb_opts = {}
      if opts then for k, v in pairs(opts) do rb_opts[k] = v end end
      rb_opts.vn_defs = self.vn_defs
      local route = rb.build(path, self.graph, rb_opts)
      return route, { path = path, cost = #path - 1, segments = #route }
    end,
    plan_v2 = function(self, from, to, opts)
      local path = path_lookup[from .. ">" .. to]
      if not path then
        return nil, { error = "no path found", path = nil, cost = math.huge, segments = 0 }
      end
      local rb_opts = {}
      if opts then for k, v in pairs(opts) do rb_opts[k] = v end end
      rb_opts.vn_defs = self.vn_defs
      local entries = rb.build_v2(path, self.graph, rb_opts)
      return entries, { path = path, cost = #path - 1, segments = #entries }
    end,
  }
end

------------------------------------------------------------------------
local FIXTURE_GRAPH = {
  nodes = {
    n1 = { x = 0,  y = 0  },
    n2 = { x = 10, y = 0  },
    n3 = { x = 10, y = 10 },
  },
  adj = {
    n1 = { { to = "n2", nav = "path_spline", speed = 0.40,
             path = { 0,0,  3,1,  7,2,  10,0 } } },
    n2 = { { to = "n3", nav = "path_line",   speed = 0.25,
             path = { 10,0,  10,10 } } },
    n3 = {},
  },
}

------------------------------------------------------------------------
-- Mini dispatch loop: walk a kind-discriminated route, exercise
-- drive_packet entries via hub_runtime + simulator, count legacy
-- bookends without dispatching them. Returns a summary.
--
-- Mirrors action_server._run_with_yield's drive_packet branch:
-- send_drive_packet -> tick until drive_is_complete -> drive_clear.
-- On simulator failure, halts and reports the fault.
--
-- @param entries  route from mission_builder.build with use_drive_v2=true
-- @param hub_rt   hub_runtime instance (already constructed with stub tx)
-- @param tx       the stub transport (so we can inject sim responses)
-- @param sim_fn   function(packet_id) -> 'ok'|'reject_ack'|'fail_done'
--                 controls per-packet simulator behavior
-- @return summary {dispatched, completed, faults, legacy_skipped, packet_ids}
------------------------------------------------------------------------
local function walk_route(entries, hub_rt, tx, sim_fn)
  local summary = {
    dispatched     = 0,
    completed      = 0,
    faults         = {},
    legacy_skipped = 0,
    packet_ids     = {},
  }
  for i, entry in ipairs(entries) do
    if entry.kind == "drive_packet" then
      local pkt = entry.packet
      summary.packet_ids[#summary.packet_ids + 1] = pkt.packet_id
      hub_rt:send_drive_packet(pkt)
      summary.dispatched = summary.dispatched + 1

      local mode = sim_fn and sim_fn(pkt.packet_id) or "ok"
      if mode == "reject_ack" then
        tx:queue_recv(sim.make_drive_ack(pkt.packet_id, "rejected"))
      elseif mode == "fail_done" then
        tx:queue_recv(sim.make_drive_ack(pkt.packet_id, "ok"))
        tx:queue_recv(sim.make_drive_done(pkt.packet_id, false, "wall_lost"))
      else
        tx:queue_recv(sim.make_drive_ack(pkt.packet_id, "ok"))
        tx:queue_recv(sim.make_drive_done(pkt.packet_id, true, nil,
          { x = 1, y = 0, heading = 0 }))
      end
      hub_rt:tick()

      local s, _, fault = hub_rt:drive_state_get()
      if s == "drive_done" then
        summary.completed = summary.completed + 1
      else
        summary.faults[#summary.faults + 1] = {
          packet_id = pkt.packet_id, state = s, fault = fault,
          entry_index = i,
        }
        hub_rt:drive_clear()
        return summary  -- halt on first fault
      end
      hub_rt:drive_clear()
    else
      -- Legacy entry (init_check / idle / operation). Counted but not
      -- driven here -- legacy activate_kb dispatch is exercised by
      -- the broader Phase 1/2 tests + action_server's _run_with_yield
      -- legacy branch (which this test doesn't exercise).
      summary.legacy_skipped = summary.legacy_skipped + 1
    end
  end
  return summary
end

------------------------------------------------------------------------
print("== happy path: 2-leg mission, all packets complete ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local mission_cmd = {
    start        = "n1",
    stops        = { { node = "n3" } },
    use_drive_v2 = true,
    mission_id   = 1234,
  }
  local route = mission_builder.build(mission_cmd, planner, nil, 1.0)
  ok("route built", route ~= nil)
  -- init_check + 2 drive_packets (n1->n2, n2->n3) + idle = 4
  ok("route length = 4", #route == 4, "got " .. #route)

  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local s   = walk_route(route, hub, tx)

  ok("dispatched 2 drive packets",     s.dispatched == 2)
  ok("completed 2 drive packets",      s.completed == 2)
  ok("0 faults",                       #s.faults == 0)
  ok("legacy bookends skipped (2)",    s.legacy_skipped == 2)
  ok("packet_ids monotonic (1, 2)",
     s.packet_ids[1] == 1 and s.packet_ids[2] == 2,
     "got " .. table.concat(s.packet_ids, ", "))

  -- delta_pose accumulation: each packet contributed (1, 0, 0); after 2
  -- packets, global_pose.x should be 2.
  local pose = hub:get_global_pose()
  ok("global_pose.x = 2 (delta accumulation)", pose.x == 2,
     "got " .. tostring(pose.x))

  -- transport saw 2 cbor publishes
  ok("transport got 2 publishes", #tx.sent == 2)
end

------------------------------------------------------------------------
print()
print("== mission_id flows to every packet ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local mission_cmd = {
    start        = "n1",
    stops        = { { node = "n3" } },
    use_drive_v2 = true,
    mission_id   = 9999,
  }
  local route = mission_builder.build(mission_cmd, planner, nil, 1.0)
  local mids = {}
  for _, e in ipairs(route) do
    if e.kind == "drive_packet" then
      mids[#mids + 1] = e.packet.mission_id
    end
  end
  ok("both packets carry mission_id",
     mids[1] == 9999 and mids[2] == 9999,
     "got " .. table.concat(mids, ", "))
end

------------------------------------------------------------------------
print()
print("== failure injection: simulator rejects 2nd packet's ack ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local mission_cmd = {
    start = "n1",
    stops = { { node = "n3" } },
    use_drive_v2 = true,
  }
  local route = mission_builder.build(mission_cmd, planner, nil, 1.0)

  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  -- Reject the second packet's ack
  local s = walk_route(route, hub, tx, function(pkt_id)
    if pkt_id == 2 then return "reject_ack" end
    return "ok"
  end)

  ok("dispatched 2 (reached the failing packet)", s.dispatched == 2)
  ok("completed only 1",                          s.completed == 1)
  ok("1 fault recorded",                          #s.faults == 1)
  ok("fault is on packet_id=2",
     s.faults[1] and s.faults[1].packet_id == 2,
     s.faults[1] and ("got " .. tostring(s.faults[1].packet_id)) or "no fault")
  ok("fault state = drive_error",
     s.faults[1] and s.faults[1].state == "drive_error",
     s.faults[1] and ("got " .. tostring(s.faults[1].state)) or "no fault")
end

------------------------------------------------------------------------
print()
print("== failure injection: drive_done success=false ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local route = mission_builder.build({
    start = "n1", stops = { { node = "n3" } },
    use_drive_v2 = true,
  }, planner, nil, 1.0)

  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local s = walk_route(route, hub, tx, function(pkt_id)
    if pkt_id == 1 then return "fail_done" end
    return "ok"
  end)

  ok("first packet failed, halt before second", s.dispatched == 1)
  ok("no completions",                          s.completed == 0)
  ok("fault carries simulator reason",
     s.faults[1] and s.faults[1].fault == "wall_lost",
     s.faults[1] and ("got " .. tostring(s.faults[1].fault)) or "no fault")
end

------------------------------------------------------------------------
print()
print("== multi-leg packet_id chain (3-stop mission) ==")
------------------------------------------------------------------------

do
  -- 3 stops -> 3 nav legs (n1->n2, n2->n3, n3->n1 [back]).
  local graph = {
    nodes = {
      n1 = { x = 0,  y = 0  },
      n2 = { x = 10, y = 0  },
      n3 = { x = 10, y = 10 },
    },
    adj = {
      n1 = {
        { to = "n2", nav = "path_line", speed = 0.3, path = {0,0, 10,0} },
      },
      n2 = {
        { to = "n3", nav = "path_line", speed = 0.3, path = {10,0, 10,10} },
        { to = "n1", nav = "path_line", speed = 0.3, path = {10,0, 0,0} },
      },
      n3 = {
        { to = "n2", nav = "path_line", speed = 0.3, path = {10,10, 10,0} },
        { to = "n1", nav = "path_line", speed = 0.3, path = {10,10, 0,0} },
      },
    },
  }
  local planner = make_stub_planner(graph, {
    ["n1>n2"] = { "n1", "n2" },
    ["n2>n3"] = { "n2", "n3" },
    ["n3>n1"] = { "n3", "n1" },
  })
  local route = mission_builder.build({
    start = "n1",
    stops = { { node = "n2" }, { node = "n3" }, { node = "n1" } },
    use_drive_v2 = true,
  }, planner, nil, 1.0)

  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local s   = walk_route(route, hub, tx)

  ok("3 packets dispatched", s.dispatched == 3)
  ok("3 packets completed",  s.completed == 3)
  ok("packet_ids = 1,2,3 monotonic across legs",
     s.packet_ids[1] == 1
       and s.packet_ids[2] == 2
       and s.packet_ids[3] == 3,
     "got " .. table.concat(s.packet_ids, ", "))
end

------------------------------------------------------------------------
print()
print("== rebuild() use_drive_v2 forwarding ==")
------------------------------------------------------------------------

local function count_kinds(route)
  local drive, legacy_nav = 0, 0
  for _, e in ipairs(route) do
    if e.kind == "drive_packet" then
      drive = drive + 1
    elseif e.kb_name == "path_spline" or e.kb_name == "path_line"
        or e.kb_name == "path_wall" then
      legacy_nav = legacy_nav + 1
    end
  end
  return drive, legacy_nav
end

do
  -- Default (no use_drive_v2 supplied): rebuild stays legacy. Backward
  -- compat for any caller that didn't opt in.
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local rebuilt = mission_builder.rebuild(
    { { node = "n3" } }, planner, "n1")
  local drive, legacy = count_kinds(rebuilt)
  ok("rebuild default (no flag): 0 drive_packet entries", drive == 0,
     "got " .. drive)
  ok("rebuild default (no flag): legacy nav entries present", legacy == 4,
     "got " .. legacy)
end

do
  -- WITH use_drive_v2=true forwarded: rebuild produces drive_packet
  -- entries (the fix landed in this commit; pre-fix this assertion
  -- would have been "drive == 0").
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local rebuilt = mission_builder.rebuild(
    { { node = "n3" } }, planner, "n1", 0, true)
  local drive, legacy = count_kinds(rebuilt)
  ok("rebuild with use_drive_v2=true: 2 drive_packet entries (one per edge)",
     drive == 2, "got " .. drive)
  ok("rebuild with use_drive_v2=true: 0 legacy nav entries",
     legacy == 0, "got " .. legacy)
end

do
  -- Replan-walk integration: build, fault on packet 1, "rebuild" with
  -- the forwarded flag, walk the rebuilt route. Verifies the dispatch
  -- chain handles a replanned route end-to-end.
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local rebuilt = mission_builder.rebuild(
    { { node = "n3" } }, planner, "n1", 0, true)

  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local s   = walk_route(rebuilt, hub, tx)
  ok("replan dispatched 2 packets",  s.dispatched == 2)
  ok("replan completed 2 packets",   s.completed == 2)
  ok("replan: 0 faults",             #s.faults == 0)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
