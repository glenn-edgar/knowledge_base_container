#!/usr/bin/env luajit
-- =============================================================================
-- test_phase3a_simulator_round_trip.lua -- Phase 3a C1 acceptance for
-- the drive-packet round-trip via the mock_mqtt_robot_lib simulator.
--
-- Coverage:
--   make_drive_ack / make_drive_done factories: shape, status, fault,
--     delta_pose handling
--
--   round-trip via hub_runtime._process_stream:
--     - planner emits cmd_drive_t (via send_drive_packet)
--     - simulator generates drive_ack + drive_done JSON
--     - pushed into transport recv buffer
--     - hub_runtime:tick() drains stream, dispatches drive_ack ->
--       on_drive_ack, drive_done -> on_drive_done
--     - state machine flows wait_ack -> active -> done
--
--   failure injection:
--     - drive_ack with non-ok status -> drive_error
--     - drive_done with success=false + fault_reason -> drive_error
--       carrying the reason
--
--   delta_pose application: drive_done with delta_x/y/heading updates
--     hub_control's global_pose
--
--   stream message-type dispatch:
--     - mismatched packet_id silently ignored
--     - unrelated message types (ack, kb_done) don't perturb drive
--       state when the legacy path is in idle
--
-- Required env at run:
--   LD_LIBRARY_PATH=<prebuilt_libs> for liblua_cbor.so
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

local hub_runtime = require("hub_runtime")
local rb          = require("route_builder")
local cmds        = require("command_packets")
local sim         = require("mock_mqtt_robot_lib")
local dkjson      = require("dkjson")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- Stub transport: bidirectional. send_rpc captures bytes; recv_stream
-- returns the next queued response (FIFO).
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
    energy_max      = 1000,
    energy_infinite = true,
  })
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
print("== make_drive_ack / make_drive_done factories ==")
------------------------------------------------------------------------

do
  local s = sim.make_drive_ack(42, "ok")
  local m = dkjson.decode(s)
  ok("drive_ack: type",      m.type == "drive_ack")
  ok("drive_ack: packet_id", m.packet_id == 42)
  ok("drive_ack: status",    m.status == "ok")
  ok("drive_ack: ts present", type(m.ts) == "string")

  local sr = sim.make_drive_ack(7, "rejected")
  local mr = dkjson.decode(sr)
  ok("drive_ack: status pass-through", mr.status == "rejected")

  -- Default status should be "ok"
  local sd = sim.make_drive_ack(99)
  local md = dkjson.decode(sd)
  ok("drive_ack: default status = ok", md.status == "ok")
end

do
  local s = sim.make_drive_done(100, true, nil,
    { x = 1.5, y = 2.5, heading = 0.3, arm_angle = 0 })
  local m = dkjson.decode(s)
  ok("drive_done: type",        m.type == "drive_done")
  ok("drive_done: packet_id",   m.packet_id == 100)
  ok("drive_done: success",     m.success == true)
  ok("drive_done: fault_reason omitted on success", m.fault_reason == nil)
  ok("drive_done: delta_x preserved",   m.delta_x == 1.5)
  ok("drive_done: delta_y preserved",   m.delta_y == 2.5)
  ok("drive_done: delta_heading",       math.abs(m.delta_heading - 0.3) < 1e-9)

  local sf = sim.make_drive_done(101, false, "wall_lost")
  local mf = dkjson.decode(sf)
  ok("drive_done failure: success",     mf.success == false)
  ok("drive_done failure: fault_reason",
     mf.fault_reason == "wall_lost")
  ok("drive_done failure: deltas default to zero",
     mf.delta_x == 0 and mf.delta_y == 0)

  -- Default fault_reason
  local sf2 = sim.make_drive_done(102, false)
  local mf2 = dkjson.decode(sf2)
  ok("drive_done failure: default fault_reason is non-nil",
     type(mf2.fault_reason) == "string" and #mf2.fault_reason > 0)
end

------------------------------------------------------------------------
print()
print("== round-trip happy: send -> ack -> done -> drive_done state ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 100, mission_id = 7 })

  hub:send_drive_packet(pkts[1])
  ok("after send: state = drive_wait_ack",
     hub:drive_state_get() == "drive_wait_ack")

  -- Simulator generates the responses (queues them on the tx recv side)
  tx:queue_recv(sim.make_drive_ack(100, "ok"))
  hub:tick()
  ok("after ack tick: state = drive_active",
     hub:drive_state_get() == "drive_active")

  tx:queue_recv(sim.make_drive_done(100, true, nil,
    { x = 10, y = 0, heading = 0 }))
  hub:tick()
  ok("after done tick: state = drive_done",
     hub:drive_state_get() == "drive_done")
  ok("drive_is_complete() true", hub:drive_is_complete())

  -- delta_pose should have been applied via hub_control
  local pose = hub:get_global_pose()
  ok("global_pose.x updated from delta", pose.x == 10)
  ok("global_pose.y updated from delta", pose.y == 0)
end

------------------------------------------------------------------------
print()
print("== round-trip failure: drive_ack rejected ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 200 })
  hub:send_drive_packet(pkts[1])

  tx:queue_recv(sim.make_drive_ack(200, "rejected"))
  hub:tick()
  local s, _, fault = hub:drive_state_get()
  ok("rejected ack: state = drive_error", s == "drive_error")
  ok("rejected ack: fault carries status",
     fault == "drive_ack_status=rejected", "got " .. tostring(fault))
end

------------------------------------------------------------------------
print()
print("== round-trip failure: drive_done success=false ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 300 })
  hub:send_drive_packet(pkts[1])

  tx:queue_recv(sim.make_drive_ack(300, "ok"))
  tx:queue_recv(sim.make_drive_done(300, false, "wall_lost"))
  hub:tick()  -- drains both messages in one tick
  local s, _, fault = hub:drive_state_get()
  ok("done failure: state = drive_error", s == "drive_error")
  ok("done failure: fault carries reason",
     fault == "wall_lost", "got " .. tostring(fault))
end

------------------------------------------------------------------------
print()
print("== mismatched packet_id silently ignored ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 400 })
  hub:send_drive_packet(pkts[1])

  -- Wrong packet_id ack arrives first
  tx:queue_recv(sim.make_drive_ack(999, "ok"))
  hub:tick()
  ok("wrong-id ack: state still wait_ack",
     hub:drive_state_get() == "drive_wait_ack")

  -- Then the correct one
  tx:queue_recv(sim.make_drive_ack(400, "ok"))
  hub:tick()
  ok("matching-id ack: state advances to active",
     hub:drive_state_get() == "drive_active")
end

------------------------------------------------------------------------
print()
print("== legacy ack/kb_done don't perturb drive state ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 500 })
  hub:send_drive_packet(pkts[1])

  -- Inject a legacy-shaped ack and kb_done. The drive state shouldn't
  -- care; legacy machine is in STATE_IDLE so they're ignored too.
  tx:queue_recv(sim.make_ack({ seq = 1 }))
  tx:queue_recv(sim.make_kb_done_success({ seq = 1, test_id = 1 }, 999))
  hub:tick()

  ok("legacy ack/kb_done: drive state unchanged",
     hub:drive_state_get() == "drive_wait_ack")
end

------------------------------------------------------------------------
print()
print("== full mission round-trip: 2 packets sequenced via drive_clear ==")
------------------------------------------------------------------------

do
  local tx  = make_stub_tx()
  local hub = make_hub_rt(tx)
  local pkts = rb.build_drive_packets({"n1", "n2", "n3"}, FIXTURE_GRAPH,
    { packet_id_start = 600 })
  ok("two packets built (sanity)", #pkts == 2)

  -- Packet 1
  hub:send_drive_packet(pkts[1])
  tx:queue_recv(sim.make_drive_ack(600, "ok"))
  tx:queue_recv(sim.make_drive_done(600, true, nil))
  hub:tick()
  ok("p1 reached drive_done", hub:drive_state_get() == "drive_done")
  hub:drive_clear()

  -- Packet 2
  hub:send_drive_packet(pkts[2])
  tx:queue_recv(sim.make_drive_ack(601, "ok"))
  tx:queue_recv(sim.make_drive_done(601, true, nil))
  hub:tick()
  ok("p2 reached drive_done", hub:drive_state_get() == "drive_done")

  -- Both publishes happened
  ok("transport got 2 cbor publishes", #tx.sent == 2)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
