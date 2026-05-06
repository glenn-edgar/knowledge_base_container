#!/usr/bin/env luajit
-- =============================================================================
-- test_mock_mqtt_robot.lua -- host-side smoke for mock_mqtt_robot_lib.
--
-- Exercises the parts of A.5 Phase 2a that don't need an MQTT broker:
--   1. Topic computation: site path conversion (dots → slashes), per-
--      robot RPC / stream_bus / link / planner-glob URIs.
--   2. ack / kb_done factories: produce the JSON shapes hub_runtime
--      expects on stream_bus (type ∈ {ack, heartbeat, kb_done}; success
--      flag, seq propagation, delta-pose fields all present).
--   3. LinkState handshake: announce / confirm / heartbeat / disconnect
--      payloads carry the right type strings; on_planner_verb produces
--      the right transition + reply for each planner-side verb.
--
-- Live-cluster smoke (real MQTT round-trip with the planner) is the
-- user-driven step after image rebuild + landing_zone upload. Run the
-- mock with:  LD_LIBRARY_PATH=$NDC/kb/mqtt mock_mqtt_robot.lua --robot rover_1
--
-- Usage: luajit construction/tests/test_mock_mqtt_robot.lua
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
package.path = SCRIPT_DIR .. "../scripts/?.lua;" .. package.path

local lib    = require("mock_mqtt_robot_lib")
local dkjson = require("dkjson")

local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.stdout:write("  ✓ " .. msg .. "\n")
    else         fail = fail + 1; io.stdout:write("  ✗ " .. msg .. "\n") end
end

---------------------------------------------------------------------------
-- 1. Topic computation
---------------------------------------------------------------------------

print("=== 1. Topic computation ===")
local t = lib.make_topics("moon_base_alpha", "rover_1")
check(t.rpc          == "moon_base_alpha/robots/rover_1/rpc",           "rpc topic")
check(t.stream_bus   == "moon_base_alpha/robots/rover_1/stream_bus",    "stream_bus topic")
check(t.link_out     == "moon_base_alpha/robots/rover_1/link",          "link_out topic")
check(t.planner_glob == "moon_base_alpha/robots/rover_1/planner/+",     "planner glob subscribe")
check(t.planner_ack  == "moon_base_alpha/robots/rover_1/planner/ack",   "planner ack topic")

-- v2-style site name with dots converts to slashes
local t2 = lib.make_topics("moonbase.alpha.surface_ops", "rover_2")
check(t2.rpc == "moonbase/alpha/surface_ops/robots/rover_2/rpc",
      "site dots → slashes")

---------------------------------------------------------------------------
-- 2. ack / kb_done factories
---------------------------------------------------------------------------

print("=== 2. ack / kb_done factories ===")

local synthetic_cmd = {
    kb_name = "path_spline", packet_type = 2, seq = 17, test_id = 3,
    from_x = 0, from_y = 0, to_x = 200, to_y = 0,
    speed = 100, distance = 200,
    segment_index = 1, total_segments = 3,
}

local ack_json = lib.make_ack(synthetic_cmd)
local ack = dkjson.decode(ack_json)
check(ack.type   == "ack",        "ack: type")
check(ack.seq    == 17,           "ack: seq forwarded")
check(ack.status == "ok",         "ack: status=ok")
check(type(ack.ts) == "string",   "ack: timestamp present")

local done_json = lib.make_kb_done_success(synthetic_cmd, 9800)
local done = dkjson.decode(done_json)
check(done.type             == "kb_done", "kb_done success: type")
check(done.success          == true,      "kb_done success: success=true")
check(done.seq              == 17,        "kb_done success: seq forwarded")
check(done.test_id          == 3,         "kb_done success: test_id forwarded")
check(done.energy_remaining == 9800,      "kb_done success: energy_remaining")
check(done.delta_x          == 0
   and done.delta_y         == 0
   and done.delta_heading   == 0
   and done.delta_arm_angle == 0,
      "kb_done success: zero delta-pose (no movement simulation)")

local fail_json = lib.make_kb_done_failure(synthetic_cmd, "obstacle", 9700)
local f = dkjson.decode(fail_json)
check(f.type == "kb_done" and f.success == false, "kb_done failure: success=false")
check(f.fault_reason == "obstacle",                "kb_done failure: fault_reason")
check(f.seq == 17 and f.test_id == 3,              "kb_done failure: seq + test_id forwarded")
check(f.energy_remaining == 9700,                  "kb_done failure: energy_remaining")

---------------------------------------------------------------------------
-- 3. LinkState handshake
---------------------------------------------------------------------------

print("=== 3. LinkState ===")

local link = lib.LinkState.new("rover_1", "lunar_rover", nil, 10000, 9500)
check(link.state            == "init",          "initial state = init")
check(link.energy_remaining == 9500,            "energy_remaining honored")
check(#link.capabilities    >= 5,               "default capabilities populated (path/op/idle/init)")

-- announce
local ann = dkjson.decode(link:make_announce())
check(ann.type == "link_announce" and ann.robot_id == "rover_1",
      "announce shape")
check(ann.energy_remaining == 9500, "announce carries energy_remaining")

-- planner sends link_bridge_ack → we transition to registering and reply with confirm
local outcome = link:on_planner_verb(
    "moon_base_alpha/robots/rover_1/planner/ack",
    dkjson.encode({ type = "link_bridge_ack", robot_id = "rover_1",
                    bridge_id = "planner", ack_seq = 1, seq = 0 }))
check(outcome ~= nil, "on_planner_verb returns outcome for bridge_ack")
check(outcome.transitioned_to == "registering",
      "init + bridge_ack → registering")
check(outcome.send_payload ~= nil, "registering transition carries reply")
local confirm = dkjson.decode(outcome.send_payload)
check(confirm.type == "link_confirm" and confirm.wire_format == "json",
      "reply is link_confirm with wire_format=json")
check(#(confirm.capabilities or {}) >= 5,
      "link_confirm carries capabilities array")
check(link.state == "registering", "state advanced to registering")

-- duplicate bridge_ack while registering: idempotent (no reply)
local outcome2 = link:on_planner_verb(
    "moon_base_alpha/robots/rover_1/planner/ack",
    dkjson.encode({ type = "link_bridge_ack", robot_id = "rover_1",
                    bridge_id = "planner", ack_seq = 2, seq = 1 }))
check(outcome2 ~= nil and outcome2.transitioned_to == "registering"
   and outcome2.send_payload == nil,
      "duplicate bridge_ack is idempotent")

-- planner sends bridge_heartbeat → registering → live
local outcome3 = link:on_planner_verb(
    "moon_base_alpha/robots/rover_1/planner/heartbeat",
    dkjson.encode({ type = "link_bridge_heartbeat", seq = 5 }))
check(outcome3 ~= nil and outcome3.transitioned_to == "live",
      "bridge_heartbeat advances registering → live")
check(link.state == "live", "state = live after heartbeat")

-- heartbeat shape
local hb = dkjson.decode(link:make_heartbeat())
check(hb.type == "link_heartbeat" and hb.energy_remaining == 9500,
      "heartbeat shape")

-- disconnect
local outcome4 = link:on_planner_verb(
    "moon_base_alpha/robots/rover_1/planner/disconnect",
    dkjson.encode({ type = "link_bridge_disconnect" }))
check(outcome4 ~= nil and outcome4.transitioned_to == "disconnected",
      "planner disconnect → state=disconnected")

local d = dkjson.decode(link:make_disconnect("ctrl_c"))
check(d.type == "link_disconnect" and d.reason == "ctrl_c",
      "robot-side disconnect shape carries reason")

-- Unknown verb: nil
local none = link:on_planner_verb(
    "moon_base_alpha/robots/rover_1/planner/banana", "{}")
check(none == nil, "unknown verb → nil")

---------------------------------------------------------------------------
-- summary
---------------------------------------------------------------------------

print(string.format("\n=== SUMMARY: %d passed, %d failed ===", pass, fail))
os.exit(fail == 0 and 0 or 1)
