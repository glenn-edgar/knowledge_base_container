#!/usr/bin/env luajit
-- =============================================================================
-- test_hub_runtime_send_drive.lua -- Phase 5 C2 acceptance for
-- hub_runtime:send_drive_packet().
--
-- Coverage:
--   - happy: build a valid cmd_drive_t via route_builder + ship via
--     hub_runtime:send_drive_packet -> stub transport receives bytes
--     that round-trip via encoder.decode_drive to the same packet
--   - return value is packet.packet_id
--   - multiple sends -> one tx publish per call, monotonic packet_id
--   - mutation: invalid packet (corrupted packet_type) -> validate_drive
--     errors at send time, transport NOT called
--   - mutation: missing required field -> errors before tx
--
-- Required env at run:
--   LD_LIBRARY_PATH=<prebuilt_libs> for liblua_cbor.so
--
-- Usage (from system_node_control/):
--   PLANNER=../../../nano_data_center_instance/app_containers/mission_planner
--   LD_LIBRARY_PATH="$(realpath $PLANNER/container/prebuilt_libs)" \
--     luajit construction/tests/test_hub_runtime_send_drive.lua
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"

-- system_node_control/construction/tests/  ->  repo root
local REPO_ROOT = SCRIPT_DIR .. "../../../../../"
local PLANNER   = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
local LUA_SHARE = REPO_ROOT
    .. "nano_data_center_base/luajit/luajit_base/container/prebuilt_lua_share"

-- planner/lib       -> hub_runtime, route_builder, kb_query, lua_cbor (via "lib.")
-- planner/hub_dsl/  -> hub_control, event_ids, command_packets, encoder
-- prebuilt_lua_share -> dkjson
-- prebuilt_lua_share/chain_tree/lua_dsl/luajit_pipeline -> json_util
package.path = PLANNER   .. "/lib/?.lua;"
            .. PLANNER   .. "/hub_dsl/protocol/?.lua;"
            .. PLANNER   .. "/hub_dsl/hub_functions/?.lua;"
            .. LUA_SHARE .. "/?.lua;"
            .. LUA_SHARE .. "/chain_tree/lua_dsl/luajit_pipeline/?.lua;"
            .. package.path

local hub_runtime = require("hub_runtime")
local rb          = require("route_builder")
local cmds        = require("command_packets")
local encoder     = require("encoder")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end
local function expect_error(name, fn, want)
  local good, err = pcall(fn)
  if good then fail = fail + 1; print("  FAIL " .. name .. " -- expected error containing " .. tostring(want)); return end
  if want and not tostring(err):find(want, 1, true) then
    fail = fail + 1; print("  FAIL " .. name .. " -- err " .. tostring(err) .. " missing " .. want); return
  end
  pass = pass + 1; print("  ok  " .. name)
end

------------------------------------------------------------------------
-- Stub transport: capture every send_rpc call's bytes.
------------------------------------------------------------------------
local function make_stub_tx()
  local sent = {}
  return {
    sent = sent,
    send_rpc = function(self, bytes) sent[#sent + 1] = bytes end,
    recv_stream = function() return nil end,
    close = function() end,
  }
end

------------------------------------------------------------------------
-- Minimal hub_runtime instance: no pg_conn, stub transport.
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
-- Fixture graph: same shape as C1 test (3 nodes, 2 edges).
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

local function deep_eq(a, b, path)
  path = path or "<root>"
  if type(a) ~= type(b) then
    return false, string.format("%s: type %s != %s", path, type(a), type(b))
  end
  if type(a) ~= "table" then
    -- Floats lose a few ulps through dkjson encode/decode; tolerate.
    if type(a) == "number" then
      if math.abs(a - b) > 1e-9 then
        return false, string.format("%s: %s != %s", path, tostring(a), tostring(b))
      end
      return true
    end
    if a ~= b then
      return false, string.format("%s: %s != %s", path, tostring(a), tostring(b))
    end
    return true
  end
  for k, v in pairs(a) do
    local sub_ok, err = deep_eq(v, b[k], path .. "." .. tostring(k))
    if not sub_ok then return false, err end
  end
  for k, _ in pairs(b) do
    if a[k] == nil then
      return false, string.format("%s.%s: extra key on rhs", path, tostring(k))
    end
  end
  return true
end

------------------------------------------------------------------------
print("== happy: build via route_builder + send via hub_runtime ==")
------------------------------------------------------------------------

do
  local pkts = rb.build_drive_packets({"n1", "n2", "n3"}, FIXTURE_GRAPH,
    { packet_id_start = 100, mission_id = 7, initial_heading = 0 })
  ok("two packets built (sanity)", #pkts == 2)

  local tx = make_stub_tx()
  local hub = make_hub_rt(tx)

  local rid1 = hub:send_drive_packet(pkts[1])
  local rid2 = hub:send_drive_packet(pkts[2])

  ok("first send returned packet_id 100", rid1 == 100)
  ok("second send returned packet_id 101", rid2 == 101)
  ok("transport got 2 publishes", #tx.sent == 2)
  ok("publish 1 is non-empty bytes",
     type(tx.sent[1]) == "string" and #tx.sent[1] > 0)
  ok("publish 2 is non-empty bytes",
     type(tx.sent[2]) == "string" and #tx.sent[2] > 0)
end

------------------------------------------------------------------------
print()
print("== wire round-trip: bytes -> decode_drive -> equal packet ==")
------------------------------------------------------------------------

do
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 42, mission_id = 99 })
  local tx = make_stub_tx()
  local hub = make_hub_rt(tx)

  hub:send_drive_packet(pkts[1])
  ok("transport got 1 publish", #tx.sent == 1)

  local decoded = encoder.decode_drive(tx.sent[1])
  ok("decoded packet_type matches", decoded.packet_type == cmds.TYPE_DRIVE)
  ok("decoded packet_id matches",   decoded.packet_id == 42)
  ok("decoded mission_id matches",  decoded.mission_id == 99)
  ok("decoded segment count matches",
     #decoded.segments == #pkts[1].segments)

  local eq, err = deep_eq(pkts[1], decoded)
  ok("decoded packet deep-equals sent packet", eq, err)
end

------------------------------------------------------------------------
print()
print("== bytes equal direct encoder.encode_drive (no extra wrapping) ==")
------------------------------------------------------------------------

do
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH,
    { packet_id_start = 1 })
  local direct = encoder.encode_drive(pkts[1])

  local tx = make_stub_tx()
  local hub = make_hub_rt(tx)
  hub:send_drive_packet(pkts[1])

  ok("hub_runtime ships exactly what encoder produces",
     tx.sent[1] == direct,
     string.format("len hub=%d direct=%d", #tx.sent[1], #direct))
end

------------------------------------------------------------------------
print()
print("== mutations: invalid packet errors before tx is touched ==")
------------------------------------------------------------------------

do
  -- corrupted packet_type
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH)
  local bad1 = pkts[1]
  bad1.packet_type = 999    -- not TYPE_DRIVE

  local tx = make_stub_tx()
  local hub = make_hub_rt(tx)

  expect_error("bad packet_type rejected", function()
    hub:send_drive_packet(bad1)
  end, "packet_type")
  ok("transport NOT called on validation failure", #tx.sent == 0)
end

do
  -- missing required field (packet_id)
  local bad = { packet_type = cmds.TYPE_DRIVE,
                start_pos = { x = 0, y = 0, heading = 0 },
                default_speed = 0.3,
                stop_at_end = true,
                segments = { { kind = "straight_line",
                               end_pos = { x = 1, y = 0 } } } }
  local tx = make_stub_tx()
  local hub = make_hub_rt(tx)

  expect_error("missing packet_id rejected", function()
    hub:send_drive_packet(bad)
  end, "packet_id")
  ok("transport NOT called on missing packet_id", #tx.sent == 0)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
