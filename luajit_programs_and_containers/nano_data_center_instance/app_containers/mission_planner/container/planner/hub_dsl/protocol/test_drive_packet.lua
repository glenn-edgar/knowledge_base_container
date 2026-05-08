#!/usr/bin/env luajit
--[[
    test_drive_packet.lua -- round-trip + size + activate_action tests
    for the new Phase 2 composite packets.

    Coverage:
      - encode/decode every sub-segment kind (5 individually + 1 mixed)
      - wall_follow + line_follow with both line and spline base kinds
      - cmd_activate_action_t with various param shapes
      - byte-size sanity: a 5-segment drive packet stays under ~500 bytes
      - decode-side validation: corrupted packet (bad packet_type) rejected

    Round-trip preservation is the regression net (any change to encoder
    semantics or wire shape breaks round-trip equality just as well as
    a byte-exact hex golden would, without the dkjson key-ordering
    flakiness that makes byte-exact CBOR golden tests brittle).

    Usage (from .../mission_planner/container/planner/):
      LD_LIBRARY_PATH=$(realpath ../prebuilt_libs) \
        luajit hub_dsl/protocol/test_drive_packet.lua
]]

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
-- planner/hub_dsl/protocol/ -> planner/lib/
local LIB_DIR = SCRIPT_DIR .. "../../lib/"
package.path = LIB_DIR    .. "?.lua;"
            .. SCRIPT_DIR .. "?.lua;"
            .. package.path

local cmds    = require("command_packets")
local encoder = require("encoder")

-- depth-aware deep equality
local function deep_eq(a, b, path)
  path = path or "<root>"
  if type(a) ~= type(b) then
    return false, string.format("%s: type %s != %s", path, type(a), type(b))
  end
  if type(a) ~= "table" then
    if a ~= b then
      return false, string.format("%s: %s != %s", path, tostring(a), tostring(b))
    end
    return true
  end
  for k, v in pairs(a) do
    local ok, err = deep_eq(v, b[k], path .. "." .. tostring(k))
    if not ok then return false, err end
  end
  for k, _ in pairs(b) do
    if a[k] == nil then
      return false, string.format("%s.%s: extra key on rhs", path, tostring(k))
    end
  end
  return true
end

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then
    pass = pass + 1
    print("  ok  " .. name)
  else
    fail = fail + 1
    print("  FAIL " .. name .. (detail and (" -- " .. detail) or ""))
  end
end

local function round_trip_drive(name, packet)
  local enc_ok, bytes = pcall(encoder.encode_drive, packet)
  if not enc_ok then
    ok("encode " .. name, false, tostring(bytes)); return
  end
  ok("encode " .. name, true)
  local dec_ok, decoded = pcall(encoder.decode_drive, bytes)
  if not dec_ok then
    ok("decode " .. name, false, tostring(decoded)); return
  end
  ok("decode " .. name, true)
  local eq, err = deep_eq(packet, decoded)
  ok("round-trip equality " .. name, eq, err)
  return bytes, decoded
end

local function round_trip_aa(name, packet)
  local enc_ok, bytes = pcall(encoder.encode_activate_action, packet)
  if not enc_ok then
    ok("encode " .. name, false, tostring(bytes)); return
  end
  ok("encode " .. name, true)
  local dec_ok, decoded = pcall(encoder.decode_activate_action, bytes)
  if not dec_ok then
    ok("decode " .. name, false, tostring(decoded)); return
  end
  ok("decode " .. name, true)
  local eq, err = deep_eq(packet, decoded)
  ok("round-trip equality " .. name, eq, err)
  return bytes, decoded
end

local function frame(segs)
  return { packet_type = cmds.TYPE_DRIVE, packet_id = 1,
           start_pos = { x = 0, y = 0, heading = 0 },
           default_speed = 0.30, stop_at_end = true,
           segments = segs }
end

print("== round-trip: each sub-segment kind individually ==")
round_trip_drive("straight_line",
  frame({{ kind = "straight_line", end_pos = { x = 1.0, y = 0.0 } }}))
round_trip_drive("spline",
  frame({{ kind = "spline", end_pos = { x = 2.0, y = 1.0 },
           end_heading = 1.5708 }}))
round_trip_drive("rotate",
  frame({{ kind = "rotate", end_heading = 3.14159 }}))
round_trip_drive("wall_follow with straight_line base",
  frame({{ kind = "wall_follow",
           base = { kind = "straight_line", end_pos = { x = 5.0, y = 0.0 } },
           offset = 0.30 }}))
round_trip_drive("wall_follow with spline base",
  frame({{ kind = "wall_follow",
           base = { kind = "spline", end_pos = { x = 5.0, y = 2.0 },
                    end_heading = 0.5 },
           offset = -0.45,
           speed = 0.20, direction = "reverse" }}))
round_trip_drive("line_follow with straight_line base",
  frame({{ kind = "line_follow",
           base = { kind = "straight_line", end_pos = { x = 3.0, y = 0.0 } } }}))
round_trip_drive("line_follow with spline base",
  frame({{ kind = "line_follow",
           base = { kind = "spline", end_pos = { x = 3.0, y = 1.0 },
                    end_heading = 1.0 },
           speed = 0.15, direction = "forward" }}))

print()
print("== round-trip: 5-kind mixed packet ==")
local mixed = {
  packet_type = cmds.TYPE_DRIVE, packet_id = 42, mission_id = 7,
  start_pos = { x = 0.0, y = 0.0, heading = 0.0 },
  default_speed = 0.30, stop_at_end = false,
  segments = {
    { kind = "straight_line", end_pos = { x = 1.0, y = 0.0 } },
    { kind = "spline", end_pos = { x = 2.0, y = 1.0 }, end_heading = 1.5708 },
    { kind = "rotate", end_heading = 3.14159 },
    { kind = "wall_follow",
      base = { kind = "straight_line", end_pos = { x = 4.0, y = 1.0 } },
      offset = 0.30, speed = 0.20 },
    { kind = "line_follow",
      base = { kind = "spline", end_pos = { x = 5.0, y = 2.0 },
               end_heading = 0.0 },
      direction = "reverse" },
  },
}
local mixed_bytes = round_trip_drive("5-kind mix", mixed)

print()
print("== byte-size sanity: 5-segment drive packet < 500 bytes ==")
if mixed_bytes then
  local sz = #mixed_bytes
  ok(string.format("byte size = %d bytes (target < 500)", sz), sz < 500,
     "exceeds target")
end

print()
print("== round-trip: cmd_activate_action_t ==")
round_trip_aa("recharge with target_soc param",
  { packet_type = cmds.TYPE_ACTIVATE_ACTION, packet_id = 100,
    mission_id = 7,
    action_id = "recharge", active_node_id = "dock_3",
    topics = {
      cmd    = "dock/dock_3/recharge/cmd/rover_1",
      status = "dock/dock_3/recharge/status/rover_1",
    },
    params = { target_soc = 0.85 } })

round_trip_aa("dock_in with empty params",
  { packet_type = cmds.TYPE_ACTIVATE_ACTION, packet_id = 101,
    action_id = "dock_in", active_node_id = "dock_3",
    topics = {
      cmd    = "dock/dock_3/dock_in/cmd/rover_1",
      status = "dock/dock_3/dock_in/status/rover_1",
    },
    params = {} })

round_trip_aa("dock_out with no params field",
  { packet_type = cmds.TYPE_ACTIVATE_ACTION, packet_id = 102,
    action_id = "dock_out", active_node_id = "dock_3",
    topics = {
      cmd    = "dock/dock_3/dock_out/cmd/rover_1",
      status = "dock/dock_3/dock_out/status/rover_1",
    } })

round_trip_aa("complex nested params",
  { packet_type = cmds.TYPE_ACTIVATE_ACTION, packet_id = 103,
    mission_id = 99,
    action_id = "recharge", active_node_id = "dock_alpha_port_b",
    topics = {
      cmd    = "dock/dock_alpha/port_b/recharge/cmd/hauler_07",
      status = "dock/dock_alpha/port_b/recharge/status/hauler_07",
    },
    params = {
      target_soc = 0.95,
      preferred_phase = "fast",
      bumper_check = true,
      retries_allowed = 0,
    } })

print()
print("== decode-side validation: corrupted packet rejected ==")
do
  -- encode a valid activate_action then mutate it (replace the action_id
  -- string with one that's empty). dkjson decode will succeed but
  -- validator must reject.
  local bad = {
    packet_type = cmds.TYPE_ACTIVATE_ACTION, packet_id = 999,
    action_id = "", active_node_id = "dock_3",
    topics = { cmd = "x", status = "y" },
  }
  local enc_ok = pcall(encoder.encode_activate_action, bad)
  ok("encode rejects empty action_id", not enc_ok)
end

do
  -- Wrong packet_type at decode: encode_drive a valid drive, then try to
  -- decode_activate_action on it. Validator catches.
  local p = frame({{ kind = "rotate", end_heading = 0 }})
  local bytes = encoder.encode_drive(p)
  local dec_ok, err = pcall(encoder.decode_activate_action, bytes)
  ok("decode_activate_action rejects drive bytes", not dec_ok)
end

print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
