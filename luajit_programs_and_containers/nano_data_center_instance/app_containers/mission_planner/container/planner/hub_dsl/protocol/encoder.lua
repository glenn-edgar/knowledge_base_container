--[[
    encoder.lua -- CBOR encode/decode for Phase 2 composite packets.

    Pipeline:
        encode:  Lua table -> validate -> dkjson.encode -> cbor.encode -> bytes
        decode:  bytes     -> cbor.decode -> dkjson.decode -> validate -> Lua table

    Two-stage encode keeps shape policing in command_packets.lua and the
    wire codec in lua_cbor (FFI). Validation runs on BOTH sides so a
    corrupt packet at decode time fails loud (no silent half-formed
    drive packets reaching the chain-tree decomposer).

    Per-type encoders/decoders so callers explicitly state intent and
    the validator can pin specific errors. A generic dispatch by
    packet_type is intentionally NOT provided -- the caller usually
    knows what shape they want, and a wrong-type packet should fail at
    the validator, not at a guess.

    Used by:
      - planner route_builder (Phase 5) when emitting drive + activate
        packets to the robot
      - robot-side decode (Phase 3a) when receiving them
]]

local cbor   = require("lib.lua_cbor")
local dkjson = require("dkjson")
local cmds   = require("command_packets")

local M = {}

------------------------------------------------------------------------
-- cmd_drive_t
------------------------------------------------------------------------

function M.encode_drive(packet)
  cmds.validate_drive(packet)
  local json = dkjson.encode(packet)
  return cbor.encode(json)
end

function M.decode_drive(bytes)
  local json = cbor.decode(bytes)
  local p, _, err = dkjson.decode(json)
  if not p then error("decode_drive: bad JSON after CBOR -- " .. tostring(err)) end
  cmds.validate_drive(p)
  return p
end

------------------------------------------------------------------------
-- cmd_activate_action_t
------------------------------------------------------------------------

function M.encode_activate_action(packet)
  cmds.validate_activate_action(packet)
  local json = dkjson.encode(packet)
  return cbor.encode(json)
end

function M.decode_activate_action(bytes)
  local json = cbor.decode(bytes)
  local p, _, err = dkjson.decode(json)
  if not p then
    error("decode_activate_action: bad JSON after CBOR -- " .. tostring(err))
  end
  cmds.validate_activate_action(p)
  return p
end

------------------------------------------------------------------------
-- helpers (test + debug)
------------------------------------------------------------------------

-- Hex-encode a Lua byte string. Used by the test harness to print
-- golden CBOR fixtures.
function M.bytes_to_hex(bytes)
  local out = {}
  for i = 1, #bytes do
    out[i] = string.format("%02x", string.byte(bytes, i))
  end
  return table.concat(out)
end

-- Inverse of bytes_to_hex.
function M.hex_to_bytes(hex)
  if #hex % 2 ~= 0 then error("hex_to_bytes: odd-length input") end
  local out = {}
  for i = 1, #hex, 2 do
    out[#out + 1] = string.char(tonumber(hex:sub(i, i + 1), 16))
  end
  return table.concat(out)
end

return M
