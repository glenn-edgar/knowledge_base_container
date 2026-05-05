--[[
    packet_mapper.lua -- Generic JSON → FFI struct mapper.

    Driven by KB plugin schemas. No per-KB code needed.

    Usage:
        local mapper = require("packet_mapper")
        local pkt, size = mapper.json_to_packet(kb_def, json_table)
        -- pkt is an FFI cdata pointer, size is byte count
]]

local ffi = require("ffi")

local M = {}

-- Sequence counter (shared across all packet types)
local seq = 0

-- Type coercion for FFI struct fields
local coerce = {
    float  = function(v) return tonumber(v) or 0 end,
    double = function(v) return tonumber(v) or 0 end,
    uint8  = function(v) return tonumber(v) or 0 end,
    uint16 = function(v) return tonumber(v) or 0 end,
    uint32 = function(v) return tonumber(v) or 0 end,
    int8   = function(v) return tonumber(v) or 0 end,
    int16  = function(v) return tonumber(v) or 0 end,
    int32  = function(v) return tonumber(v) or 0 end,
    bool   = function(v) return v and 1 or 0 end,
}

local function coerce_value(val, field_type)
    local fn = coerce[field_type]
    if fn then return fn(val) end
    return tonumber(val) or 0
end

--[[
    Convert a JSON action table to an FFI command packet.

    kb_def must have:
      .packet_ctype    — FFI ctype string (e.g., "cmd_init_check_t")
      .packet_type_id  — uint32 packet type constant
      .json_schema     — list of {name, type, required, default}
      .mapping         — {json_field = struct_field} or nil for identity

    Returns: pkt (FFI cdata), size (bytes)
]]
function M.json_to_packet(kb_def, json_tbl)
    local pkt = ffi.new(kb_def.packet_ctype)

    -- Fill header
    seq = seq + 1
    pkt.header.packet_type = kb_def.packet_type_id
    pkt.header.seq = seq
    pkt.header.test_id = json_tbl.test_id or 0

    -- Fill data fields from JSON via mapping
    local mapping = kb_def.mapping or {}
    for _, field_def in ipairs(kb_def.json_schema) do
        local json_name = field_def.name
        local struct_name = mapping[json_name] or json_name
        local value = json_tbl[json_name]

        -- Check nested: planner puts some fields inside .params
        if value == nil and json_tbl.params then
            value = json_tbl.params[json_name]
        end

        -- Apply default
        if value == nil then
            value = field_def.default
        end

        -- Validate required
        if value == nil and field_def.required then
            error(string.format("packet_mapper: field '%s' required for %s",
                json_name, kb_def.name))
        end

        -- Enum lookup
        if field_def.enum and type(value) == "string" then
            value = field_def.enum[value]
            if value == nil then
                value = field_def.default or 0
            end
        end

        -- Set field
        if value ~= nil then
            pkt[struct_name] = coerce_value(value, field_def.type)
        end
    end

    return pkt, ffi.sizeof(pkt)
end

--[[
    Generate the one-shot function for a KB plugin.
    Returns a function with signature: fn(handle, node)
    that reads current_test_json, maps to packet, writes bb.command_packet.
]]
function M.make_one_shot(kb_def, json_decode)
    local name = "GENERATE_" .. string.upper(kb_def.name) .. "_AVRC_PACKET"

    local json_encode = require("json_util").encode

    local fn = function(handle, node)
        local bb = handle.blackboard
        local json_str = bb.current_test_json
        if not json_str or json_str == "" then return end

        local ok, json_tbl = pcall(json_decode, json_str)
        if not ok or not json_tbl then return end

        -- Generate FFI packet (for binary wire)
        local pkt, size = M.json_to_packet(kb_def, json_tbl)
        bb.command_packet = pkt
        bb.command_packet_size = size

        -- Also store JSON command for NATS transport
        -- (includes packet_type so remote can identify the command)
        json_tbl.packet_type = kb_def.packet_type_id
        bb.command_packet_json = json_encode(json_tbl)
    end

    return name, fn
end

return M
