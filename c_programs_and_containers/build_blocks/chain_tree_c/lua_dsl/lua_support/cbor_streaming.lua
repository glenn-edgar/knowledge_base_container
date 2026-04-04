--[[
    cbor_streaming.lua — CBOR packet streaming DSL mixin

    Mirrors json_streaming.lua but uses CBOR CFL functions.
    Packets are defined as Lua tables (or JSON strings) in the DSL,
    serialized to JSON text in the node_dict. At runtime, CFL functions
    convert JSON→CBOR for emission and CBOR→JSON for sink consumption.
    User boolean functions receive cfl_json_packet_t — identical interface.

    DSL helpers:
      asm_cbor_emit_oneshot  — Queue a packet (JSON→CBOR on wire)
      asm_cbor_sink          — Consume CBOR packets (→JSON for user boolean)
      make_cbor_control_port — Create a port for CBOR controlled nodes
      cbor_controlled_node   — Define a CBOR server (dead node)
      cbor_client_controlled_node — Define a CBOR client

    Directory: lua_dsl/lua_support/
]]

local ColumnFlow = require("lua_support.column_flow")
local fnv1a      = require("lua_support.fnv1a")

-- JSON encoder: Lua table → JSON string (same as json_streaming.lua)
local function lua_to_json(val)
    local t = type(val)
    if t == "string" then
        local escaped = val:gsub('\\', '\\\\'):gsub('"', '\\"')
                           :gsub('\n', '\\n'):gsub('\r', '\\r')
                           :gsub('\t', '\\t')
        return '"' .. escaped .. '"'
    elseif t == "number" then
        return tostring(val)
    elseif t == "boolean" then
        return val and "true" or "false"
    elseif t == "table" then
        local is_array = true
        local max_idx = 0
        for k, _ in pairs(val) do
            if type(k) == "number" and k == math.floor(k) and k > 0 then
                if k > max_idx then max_idx = k end
            else
                is_array = false
                break
            end
        end
        if is_array and max_idx == #val and max_idx > 0 then
            local parts = {}
            for i = 1, #val do
                parts[i] = lua_to_json(val[i])
            end
            return "[" .. table.concat(parts, ",") .. "]"
        else
            local parts = {}
            for k, v in pairs(val) do
                parts[#parts + 1] = '"' .. tostring(k) .. '":' .. lua_to_json(v)
            end
            return "{" .. table.concat(parts, ",") .. "}"
        end
    elseif t == "nil" then
        return "null"
    else
        error("lua_to_json: unsupported type " .. t)
    end
end

local CborStreaming = setmetatable({}, { __index = ColumnFlow })
CborStreaming.__index = CborStreaming

function CborStreaming.new(ctb)
    local self = ColumnFlow.new(ctb)
    return setmetatable(self, CborStreaming)
end

--------------------------------------------------------------------------------
-- asm_cbor_emit_oneshot — Queue a CBOR-encoded packet as a one-shot
--
-- packet_data:   Lua table OR raw JSON string. Stored as JSON in node_dict.
--                At runtime, CFL_CBOR_EMIT converts JSON→CBOR before queuing.
-- event_column:  Column handle where packet is emitted
-- event_name:    Event name string for routing
--------------------------------------------------------------------------------
function CborStreaming:asm_cbor_emit_oneshot(packet_data, event_column, event_name)
    if type(event_name) ~= "string" then
        error("asm_cbor_emit_oneshot: event_name must be a string")
    end

    local packet_text
    if type(packet_data) == "table" then
        packet_text = lua_to_json(packet_data)
    elseif type(packet_data) == "string" then
        packet_text = packet_data
    else
        error("asm_cbor_emit_oneshot: packet_data must be a table or JSON string")
    end

    local event_column_id = self.ctb:get_node_index(event_column)
    local event_id = self.ctb:register_event(event_name)

    local node_data = {
        packet_text = packet_text,
        event_id = event_id,
        event_column_id = event_column_id,
    }

    self:define_column_link("CFL_DISABLE", "CFL_CBOR_EMIT", "CFL_NULL", "CFL_CBOR_EMIT_TERM", node_data)
end

--------------------------------------------------------------------------------
-- asm_cbor_sink — Consume CBOR packets
--
-- aux_function:      User boolean function name (receives parsed JSON packet)
-- aux_function_data: Table passed as node_dict.aux_data
-- event_name:        Event name to listen for
--------------------------------------------------------------------------------
function CborStreaming:asm_cbor_sink(aux_function, aux_function_data, event_name)
    if type(event_name) ~= "string" then
        error("asm_cbor_sink: event_name must be a string")
    end
    if type(aux_function) ~= "string" then
        error("asm_cbor_sink: aux_function must be a string")
    end

    local event_id = self.ctb:register_event(event_name)

    local node_data = {
        aux_data = aux_function_data,
        event_id = event_id,
    }

    self:define_column_link(
        "CFL_CBOR_SINK",
        "CFL_CBOR_SINK_INIT",
        aux_function,
        "CFL_CBOR_SINK_TERM",
        node_data
    )
end

--------------------------------------------------------------------------------
-- make_cbor_control_port — Create a simplified port for CBOR controlled nodes
--------------------------------------------------------------------------------
function CborStreaming:make_cbor_control_port(event_name)
    if type(event_name) ~= "string" then
        error("make_cbor_control_port: event_name must be a string")
    end
    local event_id = self.ctb:register_event(event_name)
    return { schema_hash = 0, handler_id = 0, event_id = event_id }
end

--------------------------------------------------------------------------------
-- cbor_controlled_node — Define a CBOR-based controlled (dead) node
--------------------------------------------------------------------------------
function CborStreaming:cbor_controlled_node(api_name, column_name, aux_function_name, aux_data,
                                            request_port, response_port)
    self:_validate_port_defined(request_port, "request_port", column_name)
    self:_validate_port_defined(response_port, "response_port", column_name)

    local ltree_stack = self.ctb.ltree_stack
    local parent_node = ltree_stack[#ltree_stack]
    local parent_data = self.ctb.yaml_data[parent_node]
    local parent_main_function = parent_data.label_dict.main_function_name
    if parent_main_function ~= "CFL_CONTROLLED_NODE_CONTAINER_MAIN" then
        error("Parent node " .. parent_node .. " is not a controlled node container")
    end

    local column_data = {
        request_port = { event_id = request_port.event_id },
        response_port = { event_id = response_port.event_id },
        aux_data = aux_data,
    }

    local return_value = self:define_column(
        column_name,
        "CFL_CBOR_CONTROLLED_NODE_MAIN",
        "CFL_CBOR_CONTROLLED_NODE_INIT",
        "CFL_CBOR_CONTROLLED_NODE_TERM",
        aux_function_name,
        column_data,
        false   -- dead node
    )

    self.ctb:register_node_alias(api_name, return_value)
    return return_value
end

--------------------------------------------------------------------------------
-- cbor_client_controlled_node — Define a CBOR client that controls a dead node
--------------------------------------------------------------------------------
function CborStreaming:cbor_client_controlled_node(api_name, aux_function_name, aux_data,
                                                    request_port, response_port)
    self:_validate_port_defined(request_port, "request_port", api_name)
    self:_validate_port_defined(response_port, "response_port", api_name)

    local server_ltree = self.ctb:get_ltree_by_alias(api_name)
    local server_node_index = self.ctb:get_node_by_alias(api_name)

    local request_text = lua_to_json(aux_data)

    local node_data = {
        request_port = { event_id = request_port.event_id },
        response_port = { event_id = response_port.event_id },
        aux_data = aux_data,
        api_name = api_name,
        server_node_index = server_node_index,
        request_text = request_text,
    }

    return self:define_column_link(
        "CFL_CBOR_CLIENT_CONTROLLED_NODE_MAIN",
        "CFL_CBOR_CLIENT_CONTROLLED_NODE_INIT",
        aux_function_name,
        "CFL_CBOR_CLIENT_CONTROLLED_NODE_TERM",
        node_data,
        "CLIENT"
    )
end

return CborStreaming
