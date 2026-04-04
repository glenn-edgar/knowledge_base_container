--[[
    json_streaming.lua — JSON packet streaming DSL mixin

    Provides asm_json_emit_oneshot, asm_json_sink, asm_json_transform,
    and asm_json_dispatch methods for pure JSON text packets.

    No schema definitions. No ports. Packets are arbitrary JSON strings
    routed by event name. Dispatch routes by "type" field with mandatory
    default column.

    Emit is a one-shot: the JSON string in the DSL IS the packet.
    Uses cfl_allocate_state/cfl_smart_arena_alloc — safe for reset loops.

    Directory: lua_dsl/lua_support/
]]

local ColumnFlow = require("lua_support.column_flow")
local fnv1a      = require("lua_support.fnv1a")

-- JSON encoder: Lua table → JSON string
-- Handles strings, numbers, booleans, arrays, and objects.
local function lua_to_json(val)
    local t = type(val)
    if t == "string" then
        -- Escape special chars
        local escaped = val:gsub('\\', '\\\\'):gsub('"', '\\"')
                           :gsub('\n', '\\n'):gsub('\r', '\\r')
                           :gsub('\t', '\\t')
        return '"' .. escaped .. '"'
    elseif t == "number" then
        return tostring(val)
    elseif t == "boolean" then
        return val and "true" or "false"
    elseif t == "table" then
        -- Detect array vs object: array if sequential integer keys from 1
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

local JsonStreaming = setmetatable({}, { __index = ColumnFlow })
JsonStreaming.__index = JsonStreaming

function JsonStreaming.new(ctb)
    local self = ColumnFlow.new(ctb)
    return setmetatable(self, JsonStreaming)
end

--------------------------------------------------------------------------------
-- asm_json_emit_oneshot — Queue a JSON packet as a one-shot
--
-- packet_data:   Lua table OR raw JSON string. If table, auto-serialized.
-- event_column:  Column handle where packet is emitted
-- event_name:    Event name string for routing
--
-- The JSON string IS the packet. The C one-shot copies it to an arena
-- buffer and queues it. Universal — no interpretation.
-- Uses cfl_allocate_state — safe for CFL_RESET loops.
--------------------------------------------------------------------------------
function JsonStreaming:asm_json_emit_oneshot(packet_data, event_column, event_name)
    if type(event_name) ~= "string" then
        error("asm_json_emit_oneshot: event_name must be a string")
    end

    -- Accept table or string
    local packet_text
    if type(packet_data) == "table" then
        packet_text = lua_to_json(packet_data)
    elseif type(packet_data) == "string" then
        packet_text = packet_data
    else
        error("asm_json_emit_oneshot: packet_data must be a table or JSON string")
    end

    local event_column_id = self.ctb:get_node_index(event_column)
    local event_id = self.ctb:register_event(event_name)

    local node_data = {
        packet_text = packet_text,
        event_id = event_id,
        event_column_id = event_column_id,
    }

    -- One-shot with term: CFL_DISABLE main, init fires emit, term frees heap buffer
    self:define_column_link("CFL_DISABLE", "CFL_JSON_EMIT", "CFL_NULL", "CFL_JSON_EMIT_TERM", node_data)
end

--------------------------------------------------------------------------------
-- asm_json_sink — Consume JSON packets
--
-- aux_function:      User boolean function name (processes parsed packet)
-- aux_function_data: Table passed as node_dict.aux_data
-- event_name:        Event name to listen for
--------------------------------------------------------------------------------
function JsonStreaming:asm_json_sink(aux_function, aux_function_data, event_name)
    if type(event_name) ~= "string" then
        error("asm_json_sink: event_name must be a string")
    end
    if type(aux_function) ~= "string" then
        error("asm_json_sink: aux_function must be a string")
    end

    local event_id = self.ctb:register_event(event_name)

    local node_data = {
        aux_data = aux_function_data,
        event_id = event_id,
    }

    self:define_column_link(
        "CFL_JSON_SINK",
        "CFL_JSON_SINK_INIT",
        aux_function,
        "CFL_JSON_SINK_TERM",
        node_data
    )
end

--------------------------------------------------------------------------------
-- asm_json_transform — Receive JSON, transform, emit new JSON
--
-- aux_function:      User boolean (reads input, writes output packet)
-- aux_function_data: Table passed as node_dict.aux_data
-- input_event_name:  Event name to listen for
-- output_event_name: Event name for emitted output
-- output_event_column: Column handle for output events
--------------------------------------------------------------------------------
function JsonStreaming:asm_json_transform(aux_function, aux_function_data,
                                          input_event_name, output_event_name,
                                          output_event_column)
    if type(input_event_name) ~= "string" then
        error("asm_json_transform: input_event_name must be a string")
    end
    if type(output_event_name) ~= "string" then
        error("asm_json_transform: output_event_name must be a string")
    end
    if type(aux_function) ~= "string" then
        error("asm_json_transform: aux_function must be a string")
    end

    local input_event_id = self.ctb:register_event(input_event_name)
    local output_event_id = self.ctb:register_event(output_event_name)
    local output_event_column_id = self.ctb:get_node_index(output_event_column)

    local node_data = {
        aux_data = aux_function_data,
        input_event_id = input_event_id,
        output_event_id = output_event_id,
        output_event_column_id = output_event_column_id,
    }

    self:define_column_link(
        "CFL_JSON_TRANSFORM",
        "CFL_JSON_TRANSFORM_INIT",
        aux_function,
        "CFL_JSON_TRANSFORM_TERM",
        node_data
    )
end

--------------------------------------------------------------------------------
-- asm_json_dispatch — Route JSON packets by "type" field
--
-- aux_function_data: Table passed as node_dict.aux_data
-- event_name:        Event name to listen for
-- routes:            Array of {type = "name", column = column_handle}
-- default_column:    Mandatory column handle for unmatched types
--------------------------------------------------------------------------------
function JsonStreaming:asm_json_dispatch(aux_function_data,
                                         event_name, output_event_name,
                                         routes, default_column)
    if type(event_name) ~= "string" then
        error("asm_json_dispatch: event_name must be a string")
    end
    if type(output_event_name) ~= "string" then
        error("asm_json_dispatch: output_event_name must be a string")
    end
    if type(routes) ~= "table" then
        error("asm_json_dispatch: routes must be a table")
    end
    if default_column == nil then
        error("asm_json_dispatch: default_column is mandatory")
    end

    local event_id = self.ctb:register_event(event_name)
    local output_event_id = self.ctb:register_event(output_event_name)
    local default_column_id = self.ctb:get_node_index(default_column)

    -- Build route entries with pre-computed FNV-1a hashes
    local route_entries = {}
    for i, route in ipairs(routes) do
        if type(route.type) ~= "string" then
            error("asm_json_dispatch: route[" .. i .. "].type must be a string")
        end
        if route.column == nil then
            error("asm_json_dispatch: route[" .. i .. "].column is required")
        end
        route_entries[i] = {
            type_hash = fnv1a.fnv1a_32(route.type),
            column_id = self.ctb:get_node_index(route.column),
        }
    end

    local node_data = {
        aux_data = aux_function_data,
        event_id = event_id,
        output_event_id = output_event_id,
        default_column_id = default_column_id,
        route_count = #route_entries,
        routes = route_entries,
    }

    -- Dispatch has no user boolean — use CFL_NULL
    self:define_column_link(
        "CFL_JSON_DISPATCH",
        "CFL_JSON_DISPATCH_INIT",
        "CFL_NULL",
        "CFL_JSON_DISPATCH_TERM",
        node_data
    )
end

--------------------------------------------------------------------------------
-- make_json_control_port — Create a simplified port for JSON controlled nodes
--
-- Only event_id matters. schema_hash and handler_id are set to 0 for
-- compatibility with _validate_port_defined.
--
-- event_name: Event name string
-- returns: Port table compatible with controlled_nodes.lua validation
--------------------------------------------------------------------------------
function JsonStreaming:make_json_control_port(event_name)
    if type(event_name) ~= "string" then
        error("make_json_control_port: event_name must be a string")
    end
    local event_id = self.ctb:register_event(event_name)
    return { schema_hash = 0, handler_id = 0, event_id = event_id }
end

--------------------------------------------------------------------------------
-- json_controlled_node — Define a JSON-based controlled (dead) node
--
-- Same pattern as controlled_node but uses CFL_JSON_CONTROLLED_NODE_* functions.
-- No Avro schema matching — event_id routing only.
--
-- api_name:           Registry key for client binding (must be unique)
-- column_name:        Identifier in the tree
-- aux_function_name:  User boolean C function name
-- aux_data:           Static configuration table
-- request_port:       Port from make_json_control_port
-- response_port:      Port from make_json_control_port
--------------------------------------------------------------------------------
function JsonStreaming:json_controlled_node(api_name, column_name, aux_function_name, aux_data,
                                            request_port, response_port)
    self:_validate_port_defined(request_port, "request_port", column_name)
    self:_validate_port_defined(response_port, "response_port", column_name)

    -- Validate parent is a container
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
        "CFL_JSON_CONTROLLED_NODE_MAIN",
        "CFL_JSON_CONTROLLED_NODE_INIT",
        "CFL_JSON_CONTROLLED_NODE_TERM",
        aux_function_name,
        column_data,
        false   -- dead node
    )

    self.ctb:register_node_alias(api_name, return_value)
    return return_value
end

--------------------------------------------------------------------------------
-- json_client_controlled_node — Define a JSON client that controls a dead node
--
-- Same pattern as client_controlled_node but uses CFL_JSON_CLIENT_CONTROLLED_NODE_*
-- functions. request_text is pre-built from aux_data at DSL time.
--
-- api_name:           Registry key matching a json_controlled_node
-- aux_function_name:  User boolean (called on response event)
-- aux_data:           Table — serialized to JSON as request_text
-- request_port:       Port from make_json_control_port
-- response_port:      Port from make_json_control_port
--------------------------------------------------------------------------------
function JsonStreaming:json_client_controlled_node(api_name, aux_function_name, aux_data,
                                                    request_port, response_port)
    self:_validate_port_defined(request_port, "request_port", api_name)
    self:_validate_port_defined(response_port, "response_port", api_name)

    -- Get server node
    local server_ltree = self.ctb:get_ltree_by_alias(api_name)
    local server_node_index = self.ctb:get_node_by_alias(api_name)

    -- Build request text from aux_data at DSL time
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
        "CFL_JSON_CLIENT_CONTROLLED_NODE_MAIN",
        "CFL_JSON_CLIENT_CONTROLLED_NODE_INIT",
        aux_function_name,
        "CFL_JSON_CLIENT_CONTROLLED_NODE_TERM",
        node_data,
        "CLIENT"
    )
end

return JsonStreaming
