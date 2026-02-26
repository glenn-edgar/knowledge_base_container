local ColumnFlow = require("lua_support.column_flow")

local Streaming = setmetatable({}, { __index = ColumnFlow })
Streaming.__index = Streaming

function Streaming.new(ctb)
    local self = ColumnFlow.new(ctb)
    return setmetatable(self, Streaming)
end

function Streaming:make_port(file_name, handler_id, event)
    if type(file_name) ~= "string" then
        error("File name must be a string")
    end
    if type(handler_id) ~= "number" then
        error("Handler id must be a number")
    end
    if type(event) ~= "string" then
        error("Event must be a string")
    end
    local event_id = self.ctb:register_event(event)
    return { file_name = file_name, handler_id = handler_id, event_id = event_id }
end

function Streaming:asm_streaming_emit_packet(aux_function, aux_function_data, event_column, outport)
    local event_column_id = self.ctb:get_node_index(event_column)
    local node_data = {
        aux_data = aux_function_data,
        event_id = outport.event_id,
        outport = outport,
        event_column = event_column_id,
    }
    self:asm_one_shot_handler(aux_function, node_data)
end

function Streaming:asm_streaming_sink_packet(aux_function, aux_function_data, inport)
    local node_data = {
        aux_data = aux_function_data,
        event_id = inport.event_id,
        inport = inport,
    }
    self:define_column_link(
        "CFL_STREAMING_SINK_PACKET",
        "CFL_STREAMING_SINK_PACKET_INIT",
        aux_function,
        "CFL_STREAMING_SINK_PACKET_TERM",
        node_data
    )
end

function Streaming:asm_streaming_transform_packet(aux_function, aux_function_data, inport, outport, output_event_column)
    local output_event_column_id = self.ctb:get_node_index(output_event_column)
    local node_data = {
        aux_data = aux_function_data,
        event_id = inport.event_id,
        inport = inport,
        output_event_id = outport.event_id,
        outport = outport,
        output_event_column_id = output_event_column_id,
    }
    self:define_column_link(
        "CFL_STREAMING_TRANSFORM_PACKET",
        "CFL_STREAMING_TRANSFORM_PACKET_INIT",
        aux_function,
        "CFL_STREAMING_TRANSFORM_PACKET_TERM",
        node_data
    )
end

function Streaming:asm_streaming_filter_packet(aux_function, aux_function_data, inport)
    local node_data = {
        aux_data = aux_function_data,
        event_id = inport.event_id,
        inport = inport,
    }
    self:define_column_link(
        "CFL_STREAMING_FILTER_PACKET",
        "CFL_STREAMING_FILTER_PACKET_INIT",
        aux_function,
        "CFL_STREAMING_FILTER_PACKET_TERM",
        node_data
    )
end

--- Collector node - multiple verified inports, event-only output (no schema verification).
function Streaming:asm_streaming_collect_packets(aux_function, aux_function_data, inports, output_event, output_event_column)
    local output_event_column_id = self.ctb:get_node_index(output_event_column)
    local output_event_id = self.ctb:register_event(output_event)
    local node_data = {
        aux_data = aux_function_data,
        inports = inports,
        output_event_id = output_event_id,
        output_event_column_id = output_event_column_id,
    }
    self:define_column_link(
        "CFL_STREAMING_COLLECT_PACKETS",
        "CFL_STREAMING_COLLECT_PACKETS_INIT",
        aux_function,
        "CFL_STREAMING_COLLECT_PACKETS_TERM",
        node_data
    )
end

function Streaming:asm_streaming_tap_packet(aux_function, aux_function_data, inport)
    local node_data = {
        aux_data = aux_function_data,
        event_id = inport.event_id,
        inport = inport,
    }
    self:define_column_link(
        "CFL_STREAMING_TAP_PACKET",
        "CFL_STREAMING_TAP_PACKET_INIT",
        aux_function,
        "CFL_STREAMING_TAP_PACKET_TERM",
        node_data
    )
end

--- Sink for collector output packets - no port/schema verification, just event matching.
function Streaming:asm_streaming_sink_collected_packets(aux_function, aux_function_data, event_name)
    local event_id = self.ctb:register_event(event_name)
    local node_data = {
        aux_data = aux_function_data,
        event_id = event_id,
    }
    self:define_column_link(
        "CFL_STREAMING_SINK_COLLECTED_PACKETS",
        "CFL_STREAMING_SINK_COLLECTED_PACKETS_INIT",
        aux_function,
        "CFL_STREAMING_SINK_COLLECTED_PACKETS_TERM",
        node_data
    )
end

--- Verify packet with user-defined test function.
--- Library boolean function handles generic streaming setup (inport matching, event matching)
--- and calls user's aux_function for specific verification logic.
--- User's aux_function returns true if packet passes verification, false otherwise.
function Streaming:asm_streaming_verify_packet(aux_function, aux_function_data, inport,
                                                reset_flag, error_fn, error_data)
    if reset_flag == nil then reset_flag = false end
    error_fn = error_fn or "CFL_NULL"

    local node_data = {
        aux_data = aux_function_data,
        inport = inport,
        user_aux_function = aux_function,
    }
    self:asm_verify(
        "CFL_STREAMING_VERIFY_PACKET",
        node_data,
        reset_flag,
        error_fn,
        error_data
    )
end

return Streaming