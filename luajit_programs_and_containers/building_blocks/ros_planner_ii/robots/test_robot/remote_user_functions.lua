--[[
    remote_user_functions.lua -- ChainTree user functions for test_robot.

    Event-driven: receives ROBOT_RPC_COMMAND events from queue monitor.
    The event_data is the parsed JSON command payload.

    Receiver:  handles ROBOT_RPC_COMMAND events, starts execution
    Executor:  simulates action with tick countdown
    Streamer:  sends ack + bitmap responses via NATS transport

    Transport handle set via set_transport() before runtime starts.
]]

local defs        = require("ct_definitions")
local event_ids   = require("event_ids")
local cmd_packets = require("command_packets")
local json_util   = require("json_util")

local M = {}

-- Transport handle (set by entry point)
local transport = nil

function M.set_transport(tx)
    transport = tx
end

-- Action durations (ticks per command type)
local action_durations = {
    [cmd_packets.TYPE_INIT_CHECK]   = 2,
    [cmd_packets.TYPE_PATH_SEGMENT] = 3,
    [cmd_packets.TYPE_ROTATE]       = 2,
    [cmd_packets.TYPE_ARM]          = 4,
    [cmd_packets.TYPE_RPC_REQUEST]  = 2,
    [cmd_packets.TYPE_SENSOR_READ]  = 1,
    [cmd_packets.TYPE_IDLE]         = 1,
}

---------------------------------------------------------------------------
-- Receiver: handles incoming command events
---------------------------------------------------------------------------

M.main = {}
M.one_shot = {}
M.boolean = {}

M.main.REMOTE_RECV_MAIN = function(handle, bool_fn, node, event_id, event_data)
    if event_id == event_ids.ROBOT_RPC_COMMAND and event_data then
        local bb = handle.blackboard
        local cmd = event_data

        -- Send ack immediately via transport
        if transport then
            transport:send_stream(json_util.encode({
                type    = "ack",
                seq     = cmd.seq or 0,
                test_id = cmd.test_id or 0,
                status  = "ok",
            }))
        end

        -- Double buffer: if already executing, stage as next
        if bb.exec_active then
            bb.next_command = cmd
        else
            bb.current_command = cmd
            bb.exec_start = true
            bb.ticks_remaining = action_durations[cmd.packet_type] or 2
        end

        return defs.CFL_CONTINUE
    end

    if event_id == event_ids.ROBOT_RPC_SHUTDOWN then
        -- Send ack and flag shutdown
        if transport and event_data then
            transport:send_stream(json_util.encode({
                type = "ack", seq = event_data.seq or 0, status = "ok",
            }))
        end
        handle.blackboard.shutdown_requested = true
        return defs.CFL_CONTINUE
    end

    -- Timer tick — no-op
    if event_id == defs.CFL_TIMER_EVENT then
        return defs.CFL_CONTINUE
    end

    return defs.CFL_CONTINUE
end

M.one_shot.REMOTE_RECV_INIT = function(handle, node) end

---------------------------------------------------------------------------
-- Executor: tick countdown, sends completion via transport
---------------------------------------------------------------------------

M.main.REMOTE_EXEC_MAIN = function(handle, bool_fn, node, event_id, event_data)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard

    if bb.exec_start then
        bb.exec_start = false
        bb.exec_active = true
    end

    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false

            if transport and bb.current_command then
                local cmd = bb.current_command
                local test_id = cmd.test_id or 0

                -- Send kb_done with delta pose and success flag
                -- This is the definitive "I'm done" signal
                local kb_done = {
                    type    = "kb_done",
                    test_id = test_id,
                    success = true,  -- simulated: always succeeds
                }

                -- Compute delta pose (simulated: use command params)
                if cmd.packet_type == cmd_packets.TYPE_PATH_SEGMENT then
                    kb_done.delta_x = (cmd.to_x or 0) - (cmd.from_x or 0)
                    kb_done.delta_y = (cmd.to_y or 0) - (cmd.from_y or 0)
                    kb_done.delta_heading = 0
                elseif cmd.packet_type == cmd_packets.TYPE_ROTATE then
                    kb_done.delta_heading = (cmd.to_heading or 0) - (cmd.from_heading or 0)
                elseif cmd.packet_type == cmd_packets.TYPE_ARM then
                    kb_done.delta_arm_angle = cmd.arm_target or 0
                end

                transport:send_stream(json_util.encode(kb_done))

                -- Check for next command (double buffer)
                if bb.next_command then
                    bb.current_command = bb.next_command
                    bb.next_command = nil
                    bb.exec_start = true
                    bb.ticks_remaining = action_durations[bb.current_command.packet_type] or 2
                end
            end
        end
    end

    return defs.CFL_CONTINUE
end

M.one_shot.REMOTE_EXEC_INIT = function(handle, node)
    handle.blackboard.exec_active = false
    handle.blackboard.ticks_remaining = 0
end

M.one_shot.REMOTE_EXEC_TERM = function(handle, node)
    handle.blackboard.exec_active = false
end

---------------------------------------------------------------------------
-- Streamer: no-op for now (ack/bitmap sent directly by recv/exec)
---------------------------------------------------------------------------

M.main.REMOTE_STREAM_MAIN = function(handle, bool_fn, node, event_id, event_data)
    return defs.CFL_CONTINUE
end

M.one_shot.REMOTE_STREAM_INIT = function(handle, node) end

---------------------------------------------------------------------------
-- Registry
---------------------------------------------------------------------------
M.registry = {
    main     = M.main,
    one_shot = M.one_shot,
    boolean  = M.boolean,
}

return M
