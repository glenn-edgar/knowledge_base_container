--[[
    remote_nats.lua -- NATS JobQueue-based remote robot process.

    Standalone process. Connects to NATS independently.
    Claims RPC commands from job queue, submits stream responses.

    Usage: luajit remote_nats.lua <robot_id>
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local robot_id = arg and arg[1] or "test_robot_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"

io.stderr:write(string.format("REMOTE [%s]: connecting to %s\n", robot_id, server))

local nats_transport = require("nats_transport")
local json_util      = require("json_util")
local cmd_packets    = require("command_packets")

local tx = nats_transport.remote_side(robot_id, server)
tx:flush()  -- drain stale jobs

io.stderr:write(string.format("REMOTE [%s]: connected, waiting for commands\n", robot_id))
io.stderr:flush()

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

-- Execution state
local exec_active = false
local ticks_remaining = 0
local current_cmd = nil

-- Main loop
while true do
    -- Check for incoming command
    local data = tx:recv_rpc()
    if data then
        local ok, cmd = pcall(json_util.decode, data)
        if ok and cmd then
            if cmd.packet_type == cmd_packets.TYPE_SHUTDOWN then
                -- Send ack before exiting
                tx:send_stream(json_util.encode({
                    type = "ack", seq = cmd.seq or 0, status = "ok",
                }))
                io.stderr:write(string.format("REMOTE [%s]: shutdown\n", robot_id))
                io.stderr:flush()
                break
            end

            -- Send ack
            tx:send_stream(json_util.encode({
                type = "ack",
                seq = cmd.seq or 0,
                test_id = cmd.test_id or 0,
                status = "ok",
            }))

            -- Start execution
            current_cmd = cmd
            exec_active = true
            ticks_remaining = action_durations[cmd.packet_type] or 2
        end
    end

    -- Tick execution
    if exec_active then
        ticks_remaining = ticks_remaining - 1
        if ticks_remaining <= 0 then
            exec_active = false

            -- Send kb_done with delta pose and success
            local kb_done = {
                type    = "kb_done",
                test_id = current_cmd.test_id or 0,
                success = true,
            }
            if current_cmd.packet_type == cmd_packets.TYPE_PATH_SEGMENT then
                kb_done.delta_x = (current_cmd.to_x or 0) - (current_cmd.from_x or 0)
                kb_done.delta_y = (current_cmd.to_y or 0) - (current_cmd.from_y or 0)
            elseif current_cmd.packet_type == cmd_packets.TYPE_ROTATE then
                kb_done.delta_heading = (current_cmd.to_heading or 0) - (current_cmd.from_heading or 0)
            elseif current_cmd.packet_type == cmd_packets.TYPE_ARM then
                kb_done.delta_arm_angle = current_cmd.arm_target or 0
            end
            tx:send_stream(json_util.encode(kb_done))
        end
    end

    ffi.C.usleep(1000)  -- 1ms tick
end

tx:close()
