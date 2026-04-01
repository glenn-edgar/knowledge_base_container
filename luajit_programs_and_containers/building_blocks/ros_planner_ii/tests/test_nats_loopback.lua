--[[
    test_nats_loopback.lua -- NATS JobQueue loopback test.

    Hub submits commands to rpc queue, remote claims and processes.
    Remote submits ack + bitmap to stream queue, hub claims.

    No callbacks, no sockets, no fork. Pure job queue claim/complete.
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local json_util      = require("json_util")
local cmd_packets    = require("command_packets")
local nats_transport = require("nats_transport")

local robot_id = os.getenv("ROBOT_ID") or "test_robot_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"

print("=== NATS JobQueue Loopback Test ===\n")
print(string.format("Robot: %s, Server: %s\n", robot_id, server))

-- Connect hub side
local tx = nats_transport.hub_side(robot_id, server)
tx:flush()
print("Hub connected to NATS (queues flushed).\n")

-- Give remote time to connect (started by shell script)
ffi.C.usleep(1000000)  -- 1s

---------------------------------------------------------------------------
-- Test helpers
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("  FAIL: %s — %s", name, msg or ""))
    end
end

-- Pending messages buffer (messages claimed out of order)
local pending_msgs = {}

local function claim_stream_msg(msg_type, timeout_ms)
    -- Check pending buffer first
    for i = #pending_msgs, 1, -1 do
        if pending_msgs[i].type == msg_type then
            return table.remove(pending_msgs, i)
        end
    end
    -- Poll job queue
    timeout_ms = timeout_ms or 2500
    local elapsed = 0
    while elapsed < timeout_ms do
        local msg = tx:recv_stream()
        if msg then
            local ok, parsed = pcall(json_util.decode, msg)
            if ok and parsed then
                if parsed.type == msg_type then
                    return parsed
                else
                    -- Not what we're looking for — save it
                    pending_msgs[#pending_msgs + 1] = parsed
                end
            end
        end
        ffi.C.usleep(5000)
        elapsed = elapsed + 5
    end
    return nil
end

local function test_virtual_node(name, packet_type, test_id, extra_fields)
    local cmd = {
        packet_type = packet_type,
        test_id = test_id,
        seq = test_id,
    }
    if extra_fields then
        for k, v in pairs(extra_fields) do cmd[k] = v end
    end

    -- Submit to rpc queue
    local job_id = tx:send_rpc(json_util.encode(cmd))
    check(name .. " submit", job_id ~= nil, "submit failed")

    -- Wait for ack
    local ack = claim_stream_msg("ack")
    check(name .. " ack", ack ~= nil and ack.status == "ok",
        "no ack or bad status: " .. tostring(ack and ack.status))

    -- Wait for kb_done (except idle which may not send one)
    local done_ok = true
    if packet_type ~= cmd_packets.TYPE_IDLE then
        local kb_done = claim_stream_msg("kb_done")
        done_ok = kb_done ~= nil and kb_done.success == true
        check(name .. " kb_done", done_ok,
            "no kb_done or success=" .. tostring(kb_done and kb_done.success))
    end

    if (job_id and ack and done_ok) then
        print(string.format("  PASS: %s", name))
    end
end

---------------------------------------------------------------------------
-- Test each virtual node
---------------------------------------------------------------------------

test_virtual_node("init_check",      cmd_packets.TYPE_INIT_CHECK,   1)
test_virtual_node("path_spline",     cmd_packets.TYPE_PATH_SEGMENT, 2, { nav_method = 0 })
test_virtual_node("path_line",       cmd_packets.TYPE_PATH_SEGMENT, 3, { nav_method = 1 })
test_virtual_node("path_wall",       cmd_packets.TYPE_PATH_SEGMENT, 4, { nav_method = 2 })
test_virtual_node("path_rotate",     cmd_packets.TYPE_ROTATE,       5)
test_virtual_node("deliver_part",    cmd_packets.TYPE_ARM,          6)
test_virtual_node("paint_sample",    cmd_packets.TYPE_ARM,          7)
test_virtual_node("load_shipping",   cmd_packets.TYPE_ARM,          8)
test_virtual_node("pass_gate",       cmd_packets.TYPE_RPC_REQUEST,  9)
test_virtual_node("inspection_scan", cmd_packets.TYPE_SENSOR_READ,  10)
test_virtual_node("idle",            cmd_packets.TYPE_IDLE,         11)

---------------------------------------------------------------------------
-- Shutdown remote
---------------------------------------------------------------------------
tx:send_rpc(json_util.encode({
    packet_type = cmd_packets.TYPE_SHUTDOWN,
    seq = 99,
}))
ffi.C.usleep(500000)

tx:close()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
