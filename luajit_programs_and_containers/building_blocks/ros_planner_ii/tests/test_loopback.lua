--[[
    test_loopback.lua -- Virtual node loopback test.

    Spawns:
      - Remote process (robots/test_robot/remote_loopback.lua)
      - Hub stub (inline, sends one packet per virtual node type)

    For each virtual node type:
      1. Hub sends AVRC command packet over RPC channel
      2. Remote loopback receives it, sends ack + bitmap over stream channel
      3. Hub reads ack, verifies seq matches
      4. Hub reads bitmap, verifies expected fields

    Tests the full round-trip: hub thread → socket → remote process → socket → hub thread
]]

local ffi = require("ffi")
local vmrt           = require("vmrt_ffi")
local cmd_packets    = require("command_packets")
local stream_packets = require("stream_packets")

ffi.cdef[[ int usleep(unsigned int usec); ]]

print("=== Virtual Node Loopback Test ===\n")

---------------------------------------------------------------------------
-- Resolve paths
---------------------------------------------------------------------------
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local robot_dir  = root_dir .. "robots/test_robot/"
local runtime_dir = root_dir .. "runtime/"

local remote_script = robot_dir .. "remote_loopback.lua"

-- LUA_PATH for remote process
local remote_lua_path = table.concat({
    root_dir .. "hub_dsl/protocol/?.lua",
    runtime_dir .. "?.lua",
    "?.lua",
    "",
}, ";")

---------------------------------------------------------------------------
-- Create channel pair and spawn remote process
---------------------------------------------------------------------------
local cp = vmrt.channel_pair_create()

local remote = vmrt.remote_spawn(remote_script, remote_lua_path, cp)

-- Give remote time to start
ffi.C.usleep(100000)  -- 100ms

---------------------------------------------------------------------------
-- Hub-side socket refs (after spawn, parent keeps rpc_hub + stream_hub)
---------------------------------------------------------------------------
local rpc_sock    = cp.rpc_hub       -- hub writes RPC here
local stream_sock = cp.stream_hub    -- hub reads stream here

local function hub_send(pkt, size)
    return vmrt.lib.vmrt_socket_send(rpc_sock, pkt, size) == 0
end

local READ_BUF_SIZE = 4096
local read_buf = ffi.new("uint8_t[?]", READ_BUF_SIZE)

local function hub_recv_stream(timeout_ms)
    timeout_ms = timeout_ms or 2000
    local elapsed = 0
    while elapsed < timeout_ms do
        local len = vmrt.lib.vmrt_socket_recv_nb(stream_sock, read_buf, READ_BUF_SIZE)
        if len and len > 0 then
            return read_buf, len
        end
        ffi.C.usleep(1000)
        elapsed = elapsed + 1
    end
    return nil, 0
end

---------------------------------------------------------------------------
-- Test helpers
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        pass_count = pass_count + 1
        print(string.format("  PASS: %s", name))
    else
        fail_count = fail_count + 1
        print(string.format("  FAIL: %s — %s", name, msg or ""))
    end
end

local function test_virtual_node(name, pkt, pkt_size, expect_bitmap_field)
    -- Send command
    local sent = hub_send(pkt, pkt_size)
    check(name .. " send", sent, "send failed")
    if not sent then return end

    -- Read ack
    local data, len = hub_recv_stream()
    check(name .. " ack recv", data ~= nil, "timeout")
    if not data then return end

    local ack = stream_packets.read_ack(data, len)
    check(name .. " ack type", ack and ack.packet_type == stream_packets.TYPE_ACK,
        "expected ack type " .. stream_packets.TYPE_ACK)
    check(name .. " ack seq", ack and ack.ack_seq == pkt.header.seq,
        string.format("expected seq %d, got %d", pkt.header.seq, ack and ack.ack_seq or -1))
    check(name .. " ack ok", ack and ack.status == stream_packets.ACK_OK,
        "expected ACK_OK")

    -- Read bitmap (if expected)
    if expect_bitmap_field then
        local bdata, blen = hub_recv_stream()
        check(name .. " bitmap recv", bdata ~= nil, "timeout")
        if bdata then
            local bhdr = stream_packets.read_header(bdata, blen)
            check(name .. " bitmap type", bhdr and bhdr.packet_type == stream_packets.TYPE_BITMAP,
                "expected bitmap type")
        end
    end
end

---------------------------------------------------------------------------
-- Test each virtual node type
---------------------------------------------------------------------------

-- 1. init_check
local pkt, size = cmd_packets.make_init_check(1)
test_virtual_node("init_check", pkt, size, true)

-- 2. path_segment (spline)
pkt, size = cmd_packets.make_path_segment(2, 0, 0, 800, 0, 150, 800, 0, 1, cmd_packets.NAV_SPLINE)
test_virtual_node("path_spline", pkt, size, true)

-- 3. path_segment (line)
pkt, size = cmd_packets.make_path_segment(3, 0, 800, 0, 400, 120, 400, 0, 1, cmd_packets.NAV_LINE)
test_virtual_node("path_line", pkt, size, true)

-- 4. path_segment (wall)
pkt, size = cmd_packets.make_path_segment(4, 1600, 0, 1600, 400, 100, 400, 0, 1, cmd_packets.NAV_WALL)
test_virtual_node("path_wall", pkt, size, true)

-- 5. rotate
pkt, size = cmd_packets.make_rotate(5, 0, 90)
test_virtual_node("path_rotate", pkt, size, true)

-- 6. arm (deliver_part)
pkt, size = cmd_packets.make_arm(6, -45, 80, 0, cmd_packets.PAYLOAD_PART)
test_virtual_node("deliver_part", pkt, size, true)

-- 7. arm (paint_sample)
pkt, size = cmd_packets.make_arm(7, -60, 60, 0, cmd_packets.PAYLOAD_NONE)
test_virtual_node("paint_sample", pkt, size, true)

-- 8. arm (load_shipping)
pkt, size = cmd_packets.make_arm(8, -30, 80, 0, cmd_packets.PAYLOAD_CONTAINER)
test_virtual_node("load_shipping", pkt, size, true)

-- 9. rpc_request (pass_gate)
pkt, size = cmd_packets.make_rpc_request(9, 12345, 200, 0)
test_virtual_node("pass_gate", pkt, size, true)

-- 10. sensor_read (inspection_scan)
pkt, size = cmd_packets.make_sensor_read(10, 0, cmd_packets.SENSOR_COLOR)
test_virtual_node("inspection_scan", pkt, size, true)

-- 11. idle
pkt, size = cmd_packets.make_idle(11)
test_virtual_node("idle", pkt, size, false)  -- idle has no bitmap response

---------------------------------------------------------------------------
-- Shutdown remote
---------------------------------------------------------------------------
pkt, size = cmd_packets.make_shutdown()
hub_send(pkt, size)

-- Read shutdown ack
local data, len = hub_recv_stream(1000)
if data then
    local ack = stream_packets.read_ack(data, len)
    check("shutdown ack", ack and ack.status == stream_packets.ACK_OK, "no ack")
end

-- Wait for remote to exit
local exit_status = vmrt.remote_wait(remote)
check("remote exit", exit_status == 0,
    "exit status " .. tostring(exit_status))

-- Cleanup
vmrt.channel_pair_close(cp)
vmrt.cleanup()

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
