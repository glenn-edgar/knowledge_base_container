--[[
    remote_loopback.lua -- Loopback remote robot process.

    Runs as a separate OS process. Receives AVRC command packets on
    the RPC channel, sends ack + stream responses on the stream channel.

    Socket fds passed via environment:
      VMRT_RPC_FD    — read RPC commands from hub
      VMRT_STREAM_FD — write stream data to hub

    For each command received:
      1. Parse the header to get packet_type and seq
      2. Send an ack (stream_ack_t) with the command's seq
      3. Send a bitmap update appropriate for the command type

    This proves the full channel round-trip works for every virtual node.
]]

local ffi = require("ffi")
local cmd_packets    = require("command_packets")
local stream_packets = require("stream_packets")

ffi.cdef[[
    int usleep(unsigned int usec);
    typedef struct { int fd; } vmrt_socket_t;
    int     vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len);
    int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    void    vmrt_socket_close(vmrt_socket_t *sock);
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "../../runtime/libvmrt.so")

-- Get socket fds from environment
local rpc_fd_str    = os.getenv("VMRT_RPC_FD")
local stream_fd_str = os.getenv("VMRT_STREAM_FD")

if not rpc_fd_str or not stream_fd_str then
    io.stderr:write("remote_loopback: VMRT_RPC_FD and VMRT_STREAM_FD required\n")
    os.exit(1)
end

local rpc_sock    = ffi.new("vmrt_socket_t", { fd = tonumber(rpc_fd_str) })
local stream_sock = ffi.new("vmrt_socket_t", { fd = tonumber(stream_fd_str) })

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local READ_BUF_SIZE = 4096
local read_buf = ffi.new("uint8_t[?]", READ_BUF_SIZE)

local function send_packet(pkt, size)
    lib.vmrt_socket_send(stream_sock, pkt, size)
end

local function send_ack(test_id, cmd_seq, status)
    local pkt, size = stream_packets.make_ack(test_id, cmd_seq, status)
    send_packet(pkt, size)
end

local function send_bitmap(test_id, fields)
    local pkt, size = stream_packets.make_bitmap(test_id, fields)
    send_packet(pkt, size)
end

---------------------------------------------------------------------------
-- Command handlers (one per packet type)
---------------------------------------------------------------------------

local handlers = {}

handlers[cmd_packets.TYPE_INIT_CHECK] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { action_complete = true })
end

handlers[cmd_packets.TYPE_PATH_SEGMENT] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { seg_complete = true })
end

handlers[cmd_packets.TYPE_ROTATE] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { action_complete = true })
end

handlers[cmd_packets.TYPE_ARM] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { action_complete = true })
end

handlers[cmd_packets.TYPE_RPC_REQUEST] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { action_complete = true })
end

handlers[cmd_packets.TYPE_SENSOR_READ] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
    send_bitmap(hdr.test_id, { action_complete = true })
end

handlers[cmd_packets.TYPE_IDLE] = function(hdr)
    send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_OK)
end

handlers[cmd_packets.TYPE_SHUTDOWN] = function(hdr)
    send_ack(0, hdr.seq, stream_packets.ACK_OK)
    return true  -- signal exit
end

---------------------------------------------------------------------------
-- Main loop: read RPC commands, dispatch, respond
---------------------------------------------------------------------------

while true do
    local len = lib.vmrt_socket_recv(rpc_sock, read_buf, READ_BUF_SIZE)
    if len <= 0 then break end  -- EOF or error

    local hdr = cmd_packets.read_header(read_buf, len)
    if hdr then
        local handler = handlers[hdr.packet_type]
        if handler then
            local should_exit = handler(hdr)
            if should_exit then break end
        else
            -- Unknown packet type — ack with unsupported
            send_ack(hdr.test_id, hdr.seq, stream_packets.ACK_UNSUPPORTED)
        end
    end
end

-- Cleanup
lib.vmrt_socket_close(rpc_sock)
lib.vmrt_socket_close(stream_sock)
