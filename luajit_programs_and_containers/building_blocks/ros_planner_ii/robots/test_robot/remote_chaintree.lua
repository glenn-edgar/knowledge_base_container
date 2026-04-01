--[[
    remote_chaintree.lua -- ChainTree remote process for test_robot.

    Runs as a separate OS process. Loads remote.json, registers user
    functions, ticks the ChainTree runtime. Communication with the hub
    is via socket transport (RPC + stream channels).

    Socket fds passed via environment:
      VMRT_RPC_FD    — read RPC commands from hub
      VMRT_STREAM_FD — write stream data to hub
      VMRT_REMOTE_JSON — path to compiled remote.json
]]

local ffi = require("ffi")

ffi.cdef[[
    int usleep(unsigned int usec);
    typedef struct { int fd; } vmrt_socket_t;
    int     vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len);
    int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    int32_t vmrt_socket_recv_nb(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    void    vmrt_socket_close(vmrt_socket_t *sock);
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "../../runtime/libvmrt.so")

-- Get socket fds from environment
local rpc_fd_str    = os.getenv("VMRT_RPC_FD")
local stream_fd_str = os.getenv("VMRT_STREAM_FD")
local remote_json   = os.getenv("VMRT_REMOTE_JSON") or (script_dir .. "remote.json")

if not rpc_fd_str or not stream_fd_str then
    io.stderr:write("remote_chaintree: VMRT_RPC_FD and VMRT_STREAM_FD required\n")
    os.exit(1)
end

local rpc_sock    = ffi.new("vmrt_socket_t", { fd = tonumber(rpc_fd_str) })
local stream_sock = ffi.new("vmrt_socket_t", { fd = tonumber(stream_fd_str) })

---------------------------------------------------------------------------
-- Load ChainTree runtime
---------------------------------------------------------------------------
local ct_runtime  = require("ct_runtime")
local ct_loader   = require("ct_loader_pure")
local builtins    = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs        = require("ct_definitions")
local engine      = require("ct_engine")

-- Load remote tree
local remote_data = ct_loader.load(remote_json)

-- Load and register user functions
local remote_fns = require("remote_user_functions")
remote_fns.set_transport(rpc_sock, stream_sock, lib)

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("remote_chaintree: missing functions:\n")
    for _, m in ipairs(missing) do
        io.stderr:write("  " .. m .. "\n")
    end
    os.exit(1)
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Activate remote_unit KB
engine.init_test(remote_handle, "remote_unit")
remote_handle.active_tests["remote_unit"] = true
remote_handle.active_test_count = 1

io.stderr:write(string.format("REMOTE: started, rpc_fd=%d stream_fd=%d\n",
    rpc_sock.fd, stream_sock.fd))
io.stderr:flush()

-- Test: try a blocking read to see if the socket works
io.stderr:write("REMOTE: entering tick loop\n")
io.stderr:flush()

---------------------------------------------------------------------------
-- Receiver needs to send ack immediately when command arrives.
-- We hook into the receiver's command_pending flag:
-- when receiver sets command_pending, we also queue an ack.
---------------------------------------------------------------------------
local bb = remote_handle.blackboard
local last_cmd_seq = -1

---------------------------------------------------------------------------
-- Tick loop
---------------------------------------------------------------------------
while true do
    -- Check for shutdown
    if bb.shutdown_requested then
        break
    end

    -- Tick remote runtime
    local kb = remote_handle.kb_table["remote_unit"]
    if kb then
        table.insert(remote_handle.event_queue, {
            node_id = kb.root_node,
            event_id = defs.CFL_TIMER_EVENT,
        })
    end
    while #remote_handle.event_queue > 0 do
        local ev = table.remove(remote_handle.event_queue, 1)
        engine.execute_event(remote_handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end

    -- Queue ack when new command arrives
    if bb.command_seq ~= last_cmd_seq and bb.command_seq ~= 0 then
        last_cmd_seq = bb.command_seq
        bb.ack_pending = true
        bb.ack_seq     = bb.command_seq
        bb.ack_status  = 0  -- ACK_OK
    end

    ffi.C.usleep(100)
end

-- Cleanup
lib.vmrt_socket_close(rpc_sock)
lib.vmrt_socket_close(stream_sock)
