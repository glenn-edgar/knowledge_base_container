--[[
    vmrt_ffi.lua -- FFI bindings for the multi-VM runtime.

    Socket-based IPC for hub↔remote communication.
    Hub runs as a thread (in-process), remote runs as a separate process.
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Socket */
    typedef struct { int fd; } vmrt_socket_t;

    /* Channel pair: RPC + Stream */
    typedef struct {
        vmrt_socket_t rpc_hub;
        vmrt_socket_t rpc_remote;
        vmrt_socket_t stream_hub;
        vmrt_socket_t stream_remote;
    } vmrt_channel_pair_t;

    /* Remote process */
    typedef struct {
        int           pid;
        vmrt_socket_t rpc_fd;
        vmrt_socket_t stream_fd;
        char          script_path[512];
        char          lua_path[2048];
        volatile int  running;
        int           exit_status;
    } vmrt_remote_proc_t;

    /* Hub thread */
    typedef struct {
        vmrt_socket_t rpc_fd;
        vmrt_socket_t stream_fd;
        char          script_path[512];
        char          lua_path[2048];
        volatile int  running;
        volatile int  ready;
        volatile int  exited;
        int           exit_code;
        char          error_msg[256];
    } vmrt_hub_t;

    /* Socket API */
    int     vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len);
    int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    int32_t vmrt_socket_recv_nb(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    void    vmrt_socket_close(vmrt_socket_t *sock);

    /* Channel pair API */
    int  vmrt_channel_pair_create(vmrt_channel_pair_t *cp);
    void vmrt_channel_pair_close(vmrt_channel_pair_t *cp);

    /* Remote process API */
    int vmrt_remote_spawn(vmrt_remote_proc_t *proc, const char *script_path,
                          const char *lua_path, vmrt_channel_pair_t *cp);
    int vmrt_remote_wait(vmrt_remote_proc_t *proc);
    int vmrt_remote_stop(vmrt_remote_proc_t *proc);

    /* Hub thread API */
    int vmrt_hub_spawn(vmrt_hub_t *hub, const char *script_path,
                       const char *lua_path, vmrt_channel_pair_t *cp);
    int vmrt_hub_join(vmrt_hub_t *hub);
    int vmrt_hub_stop(vmrt_hub_t *hub);

    /* Cleanup */
    void vmrt_cleanup(void);

    /* For usleep */
    int usleep(unsigned int usec);
]]

-- Load shared library
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "libvmrt.so")

local M = {}
M.lib = lib

-- Read buffer (reused)
local READ_BUF_SIZE = 16384
local read_buf = ffi.new("uint8_t[?]", READ_BUF_SIZE)

---------------------------------------------------------------------------
-- Socket send/recv (string-oriented for JSON messages)
---------------------------------------------------------------------------

function M.send(sock, msg)
    return lib.vmrt_socket_send(sock, msg, #msg) == 0
end

function M.recv(sock)
    local len = lib.vmrt_socket_recv(sock, read_buf, READ_BUF_SIZE)
    if len <= 0 then return nil end
    return ffi.string(read_buf, len)
end

function M.recv_nb(sock)
    local len = lib.vmrt_socket_recv_nb(sock, read_buf, READ_BUF_SIZE)
    if len <= 0 then return nil end
    return ffi.string(read_buf, len)
end

function M.close(sock)
    lib.vmrt_socket_close(sock)
end

---------------------------------------------------------------------------
-- Channel pair
---------------------------------------------------------------------------

function M.channel_pair_create()
    local cp = ffi.new("vmrt_channel_pair_t")
    if lib.vmrt_channel_pair_create(cp) < 0 then
        error("vmrt: failed to create channel pair")
    end
    return cp
end

function M.channel_pair_close(cp)
    lib.vmrt_channel_pair_close(cp)
end

---------------------------------------------------------------------------
-- Remote process
---------------------------------------------------------------------------

function M.remote_spawn(script_path, lua_path, cp)
    local proc = ffi.new("vmrt_remote_proc_t")
    if lib.vmrt_remote_spawn(proc, script_path, lua_path or "", cp) < 0 then
        error("vmrt: failed to spawn remote process")
    end
    return proc
end

function M.remote_wait(proc)
    return lib.vmrt_remote_wait(proc)
end

function M.remote_stop(proc)
    return lib.vmrt_remote_stop(proc)
end

---------------------------------------------------------------------------
-- Hub thread
---------------------------------------------------------------------------

function M.hub_spawn(script_path, lua_path, cp)
    local hub = ffi.new("vmrt_hub_t")
    if lib.vmrt_hub_spawn(hub, script_path, lua_path or "", cp) < 0 then
        error("vmrt: failed to spawn hub thread")
    end
    return hub
end

function M.hub_join(hub)
    return lib.vmrt_hub_join(hub)
end

function M.hub_stop(hub)
    return lib.vmrt_hub_stop(hub)
end

function M.hub_is_ready(hub)
    return hub.ready ~= 0
end

function M.hub_is_exited(hub)
    return hub.exited ~= 0
end

function M.hub_error(hub)
    if hub.error_msg[0] ~= 0 then
        return ffi.string(hub.error_msg)
    end
    return nil
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

function M.cleanup()
    lib.vmrt_cleanup()
end

return M
