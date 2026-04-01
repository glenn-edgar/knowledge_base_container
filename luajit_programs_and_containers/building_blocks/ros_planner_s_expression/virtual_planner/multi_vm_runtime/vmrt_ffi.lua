--[[
    vmrt_ffi.lua -- FFI bindings for the multi-VM runtime library.

    Provides:
      vmrt.pipe_create(capacity)   -> pipe handle
      vmrt.pipe_destroy(pipe)
      vmrt.send(ringbuf, string)   -> true/false
      vmrt.recv(ringbuf)           -> string or nil
      vmrt.hub_spawn(script, lua_path, pipe) -> hub handle
      vmrt.hub_join(hub)           -> exit_code
      vmrt.hub_stop(hub)           -> exit_code
      vmrt.hub_destroy(hub)

    Usage from main VM:
      local vmrt = require("vmrt_ffi")
      local pipe = vmrt.pipe_create()
      local hub  = vmrt.hub_spawn("hub_vm.lua", package.path, pipe)
      vmrt.send(pipe.to_hub, '{"test_id":1}')
      local msg = vmrt.recv(pipe.from_hub)
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Ringbuffer */
    typedef struct {
        uint8_t  *buf;
        uint32_t  capacity;
        uint32_t  head;   /* atomic, but FFI sees as plain uint32 */
        uint32_t  tail;
    } vmrt_ringbuf_t;

    /* Bidirectional pipe */
    typedef struct {
        vmrt_ringbuf_t *to_hub;
        vmrt_ringbuf_t *from_hub;
    } vmrt_pipe_t;

    /* Hub thread */
    typedef struct {
        vmrt_pipe_t *pipe;
        char         script_path[512];
        char         lua_path[2048];
        volatile int running;
        volatile int ready;
        volatile int exited;
        int          exit_code;
        char         error_msg[256];
        /* pthread_t follows but we don't need to access it from Lua */
    } vmrt_hub_t;

    /* Ringbuffer API */
    vmrt_ringbuf_t *vmrt_ringbuf_create(uint32_t capacity);
    void            vmrt_ringbuf_destroy(vmrt_ringbuf_t *rb);
    int             vmrt_ringbuf_write(vmrt_ringbuf_t *rb, const void *data, uint32_t len);
    int32_t         vmrt_ringbuf_read(vmrt_ringbuf_t *rb, void *buf, uint32_t buf_size);
    uint32_t        vmrt_ringbuf_available(const vmrt_ringbuf_t *rb);

    /* Pipe API */
    vmrt_pipe_t *vmrt_pipe_create(uint32_t capacity);
    void         vmrt_pipe_destroy(vmrt_pipe_t *pipe);

    /* Hub API */
    vmrt_hub_t *vmrt_hub_spawn(const char *script_path, const char *lua_path,
                               vmrt_pipe_t *pipe);
    int          vmrt_hub_join(vmrt_hub_t *hub);
    int          vmrt_hub_stop(vmrt_hub_t *hub);
    void         vmrt_hub_destroy(vmrt_hub_t *hub);
]]

-- Load the shared library from the same directory as this module
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "libvmrt.so")

local M = {}

-- Read buffer for recv (reused across calls)
local READ_BUF_SIZE = 8192
local read_buf = ffi.new("uint8_t[?]", READ_BUF_SIZE)

---------------------------------------------------------------------------
-- Pipe
---------------------------------------------------------------------------

function M.pipe_create(capacity)
    capacity = capacity or 0  -- 0 = default 64KB
    local pipe = lib.vmrt_pipe_create(capacity)
    if pipe == nil then error("vmrt: failed to create pipe") end
    return pipe
end

function M.pipe_destroy(pipe)
    lib.vmrt_pipe_destroy(pipe)
end

---------------------------------------------------------------------------
-- Send/Recv (string-oriented, for JSON messages)
---------------------------------------------------------------------------

function M.send(ringbuf, msg)
    local rc = lib.vmrt_ringbuf_write(ringbuf, msg, #msg)
    return rc == 0
end

function M.recv(ringbuf)
    local len = lib.vmrt_ringbuf_read(ringbuf, nil, 0)
    if len <= 0 then return nil end

    -- Ensure read buffer is large enough
    if len > READ_BUF_SIZE then
        read_buf = ffi.new("uint8_t[?]", len)
        READ_BUF_SIZE = len
    end

    local got = lib.vmrt_ringbuf_read(ringbuf, read_buf, READ_BUF_SIZE)
    if got <= 0 then return nil end

    return ffi.string(read_buf, got)
end

function M.available(ringbuf)
    return lib.vmrt_ringbuf_available(ringbuf)
end

---------------------------------------------------------------------------
-- Hub thread
---------------------------------------------------------------------------

function M.hub_spawn(script_path, lua_path, pipe)
    local hub = lib.vmrt_hub_spawn(script_path, lua_path or "", pipe)
    if hub == nil then error("vmrt: failed to spawn hub thread") end
    return hub
end

function M.hub_join(hub)
    return lib.vmrt_hub_join(hub)
end

function M.hub_stop(hub)
    return lib.vmrt_hub_stop(hub)
end

function M.hub_destroy(hub)
    lib.vmrt_hub_destroy(hub)
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

return M
