-- test_hub_script.lua -- Minimal hub script for thread testing.
--
-- Receives the pipe and hub pointers via _VMRT_PIPE and _VMRT_HUB globals
-- (set by the C thread launcher before running this script).
-- Echoes messages from to_hub back to from_hub with "reply:" prefix.

local ffi = require("ffi")

ffi.cdef[[
    int usleep(unsigned int usec);

    typedef struct {
        uint8_t  *buf;
        uint32_t  capacity;
        uint32_t  head;
        uint32_t  tail;
    } vmrt_ringbuf_t;

    typedef struct {
        vmrt_ringbuf_t *to_hub;
        vmrt_ringbuf_t *from_hub;
    } vmrt_pipe_t;

    typedef struct {
        vmrt_pipe_t *pipe;
        char         script_path[512];
        char         lua_path[2048];
        volatile int running;
        volatile int ready;
        volatile int exited;
        int          exit_code;
        char         error_msg[256];
    } vmrt_hub_t;

    int             vmrt_ringbuf_write(vmrt_ringbuf_t *rb, const void *data, uint32_t len);
    int32_t         vmrt_ringbuf_read(vmrt_ringbuf_t *rb, void *buf, uint32_t buf_size);
    uint32_t        vmrt_ringbuf_available(const vmrt_ringbuf_t *rb);
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "libvmrt.so")

-- Get pointers passed from C launcher
local pipe = ffi.cast("vmrt_pipe_t *", _VMRT_PIPE)
local hub  = ffi.cast("vmrt_hub_t *", _VMRT_HUB)

local read_buf = ffi.new("uint8_t[?]", 8192)

local function recv(rb)
    local len = lib.vmrt_ringbuf_read(rb, nil, 0)
    if len <= 0 then return nil end
    local got = lib.vmrt_ringbuf_read(rb, read_buf, 8192)
    if got <= 0 then return nil end
    return ffi.string(read_buf, got)
end

local function send(rb, msg)
    return lib.vmrt_ringbuf_write(rb, msg, #msg) == 0
end

-- Signal ready
hub.ready = 1

-- Echo loop
while hub.running ~= 0 do
    local msg = recv(pipe.to_hub)
    if msg then
        if msg == "shutdown" then
            send(pipe.from_hub, "reply:shutdown")
            break
        end
        send(pipe.from_hub, "reply:" .. msg)
    end
    -- Yield CPU briefly (no busy spin)
    ffi.C.usleep(100)
end
