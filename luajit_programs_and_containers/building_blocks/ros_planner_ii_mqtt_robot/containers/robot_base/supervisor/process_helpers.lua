-- process_helpers.lua -- robot_base subprocess + pipe helpers.
--
-- Wraps process_primitives (luajit_base) with two robot-specific needs:
--   1. spawn_with_stdout_pipe(argv, env_extra) -> pid, read_fd
--      robot_sim's PTY=/dev/pts/N and READY lines come on stdout. The
--      supervisor must capture them to discover the slave path before
--      spawning mqtt_robot_main with HAL_MODE=dongle.
--   2. read_lines_nonblocking(fd) -> { line, ... }
--      Drain whatever's pending. Returns empty list if no full line yet.

local ffi = require("ffi")
local C   = ffi.C
local pp  = require("process_primitives")

pcall(ffi.cdef, [[
    int pipe(int pipefd[2]);
    int dup2(int oldfd, int newfd);
    int close(int fd);
    int fcntl(int fd, int cmd, int arg);
    long read(int fd, void *buf, unsigned long count);
]])

local F_GETFL    = 3
local F_SETFL    = 4
local O_NONBLOCK = 2048

local M = {}

local function set_nonblocking(fd)
    local fl = C.fcntl(fd, F_GETFL, 0)
    if fl < 0 then return false end
    return C.fcntl(fd, F_SETFL, fl + O_NONBLOCK) >= 0
end

-- Fork+exec a child with stdout (and stderr) redirected to a pipe we
-- can read from. Returns (pid, parent_read_fd) or (nil, err).
function M.spawn_with_stdout_pipe(argv, env_extra)
    local pipefd = ffi.new("int[2]")
    if C.pipe(pipefd) ~= 0 then return nil, "pipe failed" end
    local r_fd, w_fd = pipefd[0], pipefd[1]

    local pid, err = pp.fork()
    if not pid then
        C.close(r_fd); C.close(w_fd); return nil, err
    end
    if pid == 0 then
        -- child
        C.close(r_fd)
        C.dup2(w_fd, 1)   -- stdout
        C.dup2(w_fd, 2)   -- stderr (so any panic also lands in our buffer)
        C.close(w_fd)
        if env_extra then
            for k, v in pairs(env_extra) do
                C.setenv(k, tostring(v), 1)
            end
        end
        local _, exerr = pp.execvp(argv)
        io.stderr:write("spawn exec failed: " .. (exerr or "?") .. "\n")
        io.stderr:flush()
        os.exit(127)
    end

    -- parent
    C.close(w_fd)
    set_nonblocking(r_fd)
    return pid, r_fd
end

-- Drain whatever bytes are ready into per-fd line buffers. Returns a
-- list of complete lines (LF-terminated) seen since the last call.
local _bufs = {}

local function buf_for(fd)
    local b = _bufs[fd]
    if not b then b = ""; _bufs[fd] = b end
    return b
end

function M.read_lines_nonblocking(fd)
    local out = {}
    local chunk = ffi.new("char[?]", 4096)
    while true do
        local n = C.read(fd, chunk, 4096)
        if n <= 0 then break end
        _bufs[fd] = (buf_for(fd)) .. ffi.string(chunk, n)
    end
    local b = buf_for(fd)
    while true do
        local nl = b:find("\n", 1, true)
        if not nl then break end
        out[#out + 1] = b:sub(1, nl - 1)
        b = b:sub(nl + 1)
    end
    _bufs[fd] = b
    return out
end

function M.close_fd(fd)
    if fd and fd >= 0 then
        _bufs[fd] = nil
        C.close(fd)
    end
end

return M
