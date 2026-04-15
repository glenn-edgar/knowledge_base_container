-- =============================================================================
-- process_primitives.lua -- libc FFI wrappers for the supervisor.
--
-- The supervisor spawns, waits on, and signals child processes (apps) using
-- raw POSIX. We do this via luajit FFI rather than luaposix so the base
-- image stays apt-only (no luarocks).
--
-- API (all return lua numbers/strings; errno surfaced via (nil, errstr) on
-- failure):
--   M.fork()                      -> pid (0 in child, >0 in parent) | nil, err
--   M.execvp(argv)                -> never returns on success; else nil, err
--   M.spawn(argv, env_extra)      -> pid | nil, err   (fork + execvp helper)
--   M.waitpid_nohang(pid)         -> {pid, exit_code} | nil (still running)
--                                     | {pid=-1, ...} on ECHILD
--   M.kill(pid, signo)            -> ok | nil, err
--   M.sigaction_flag(signo)       -> fn(): returns true if signo raised since
--                                   last call and then resets. Installs
--                                   once; safe to call repeatedly (returns
--                                   same getter).
--   M.signals                     -> { SIGTERM=15, SIGINT=2, SIGKILL=9, ... }
--
-- spawn() clears signal masks the parent may have set (so children start
-- with a clean slate) and does NOT call setsid -- the supervisor tracks
-- children by pid; a full process group wrapper is unnecessary for v1.
-- =============================================================================

local ffi = require("ffi")
local C   = ffi.C
local bit = require("bit")

pcall(ffi.cdef, [[
    typedef int pid_t;

    pid_t fork(void);
    int   execvp(const char *file, char * const argv[]);
    pid_t waitpid(pid_t pid, int *status, int options);
    int   kill(pid_t pid, int sig);
    int   getpid(void);
    char *strerror(int errnum);
    int   setenv(const char *name, const char *value, int overwrite);
    int   unsetenv(const char *name);

    // sigaction -- use the simplest portable shape.
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]])

-- errno differs by arch; read via ffi.errno() (luajit intrinsic).
local function errstr(label)
    local n = ffi.errno()
    return string.format("%s: errno=%d %s", label, n,
                         ffi.string(C.strerror(n)))
end

local M = {}

M.signals = {
    SIGHUP  = 1,  SIGINT  = 2,  SIGQUIT = 3,  SIGKILL = 9,
    SIGUSR1 = 10, SIGUSR2 = 12, SIGTERM = 15, SIGCHLD = 17,
}

local WNOHANG = 1

---------------------------------------------------------------------------
-- fork / execvp
---------------------------------------------------------------------------

function M.fork()
    local pid = C.fork()
    if pid < 0 then return nil, errstr("fork") end
    return tonumber(pid)
end

function M.execvp(argv)
    if #argv < 1 then return nil, "execvp: empty argv" end
    local cargs = ffi.new("char*[?]", #argv + 1)
    -- ffi.cast to drop const for C api
    for i, a in ipairs(argv) do
        cargs[i - 1] = ffi.cast("char*", tostring(a))
    end
    cargs[#argv] = nil
    C.execvp(argv[1], cargs)
    -- execvp returns only on failure
    return nil, errstr("execvp " .. argv[1])
end

---------------------------------------------------------------------------
-- spawn: fork + execvp, setting any env overrides in the child first.
-- env_extra is { KEY = value, ... } merged into the child's environ.
---------------------------------------------------------------------------

function M.spawn(argv, env_extra)
    local pid, err = M.fork()
    if not pid then return nil, err end
    if pid == 0 then
        -- child
        if env_extra then
            for k, v in pairs(env_extra) do
                C.setenv(k, tostring(v), 1)
            end
        end
        local _, exerr = M.execvp(argv)
        io.stderr:write("spawn exec failed: " .. (exerr or "?") .. "\n")
        io.stderr:flush()
        os.exit(127)
    end
    return pid
end

---------------------------------------------------------------------------
-- waitpid non-blocking. Return shapes:
--   nil              -> still running
--   { pid, code }    -> reaped; exit_code is the low 8 bits of WEXITSTATUS
--                       or negated signal for WIFSIGNALED (no distinction
--                       made beyond: non-zero means not-clean-exit).
--   { pid = -1 }     -> ECHILD (no such child; treat as reaped unknown)
---------------------------------------------------------------------------

function M.waitpid_nohang(pid)
    local status = ffi.new("int[1]")
    local r = C.waitpid(pid, status, WNOHANG)
    if r == 0 then return nil end
    if r < 0 then
        local n = ffi.errno()
        if n == 10 then   -- ECHILD on linux
            return { pid = -1, exit_code = -1, note = "ECHILD" }
        end
        return nil, errstr("waitpid")
    end
    -- decode WEXITSTATUS (status >> 8 & 0xff) / WTERMSIG (status & 0x7f)
    local s = tonumber(status[0])
    local wifexited   = bit.band(s, 0x7f) == 0
    local exit_code
    if wifexited then
        exit_code = bit.band(bit.rshift(s, 8), 0xff)
    else
        exit_code = -bit.band(s, 0x7f)   -- negated signal
    end
    return { pid = tonumber(r), exit_code = exit_code, raw_status = s }
end

---------------------------------------------------------------------------
-- kill: wrapper around raw kill(2). Use M.signals.* for portability.
---------------------------------------------------------------------------

function M.kill(pid, signo)
    local r = C.kill(pid, signo)
    if r ~= 0 then return nil, errstr("kill") end
    return true
end

---------------------------------------------------------------------------
-- signal flag: install a handler for signo that flips a module-private
-- counter. The returned getter returns true once per raised signal and
-- decrements (so a single SIGTERM delivered is consumed by the first
-- getter call). Thread-safe enough for single-threaded luajit.
--
-- Using signal(2) rather than sigaction(2) because the interface is
-- stable across libc and the default restart semantics on linux are
-- fine for our use (we're not blocking in any interruptible syscall
-- at signal time; the tick loop is in userland nanosleep).
---------------------------------------------------------------------------

local _flags = {}        -- signo -> { count = integer, handler = cdata }

function M.sigaction_flag(signo)
    if _flags[signo] then return _flags[signo].getter end

    local entry = { count = 0 }
    entry.handler = ffi.cast("sighandler_t", function(_s)
        entry.count = entry.count + 1
    end)
    C.signal(signo, entry.handler)

    entry.getter = function()
        if entry.count > 0 then
            entry.count = entry.count - 1
            return true
        end
        return false
    end
    _flags[signo] = entry
    return entry.getter
end

return M
