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

    // Signal handling via block+poll (sigtimedwait). NO async callbacks
    // into Lua -- luajit ffi.cast("...", fn) is NOT signal-safe.
    //
    // glibc sigset_t on linux = __val[16] of unsigned long (8-byte aligned).
    // We match exactly, then fill bits ourselves to avoid calling the
    // sigemptyset/sigaddset helpers (which on some libcs are macros and
    // on others require strict alignment we can't easily guarantee via
    // ffi.new over a struct of bytes).
    typedef struct { unsigned long val[16]; } dcs_sigset_t;

    int sigprocmask(int how, const dcs_sigset_t *set, dcs_sigset_t *oldset);
    // Returns signo on success, -1 (errno=EAGAIN) if no pending signal.
    int sigtimedwait(const dcs_sigset_t *set, void *info,
                     const void *timeout);
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
-- signal flag: block the specified signals process-wide and expose a
-- getter that drains pending deliveries via sigtimedwait(2) with a
-- zero timeout (non-blocking poll). Safe under luajit: no async callback
-- into Lua, all signal inspection happens on the main thread's tick.
--
-- Usage: install once at startup with a list of signals; call the
-- returned getter each tick. Returns true if any of the registered
-- signals was pending at call time and drains ALL of them (so one
-- SIGTERM + one SIGINT queued shows up as a single "true" and both
-- are consumed).
---------------------------------------------------------------------------

local SIG_BLOCK = 0

-- Fill a dcs_sigset_t with the given signal numbers. Linux layout:
-- bit (signo-1) of val[(signo-1)/64] is set. We zero first, then OR in.
local function fill_sigset(set, signos)
    ffi.fill(set, ffi.sizeof("dcs_sigset_t"), 0)
    for _, s in ipairs(signos) do
        local bit_index = s - 1
        local word  = math.floor(bit_index / 64)
        local shift = bit_index % 64
        -- uint64 (1ULL << shift) via 2^shift; cast into cdata and add.
        local mask
        if shift < 63 then
            mask = ffi.new("unsigned long", 2 ^ shift)
        else
            mask = ffi.new("unsigned long", 0x8000000000000000ULL)
        end
        set.val[word] = set.val[word] + mask
    end
end

local _installed_list = nil

local function install_block(signos)
    if _installed_list then return end
    local set = ffi.new("dcs_sigset_t")
    fill_sigset(set, signos)
    if C.sigprocmask(SIG_BLOCK, set, nil) ~= 0 then
        error(errstr("sigprocmask(BLOCK)"))
    end
    io.stderr:write(string.format(
        "process_primitives: blocked signals { %s } via sigprocmask\n",
        table.concat(signos, ", ")))
    _installed_list = signos
end

-- Module-level cdata — survives as long as the module is loaded so the
-- GC can't collect them out from under a long-running poll loop.
-- (zero_ts is only a single dcs_sigset_t's worth of bytes; tiny.)
local _any_set    = ffi.new("dcs_sigset_t")
local _any_zerots = ffi.new("struct { long tv_sec; long tv_nsec; }")
_any_zerots.tv_sec  = 0
_any_zerots.tv_nsec = 0

-- Block all `signos` process-wide in one shot; return a getter that
-- reports true if ANY was pending at call time and drains them all.
function M.sigaction_any_flag(signos)
    install_block(signos)
    fill_sigset(_any_set, signos)
    local debug = os.getenv("DCS_SIG_DEBUG") == "1"
    return function()
        local fired = false
        while true do
            local r = C.sigtimedwait(_any_set, nil, _any_zerots)
            if r > 0 then
                fired = true
                if debug then
                    io.stderr:write(string.format(
                        "sigaction_any_flag: dequeued signo=%d\n", r))
                end
            else
                break   -- -1 with errno=EAGAIN means "no pending signal"
            end
        end
        return fired
    end
end

return M
