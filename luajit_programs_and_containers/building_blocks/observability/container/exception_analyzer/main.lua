#!/usr/bin/env luajit
-- exception_analyzer/main.lua -- SHELL (Phase 5).
--
-- Will eventually: discover SYS_EXCEPTION paths, run ack-lease janitor,
-- compute flap_rate_5min, auto-shelve flapping exceptions, raise
-- alarm_flood_detected meta-exceptions. Phase 7 fills in the logic.
--
-- For now: periodic heartbeat to stderr so the supervisor sees a live
-- process and the container-registry round-trip works.

local ffi = require("ffi")
pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

local function env(k) return os.getenv(k) or "" end
local app  = env("APP_NAME")
local cont = env("CONTAINER_NAME")

io.stderr:write(string.format(
    "exception_analyzer: started name=%s container=%s (Phase 5 shell)\n",
    app, cont))
io.stderr:flush()

local req = ffi.new("ts_t")
req.tv_sec, req.tv_nsec = 10, 0

local tick = 0
while true do
    tick = tick + 1
    io.stderr:write(string.format(
        "exception_analyzer: tick=%d (janitor not yet wired)\n", tick))
    io.stderr:flush()
    ffi.C.nanosleep(req, nil)
end
