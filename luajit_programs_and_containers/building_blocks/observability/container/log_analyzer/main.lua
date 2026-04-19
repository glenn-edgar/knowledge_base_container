#!/usr/bin/env luajit
-- log_analyzer/main.lua -- SHELL (Phase 5).
--
-- Will eventually: discover KB_LOG paths (WHERE label='KB_LOG'), ingest
-- new samples per tick, maintain live_stats (welford + MA + envelope +
-- slope + cusum), evaluate KB_RULE children, raise SYS_EXCEPTIONs on
-- rule trips, compact tier-0 rollups into tier-1/2/3, and trim retention.
-- Phase 6 fills in the logic.
--
-- For now: periodic heartbeat so the supervisor sees a live process.

local ffi = require("ffi")
pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

local function env(k) return os.getenv(k) or "" end
local app  = env("APP_NAME")
local cont = env("CONTAINER_NAME")

io.stderr:write(string.format(
    "log_analyzer: started name=%s container=%s (Phase 5 shell)\n",
    app, cont))
io.stderr:flush()

local req = ffi.new("ts_t")
req.tv_sec, req.tv_nsec = 10, 0

local tick = 0
while true do
    tick = tick + 1
    io.stderr:write(string.format(
        "log_analyzer: tick=%d (discovery + stats not yet wired)\n", tick))
    io.stderr:flush()
    ffi.C.nanosleep(req, nil)
end
