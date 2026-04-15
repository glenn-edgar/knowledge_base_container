#!/usr/bin/env luajit
-- dummy_app/main.lua -- minimal app for the luajit-base smoke test.
--
-- Prints a heartbeat line once per second to stderr (so the supervisor's
-- log stream picks it up with no extra plumbing). Runs until SIGTERM.
--
-- Env picked up (set by supervisor in app_env):
--   APP_NAME, CONTAINER_NAME, APP_SITE, APP_CPU_ID, APP_NAMESPACE

local ffi = require("ffi")

pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

local function env(k) return os.getenv(k) or "" end
local app  = env("APP_NAME")
local cont = env("CONTAINER_NAME")
local ns   = env("APP_NAMESPACE")

io.stderr:write(string.format(
    "dummy_app: started name=%s container=%s ns=%s\n",
    app, cont, ns))
io.stderr:flush()

local req = ffi.new("ts_t")
req.tv_sec  = 1
req.tv_nsec = 0

local i = 0
while true do
    i = i + 1
    io.stderr:write(string.format("dummy_app[%s]: tick %d\n", app, i))
    io.stderr:flush()
    ffi.C.nanosleep(req, nil)
end
