#!/usr/bin/env luajit
-- log_aggregator/main.lua -- SHELL. Registration-test only.

local ffi = require("ffi")
pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

local function env(k) return os.getenv(k) or "" end
local app  = env("APP_NAME")
local cont = env("CONTAINER_NAME")

io.stderr:write(string.format(
    "log_aggregator: started name=%s container=%s\n", app, cont))
io.stderr:flush()

local req = ffi.new("ts_t")
req.tv_sec, req.tv_nsec = 1, 0

local i = 0
while true do
    i = i + 1
    io.stderr:write(string.format("log_aggregator[%s]: tick %d\n", app, i))
    io.stderr:flush()
    ffi.C.nanosleep(req, nil)
end
