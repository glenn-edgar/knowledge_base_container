#!/usr/bin/env luajit
--[[
  test_rpc.lua — Integration test for nats_rpc LuaJIT binding

  Requires:
    - NATS server at nats://127.0.0.1:4222
    - libnats_rpc.so on LD_LIBRARY_PATH

  Run:
    cd nats_luajit
    luajit test/test_rpc.lua
]]

package.path = package.path .. ";./?.lua"
local rpc_lib = require("lib.nats_rpc")

local SERVER = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"

-- ----------------------------------------------------------------
--  Test framework
-- ----------------------------------------------------------------

local pass_count = 0
local fail_count = 0

local function test(name, fn)
    io.write(string.format("  %-40s ", name))
    local ok, err = pcall(fn)
    if ok then
        pass_count = pass_count + 1
        print("PASS")
    else
        fail_count = fail_count + 1
        print("FAIL: " .. tostring(err))
    end
end

local function expect(cond, msg)
    if not cond then error(msg or "assertion failed", 2) end
end

local function expect_eq(a, b, msg)
    if a ~= b then
        error(string.format("%s: expected %s, got %s",
              msg or "mismatch", tostring(b), tostring(a)), 2)
    end
end

local ffi = require("ffi")
pcall(ffi.cdef, "unsigned int usleep(unsigned int usec);")

local function msleep(ms)
    ffi.C.usleep(ms * 1000)
end

-- ----------------------------------------------------------------
--  Tests
-- ----------------------------------------------------------------

print("\n=== RPC LuaJIT FFI Tests ===\n")

test("create and destroy client", function()
    local cli = rpc_lib.RpcClient.new({ server = SERVER })
    expect(cli ~= nil)
    cli:destroy()
end)

test("create and destroy server", function()
    local srv = rpc_lib.RpcServer.new({ server = SERVER })
    expect(srv ~= nil)
    srv:destroy()
end)

test("client connect and disconnect", function()
    local cli = rpc_lib.RpcClient.new({ server = SERVER })
    cli:connect()
    cli:disconnect()
    cli:destroy()
end)

test("server register handler", function()
    local srv = rpc_lib.RpcServer.new({ server = SERVER })
    srv:register("echo", function(req)
        return req
    end)
    srv:destroy()
end)

-- NOTE: RpcServer with Lua callbacks does NOT work reliably because
-- nats.c dispatches handler callbacks on its internal thread, not the
-- Lua thread.  LuaJIT FFI callbacks require the calling thread to own
-- the Lua state.  Use C/Zig RPC servers with the LuaJIT RPC client.

test("client call timeout (no server)", function()
    local cli = rpc_lib.RpcClient.new({
        server = SERVER,
        namespace_ = "test_rpc_noserver",
    })
    cli:connect()

    local ok, err = pcall(function()
        cli:call("rpc.nobody.home", '{"a":1}', 0.5)
    end)
    expect(not ok, "should fail with no server")
    expect(tostring(err):find("timeout") or tostring(err):find("error"),
           "should be timeout or error")

    cli:disconnect()
    cli:destroy()
end)

test("server create/register/destroy", function()
    -- Server lifecycle works — just don't use Lua callbacks
    -- with start() for actual request handling
    local srv = rpc_lib.RpcServer.new({
        server = SERVER,
        namespace_ = "test_rpc_lifecycle",
    })
    srv:register("echo", function(req) return req end)
    -- Don't call start() — that triggers the threading issue
    srv:destroy()
end)

test("status_str", function()
    expect_eq(rpc_lib.status_str(0), "ok", "RPC_OK")
    expect_eq(rpc_lib.status_str(3), "timeout", "RPC_ERR_TIMEOUT")
end)

-- ------------------------------------------------------------------
--  End-to-end server-side test (queue+poll path)
--
--  start() now uses queue+poll under the hood — Lua handlers run on
--  the caller's thread, not nats.c's internal thread. This test
--  spawns the server in a subprocess so the parent can issue client
--  calls in parallel.
-- ------------------------------------------------------------------

test("server start+register e2e via queue+poll", function()
    local SERVER_SCRIPT = "/tmp/nats_rpc_lj_server.lua"
    local STOP_FLAG     = "/tmp/nats_rpc_lj_server.stop"
    os.execute("rm -f " .. STOP_FLAG)

    local f = io.open(SERVER_SCRIPT, "w")
    f:write(string.format([[
package.path = package.path .. ";./?.lua"
local rpc = require("lib.nats_rpc")
local srv = rpc.RpcServer.new({ server = %q, namespace_ = "test_rpc_e2e", enable_health = false })
srv:register("echo", function(params_json)
    -- params_json is the contents of "params" from the request.
    -- Return a JSON string that becomes the "result" field.
    return params_json
end)
-- Watcher to stop server after a flag file appears (or 10s timeout)
local ffi = require("ffi")
pcall(ffi.cdef, "unsigned int usleep(unsigned int usec);")
local t = 0
-- We want start() to run in the foreground for the polling loop, but it
-- blocks. So we hack: run start() in this same thread but with a custom
-- override: replace the loop iteration to also check the stop flag.
-- Simpler: monkey-patch start() to poll its own _running flag and we set
-- _running=false from a different... still single threaded.
-- Cleanest: don't use srv:start(), inline a minimal poll loop here:
local C = rpc._C
local qmap = {}
local q_h = ffi.new("RpcServerQueue*[1]")
assert(C.rpc_server_register_queue(srv._handle, "echo", 64, false, q_h) == C.RPC_OK)
qmap["echo"] = q_h[0]
assert(C.rpc_server_start(srv._handle, "rpc") == C.RPC_OK)
local start = os.time()
while os.time() - start < 10 do
    local f = io.open(%q, "r")
    if f then f:close(); break end
    local req_p = ffi.new("RpcRequest*[1]")
    local st = C.rpc_server_poll(qmap["echo"], req_p)
    if st == C.RPC_OK then
        local req = req_p[0]
        local params = ffi.string(C.rpc_request_params_json(req))
        C.rpc_request_reply(req, params)
    else
        ffi.C.usleep(5000)
    end
end
C.rpc_server_stop(srv._handle)
srv:destroy()
]], SERVER, STOP_FLAG))
    f:close()

    -- Spawn the server subprocess
    local ld_path = os.getenv("LD_LIBRARY_PATH") or ""
    local ztop    = os.getenv("PWD") or "."
    os.execute(string.format(
        "(cd %s && LD_LIBRARY_PATH=%s luajit %s >/dev/null 2>&1) &",
        ztop, ld_path, SERVER_SCRIPT))
    msleep(500)   -- let server come up

    -- Make a client call
    local cli = rpc_lib.RpcClient.new({ server = SERVER, namespace_ = "test_rpc_e2e" })
    cli:connect()
    local ok, result = pcall(function()
        return cli:call("rpc.echo", '{"hello":"world"}', 3.0)
    end)
    -- Signal server to stop and clean up
    os.execute("touch " .. STOP_FLAG)
    cli:disconnect()
    cli:destroy()

    expect(ok, "client:call should succeed: " .. tostring(result))
    expect(tostring(result):find('"hello":"world"'),
           "result should echo params, got: " .. tostring(result))
end)

-- ----------------------------------------------------------------
--  Summary
-- ----------------------------------------------------------------

print(string.format("\n  %d passed, %d failed\n",
      pass_count, fail_count))

os.exit(fail_count == 0 and 0 or 1)