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

-- ----------------------------------------------------------------
--  Summary
-- ----------------------------------------------------------------

print(string.format("\n  %d passed, %d failed\n",
      pass_count, fail_count))

os.exit(fail_count == 0 and 0 or 1)