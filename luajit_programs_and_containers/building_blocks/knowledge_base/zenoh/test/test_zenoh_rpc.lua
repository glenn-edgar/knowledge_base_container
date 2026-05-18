#!/usr/bin/env luajit
--[[
  test_zenoh_rpc.lua — integration test for the LuaJIT FFI binding to
  libzenoh_rpc (client side).

  We need a queryable on the other end. The C-side test exercises both
  client and server in the same process; from LuaJIT we only test the
  client, so we spawn the C-side rpc test binary as a background server
  if available. Otherwise we just verify timeout behaviour against a
  zenohd that has no matching queryables.

  Env vars:
    ZENOH_LOCATOR   default "udp/127.0.0.1:17447"
]]

package.path = package.path .. ";./?.lua"
local zrpc = require("lib.zenoh_rpc")
local zt   = require("lib.zenoh_token")

local LOCATOR = os.getenv("ZENOH_LOCATOR") or "udp/127.0.0.1:17447"

local pass, fail = 0, 0
local function test(name, fn)
    io.write(string.format("  %-50s ", name))
    local ok, err = pcall(fn)
    if ok then pass = pass + 1; print("PASS")
    else fail = fail + 1; print("FAIL: " .. tostring(err)) end
end
local function expect(cond, msg) if not cond then error(msg or "assertion failed", 2) end end
local function expect_eq(a, b, msg)
    if a ~= b then
        error(string.format("%s: expected %s, got %s", msg or "mismatch", tostring(b), tostring(a)), 2)
    end
end

print("=== zenoh_rpc LuaJIT tests (locator=" .. LOCATOR .. ") ===")

test("Client.new + destroy (no connect)", function()
    local cli = zrpc.Client.new({ locators = { LOCATOR } })
    cli:destroy()
end)

test("Client: empty locators rejected", function()
    expect(not pcall(zrpc.Client.new, {}), "empty opts rejected")
end)

test("Client: connect + disconnect", function()
    local cli = zrpc.Client.new({ locators = { LOCATOR } })
    cli:connect()
    cli:disconnect()
    cli:destroy()
end)

test("Client:call against unregistered method raises", function()
    local cli = zrpc.Client.new({ locators = { LOCATOR } })
    cli:connect()
    local bogus = zt.hash("luajit/no/such/method")
    local ok, err = pcall(cli.call, cli, bogus, "", 300)
    expect(not ok, "call should raise (no queryable matches)")
    -- zenohd may respond with response-final-only ("no reply") or we may
    -- hit timeout — accept either.
    local s = tostring(err)
    expect(s:match("timeout") or s:match("no reply"),
           "error mentions timeout or no reply, got: " .. s)
    cli:disconnect()
    cli:destroy()
end)

print()
print(string.format("zenoh_rpc tests: %d passed, %d failed", pass, fail))
os.exit(fail == 0 and 0 or 1)
