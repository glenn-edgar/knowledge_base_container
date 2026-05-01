#!/usr/bin/env luajit
--[[
  test_key_store.lua — Integration test for nats_key_store LuaJIT binding

  Requires:
    - NATS server with JetStream at nats://127.0.0.1:4222
    - libnats_key_store.so on LD_LIBRARY_PATH or in /usr/local/lib

  Run:
    cd nats_luajit
    luajit test/test_key_store.lua
]]

package.path = package.path .. ";./?.lua"
local ks_lib = require("lib.nats_key_store")
local KeyStore = ks_lib.KeyStore

local SERVER = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"
local BUCKET = "test_luajit_ks"

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

-- ----------------------------------------------------------------
--  Helper: cleanup bucket
-- ----------------------------------------------------------------

local function cleanup(ks)
    local keys = ks:keys("*")
    for _, k in ipairs(keys) do
        pcall(function() ks:delete(k) end)
    end
end

-- ----------------------------------------------------------------
--  Tests
-- ----------------------------------------------------------------

print("\n=== KeyStore LuaJIT FFI Tests ===\n")

test("create and destroy", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    expect(ks ~= nil, "create failed")
    ks:destroy()
end)

test("connect and disconnect", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    expect(ks:is_connected(), "not connected")
    ks:disconnect()
    ks:destroy()
end)

test("put and get", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    local rev = ks:put("test.name", '"Alice"')
    expect(rev > 0, "revision should be > 0")

    local val = ks:get("test.name")
    expect_eq(val, '"Alice"', "get value")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

test("get non-existent returns nil", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    local val = ks:get("does.not.exist")
    expect(val == nil, "expected nil for missing key")

    ks:disconnect()
    ks:destroy()
end)

test("delete", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    ks:put("del.me", '"gone"')
    ks:delete("del.me")
    local val = ks:get("del.me")
    expect(val == nil, "expected nil after delete")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

test("exists", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    expect(not ks:exists("no.such"), "should not exist")
    ks:put("yes.here", '"hi"')
    expect(ks:exists("yes.here"), "should exist")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

test("keys with pattern", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    ks:put("prefix.a", '"1"')
    ks:put("prefix.b", '"2"')
    ks:put("other.c",  '"3"')

    local keys = ks:keys("prefix.*")
    expect_eq(#keys, 2, "key count")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

test("increment and decrement", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    local v1 = ks:increment("counter", 1)
    expect_eq(v1, 1, "first increment")
    local v2 = ks:increment("counter", 5)
    expect_eq(v2, 6, "second increment")
    local v3 = ks:decrement("counter", 2)
    expect_eq(v3, 4, "decrement")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

test("put_sync / get_sync", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    -- Not connected — sync methods auto-connect/disconnect
    ks:put_sync("sync.test", '"hello"')
    local val = ks:get_sync("sync.test")
    expect_eq(val, '"hello"', "sync get")
    ks:delete_sync("sync.test")
    ks:destroy()
end)

test("JSON object storage", function()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    cleanup(ks)

    local json = '{"id":1,"name":"Bob","age":30}'
    ks:put("user.1", json)
    local val = ks:get("user.1")
    expect_eq(val, json, "JSON round-trip")

    cleanup(ks)
    ks:disconnect()
    ks:destroy()
end)

-- ----------------------------------------------------------------
--  Summary
-- ----------------------------------------------------------------

print(string.format("\n  %d passed, %d failed\n",
      pass_count, fail_count))

os.exit(fail_count == 0 and 0 or 1)

