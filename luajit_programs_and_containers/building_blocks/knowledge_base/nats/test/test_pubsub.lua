#!/usr/bin/env luajit
--[[
  test_pubsub.lua — Integration test for nats_pubsub LuaJIT binding

  Requires:
    - NATS server at nats://127.0.0.1:4222
    - libnats_pubsub.so on LD_LIBRARY_PATH

  Run:
    cd nats_luajit
    luajit test/test_pubsub.lua
]]

package.path = package.path .. ";./?.lua"
local ps_lib = require("lib.nats_pubsub")
local PubSub = ps_lib.PubSub

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

-- Small sleep to let nats.c internal threads process messages
local function msleep(ms)
    ffi.cdef[[unsigned int usleep(unsigned int usec);]]
    ffi.C.usleep(ms * 1000)
end

-- ----------------------------------------------------------------
--  Tests
-- ----------------------------------------------------------------

print("\n=== PubSub LuaJIT FFI Tests ===\n")

test("create and destroy", function()
    local ps = PubSub.new({ server = SERVER })
    expect(ps ~= nil)
    ps:destroy()
end)

test("connect and disconnect", function()
    local ps = PubSub.new({ server = SERVER })
    ps:connect()
    expect(ps:is_connected())
    ps:disconnect()
    ps:destroy()
end)

test("namespace and client_name", function()
    local ps = PubSub.new({
        server = SERVER,
        namespace_ = "myns",
        client_name = "myclient",
    })
    ps:connect()
    expect_eq(ps:namespace(), "myns", "namespace")
    expect_eq(ps:client_name(), "myclient", "client_name")
    ps:disconnect()
    ps:destroy()
end)

test("publish and subscribe", function()
    local ps = PubSub.new({ server = SERVER, namespace_ = "test_ps" })
    ps:connect()

    local received = nil
    local sub = ps:subscribe("hello", function(msg)
        received = msg
    end)

    msleep(100)  -- let subscription activate
    ps:publish_str("hello", "world")
    msleep(200)  -- let message arrive

    expect(received ~= nil, "no message received")
    expect_eq(received.data, "world", "payload")
    expect(received.original_subject:find("hello"), "subject")

    ps:unsubscribe(sub)
    ps:disconnect()
    ps:destroy()
end)

test("publish binary data", function()
    local ps = PubSub.new({ server = SERVER, namespace_ = "test_bin" })
    ps:connect()

    local received = nil
    local sub = ps:subscribe("binary", function(msg)
        received = msg
    end)

    msleep(100)
    local binary = "\x00\x01\x02\xFF\xFE"
    ps:publish("binary", binary)
    msleep(200)

    expect(received ~= nil, "no message")
    expect_eq(#received.data, 5, "binary length")

    ps:unsubscribe(sub)
    ps:disconnect()
    ps:destroy()
end)

test("unsubscribe stops messages", function()
    local ps = PubSub.new({ server = SERVER, namespace_ = "test_unsub" })
    ps:connect()

    local count = 0
    local sub = ps:subscribe("counter", function(msg)
        count = count + 1
    end)

    msleep(100)
    ps:publish_str("counter", "1")
    msleep(200)
    expect_eq(count, 1, "first message")

    ps:unsubscribe(sub)
    msleep(50)
    ps:publish_str("counter", "2")
    msleep(200)
    expect_eq(count, 1, "should not receive after unsub")

    ps:disconnect()
    ps:destroy()
end)

test("get_stats", function()
    local ps = PubSub.new({ server = SERVER, namespace_ = "test_stats" })
    ps:connect()

    ps:publish_str("stat.test", "hello")
    msleep(50)

    local stats = ps:get_stats()
    expect(stats.messages_published >= 1, "published count")

    ps:disconnect()
    ps:destroy()
end)

-- ----------------------------------------------------------------
--  Summary
-- ----------------------------------------------------------------

print(string.format("\n  %d passed, %d failed\n",
      pass_count, fail_count))

os.exit(fail_count == 0 and 0 or 1)

