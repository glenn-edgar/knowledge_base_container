#!/usr/bin/env luajit
--[[
  test_stream.lua — Integration test for persistent nats_stream circular buffer

  Requires:
    - NATS server with JetStream at nats://127.0.0.1:4222
    - libnats_pubsub.so and libnats_key_store.so on LD_LIBRARY_PATH

  Run:
    cd nats_luajit
    luajit test/test_stream.lua
]]

package.path = package.path .. ";./?.lua"
local ps_lib = require("lib.nats_pubsub")
local ks_lib = require("lib.nats_key_store")
local stream = require("lib.nats_stream")
local PubSub = ps_lib.PubSub
local KeyStore = ks_lib.KeyStore
local StreamBuffer = stream.StreamBuffer

local SERVER = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"
local BUCKET = "test_stream_buf"

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
ffi.cdef[[unsigned int usleep(unsigned int usec);]]
local function msleep(ms)
    ffi.C.usleep(ms * 1000)
end

--- Helper: create a connected PubSub + KeyStore pair.
local function make_pair(ns)
    local ps = PubSub.new({ server = SERVER, namespace_ = ns })
    ps:connect()
    local ks = KeyStore.new({ server = SERVER, bucket = BUCKET })
    ks:connect()
    return ps, ks
end

--- Helper: clean up a pair.
local function destroy_pair(ps, ks)
    ps:disconnect()
    ps:destroy()
    ks:disconnect()
    ks:destroy()
end

-- ----------------------------------------------------------------
--  Tests
-- ----------------------------------------------------------------

print("\n=== StreamBuffer Persistent Tests ===\n")

test("create and stop", function()
    local ps, ks = make_pair("test_stream")
    local buf = StreamBuffer.new(ps, ks, "test.create", 5)
    expect(buf ~= nil)
    expect_eq(buf:capacity(), 5, "capacity")
    expect_eq(buf:count(), 0, "initial count")
    expect(buf:latest() == nil, "initial latest")
    buf:purge()
    buf:stop()
    destroy_pair(ps, ks)
end)

test("fills buffer in order", function()
    local ps, ks = make_pair("test_stream2")
    local buf = StreamBuffer.new(ps, ks, "test.fill", 5)
    msleep(100)

    for i = 1, 3 do
        ps:publish_str("test.fill", tostring(i))
    end
    msleep(300)

    expect_eq(buf:count(), 3, "count after 3 msgs")
    expect(not buf:is_full(), "not full yet")

    local entries = buf:entries()
    expect_eq(#entries, 3, "entries length")
    expect_eq(entries[1].data, "1", "first entry")
    expect_eq(entries[2].data, "2", "second entry")
    expect_eq(entries[3].data, "3", "third entry")
    expect_eq(buf:latest().data, "3", "latest")

    buf:purge()
    buf:stop()
    destroy_pair(ps, ks)
end)

test("wraps around when full", function()
    local ps, ks = make_pair("test_stream3")
    local buf = StreamBuffer.new(ps, ks, "test.wrap", 3)
    msleep(100)

    for i = 1, 5 do
        ps:publish_str("test.wrap", tostring(i))
    end
    msleep(300)

    expect_eq(buf:count(), 3, "count capped at size")
    expect(buf:is_full(), "should be full")
    expect_eq(buf:total_received(), 5, "total received")

    local entries = buf:entries()
    expect_eq(#entries, 3, "entries length")
    expect_eq(entries[1].data, "3", "oldest after wrap")
    expect_eq(entries[2].data, "4", "middle after wrap")
    expect_eq(entries[3].data, "5", "newest after wrap")
    expect_eq(buf:latest().data, "5", "latest after wrap")

    buf:purge()
    buf:stop()
    destroy_pair(ps, ks)
end)

test("clear resets buffer and KV", function()
    local ps, ks = make_pair("test_stream4")
    local buf = StreamBuffer.new(ps, ks, "test.clear", 5)
    msleep(100)

    for i = 1, 3 do
        ps:publish_str("test.clear", tostring(i))
    end
    msleep(300)
    expect_eq(buf:count(), 3, "before clear")

    buf:clear()
    expect_eq(buf:count(), 0, "after clear")
    expect(buf:latest() == nil, "latest after clear")
    expect_eq(#buf:entries(), 0, "entries after clear")

    -- still subscribed, new messages arrive
    ps:publish_str("test.clear", "after")
    msleep(200)
    expect_eq(buf:count(), 1, "new msg after clear")
    expect_eq(buf:latest().data, "after", "latest after re-fill")

    buf:purge()
    buf:stop()
    destroy_pair(ps, ks)
end)

test("stop prevents new messages", function()
    local ps, ks = make_pair("test_stream5")
    local buf = StreamBuffer.new(ps, ks, "test.stop", 5)
    msleep(100)

    ps:publish_str("test.stop", "before")
    msleep(200)
    expect_eq(buf:count(), 1, "before stop")

    buf:stop()
    msleep(50)
    ps:publish_str("test.stop", "after")
    msleep(200)
    expect_eq(buf:count(), 1, "after stop unchanged")
    expect_eq(buf:latest().data, "before", "data preserved after stop")

    buf:purge()
    destroy_pair(ps, ks)
end)

test("size of 1", function()
    local ps, ks = make_pair("test_stream6")
    local buf = StreamBuffer.new(ps, ks, "test.one", 1)
    msleep(100)

    for i = 1, 4 do
        ps:publish_str("test.one", tostring(i))
    end
    msleep(300)

    expect_eq(buf:count(), 1, "count = 1")
    expect_eq(buf:total_received(), 4, "total = 4")
    expect_eq(buf:latest().data, "4", "only latest survives")

    local entries = buf:entries()
    expect_eq(#entries, 1, "entries length")
    expect_eq(entries[1].data, "4", "single entry")

    buf:purge()
    buf:stop()
    destroy_pair(ps, ks)
end)

test("persistence survives restart", function()
    -- Phase 1: create buffer, publish messages, stop (KV preserved)
    local ps1, ks1 = make_pair("test_persist")
    local buf1 = StreamBuffer.new(ps1, ks1, "test.persist", 5)
    msleep(100)

    for i = 1, 3 do
        ps1:publish_str("test.persist", "msg_" .. tostring(i))
    end
    msleep(300)

    expect_eq(buf1:count(), 3, "phase1 count")
    expect_eq(buf1:total_received(), 3, "phase1 total")
    expect_eq(buf1:latest().data, "msg_3", "phase1 latest")

    buf1:stop()
    destroy_pair(ps1, ks1)

    -- Phase 2: new instances, same bucket+subject — should reload
    local ps2, ks2 = make_pair("test_persist")
    local buf2 = StreamBuffer.new(ps2, ks2, "test.persist", 5)

    expect_eq(buf2:count(), 3, "reloaded count")
    expect_eq(buf2:total_received(), 3, "reloaded total")

    local entries = buf2:entries()
    expect_eq(#entries, 3, "reloaded entries length")
    expect_eq(entries[1].data, "msg_1", "reloaded oldest")
    expect_eq(entries[2].data, "msg_2", "reloaded middle")
    expect_eq(entries[3].data, "msg_3", "reloaded newest")
    expect_eq(buf2:latest().data, "msg_3", "reloaded latest")

    -- New messages still work after reload
    msleep(100)
    ps2:publish_str("test.persist", "msg_4")
    msleep(200)
    expect_eq(buf2:count(), 4, "count after new msg")
    expect_eq(buf2:latest().data, "msg_4", "latest after new msg")

    buf2:purge()
    buf2:stop()
    destroy_pair(ps2, ks2)
end)

test("persistence with wrap-around", function()
    -- Phase 1: fill past capacity
    local ps1, ks1 = make_pair("test_persist_wrap")
    local buf1 = StreamBuffer.new(ps1, ks1, "test.pwrap", 3)
    msleep(100)

    for i = 1, 5 do
        ps1:publish_str("test.pwrap", tostring(i))
    end
    msleep(300)

    expect_eq(buf1:count(), 3, "phase1 count")
    expect_eq(buf1:total_received(), 5, "phase1 total")

    buf1:stop()
    destroy_pair(ps1, ks1)

    -- Phase 2: reload — should have entries 3, 4, 5
    local ps2, ks2 = make_pair("test_persist_wrap")
    local buf2 = StreamBuffer.new(ps2, ks2, "test.pwrap", 3)

    expect_eq(buf2:count(), 3, "reloaded count")
    local entries = buf2:entries()
    expect_eq(entries[1].data, "3", "reloaded oldest after wrap")
    expect_eq(entries[2].data, "4", "reloaded middle after wrap")
    expect_eq(entries[3].data, "5", "reloaded newest after wrap")

    buf2:purge()
    buf2:stop()
    destroy_pair(ps2, ks2)
end)

test("purge deletes KV data", function()
    -- Phase 1: populate and purge
    local ps1, ks1 = make_pair("test_purge")
    local buf1 = StreamBuffer.new(ps1, ks1, "test.purge", 5)
    msleep(100)

    ps1:publish_str("test.purge", "data")
    msleep(200)
    expect_eq(buf1:count(), 1, "before purge")

    buf1:purge()
    buf1:stop()
    destroy_pair(ps1, ks1)

    -- Phase 2: reload — should be empty
    local ps2, ks2 = make_pair("test_purge")
    local buf2 = StreamBuffer.new(ps2, ks2, "test.purge", 5)

    expect_eq(buf2:count(), 0, "empty after purge")
    expect(buf2:latest() == nil, "no latest after purge")

    buf2:purge()
    buf2:stop()
    destroy_pair(ps2, ks2)
end)

-- ----------------------------------------------------------------
--  Summary
-- ----------------------------------------------------------------

print(string.format("\n  %d passed, %d failed\n",
      pass_count, fail_count))

os.exit(fail_count == 0 and 0 or 1)
