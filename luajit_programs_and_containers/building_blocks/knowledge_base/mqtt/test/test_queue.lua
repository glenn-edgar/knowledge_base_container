#!/usr/bin/env luajit
package.path = package.path .. ";./?.lua"
local kv = require("lib.mqtt_kv_store")
local mq = require("lib.mqtt_queue")
local ffi = require("ffi")
pcall(ffi.cdef, "unsigned int usleep(unsigned int usec);")
local function msleep(ms) ffi.C.usleep(ms * 1000) end

local HOST = os.getenv("MQTT_HOST") or "localhost"
local PORT = tonumber(os.getenv("MQTT_PORT") or "1883")
local pass, fail = 0, 0

local function test(name, fn)
    io.write(string.format("  %-40s ", name))
    local ok, err = pcall(fn)
    if ok then pass = pass + 1; print("PASS")
    else       fail = fail + 1; print("FAIL: " .. tostring(err)) end
end
local function expect(c, m) if not c then error(m or "assertion failed", 2) end end

print("\n=== MQTT Queue LuaJIT Tests ===\n")
kv.lib_init()

test("create/destroy publisher", function()
    mq.Publisher.new(HOST, PORT, "test-pub-cr"):destroy()
end)

test("create/destroy reader", function()
    mq.Reader.new(HOST, PORT, "test-qr-cr", true):destroy()
end)

test("publisher connect/disconnect", function()
    local p = mq.Publisher.new(HOST, PORT, "test-pub-conn")
    p:connect(5000); expect(p:is_connected()); p:disconnect(); p:destroy()
end)

test("reader connect/disconnect", function()
    local r = mq.Reader.new(HOST, PORT, "test-qr-conn", true)
    r:connect(5000); expect(r:is_connected()); r:disconnect(); r:destroy()
end)

test("publish and read", function()
    -- publish first
    local p = mq.Publisher.new(HOST, PORT, "test-pub-rw")
    p:connect(5000)
    p:publish("queue/luajit/test/m1", '{"task":"hello"}', 1, true)
    p:publish("queue/luajit/test/m2", '{"task":"world"}', 1, true)
    p:disconnect(); p:destroy()
    msleep(500)

    -- read() internally subscribes and collects retained messages
    local r = mq.Reader.new(HOST, PORT, "test-qr-rw", true)
    r:connect(5000)
    local msgs = r:read("queue/luajit/test/#", 1, 3000)
    expect(#msgs >= 2, "need >= 2 msgs, got " .. #msgs)
    r:disconnect(); r:destroy()

    -- cleanup retained
    p = mq.Publisher.new(HOST, PORT, "test-pub-cl")
    p:connect(5000)
    p:publish("queue/luajit/test/m1", "", 1, true)
    p:publish("queue/luajit/test/m2", "", 1, true)
    p:disconnect(); p:destroy()
end)

test("publish single", function()
    local p = mq.Publisher.new(HOST, PORT, "test-pub-single")
    p:connect(5000)
    expect(p:publish("queue/luajit/single", "payload", 1, false), "publish ok")
    p:disconnect(); p:destroy()
end)

kv.lib_cleanup()
print(string.format("\n  %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)