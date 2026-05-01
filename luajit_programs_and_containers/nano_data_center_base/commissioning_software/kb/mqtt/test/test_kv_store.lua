#!/usr/bin/env luajit
package.path = package.path .. ";./?.lua"
local kv = require("lib.mqtt_kv_store")
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
local function expect_eq(a, b, m)
    if a ~= b then error(string.format("%s: expected '%s', got '%s'", m or "mismatch", tostring(b), tostring(a)), 2) end
end

print("\n=== MQTT KV Store LuaJIT Tests ===\n")
kv.lib_init()

test("create/destroy writer", function()
    kv.Writer.new(HOST, PORT, "test-kvw-cr"):destroy()
end)

test("create/destroy reader", function()
    kv.Reader.new(HOST, PORT, "test-kvr-cr"):destroy()
end)

test("writer connect/disconnect", function()
    local w = kv.Writer.new(HOST, PORT, "test-kvw-conn")
    w:connect(5.0); expect(w:is_connected()); w:disconnect(); w:destroy()
end)

test("reader connect/disconnect", function()
    local r = kv.Reader.new(HOST, PORT, "test-kvr-conn")
    r:connect(5.0); expect(r:is_connected()); r:disconnect(); r:destroy()
end)

test("write and read_single", function()
    local w = kv.Writer.new(HOST, PORT, "test-kvw-wr")
    w:connect(5.0); w:write("kv/luajit/t1", "hello"); w:disconnect(); w:destroy()
    msleep(500)
    local r = kv.Reader.new(HOST, PORT, "test-kvr-rd")
    r:connect(5.0)
    expect_eq(r:read_single("kv/luajit/t1", 5.0), "hello", "value")
    r:disconnect(); r:destroy()
    w = kv.Writer.new(HOST, PORT, "test-kvw-cl"); w:connect(5.0)
    w:delete("kv/luajit/t1"); w:disconnect(); w:destroy()
end)

test("write and read_pattern", function()
    local w = kv.Writer.new(HOST, PORT, "test-kvw-pat"); w:connect(5.0)
    w:write("kv/luajit/p/a", "va"); w:write("kv/luajit/p/b", "vb")
    w:disconnect(); w:destroy()
    msleep(500)
    local r = kv.Reader.new(HOST, PORT, "test-kvr-pat"); r:connect(5.0)
    local e = r:read_pattern("kv/luajit/p/#", 1, 5.0, 64)
    expect(#e >= 2, "need >= 2, got " .. #e)
    r:disconnect(); r:destroy()
    w = kv.Writer.new(HOST, PORT, "test-kvw-pcl"); w:connect(5.0)
    w:delete("kv/luajit/p/a"); w:delete("kv/luajit/p/b"); w:disconnect(); w:destroy()
end)

test("update", function()
    local w = kv.Writer.new(HOST, PORT, "test-kvw-upd"); w:connect(5.0)
    w:write("kv/luajit/upd", "old"); msleep(200)
    w:update("kv/luajit/upd", "new"); w:disconnect(); w:destroy()
    msleep(500)
    local r = kv.Reader.new(HOST, PORT, "test-kvr-upd"); r:connect(5.0)
    expect_eq(r:read_single("kv/luajit/upd", 5.0), "new", "updated")
    r:disconnect(); r:destroy()
    w = kv.Writer.new(HOST, PORT, "test-kvw-ucl"); w:connect(5.0)
    w:delete("kv/luajit/upd"); w:disconnect(); w:destroy()
end)

test("read_single nil for missing", function()
    local r = kv.Reader.new(HOST, PORT, "test-kvr-miss"); r:connect(5.0)
    expect(r:read_single("kv/luajit/no_such_99", 1.0) == nil, "should be nil")
    r:disconnect(); r:destroy()
end)

kv.lib_cleanup()
print(string.format("\n  %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
	
