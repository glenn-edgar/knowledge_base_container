--[[
    test_kv_writer.lua -- Unit test for kv_writer coalescing queue.
    No external dependencies (uses a mock KV store).
]]

print("=== KV Writer Unit Test ===\n")

local passed, failed = 0, 0
local function check(name, cond, detail)
    if cond then
        print("  PASS: " .. name)
        passed = passed + 1
    else
        print("  FAIL: " .. name .. (detail and (" — " .. detail) or ""))
        failed = failed + 1
    end
end

-- Mock KV store
local mock_ks = { writes = {}, deletes = {} }
function mock_ks:put(key, value)
    self.writes[key] = value
    self.writes._count = (self.writes._count or 0) + 1
end
function mock_ks:delete(key)
    self.deletes[key] = true
end

---------------------------------------------------------------------------

local kv_writer = require("kv_writer")

print("--- Basic Push/Tick ---")
local w = kv_writer.new(mock_ks)

w:push("key.a", "value_a")
w:push("key.b", "value_b")

check("depth is 2", w:depth() == 2, "got " .. w:depth())
check("tick 1 writes key.a", w:tick() == true)
check("key.a written", mock_ks.writes["key.a"] == "value_a")
check("depth is 1", w:depth() == 1)
check("tick 2 writes key.b", w:tick() == true)
check("key.b written", mock_ks.writes["key.b"] == "value_b")
check("depth is 0", w:depth() == 0)
check("tick 3 returns false", w:tick() == false)

print("\n--- Coalescing ---")
mock_ks.writes = {}
local w2 = kv_writer.new(mock_ks)

w2:push("key.x", "old_value")
w2:push("key.x", "new_value")
w2:push("key.x", "newest_value")

check("depth is 1 (coalesced)", w2:depth() == 1, "got " .. w2:depth())
local s = w2:stats()
check("coalesced count is 2", s.coalesced == 2, "got " .. s.coalesced)

w2:tick()
check("only newest value written", mock_ks.writes["key.x"] == "newest_value")

print("\n--- Flush ---")
mock_ks.writes = {}
mock_ks.writes._count = 0
local w3 = kv_writer.new(mock_ks)

w3:push("a", "1")
w3:push("b", "2")
w3:push("c", "3")
w3:flush()

check("all 3 flushed", mock_ks.writes._count == 3)
check("depth 0 after flush", w3:depth() == 0)

print("\n--- Mixed coalescing + ordering ---")
mock_ks.writes = {}
mock_ks.writes._count = 0
local w4 = kv_writer.new(mock_ks)

w4:push("first", "1")
w4:push("second", "2")
w4:push("first", "1_updated")  -- coalesces, stays in original position

w4:tick()
check("first key written first", mock_ks.writes["first"] == "1_updated")
check("second not yet written", mock_ks.writes["second"] == nil)

w4:tick()
check("second written", mock_ks.writes["second"] == "2")

---------------------------------------------------------------------------

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", passed, failed))
if failed > 0 then print("FAILED"); os.exit(1) else print("PASSED") end
