-- test_ringbuf.lua -- Test SPSC ringbuffer via FFI

local vmrt = require("vmrt_ffi")

print("=== Ringbuffer Tests ===\n")

-- Test 1: basic send/recv
local pipe = vmrt.pipe_create()
assert(pipe ~= nil, "pipe creation failed")

assert(vmrt.send(pipe.to_hub, "hello"), "send failed")
local msg = vmrt.recv(pipe.to_hub)
assert(msg == "hello", "expected 'hello', got: " .. tostring(msg))
print("  1. basic send/recv: OK")

-- Test 2: empty recv returns nil
local msg2 = vmrt.recv(pipe.to_hub)
assert(msg2 == nil, "expected nil on empty recv")
print("  2. empty recv: OK")

-- Test 3: multiple messages in sequence
for i = 1, 10 do
    assert(vmrt.send(pipe.to_hub, string.format("msg_%d", i)))
end
for i = 1, 10 do
    local m = vmrt.recv(pipe.to_hub)
    assert(m == string.format("msg_%d", i), "mismatch at " .. i)
end
assert(vmrt.recv(pipe.to_hub) == nil, "should be empty")
print("  3. sequence of 10: OK")

-- Test 4: both directions
assert(vmrt.send(pipe.to_hub, "to_hub"))
assert(vmrt.send(pipe.from_hub, "from_hub"))
assert(vmrt.recv(pipe.to_hub) == "to_hub")
assert(vmrt.recv(pipe.from_hub) == "from_hub")
print("  4. bidirectional: OK")

-- Test 5: large message
local big = string.rep("x", 4096)
assert(vmrt.send(pipe.to_hub, big))
local big_back = vmrt.recv(pipe.to_hub)
assert(big_back == big, "large message mismatch")
print("  5. 4KB message: OK")

-- Test 6: fill buffer until full
local small_pipe = vmrt.pipe_create(256)
local count = 0
while vmrt.send(small_pipe.to_hub, "test") do
    count = count + 1
    if count > 100 then break end  -- safety
end
assert(count > 0 and count < 100, "fill count unexpected: " .. count)
-- drain
local drained = 0
while vmrt.recv(small_pipe.to_hub) do drained = drained + 1 end
assert(drained == count, string.format("drained %d != sent %d", drained, count))
print(string.format("  6. fill/drain (256B buf, %d msgs): OK", count))

-- Test 7: JSON-like messages
local json1 = '{"test_id":2,"next_test":3,"speed":150,"from":{"x":0,"y":0}}'
local json2 = '{"test_id":3,"next_test":4,"speed":100}'
assert(vmrt.send(pipe.to_hub, json1))
assert(vmrt.send(pipe.to_hub, json2))
assert(vmrt.recv(pipe.to_hub) == json1)
assert(vmrt.recv(pipe.to_hub) == json2)
print("  7. JSON messages: OK")

-- Cleanup
vmrt.pipe_destroy(pipe)
vmrt.pipe_destroy(small_pipe)

print("\nAll ringbuffer tests passed.")
