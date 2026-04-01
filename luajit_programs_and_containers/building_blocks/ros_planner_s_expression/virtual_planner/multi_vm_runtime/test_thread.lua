-- test_thread.lua -- Test multi-VM threading

local ffi  = require("ffi")
local vmrt = require("vmrt_ffi")

ffi.cdef[[ int usleep(unsigned int usec); ]]

print("=== Thread Tests ===\n")

-- Get absolute path to test hub script
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local hub_script = script_dir .. "test_hub_script.lua"

-- Poll recv with timeout (hub thread may need time to process)
local function recv_wait(ringbuf, timeout_ms)
    timeout_ms = timeout_ms or 500
    local elapsed = 0
    while elapsed < timeout_ms do
        local msg = vmrt.recv(ringbuf)
        if msg then return msg end
        ffi.C.usleep(1000)
        elapsed = elapsed + 1
    end
    return nil
end

-- Test 1: spawn and join
local pipe = vmrt.pipe_create()
local hub = vmrt.hub_spawn(hub_script, package.path, pipe)

-- Wait for ready
local timeout = 0
while not vmrt.hub_is_ready(hub) do
    ffi.C.usleep(1000)
    timeout = timeout + 1
    assert(timeout < 5000, "hub did not become ready")
end
print("  1. hub spawned and ready: OK")

-- Test 2: send message, get reply
vmrt.send(pipe.to_hub, "hello_from_main")
local reply = recv_wait(pipe.from_hub)
assert(reply == "reply:hello_from_main",
    "expected 'reply:hello_from_main', got: " .. tostring(reply))
print("  2. send/recv across threads: OK")

-- Test 3: multiple messages
for i = 1, 5 do
    vmrt.send(pipe.to_hub, string.format("msg_%d", i))
end
for i = 1, 5 do
    local r = recv_wait(pipe.from_hub)
    assert(r == string.format("reply:msg_%d", i),
        string.format("msg %d: expected 'reply:msg_%d', got '%s'", i, i, tostring(r)))
end
print("  3. 5 round-trips: OK")

-- Test 4: JSON message
local json = '{"test_id":2,"next_test":3,"speed":150}'
vmrt.send(pipe.to_hub, json)
local json_reply = recv_wait(pipe.from_hub)
assert(json_reply == "reply:" .. json, "JSON round-trip mismatch")
print("  4. JSON round-trip: OK")

-- Test 5: shutdown
vmrt.send(pipe.to_hub, "shutdown")
local exit_code = vmrt.hub_join(hub)
assert(exit_code == 0, "hub exit code: " .. tostring(exit_code))
assert(vmrt.hub_is_exited(hub), "hub should be exited")

-- Drain the shutdown reply
local shutdown_reply = recv_wait(pipe.from_hub)
assert(shutdown_reply == "reply:shutdown", "shutdown reply: " .. tostring(shutdown_reply))
print("  5. graceful shutdown: OK")

-- Cleanup
vmrt.hub_destroy(hub)
vmrt.pipe_destroy(pipe)

-- Test 6: spawn multiple hubs on separate pipes
local NUM_HUBS = 3
local hubs = {}
local pipes = {}

for i = 1, NUM_HUBS do
    pipes[i] = vmrt.pipe_create()
    hubs[i] = vmrt.hub_spawn(hub_script, package.path, pipes[i])
end

-- Wait for all ready
for i = 1, NUM_HUBS do
    timeout = 0
    while not vmrt.hub_is_ready(hubs[i]) do
        ffi.C.usleep(1000)
        timeout = timeout + 1
        assert(timeout < 5000, "hub " .. i .. " not ready")
    end
end

-- Send to all, recv from all
for i = 1, NUM_HUBS do
    vmrt.send(pipes[i].to_hub, string.format("hub_%d", i))
end
for i = 1, NUM_HUBS do
    local r = recv_wait(pipes[i].from_hub)
    assert(r == string.format("reply:hub_%d", i),
        string.format("hub %d: got '%s'", i, tostring(r)))
end

-- Shutdown all
for i = 1, NUM_HUBS do
    vmrt.send(pipes[i].to_hub, "shutdown")
    vmrt.hub_join(hubs[i])
    vmrt.hub_destroy(hubs[i])
    vmrt.pipe_destroy(pipes[i])
end
print(string.format("  6. %d concurrent hubs: OK", NUM_HUBS))

print("\nAll thread tests passed.")
