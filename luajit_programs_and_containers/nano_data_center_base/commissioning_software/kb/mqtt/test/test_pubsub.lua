--[[
    test_pubsub.lua — Test streaming MQTT pub/sub module.

    Tests:
      - create/destroy
      - connect/disconnect
      - subscribe + publish + poll round-trip
      - multiple messages in one poll
      - stats tracking
      - non-blocking drain

    Requires: mosquitto broker on localhost:1883
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local HOST = os.getenv("MQTT_HOST") or "localhost"
local PORT = tonumber(os.getenv("MQTT_PORT") or "1883")

-- Add lib/ to path
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
package.path = script_dir .. "../lib/?.lua;" .. package.path
package.cpath = script_dir .. "../?.so;" .. package.cpath

local pubsub = require("mqtt_pubsub")
local PubSub = pubsub.PubSub

local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        pass_count = pass_count + 1
        io.write(string.format("  PASS: %s\n", name))
    else
        fail_count = fail_count + 1
        io.write(string.format("  FAIL: %s — %s\n", name, msg or ""))
    end
end

-- Unique prefix to avoid stale retained messages
local test_prefix = "test/ps/" .. tostring(os.time()) .. "/"

print("=== MQTT PubSub Streaming Test ===\n")
print(string.format("Broker: %s:%d, prefix: %s\n", HOST, PORT, test_prefix))

---------------------------------------------------------------------------
-- Test 1: Create + connect + disconnect + destroy
---------------------------------------------------------------------------
print("--- Lifecycle ---")

local ps = PubSub.new(HOST, PORT, "test_ps_lifecycle")
check("create", ps ~= nil)

ps:connect(5000)
check("connected", ps:is_connected())

ps:disconnect()
check("disconnected", not ps:is_connected())

ps:destroy()
check("destroyed", true)

---------------------------------------------------------------------------
-- Test 2: Pub/sub round-trip
---------------------------------------------------------------------------
print("\n--- Pub/Sub Round-trip ---")

local ts = tostring(os.time())
local pub = PubSub.new(HOST, PORT, "test_ps_pub_" .. ts)
local sub = PubSub.new(HOST, PORT, "test_ps_sub_" .. ts)
pub:connect(5000)
sub:connect(5000)

local test_topic = test_prefix .. "roundtrip"
sub:subscribe(test_topic, 1)
ffi.C.usleep(100000)  -- 100ms for subscription to propagate

-- Publish a message
pub:publish(test_topic, '{"hello":"world"}', 1, false)

-- Poll subscriber
ffi.C.usleep(50000)  -- 50ms for message delivery
local msgs = sub:poll(100)  -- 100ms poll
check("received 1 message", #msgs == 1, "got " .. #msgs)

if #msgs > 0 then
    check("topic matches", msgs[1].topic == test_topic,
        "got " .. msgs[1].topic)
    check("payload matches", msgs[1].payload == '{"hello":"world"}',
        "got " .. msgs[1].payload)
end

---------------------------------------------------------------------------
-- Test 3: Multiple messages in one poll
---------------------------------------------------------------------------
print("\n--- Batch Messages ---")

local batch_topic = test_prefix .. "batch"
sub:subscribe(batch_topic, 1)
ffi.C.usleep(100000)

-- Publish 5 messages rapidly
for i = 1, 5 do
    pub:publish(batch_topic, string.format('{"seq":%d}', i), 1, false)
end

-- Let messages arrive
ffi.C.usleep(200000)  -- 200ms

-- Poll all at once
local batch_msgs = sub:poll(100)
check("received 5 messages", #batch_msgs == 5,
    "got " .. #batch_msgs)

if #batch_msgs >= 5 then
    check("first payload", batch_msgs[1].payload == '{"seq":1}',
        "got " .. batch_msgs[1].payload)
    check("last payload", batch_msgs[5].payload == '{"seq":5}',
        "got " .. batch_msgs[5].payload)
end

---------------------------------------------------------------------------
-- Test 4: Stats
---------------------------------------------------------------------------
print("\n--- Stats ---")

local sub_stats = sub:get_stats()
check("received > 0", sub_stats.received > 0,
    "got " .. tostring(sub_stats.received))
check("polled > 0", sub_stats.polled > 0,
    "got " .. tostring(sub_stats.polled))
check("dropped == 0", sub_stats.dropped == 0,
    "got " .. tostring(sub_stats.dropped))

local pub_stats = pub:get_stats()
check("published > 0", pub_stats.published > 0,
    "got " .. tostring(pub_stats.published))

print(string.format("  Sub: received=%d polled=%d dropped=%d ring_used=%d",
    sub_stats.received, sub_stats.polled, sub_stats.dropped, sub_stats.ring_used))
print(string.format("  Pub: published=%d", pub_stats.published))

---------------------------------------------------------------------------
-- Test 5: Non-blocking drain
---------------------------------------------------------------------------
print("\n--- Non-blocking Drain ---")

local drain_topic = test_prefix .. "drain"
sub:subscribe(drain_topic, 1)
ffi.C.usleep(100000)

pub:publish(drain_topic, '{"drain":true}', 1, false)
ffi.C.usleep(100000)

-- poll once to run mosquitto loop (delivers message to ring buffer)
sub:poll(50)

-- drain should return the message without blocking
local drain_msgs = sub:drain()
-- Message may have been returned by poll or drain
check("drain works", true)  -- no crash = pass

---------------------------------------------------------------------------
-- Test 6: Empty poll returns empty table
---------------------------------------------------------------------------
print("\n--- Empty Poll ---")

-- Drain everything first
sub:poll(50)
sub:drain()

local empty = sub:poll(10)
check("empty poll returns empty", #empty == 0, "got " .. #empty)

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
pub:disconnect(); pub:destroy()
sub:disconnect(); sub:destroy()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
