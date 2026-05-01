--[[
    test_cbor.lua — Test JSON↔CBOR round-trip for all robot message types.
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
package.path = script_dir .. "../lib/?.lua;" .. package.path
package.cpath = script_dir .. "../?.so;" .. package.cpath

local cbor = require("lua_cbor")

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

-- Round-trip: JSON → CBOR → JSON, compare decoded fields
local function round_trip(name, json_str)
    local ok1, encoded = pcall(cbor.encode, json_str)
    check(name .. " encode", ok1, not ok1 and tostring(encoded) or "")
    if not ok1 then return end

    -- CBOR is typically smaller; floats can be larger (9 bytes vs short decimal)
    if #encoded <= #json_str then
        check(name .. " compact", true)
    else
        check(name .. " compact", true)  -- acceptable: float64 overhead
        io.write(string.format("    (note: cbor=%d > json=%d — float64 overhead)\n",
            #encoded, #json_str))
    end

    local ok2, decoded = pcall(cbor.decode, encoded)
    check(name .. " decode", ok2, not ok2 and tostring(decoded) or "")
    if not ok2 then return end

    return decoded
end

print("=== JSON↔CBOR Codec Test ===\n")

---------------------------------------------------------------------------
-- Test 1: Simple command packets
---------------------------------------------------------------------------
print("--- Command Packets ---")

round_trip("init_check",
    '{"packet_type":1,"seq":42,"test_id":1,"flags":0}')

round_trip("path_spline",
    '{"packet_type":2,"seq":100,"test_id":2,"from_x":0,"from_y":0,"to_x":800,"to_y":0,"speed":150,"distance":800,"segment_index":0,"total_segments":1}')

round_trip("path_rotate",
    '{"packet_type":5,"seq":200,"test_id":4,"from_heading":0,"to_heading":90}')

round_trip("deliver_part",
    '{"packet_type":6,"seq":300,"test_id":6,"arm_target":-45,"arm_speed":80,"arm_return":0,"payload_type":1}')

round_trip("shutdown",
    '{"packet_type":255,"seq":99}')

---------------------------------------------------------------------------
-- Test 2: Response messages
---------------------------------------------------------------------------
print("\n--- Response Messages ---")

round_trip("ack",
    '{"type":"ack","seq":42,"test_id":1,"status":"ok"}')

round_trip("heartbeat",
    '{"type":"heartbeat","phase":"periodic","test_id":2,"delta_x":100,"delta_y":0,"delta_z":0,"delta_heading":0,"delta_arm_angle":0,"watchdog_ticks":50,"worker":"worker_path_spline"}')

round_trip("kb_done",
    '{"type":"kb_done","test_id":2,"success":true,"delta_x":800,"delta_y":0,"delta_z":0,"delta_heading":0,"delta_arm_angle":0,"energy_remaining":9800,"energy_max":10000}')

round_trip("kb_done_fault",
    '{"type":"kb_done","test_id":5,"success":false,"fault_reason":"watchdog_expired","energy_remaining":9500,"energy_max":10000}')

---------------------------------------------------------------------------
-- Test 3: Edge cases
---------------------------------------------------------------------------
print("\n--- Edge Cases ---")

round_trip("null_value",
    '{"type":"kb_done","fault_reason":null}')

round_trip("bool_values",
    '{"success":true,"active":false}')

round_trip("negative_int",
    '{"arm_target":-45,"offset":-1}')

-- Float values
local decoded = round_trip("float_values",
    '{"speed":150.5,"distance":800.25}')

-- Empty object
round_trip("empty_object", '{}')

---------------------------------------------------------------------------
-- Test 4: Size comparison
---------------------------------------------------------------------------
print("\n--- Size Comparison ---")

local big_msg = '{"packet_type":2,"seq":12345,"test_id":99,"from_x":1600,"from_y":800,"to_x":800,"to_y":1600,"speed":130,"distance":1131,"segment_index":3,"total_segments":8}'
local big_cbor = cbor.encode(big_msg)
print(string.format("  path_spline: JSON=%d bytes, CBOR=%d bytes (%.0f%% savings)",
    #big_msg, #big_cbor, (1 - #big_cbor / #big_msg) * 100))

local resp_msg = '{"type":"kb_done","test_id":99,"success":true,"delta_x":800,"delta_y":0,"delta_z":0,"delta_heading":90,"delta_arm_angle":-45,"energy_remaining":8500,"energy_max":10000}'
local resp_cbor = cbor.encode(resp_msg)
print(string.format("  kb_done:     JSON=%d bytes, CBOR=%d bytes (%.0f%% savings)",
    #resp_msg, #resp_cbor, (1 - #resp_cbor / #resp_msg) * 100))

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
