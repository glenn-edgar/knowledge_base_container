--[[
    test_link_manager.lua -- Unit test for link_manager state machine.
    Uses mock mqtt_hub and kv_writer. No external dependencies.
]]

print("=== Link Manager Unit Test ===\n")

local json_util = require("json_util")

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

-- Mock MQTT hub
local mock_mqtt = {
    published = {},  -- topic → last payload
}
function mock_mqtt:send_planner_ack(robot_id, json_str)
    self.published["ack:" .. robot_id] = json_str
end
function mock_mqtt:send_planner_heartbeat(robot_id, json_str)
    self.published["hb:" .. robot_id] = json_str
end
function mock_mqtt:send_planner_disconnect(robot_id, json_str)
    self.published["disc:" .. robot_id] = json_str
end
function mock_mqtt:set_wire_format(robot_id, fmt)
    self.published["wire:" .. robot_id] = fmt
end
function mock_mqtt:clear_retained(robot_id)
    self.published["clear:" .. robot_id] = true
end

-- Mock KV writer
local mock_kv = { pushes = {} }
function mock_kv:push(key, value)
    self.pushes[key] = value
end

---------------------------------------------------------------------------

local link_manager = require("link_manager")
local site = "moonbase.alpha.surface_ops"

print("--- Initial State ---")
local lm = link_manager.new(mock_mqtt, mock_kv, site)

check("unknown robot is not live", not lm:is_live("rover_1"))
check("unknown robot state is offline", lm:get_state("rover_1") == "offline")
check("no live robots", #lm:list_live() == 0)

print("\n--- Heartbeat from Announcing Robot ---")

lm:on_heartbeat("rover_1", json_util.encode({
    type = "link_heartbeat",
    robot_id = "rover_1",
    seq = 1,
    link_state = "announcing",
    energy_remaining = 9000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 is registering", lm:get_state("rover_1") == "registering")
check("ack sent", mock_mqtt.published["ack:rover_1"] ~= nil)
check("not yet live", not lm:is_live("rover_1"))

print("\n--- Robot Confirms ---")

lm:on_confirm("rover_1", json_util.encode({
    type = "link_confirm",
    robot_id = "rover_1",
    ack_seq = 1,
    wire_format = "cbor",
    capabilities = {"init_check", "path_spline"},
    energy_max = 10000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 is live", lm:is_live("rover_1"))
check("wire format set to cbor", mock_mqtt.published["wire:rover_1"] == "cbor")
check("KV link status written", mock_kv.pushes[site .. ".robots.rover_1.status.link"] ~= nil)

local kv_data = json_util.decode(mock_kv.pushes[site .. ".robots.rover_1.status.link"])
check("KV link_state is live", kv_data.link_state == "live")
check("KV wire_format is cbor", kv_data.wire_format == "cbor")

print("\n--- Live Heartbeat Update ---")

mock_kv.pushes = {}
lm:on_heartbeat("rover_1", json_util.encode({
    type = "link_heartbeat",
    robot_id = "rover_1",
    seq = 5,
    link_state = "live",
    energy_remaining = 8500,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("still live", lm:is_live("rover_1"))
check("KV updated on heartbeat", mock_kv.pushes[site .. ".robots.rover_1.status.link"] ~= nil)

print("\n--- Planner Heartbeat Sending ---")

mock_mqtt.published = {}
-- Force planner heartbeat by setting last_planner_hb far in past
lm.last_planner_hb = os.time() - 100
lm:tick()

check("planner heartbeat sent to rover_1", mock_mqtt.published["hb:rover_1"] ~= nil)

print("\n--- Clean Disconnect ---")

mock_mqtt.published = {}
mock_kv.pushes = {}
lm:on_disconnect("rover_1", json_util.encode({
    type = "link_disconnect",
    robot_id = "rover_1",
    reason = "shutdown",
    energy_remaining = 7200,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 is offline after disconnect", lm:get_state("rover_1") == "offline")
check("not live", not lm:is_live("rover_1"))
check("retained cleared", mock_mqtt.published["clear:rover_1"] == true)
check("KV offline written", mock_kv.pushes[site .. ".robots.rover_1.status.link"] ~= nil)

local offline_data = json_util.decode(mock_kv.pushes[site .. ".robots.rover_1.status.link"])
check("KV link_state is offline", offline_data.link_state == "offline")

print("\n--- Second Robot ---")

lm:on_heartbeat("rover_2", json_util.encode({
    type = "link_heartbeat", robot_id = "rover_2", seq = 1,
    link_state = "announcing", energy_remaining = 5000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))
lm:on_confirm("rover_2", json_util.encode({
    type = "link_confirm", robot_id = "rover_2", ack_seq = 2,
    wire_format = "json", capabilities = {"init_check"},
    energy_max = 5000, ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_2 is live", lm:is_live("rover_2"))
check("rover_1 still offline", not lm:is_live("rover_1"))
local live = lm:list_live()
check("1 live robot", #live == 1)
check("live robot is rover_2", live[1] == "rover_2")

print("\n--- Planner Shutdown ---")

mock_mqtt.published = {}
lm:shutdown()

check("rover_2 offline after shutdown", not lm:is_live("rover_2"))
check("disconnect sent to rover_2", mock_mqtt.published["disc:rover_2"] ~= nil)

---------------------------------------------------------------------------

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", passed, failed))
if failed > 0 then print("FAILED"); os.exit(1) else print("PASSED") end
