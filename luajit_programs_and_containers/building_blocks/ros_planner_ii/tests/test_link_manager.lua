--[[
    test_link_manager.lua -- Unit test for link_manager state machine.
    Uses mock mqtt_hub and kv_writer. No external dependencies.

    Tests URLP v1 protocol:
      - link_announce (separate from link_heartbeat)
      - Three-way handshake: announce → ack → confirm → live
      - Keepalive heartbeats (no state transitions)
      - Re-announce from live robot → link exception
      - Clean disconnect
      - Planner shutdown
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
    published = {},
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

-- Exception tracker
local exceptions = {}
local function on_exception(robot_id, reason)
    exceptions[#exceptions + 1] = { robot_id = robot_id, reason = reason }
end

---------------------------------------------------------------------------

local link_manager = require("link_manager")
local site = "moonbase.alpha.surface_ops"

print("--- Initial State ---")
local lm = link_manager.new(mock_mqtt, mock_kv, site, {
    on_link_exception = on_exception,
})

check("unknown robot is not live", not lm:is_live("rover_1"))
check("unknown robot state is offline", lm:get_state("rover_1") == "offline")
check("no live robots", #lm:list_live() == 0)

print("\n--- Announce from New Robot ---")

lm:on_announce("rover_1", json_util.encode({
    type = "link_announce",
    robot_id = "rover_1",
    class_name = "lunar_rover",
    seq = 1,
    energy_remaining = 9000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 is registering", lm:get_state("rover_1") == "registering")
check("ack sent", mock_mqtt.published["ack:rover_1"] ~= nil)
check("not yet live", not lm:is_live("rover_1"))
check("no exception on first announce", #exceptions == 0)
check("class_name stored from announce", lm:get_class("rover_1") == "lunar_rover")

print("\n--- Robot Confirms ---")

lm:on_confirm("rover_1", json_util.encode({
    type = "link_confirm",
    robot_id = "rover_1",
    class_name = "lunar_rover",
    ack_seq = 1,
    wire_format = "cbor",
    capabilities = {"init_check", "path_spline"},
    energy_max = 10000,
    energy_remaining = 9000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 is live", lm:is_live("rover_1"))
check("wire format set to cbor", mock_mqtt.published["wire:rover_1"] == "cbor")
check("KV link status written", mock_kv.pushes[site .. ".robots.rover_1.status.link"] ~= nil)

local kv_data = json_util.decode(mock_kv.pushes[site .. ".robots.rover_1.status.link"])
check("KV link_state is live", kv_data.link_state == "live")
check("KV wire_format is cbor", kv_data.wire_format == "cbor")

print("\n--- Keepalive Heartbeat (no state change) ---")

mock_kv.pushes = {}
lm:on_heartbeat("rover_1", json_util.encode({
    type = "link_heartbeat",
    robot_id = "rover_1",
    energy_remaining = 8500,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("still live", lm:is_live("rover_1"))
check("KV updated on heartbeat", mock_kv.pushes[site .. ".robots.rover_1.status.link"] ~= nil)

print("\n--- Heartbeat from non-live robot ignored ---")

mock_kv.pushes = {}
lm:on_heartbeat("rover_99", json_util.encode({
    type = "link_heartbeat",
    robot_id = "rover_99",
    energy_remaining = 5000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_99 not live", not lm:is_live("rover_99"))
check("no KV write for unregistered heartbeat",
    mock_kv.pushes[site .. ".robots.rover_99.status.link"] == nil)

print("\n--- Re-announce from Live Robot (link exception) ---")

mock_mqtt.published = {}
mock_kv.pushes = {}
exceptions = {}

lm:on_announce("rover_1", json_util.encode({
    type = "link_announce",
    robot_id = "rover_1",
    class_name = "lunar_rover",
    seq = 1,
    energy_remaining = 8000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("exception fired", #exceptions == 1)
check("exception robot_id", exceptions[1].robot_id == "rover_1")
check("exception reason is robot_reboot", exceptions[1].reason == "robot_reboot")
check("rover_1 back to registering", lm:get_state("rover_1") == "registering")
check("new ack sent", mock_mqtt.published["ack:rover_1"] ~= nil)

print("\n--- Complete re-registration ---")

lm:on_confirm("rover_1", json_util.encode({
    type = "link_confirm",
    robot_id = "rover_1",
    class_name = "lunar_rover",
    wire_format = "json",
    capabilities = {"init_check", "path_spline"},
    energy_max = 10000,
    energy_remaining = 8000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_1 live again", lm:is_live("rover_1"))
check("wire format now json", mock_mqtt.published["wire:rover_1"] == "json")

print("\n--- Planner Heartbeat Sending (with seq) ---")

mock_mqtt.published = {}
lm.last_planner_hb = os.time() - 100
lm:tick()

check("planner heartbeat sent to rover_1", mock_mqtt.published["hb:rover_1"] ~= nil)
local hb_data = json_util.decode(mock_mqtt.published["hb:rover_1"])
check("planner heartbeat has seq", hb_data.seq ~= nil and hb_data.seq > 0)

print("\n--- Clean Disconnect (with exception) ---")

mock_mqtt.published = {}
mock_kv.pushes = {}
exceptions = {}

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
check("exception on disconnect from live", #exceptions == 1)
check("disconnect exception reason", exceptions[1].reason == "robot_disconnect")

local offline_data = json_util.decode(mock_kv.pushes[site .. ".robots.rover_1.status.link"])
check("KV link_state is offline", offline_data.link_state == "offline")

print("\n--- Second Robot ---")

lm:on_announce("rover_2", json_util.encode({
    type = "link_announce", robot_id = "rover_2", class_name = "lunar_rover",
    seq = 1, energy_remaining = 5000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))
lm:on_confirm("rover_2", json_util.encode({
    type = "link_confirm", robot_id = "rover_2", class_name = "lunar_rover",
    wire_format = "json", capabilities = {"init_check"},
    energy_max = 5000, energy_remaining = 5000,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}))

check("rover_2 is live", lm:is_live("rover_2"))
check("rover_1 still offline", not lm:is_live("rover_1"))
local live = lm:list_live()
check("1 live robot", #live == 1)
check("live robot is rover_2", live[1] == "rover_2")

print("\n--- Planner Shutdown ---")

mock_mqtt.published = {}
exceptions = {}
lm:shutdown()

check("rover_2 offline after shutdown", not lm:is_live("rover_2"))
check("disconnect sent to rover_2", mock_mqtt.published["disc:rover_2"] ~= nil)

---------------------------------------------------------------------------

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", passed, failed))
if failed > 0 then print("FAILED"); os.exit(1) else print("PASSED") end
