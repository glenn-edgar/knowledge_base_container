--[[
    test_link_helper.lua -- Test helper: set up link protocol on mqtt_hub.

    Attaches a link_manager to the mqtt_hub, then waits for the robot
    to register via the announce → ack → confirm handshake.

    Usage:
        local link_helper = require("test_link_helper")
        local lm = link_helper.setup(mqtt_hub, site, robot_id, timeout_s)
        -- lm is the link_manager instance (optional, for inspection)
]]

local ffi = require("ffi")
local json_util = require("json_util")
local link_manager = require("link_manager")
local kv_writer = require("kv_writer")

local M = {}

-- Mock kv_writer for tests (no NATS dependency)
local function mock_kv()
    local kv = {}
    function kv:push(key, value) end
    function kv:flush() end
    return kv
end

function M.setup(mqtt_hub, site, robot_id, timeout_s)
    timeout_s = timeout_s or 10

    local kv = mock_kv()
    local lm = link_manager.new(mqtt_hub, kv, site)

    -- Route link messages from MQTT hub to link_manager
    mqtt_hub:set_link_handler(function(rid, payload)
        local ok, data = pcall(json_util.decode, payload)
        if not ok or not data then return end
        local msg_type = data.type
        if msg_type == "link_announce" then
            lm:on_announce(rid, payload)
        elseif msg_type == "link_heartbeat" then
            lm:on_heartbeat(rid, payload)
        elseif msg_type == "link_confirm" then
            lm:on_confirm(rid, payload)
        elseif msg_type == "link_disconnect" then
            lm:on_disconnect(rid, payload)
        end
    end)

    -- Wait for robot to register
    local deadline = os.time() + timeout_s
    io.stderr:write(string.format("LINK_HELPER: waiting for %s to register...\n", robot_id))

    while not lm:is_live(robot_id) do
        mqtt_hub:poll_and_route(100)
        lm:tick()
        ffi.C.usleep(10000)  -- 10ms

        if os.time() >= deadline then
            io.stderr:write(string.format(
                "LINK_HELPER: timeout waiting for %s (state=%s)\n",
                robot_id, lm:get_state(robot_id)))
            break
        end
    end

    if lm:is_live(robot_id) then
        io.stderr:write(string.format("LINK_HELPER: %s registered OK\n", robot_id))
    end

    return lm
end

return M
