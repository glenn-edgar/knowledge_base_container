#!/usr/bin/env luajit
-- =============================================================================
-- mock_mqtt_robot.lua -- Minimal MQTT robot fixture for A.5 Phase 2 smoke.
--
-- Connects to the live MQTT broker, performs the link handshake to
-- register with the planner, and instant-acks every RPC command. No
-- physics, no movement -- every action immediately publishes ack +
-- kb_done success on the stream_bus.
--
-- Goal: confirm the planner's dispatch path round-trips end-to-end
-- (mission claim → route plan → segment-shaped wire packets → ack →
-- kb_done → mission completion). Live-cluster smoke for A.4f / A.5
-- Phase 1 changes.
--
-- Usage:
--   LD_LIBRARY_PATH=$NDC_BASE/commissioning_software/kb/mqtt \
--     mock_mqtt_robot.lua --robot rover_1 --site moon_base_alpha
--
-- Args (all optional):
--   --robot <id>         robot_id (default rover_1)
--   --site <name>        site name (default moon_base_alpha)
--   --host <h>           MQTT broker host (default localhost)
--   --port <p>           MQTT broker port (default 1883)
--   --energy <int>       initial energy_remaining (default 10000)
--   --no-handshake       skip link_announce (assume already registered)
--   --once               exit after the first RPC ack
--   --quiet              only log errors
--
-- See mock_mqtt_robot_lib.lua for the pure ack/state-machine logic
-- (which the smoke harness exercises). See lib/link_manager.lua and
-- lib/hub_runtime.lua for the planner-side contract this implements.
-- =============================================================================

---------------------------------------------------------------------------
-- Path bootstrap
---------------------------------------------------------------------------

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
-- scripts/ → construction/ → system_node_control/ → commissioning_software/
--        → nano_data_center_base/ → luajit_programs_and_containers/
local PLANNER_LIB = SCRIPT_DIR
    .. "../../../../../nano_data_center_instance/app_containers/"
    .. "mission_planner/container/planner/lib"

package.path = SCRIPT_DIR        .. "?.lua;"
            .. PLANNER_LIB       .. "/?.lua;"
            .. PLANNER_LIB       .. "/lib/?.lua;"
            .. package.path

local lib    = require("mock_mqtt_robot_lib")
local dkjson = require("dkjson")

---------------------------------------------------------------------------
-- arg parsing
---------------------------------------------------------------------------

local opts = {
    robot = "rover_1", site = "moon_base_alpha",
    host  = "localhost", port = 1883,
    energy = 10000,
    once = false, no_handshake = false, quiet = false,
}

local i = 1
while i <= #arg do
    local k = arg[i]
    if     k == "--robot"        then opts.robot = arg[i + 1]; i = i + 2
    elseif k == "--site"         then opts.site = arg[i + 1]; i = i + 2
    elseif k == "--host"         then opts.host = arg[i + 1]; i = i + 2
    elseif k == "--port"         then opts.port = tonumber(arg[i + 1]); i = i + 2
    elseif k == "--energy"       then opts.energy = tonumber(arg[i + 1]); i = i + 2
    elseif k == "--once"         then opts.once = true; i = i + 1
    elseif k == "--no-handshake" then opts.no_handshake = true; i = i + 1
    elseif k == "--quiet"        then opts.quiet = true; i = i + 1
    elseif k == "-h" or k == "--help" then
        io.stderr:write([[
mock_mqtt_robot.lua -- ack-everything robot fixture for A.5 happy-path smoke.
Usage: mock_mqtt_robot.lua [--robot ID] [--site NAME] [--host H] [--port P]
                           [--energy N] [--once] [--no-handshake] [--quiet]
Set LD_LIBRARY_PATH to the dir containing libmqtt_pubsub.so before running.
]])
        os.exit(2)
    else
        io.stderr:write("unknown arg: " .. tostring(k) .. "\n"); os.exit(2)
    end
end

local function log(fmt, ...)
    if not opts.quiet then
        io.stderr:write("[mock] " .. string.format(fmt, ...) .. "\n")
    end
end

---------------------------------------------------------------------------
-- MQTT bring-up
---------------------------------------------------------------------------

local pubsub_ok, pubsub = pcall(require, "lib.mqtt_pubsub")
if not pubsub_ok then
    io.stderr:write("FATAL: cannot load lib.mqtt_pubsub. Set LD_LIBRARY_PATH "
        .. "to the dir containing libmqtt_pubsub.so. ("
        .. tostring(pubsub) .. ")\n")
    os.exit(1)
end

local topics = lib.make_topics(opts.site, opts.robot)
local link   = lib.LinkState.new(opts.robot, "lunar_rover", nil, opts.energy)

local ps = pubsub.PubSub.new(opts.host, opts.port, "mock_robot_" .. opts.robot)
local connect_ok, connect_err = pcall(function() ps:connect(5000) end)
if not connect_ok then
    io.stderr:write("FATAL: MQTT connect failed: " .. tostring(connect_err) .. "\n")
    os.exit(1)
end

ps:subscribe(topics.rpc, 1)
ps:subscribe(topics.planner_glob, 1)
log("connected mqtt://%s:%d  robot=%s site=%s",
    opts.host, opts.port, opts.robot, opts.site)
log("subs: %s  %s", topics.rpc, topics.planner_glob)

if not opts.no_handshake then
    ps:publish(topics.link_out, link:make_announce(), 1, false)
    log("→ link_announce")
end

---------------------------------------------------------------------------
-- Main loop with SIGINT (2) clean shutdown
---------------------------------------------------------------------------

local ffi = require("ffi")
ffi.cdef[[
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
    int usleep(unsigned int);
]]

local SIGINT_CAUGHT = false
local sigint_cb = ffi.cast("sighandler_t", function() SIGINT_CAUGHT = true end)
ffi.C.signal(2, sigint_cb)

local last_robot_hb = os.time()
local HB_PERIOD_S   = 10
local rpc_count     = 0

while not SIGINT_CAUGHT do
    local msgs = ps:poll(50)
    for _, m in ipairs(msgs or {}) do
        if m.topic == topics.rpc then
            rpc_count = rpc_count + 1
            local ok, cmd = pcall(dkjson.decode, m.payload)
            if ok and cmd then
                ps:publish(topics.stream_bus, lib.make_ack(cmd), 1, false)
                if cmd.distance then
                    link.energy_remaining = math.max(0,
                        link.energy_remaining - math.floor(cmd.distance + 0.5))
                end
                ps:publish(topics.stream_bus,
                    lib.make_kb_done_success(cmd, link.energy_remaining),
                    1, false)
                log("← rpc kb=%s seq=%s test_id=%s  → ack+kb_done energy=%d",
                    tostring(cmd.kb_name or cmd.packet_type),
                    tostring(cmd.seq), tostring(cmd.test_id),
                    link.energy_remaining)
            else
                log("← rpc UNDECODABLE (%d bytes)", #m.payload)
            end
            if opts.once then SIGINT_CAUGHT = true; break end
        else
            local outcome = link:on_planner_verb(m.topic, m.payload)
            if outcome then
                if outcome.send_payload then
                    ps:publish(topics.link_out, outcome.send_payload, 1, false)
                    log("→ link state=%s (sent reply)", outcome.transitioned_to)
                else
                    log("→ link state=%s", outcome.transitioned_to)
                end
            end
        end
    end

    local now = os.time()
    if (link.state == "registering" or link.state == "live")
       and now - last_robot_hb >= HB_PERIOD_S then
        ps:publish(topics.link_out, link:make_heartbeat(), 1, false)
        last_robot_hb = now
    end
end

log("shutting down (rpc_count=%d, energy=%d)",
    rpc_count, link.energy_remaining)
pcall(function()
    ps:publish(topics.link_out, link:make_disconnect("ctrl_c"), 1, false)
    ffi.C.usleep(50000)
    ps:disconnect()
    ps:destroy()
end)

os.exit(0)
