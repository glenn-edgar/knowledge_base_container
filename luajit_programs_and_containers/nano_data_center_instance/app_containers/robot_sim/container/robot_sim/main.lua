#!/usr/bin/env luajit
-- =============================================================================
-- robot_sim/main.lua -- single-process robot simulator (Phase 7 ROBSIM C2).
--
-- Connects to MQTT, performs URLP link handshake to register with the
-- planner, and echoes ack + kb_done for every RPC command (drive packets
-- + activate actions). No physics, no movement, no per-action validation
-- -- the planner sees every action complete successfully ("blind echo").
--
-- Code shape based on construction/scripts/mock_mqtt_robot.lua (the
-- Phase 3a C1 host-side test fixture, commit 2aa36b4d). Differences:
--   - env-driven (CLI args -> env vars)
--   - no SIGINT handler (docker SIGTERM kills the process; faster +
--     avoids `feedback_luajit_signal_safety.md`'s ffi.cast PANIC risk
--     in long-running processes)
--   - no --once mode (loops forever until killed)
--   - no --quiet mode (always logs; container stderr -> docker logs)
--
-- Env (all required; see container_spec.env_required):
--   APP_SITE          e.g. "moon_base_alpha" (injected by node_control)
--   ROBOT_ID          e.g. "rover_1"
--   PLANNER_NAMESPACE e.g. "mission_planner_01"
--   MQTT_HOST         e.g. "mosquitto-ram-ws_main"
--   MQTT_PORT         e.g. "1883"
--
-- Optional env:
--   ROBOT_CLASS       defaults "lunar_rover"
--   ENERGY_MAX        defaults 10000
--   HB_PERIOD_S       defaults 3 (must match planner link_manager.lua's
--                     HEARTBEAT_INTERVAL or the link will bounce stale)
--
-- Reusable protocol logic lives in lib/mock_mqtt_robot_lib.lua (LinkState
-- state machine + ack/kb_done/drive_ack/drive_done JSON factories).
-- This file owns I/O wiring (MQTT bring-up, subscribe, publish loop).
--
-- Port-ready: when porting into the real ros_planner_ii_mqtt_robot
-- container, replace the blind echo of cmd_drive_t / cmd_activate_action_t
-- with calls into physics_core (drive) and a real dock client (activate).
-- The link-protocol + topic conventions stay identical.
-- =============================================================================

local ffi = require("ffi")
pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

---------------------------------------------------------------------------
-- Path + module loading
---------------------------------------------------------------------------

-- /opt/apps/robot_sim/main.lua -> add lib/ for mock_mqtt_robot_lib + lib/lib for FFI wrapper
package.path = "/opt/apps/robot_sim/lib/?.lua;" ..
               "/opt/apps/robot_sim/?.lua;" ..
               package.path

local lib    = require("mock_mqtt_robot_lib")
local dkjson = require("dkjson")
local pubsub = require("lib.mqtt_pubsub")

---------------------------------------------------------------------------
-- env
---------------------------------------------------------------------------

local function env(k) return os.getenv(k) or "" end
local function env_or(k, fallback) local v = env(k); if v == "" then return fallback else return v end end

local CONTAINER_NAME    = env("CONTAINER_NAME")
local APP_SITE          = env("APP_SITE")
local ROBOT_ID          = env("ROBOT_ID")
local PLANNER_NAMESPACE = env("PLANNER_NAMESPACE")
-- MQTT_HOST defaults to the cluster's broker hostname on planner-net.
-- A real robot port would use infra_discovery (like the planner does)
-- to resolve this from pg; the simulator hardcodes the convention since
-- it always co-resides with the planner cluster on planner-net.
local MQTT_HOST         = env_or("MQTT_HOST", "mosquitto-ram-ws_main")
local MQTT_PORT         = tonumber(env_or("MQTT_PORT", "1883"))
local ROBOT_CLASS       = env_or("ROBOT_CLASS", "lunar_rover")
local ENERGY_MAX        = tonumber(env_or("ENERGY_MAX", "10000"))
local HB_PERIOD_S       = tonumber(env_or("HB_PERIOD_S", "3"))

assert(APP_SITE          ~= "", "APP_SITE env missing")
assert(ROBOT_ID          ~= "", "ROBOT_ID env missing")
assert(PLANNER_NAMESPACE ~= "", "PLANNER_NAMESPACE env missing")
-- MQTT_HOST has a cluster-default; only fail if the override-via-env
-- gave us empty (env_or returns the default if unset OR empty).
assert(MQTT_HOST         ~= "", "MQTT_HOST env empty (cleared override?)")
assert(MQTT_PORT         ~= nil, "MQTT_PORT env missing or invalid")

local function logf(fmt, ...)
    io.stderr:write(string.format("robot_sim[%s/%s]: " .. fmt .. "\n",
        CONTAINER_NAME ~= "" and CONTAINER_NAME or "?", ROBOT_ID, ...))
    io.stderr:flush()
end

logf("started site=%s robot=%s tenant=%s mqtt=%s:%d class=%s energy_max=%d hb=%ds",
    APP_SITE, ROBOT_ID, PLANNER_NAMESPACE, MQTT_HOST, MQTT_PORT,
    ROBOT_CLASS, ENERGY_MAX, HB_PERIOD_S)

---------------------------------------------------------------------------
-- MQTT bring-up
---------------------------------------------------------------------------

local topics = lib.make_topics(APP_SITE, ROBOT_ID)
local link   = lib.LinkState.new(ROBOT_ID, ROBOT_CLASS, nil, ENERGY_MAX)

local ps = pubsub.PubSub.new(MQTT_HOST, MQTT_PORT, "robot_sim_" .. ROBOT_ID)

-- Retry connect with backoff. Container starts in parallel with broker
-- via node_control's launch order; first connect can race the broker's
-- first-port-listen. Five retries at 2s = 10s window.
local CONNECT_TIMEOUT_MS  = 5000
local CONNECT_MAX_RETRIES = 5
local CONNECT_BACKOFF_S   = 2

local function connect_with_retry()
    local sleep_ts = ffi.new("ts_t"); sleep_ts.tv_sec = CONNECT_BACKOFF_S
    for attempt = 1, CONNECT_MAX_RETRIES do
        local ok, err = pcall(function() ps:connect(CONNECT_TIMEOUT_MS) end)
        if ok then
            logf("mqtt connected on attempt %d", attempt)
            return
        end
        logf("mqtt connect attempt %d failed: %s -- retry in %ds",
            attempt, tostring(err), CONNECT_BACKOFF_S)
        ffi.C.nanosleep(sleep_ts, nil)
    end
    error("mqtt connect failed after " .. CONNECT_MAX_RETRIES .. " attempts")
end

connect_with_retry()

ps:subscribe(topics.rpc, 1)
ps:subscribe(topics.planner_glob, 1)
logf("subscribed: %s  %s", topics.rpc, topics.planner_glob)

-- Initial link_announce. State machine handles re-announce on planner
-- heartbeat loss (LinkState.on_planner_verb tracks this internally).
ps:publish(topics.link_out, link:make_announce(), 1, false)
logf("-> link_announce")

---------------------------------------------------------------------------
-- Main loop -- no signal handling; docker SIGTERM kills the process
---------------------------------------------------------------------------

local last_robot_hb = os.time()
local rpc_count = 0
local poll_log_interval = 120  -- one summary log per 120s of idle polling
local last_summary_at = os.time()

while true do
    local msgs = ps:poll(50)
    for _, m in ipairs(msgs or {}) do
        if m.topic == topics.rpc then
            rpc_count = rpc_count + 1
            local ok, cmd = pcall(dkjson.decode, m.payload)
            if ok and cmd then
                -- Step 1: ack the command (synchronous receipt)
                ps:publish(topics.stream_bus, lib.make_ack(cmd), 1, false)

                -- Step 2: simulate execution (drain energy proportional
                -- to advertised distance, if any -- mock_mqtt_robot.lua
                -- convention)
                if cmd.distance then
                    link.energy_remaining = math.max(0,
                        link.energy_remaining - math.floor(cmd.distance + 0.5))
                end

                -- Step 3: kb_done with success=true (blind echo).
                -- Real robot would dispatch on cmd.packet_type:
                --   - cmd_drive_t  -> drive segments via physics_core
                --   - cmd_activate -> connect to dock, run SOP, etc.
                -- Sim treats them all the same.
                ps:publish(topics.stream_bus,
                    lib.make_kb_done_success(cmd, link.energy_remaining),
                    1, false)

                logf("<- rpc kb=%s seq=%s test_id=%s -> ack+kb_done energy=%d",
                    tostring(cmd.kb_name or cmd.packet_type),
                    tostring(cmd.seq), tostring(cmd.test_id),
                    link.energy_remaining)
            else
                logf("<- rpc UNDECODABLE (%d bytes)", #m.payload)
            end
        else
            -- planner_glob topic: link-protocol verb from planner
            -- (link_bridge_ack, link_bridge_heartbeat, link_disconnect).
            local outcome = link:on_planner_verb(m.topic, m.payload)
            if outcome then
                if outcome.send_payload then
                    ps:publish(topics.link_out, outcome.send_payload, 1, false)
                    logf("-> link state=%s (sent reply)", outcome.transitioned_to)
                else
                    logf("-> link state=%s", outcome.transitioned_to)
                end
            end
        end
    end

    -- Periodic heartbeat (only while registering or live)
    local now = os.time()
    if (link.state == "registering" or link.state == "live")
       and now - last_robot_hb >= HB_PERIOD_S then
        ps:publish(topics.link_out, link:make_heartbeat(), 1, false)
        last_robot_hb = now
    end

    -- Periodic idle summary so docker logs show this is alive even
    -- when nothing is happening
    if now - last_summary_at >= poll_log_interval then
        logf("idle summary: state=%s energy=%d rpc_count=%d",
            link.state, link.energy_remaining, rpc_count)
        last_summary_at = now
    end
end
