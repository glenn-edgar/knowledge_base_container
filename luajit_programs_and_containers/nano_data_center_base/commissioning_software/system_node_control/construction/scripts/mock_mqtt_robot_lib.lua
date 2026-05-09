--[[
    mock_mqtt_robot_lib.lua -- Pure logic for the mock MQTT robot fixture.

    Factored into its own module so unit-style smoke tests can require()
    the ack-shape factories and link state machine without standing up
    an MQTT broker (or even loading libmqtt_pubsub.so).

    The corresponding binary entry point is mock_mqtt_robot.lua, which
    requires this module and then runs the MQTT poll loop. See
    BOARD_FORMAT.md (nav-to-wire mapping), lib/link_manager.lua
    (planner-side state machine), and lib/hub_runtime.lua (stream_bus
    consumer) for the contract this code implements.
]]

local dkjson = require("dkjson")

local M = {}

local function iso_now() return os.date("!%Y-%m-%dT%H:%M:%SZ") end
M.iso_now = iso_now

---------------------------------------------------------------------------
-- Topic computation
---------------------------------------------------------------------------

function M.make_topics(site, robot)
    local site_path = site:gsub("%.", "/")
    return {
        rpc           = site_path .. "/robots/" .. robot .. "/rpc",
        stream_bus    = site_path .. "/robots/" .. robot .. "/stream_bus",
        link_out      = site_path .. "/robots/" .. robot .. "/link",
        planner_glob  = site_path .. "/robots/" .. robot .. "/planner/+",
        planner_ack   = site_path .. "/robots/" .. robot .. "/planner/ack",
        planner_hb    = site_path .. "/robots/" .. robot .. "/planner/heartbeat",
        planner_dis   = site_path .. "/robots/" .. robot .. "/planner/disconnect",
    }
end

---------------------------------------------------------------------------
-- stream_bus ack / kb_done factories.
-- hub_runtime.lua expects type ∈ {ack, heartbeat, kb_done}. ack moves the
-- planner-side action FSM into ACTIVE; kb_done into DONE with the success
-- flag captured.
---------------------------------------------------------------------------

function M.make_ack(cmd)
    return dkjson.encode({
        type   = "ack",
        seq    = cmd.seq,
        status = "ok",
        ts     = iso_now(),
    })
end

function M.make_kb_done_success(cmd, energy_remaining)
    return dkjson.encode({
        type             = "kb_done",
        seq              = cmd.seq,
        test_id          = cmd.test_id,
        success          = true,
        energy_remaining = energy_remaining,
        delta_x          = 0,
        delta_y          = 0,
        delta_heading    = 0,
        delta_arm_angle  = 0,
        ts               = iso_now(),
    })
end

function M.make_kb_done_failure(cmd, fault_reason, energy_remaining)
    return dkjson.encode({
        type             = "kb_done",
        seq              = cmd.seq,
        test_id          = cmd.test_id,
        success          = false,
        fault_reason     = fault_reason or "synthetic_fault",
        energy_remaining = energy_remaining,
        delta_x          = 0,  delta_y          = 0,
        delta_heading    = 0,  delta_arm_angle  = 0,
        ts               = iso_now(),
    })
end

---------------------------------------------------------------------------
-- Phase 5 drive-packet wire path (cmd_drive_t).
--
-- Simulator response factories: per-packet ACK + per-packet completion
-- keyed on packet_id. The planner's hub_runtime drive-packet state
-- machine consumes these via _process_stream when type matches
-- "drive_ack" or "drive_done".
--
-- Wire format choice: response is JSON (symmetric with legacy ack /
-- kb_done responses; the asymmetry is intentional -- only the OUTBOUND
-- command direction is CBOR for the new path. mqtt_hub_transport's
-- per-robot wire_format applies to outbound only).
---------------------------------------------------------------------------

function M.make_drive_ack(packet_id, status)
    return dkjson.encode({
        type      = "drive_ack",
        packet_id = packet_id,
        status    = status or "ok",
        ts        = iso_now(),
    })
end

function M.make_drive_done(packet_id, success, fault_reason, delta_pose)
    delta_pose = delta_pose or {}
    return dkjson.encode({
        type            = "drive_done",
        packet_id       = packet_id,
        success         = success and true or false,
        fault_reason    = (not success) and (fault_reason or "synthetic_fault") or nil,
        delta_x         = delta_pose.x         or 0,
        delta_y         = delta_pose.y         or 0,
        delta_heading   = delta_pose.heading   or 0,
        delta_arm_angle = delta_pose.arm_angle or 0,
        ts              = iso_now(),
    })
end

---------------------------------------------------------------------------
-- Link-handshake state machine
--
-- States:  init → registering → live → disconnected
--
-- We publish:  link_announce, link_confirm, link_heartbeat, link_disconnect
-- We receive:  link_bridge_ack, link_bridge_heartbeat, link_bridge_disconnect
--              (from the planner, on .../planner/<verb>)
---------------------------------------------------------------------------

local LinkState = {}
LinkState.__index = LinkState
M.LinkState = LinkState

function LinkState.new(robot_id, class_name, capabilities, energy_max,
                       energy_remaining)
    return setmetatable({
        robot_id         = robot_id,
        class_name       = class_name or "lunar_rover",
        capabilities     = capabilities or
            { "init_check", "path_spline", "path_line", "path_wall",
              "operation", "idle" },
        wire_format      = "json",
        energy_max       = energy_max or 10000,
        energy_remaining = energy_remaining or energy_max or 10000,
        state            = "init",
    }, LinkState)
end

function LinkState:make_announce()
    return dkjson.encode({
        type             = "link_announce",
        robot_id         = self.robot_id,
        class_name       = self.class_name,
        energy_remaining = self.energy_remaining,
        ts               = iso_now(),
    })
end

function LinkState:make_confirm()
    return dkjson.encode({
        type             = "link_confirm",
        robot_id         = self.robot_id,
        class_name       = self.class_name,
        wire_format      = self.wire_format,
        capabilities     = self.capabilities,
        energy_max       = self.energy_max,
        energy_remaining = self.energy_remaining,
        ts               = iso_now(),
    })
end

function LinkState:make_heartbeat()
    return dkjson.encode({
        type             = "link_heartbeat",
        robot_id         = self.robot_id,
        energy_remaining = self.energy_remaining,
        ts               = iso_now(),
    })
end

function LinkState:make_disconnect(reason)
    return dkjson.encode({
        type             = "link_disconnect",
        robot_id         = self.robot_id,
        reason           = reason or "clean_shutdown",
        energy_remaining = self.energy_remaining,
        ts               = iso_now(),
    })
end

--- Process a planner→robot verb. Returns:
--   nil                                          if the verb was unrecognized
--   { transitioned_to }                          if state changed but no reply
--   { transitioned_to, send_topic, send_payload} if a reply must be published
function LinkState:on_planner_verb(topic, payload)
    local verb = topic:match("/planner/([^/]+)$")
    if not verb then return nil end

    local ok, msg = pcall(dkjson.decode, payload)
    if not ok or not msg then return nil end

    if verb == "ack" and msg.type == "link_bridge_ack" then
        if self.state == "init" then
            self.state = "registering"
            return { transitioned_to = "registering",
                     send_topic = "link_out",
                     send_payload = self:make_confirm() }
        elseif self.state == "registering" then
            return { transitioned_to = "registering" }
        end
    elseif verb == "heartbeat" and msg.type == "link_bridge_heartbeat" then
        if self.state == "registering" then
            self.state = "live"
            return { transitioned_to = "live" }
        end
        return { transitioned_to = self.state }
    elseif verb == "disconnect" then
        self.state = "disconnected"
        return { transitioned_to = "disconnected" }
    end
    return nil
end

return M
