#!/usr/bin/env luajit
-- =============================================================================
-- test_link_manager_tenant_filter.lua -- Phase 7 Step C acceptance for
-- per-tenant robot filtering inside link_manager.
--
-- Multi-tenant deployments share `<site>/robots/+/...` MQTT topics, so
-- every link_manager sees every robot's link_announce. Without filtering
-- a planner picks up robots that belong to a peer planner's tenant.
--
-- Contract:
--   - allowed_robots = nil  -> single-tenant fallback, no filter
--   - allowed_robots = {}   -> empty set, every robot is foreign
--   - allowed_robots[rid]   -> rid is tenant-owned
--
-- This test stubs mqtt_hub + json_util + kv_writer and exercises the
-- four on_* handlers directly. Synchronous; no external dependencies.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
local LUAJIT_BASE = REPO_ROOT .. "nano_data_center_base/luajit/luajit_base"
package.path = PLANNER .. "/lib/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/" ..
                  "chain_tree/lua_dsl/luajit_pipeline/?.lua;" ..
               package.path

local pass, fail = 0, 0
local function ok(name, cond, detail)
    if cond then pass = pass + 1; print("  ok  " .. name)
    else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

print("=== link_manager: per-tenant robot filter ===\n")

------------------------------------------------------------------------
-- Stub mqtt_hub: records calls so we can assert "no ack sent for
-- foreign robot". Stub kv_writer is unused (link_manager only calls
-- it on accepted handshakes, which we don't reach for foreign robots).
------------------------------------------------------------------------

local function new_mqtt_stub()
    local self = { acks = {}, hbs = {}, wires = {}, disc = {} }
    function self:send_planner_ack(robot_id, _payload)
        self.acks[#self.acks + 1] = robot_id
    end
    function self:send_planner_heartbeat(robot_id, _payload)
        self.hbs[#self.hbs + 1] = robot_id
    end
    function self:send_planner_disconnect(robot_id, _payload)
        self.disc[#self.disc + 1] = robot_id
    end
    function self:set_wire_format(robot_id, fmt)
        self.wires[robot_id] = fmt
    end
    return self
end

local function new_kv_stub()
    return {
        ks = nil,
        write = function() end,
        tick  = function() end,
    }
end

local link_manager = require("link_manager")
local json_util    = require("json_util")

------------------------------------------------------------------------
print("== nil allowed_robots -> single-tenant fallback (every robot accepted) ==")
------------------------------------------------------------------------
do
    local hub = new_mqtt_stub()
    local lm = link_manager.new(hub, new_kv_stub(), "site_a", {
        planner_namespace = "ns_a",
        -- allowed_robots not supplied -> nil
    })
    lm:on_announce("rover_x", json_util.encode({
        type = "link_announce", class_name = "robotic_arm",
    }))
    ok("ack sent for any robot when allowed_robots=nil",
       hub.acks[1] == "rover_x", "expected 'rover_x', got " .. tostring(hub.acks[1]))
    ok("robot state created (registering)",
       lm:get_state("rover_x") == "registering")
end

------------------------------------------------------------------------
print("\n== empty allowed_robots -> all robots are foreign (no acks) ==")
------------------------------------------------------------------------
do
    local hub = new_mqtt_stub()
    local lm = link_manager.new(hub, new_kv_stub(), "site_a", {
        planner_namespace = "ns_a",
        allowed_robots    = {},
    })
    lm:on_announce("rover_x", json_util.encode({ type = "link_announce" }))
    ok("no ack sent for foreign robot",
       #hub.acks == 0, "got " .. #hub.acks .. " acks")
    ok("no state allocated for foreign robot",
       lm:get_state("rover_x") == "offline")
end

------------------------------------------------------------------------
print("\n== set allowed_robots -> only owned robot accepted ==")
------------------------------------------------------------------------
do
    local hub = new_mqtt_stub()
    local lm = link_manager.new(hub, new_kv_stub(), "site_a", {
        planner_namespace = "ns_a",
        allowed_robots    = { rover_1 = true },
    })

    lm:on_announce("rover_1", json_util.encode({
        type = "link_announce", class_name = "robotic_arm",
    }))
    lm:on_announce("rover_2", json_util.encode({
        type = "link_announce", class_name = "robotic_arm",
    }))

    ok("owned rover_1 acked", hub.acks[1] == "rover_1")
    ok("foreign rover_2 NOT acked", #hub.acks == 1,
       "got " .. #hub.acks .. " acks: " .. table.concat(hub.acks, ","))
    ok("owned rover_1 in registering", lm:get_state("rover_1") == "registering")
    ok("foreign rover_2 stays offline", lm:get_state("rover_2") == "offline")
end

------------------------------------------------------------------------
print("\n== filter applies to heartbeat/confirm/disconnect too ==")
------------------------------------------------------------------------
do
    local hub = new_mqtt_stub()
    local lm = link_manager.new(hub, new_kv_stub(), "site_a", {
        planner_namespace = "ns_a",
        allowed_robots    = { rover_1 = true },
    })

    -- Heartbeat from foreign robot should not allocate state.
    lm:on_heartbeat("rover_2", json_util.encode({ type = "link_heartbeat" }))
    ok("foreign heartbeat does not allocate state",
       lm:get_state("rover_2") == "offline")

    -- Confirm from foreign robot should not allocate / transition.
    lm:on_confirm("rover_2", json_util.encode({
        type = "link_confirm", capabilities = {}, energy_max = 100,
    }))
    ok("foreign confirm does not allocate state",
       lm:get_state("rover_2") == "offline")

    -- Disconnect from foreign robot should not even look up state.
    lm:on_disconnect("rover_2", json_util.encode({ type = "link_disconnect" }))
    ok("foreign disconnect is a no-op",
       lm:get_state("rover_2") == "offline" and #hub.disc == 0)
end

------------------------------------------------------------------------
print("\n== filter is symmetric across multiple owners (set semantics) ==")
------------------------------------------------------------------------
do
    local hub = new_mqtt_stub()
    local lm = link_manager.new(hub, new_kv_stub(), "site_a", {
        planner_namespace = "ns_a",
        allowed_robots    = { rover_1 = true, rover_3 = true },
    })
    lm:on_announce("rover_1", json_util.encode({ type = "link_announce" }))
    lm:on_announce("rover_2", json_util.encode({ type = "link_announce" }))
    lm:on_announce("rover_3", json_util.encode({ type = "link_announce" }))

    ok("rover_1 ack present", hub.acks[1] == "rover_1")
    ok("rover_3 ack present", hub.acks[2] == "rover_3")
    ok("rover_2 (foreign) not acked", #hub.acks == 2)
end

------------------------------------------------------------------------
print(string.format("\nSUMMARY: %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
