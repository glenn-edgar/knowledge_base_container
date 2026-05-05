--[[
    sequencer.lua -- KB-driven route execution with replan support.

    Stateful sequencer: create once, load routes, run until done or fault.
    On fault, caller can replan (patch or full) and load a new route.
    Global pose persists across replans. Mission telemetry continues.

    Usage:
        local sequencer = require("sequencer")

        local seq = sequencer.new({
            robot_id = "rover_1",
            pg_conn  = { host=..., port=..., dbname=..., user=..., password=... },
        })

        seq:load_route({
            { kb_name = "init_check",  params = {} },
            { kb_name = "path_spline", params = { from_x=0, to_x=800, speed=150, distance=800 } },
            { kb_name = "idle",        params = {} },
        })

        local result = seq:run()
        -- result = {
        --   success        = true/false,
        --   needs_replan   = false,
        --   final_pose     = { x=0, y=0, heading=270, arm_angle=-135 },
        --   completed      = 18,
        --   total          = 18,
        --   elapsed_ms     = 1234,
        --   fault          = nil,  -- or { reason="timeout", action_index=5, kb_name="path_wall" }
        -- }

        -- On fault with needs_replan:
        if result.needs_replan then
            local new_route = replan(result.final_pose, result.fault)
            seq:load_route(new_route)
            result = seq:run()
        end

        seq:shutdown()

    Route format (from global planner):
        { kb_name = "path_spline", params = { from_x=0, to_x=800, ... } }
        params are merged into the action JSON sent to the hub.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util   = require("json_util")
local hub_runtime = require("hub_runtime")
local mission_mod = require("mission")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    self.robot_id = opts.robot_id or error("sequencer: robot_id required")
    local pg_conn  = opts.pg_conn  or error("sequencer: pg_conn required")
    self.pg_conn   = pg_conn

    self.tick_usleep         = opts.tick_usleep or 2000
    self.max_ticks_per_action = opts.max_ticks_per_action or 500
    self.stream_size         = opts.stream_size or 200

    -- v3 sequencer requires explicit site / system_name / own_instance_id
    -- (no kb_q:get_site() autodetect — that helper doesn't exist in v3
    -- kb_query and the caller, action_server, always supplies these).
    self.site            = opts.site            or error("sequencer: site required")
    self.system_name     = opts.system_name     or error("sequencer: system_name required (threaded from action_server opts)")
    self.own_instance_id = opts.own_instance_id or error("sequencer: own_instance_id required (threaded from action_server opts)")
    self.mission_id      = opts.mission_id      or error("sequencer: mission_id required (JobQueue job.id; threaded from action_server)")
    self.nats_server     = opts.nats_server     or error("sequencer: nats_server required")

    -- Robot capabilities: passed from action_server (from link protocol or KB class)
    self.capabilities = {}
    if opts.capabilities then
        for _, cap in ipairs(opts.capabilities) do
            self.capabilities[cap] = true
        end
    end

    -- MQTT transport (optional — for MQTT-first architecture)
    self.mqtt_hub = opts.mqtt_hub
    local transport = opts.transport
    if not transport and self.mqtt_hub then
        transport = self.mqtt_hub:robot_transport(self.robot_id)
    end

    -- Initialize hub runtime (state machine, no ChainTree)
    self.hub_rt = hub_runtime.new({
        robot_id         = self.robot_id,
        pg_conn          = pg_conn,
        site             = self.site,
        system_name      = self.system_name,
        own_instance_id  = self.own_instance_id,
        initial_pose     = opts.initial_pose or
            { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
        energy_max       = opts.energy_max,
        energy_remaining = opts.energy_remaining,
        transport        = transport,
        mqtt_hub         = self.mqtt_hub,
    })

    -- Initialize mission telemetry
    self.mission = mission_mod.new({
        robot_id        = self.robot_id,
        pg_conn         = pg_conn,
        site            = self.site,
        system_name     = self.system_name,
        own_instance_id = self.own_instance_id,
        mission_id      = self.mission_id,
        nats_server     = self.nats_server,
        route_length    = 0,  -- updated on load_route
        stream_size     = self.stream_size,
    })

    -- Route state
    self.route = nil
    self.action_offset = 0  -- cumulative offset across replans

    return self
end

---------------------------------------------------------------------------
-- Route management
---------------------------------------------------------------------------

--- Load a route for execution. Can be called multiple times for replanning.
-- @param route  array of { kb_name = string, params = table }
function M:load_route(route)
    assert(type(route) == "table" and #route > 0, "sequencer: route must be non-empty array")
    self.route = route
    self.mission.route_length = (self.mission.route_length or 0) + #route
end

--- Validate that every action in the loaded route is supported by this robot.
-- @return true if valid, false + errors if unsupported actions found
function M:validate_route()
    if not self.route then return false, {"no route loaded"} end

    -- If no capabilities in KB, skip validation (accept all)
    if not next(self.capabilities) then return true, {} end

    local errors = {}
    for i, action in ipairs(self.route) do
        if not self.capabilities[action.kb_name] then
            errors[#errors + 1] = string.format(
                "action %d: '%s' not supported by %s",
                i, action.kb_name, self.robot_id)
        end
    end

    if #errors > 0 then
        return false, errors
    end
    return true, {}
end

---------------------------------------------------------------------------
-- Execution
---------------------------------------------------------------------------

--- Run the loaded route. Returns on completion, fault, or abort.
-- @return result table
function M:run()
    if not self.route then
        error("sequencer: no route loaded, call load_route() first")
    end

    local bb = self.hub_rt:get_blackboard()
    local start_time = os.clock()

    -- Start mission on first run, not on replans
    if self.action_offset == 0 then
        self.mission:start()
    end

    local completed = 0
    local fault = nil

    for i, action in ipairs(self.route) do
        local action_index = self.action_offset + i
        local kb_name = action.kb_name

        -- Build action JSON with sequencing metadata
        local action_json = {}
        if action.params then
            for k, v in pairs(action.params) do
                action_json[k] = v
            end
        end
        if action.energy then
            action_json.energy = action.energy
        end
        action_json.test_id   = action_index
        action_json.next_test = (i < #self.route) and (action_index + 1) or 0

        -- Stage and activate
        bb.current_test_json = json_util.encode(action_json)
        local activated = self.hub_rt:activate_kb(kb_name)
        if not activated then
            fault = {
                reason       = "kb_not_found",
                action_index = action_index,
                kb_name      = kb_name,
            }
            break
        end

        self.mission:action_start(action_index, action,
            self.hub_rt:get_global_pose())

        -- Tick loop
        local action_complete = false
        for tick = 1, self.max_ticks_per_action do
            self.hub_rt:tick()

            -- Heartbeat every 10 ticks
            if tick % 10 == 0 then
                self.mission:heartbeat(action_index, kb_name,
                    self.hub_rt:get_global_pose())
            end

            if self.hub_rt:kb_is_complete(kb_name) then
                action_complete = true
                self.hub_rt:deactivate_kb(kb_name)
                break
            end

            -- Check abort
            if self.mission:is_abort_requested() then
                self.hub_rt:deactivate_kb(kb_name)
                fault = {
                    reason       = "abort_requested",
                    action_index = action_index,
                    kb_name      = kb_name,
                }
                break
            end

            ffi.C.usleep(self.tick_usleep)
        end

        -- Handle results
        if fault then
            self.mission:action_failed(action_index, action, fault.reason)
            break
        end

        if action_complete then
            local pose = self.hub_rt:get_global_pose()
            local success = bb.kb_done_success

            if success == true then
                self.mission:action_complete(action_index, action, pose)
                completed = completed + 1
            else
                fault = {
                    reason       = bb.fault_reason or "kb_done_failed",
                    action_index = action_index,
                    kb_name      = kb_name,
                }
                self.mission:action_failed(action_index, action, fault.reason)
                break
            end
        else
            -- Timeout
            self.hub_rt:deactivate_kb(kb_name)
            fault = {
                reason       = "timeout",
                action_index = action_index,
                kb_name      = kb_name,
            }
            self.mission:action_failed(action_index, action, fault.reason)
            break
        end
    end

    -- Update offset for potential replan
    self.action_offset = self.action_offset + completed

    local final_pose = self.hub_rt:get_global_pose()
    local elapsed_ms = math.floor((os.clock() - start_time) * 1000)
    local success = (fault == nil) and (completed == #self.route)

    local result = {
        success      = success,
        needs_replan = (fault ~= nil) and (fault.reason ~= "abort_requested"),
        final_pose   = final_pose,
        completed    = self.action_offset,
        total        = self.mission.route_length,
        elapsed_ms   = elapsed_ms,
        fault        = fault,
    }

    -- Only finish mission if we're done (no replan expected)
    if success or (fault and fault.reason == "abort_requested") then
        self.mission:finish(result)
    end

    return result
end

---------------------------------------------------------------------------
-- Finish and cleanup
---------------------------------------------------------------------------

--- Finish the mission and record final status.
-- Call this after the last run() if you're done replanning.
function M:finish_mission(result)
    self.mission:finish(result)
end

--- Send shutdown to remote and close all connections.
function M:shutdown()
    self.hub_rt:send_shutdown()
    self.hub_rt:close()
    self.mission:close()
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------

function M:get_global_pose()
    return self.hub_rt:get_global_pose()
end

function M:get_mission()
    return self.mission
end

function M:get_hub_runtime()
    return self.hub_rt
end

return M
