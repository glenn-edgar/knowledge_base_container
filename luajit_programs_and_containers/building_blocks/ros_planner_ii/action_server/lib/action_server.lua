--[[
    action_server.lua -- Coroutine-based mission server.

    Runs N missions concurrently in one LuaJIT process using coroutines.
    Each mission gets its own sequencer + hub_runtime (own NATS transport
    per robot). No threads, no fork — cooperative multitasking.

    Missions are submitted via NATS JobQueue or direct API.
    Each coroutine yields during tick loops; the scheduler resumes all
    active coroutines each cycle.

    Usage (server mode):
        local action_server = require("action_server")
        local srv = action_server.new({
            db_file  = "surface_ops.db",
            hub_json = "hub_dsl/hub.json",
        })

        srv:submit_mission({ robot_id="rover_1", board="landing_zone", ... })
        srv:submit_mission({ robot_id="rover_2", board="landing_zone", ... })
        srv:serve()  -- runs all missions concurrently via coroutines

    Usage (direct mode — single mission, for tests):
        local result = srv:execute_mission(mission_cmd)
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local json_util       = require("json_util")
local global_planner  = require("global_planner")
local sequencer_mod   = require("sequencer")
local mission_builder = require("mission_builder")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    self.db_file     = opts.db_file     or error("action_server: db_file required")
    self.hub_json    = opts.hub_json    or error("action_server: hub_json required")
    self.nats_server = opts.nats_server or "nats://127.0.0.1:4222"
    self.ltree_path  = opts.ltree_path  or "/usr/local/lib/ltree"
    self.site        = opts.site        or "moonbase.alpha.surface_ops"
    self.max_replans = opts.max_replans or 3
    self.tick_usleep = opts.tick_usleep or 2000

    -- Active missions: { robot_id = { coroutine, result, state } }
    self.missions = {}
    self.mission_count = 0

    -- Pending queue (for submit before serve)
    self.pending = {}

    -- NATS connections (lazy init for queue mode)
    self._ks = nil
    self._jq = nil

    -- Yield function — replaced by scheduler during serve()
    -- In direct mode, this is a no-op (no coroutine)
    self._yield = function() end

    return self
end

---------------------------------------------------------------------------
-- NATS connection (lazy init)
---------------------------------------------------------------------------
function M:_ensure_nats()
    if self._ks then return end

    local ks_lib = require("lib.nats_key_store")
    local jq_lib = require("lib.nats_job_queue")

    self._ks = ks_lib.KeyStore.new({
        server        = self.nats_server,
        bucket        = "action_server",
        description   = "Action server mission queue and status",
        create_bucket = true,
        history       = 1,
        client_name   = "action_server",
    })
    self._ks:connect()

    self._jq = jq_lib.JobQueue.new(self._ks:handle(), "action_server")
end

---------------------------------------------------------------------------
-- Mission submission
---------------------------------------------------------------------------

--- Submit a mission for coroutine execution.
-- @param mission_cmd  table: { robot_id, board, start, stops[], bookend }
function M:submit(mission_cmd)
    self.pending[#self.pending + 1] = mission_cmd
end

--- Submit a mission to NATS JobQueue (for external clients).
-- @return job_id string
function M:submit_nats(mission_cmd)
    self:_ensure_nats()
    local payload = json_util.encode(mission_cmd)
    return self._jq:submit(payload, "action_server.missions", 5, 1, 600)
end

---------------------------------------------------------------------------
-- Status queries
---------------------------------------------------------------------------

function M:get_mission_status(robot_id)
    self:_ensure_nats()
    local val = self._ks:get("action_server." .. robot_id .. ".status")
    if val then
        local ok, decoded = pcall(json_util.decode, val)
        if ok then return decoded end
    end
    return nil
end

function M:get_mission_result(robot_id)
    self:_ensure_nats()
    local val = self._ks:get("action_server." .. robot_id .. ".result")
    if val then
        local ok, decoded = pcall(json_util.decode, val)
        if ok then return decoded end
    end
    return nil
end

---------------------------------------------------------------------------
-- Coroutine-based mission execution
---------------------------------------------------------------------------

--- Create a coroutine that executes one mission.
-- The coroutine yields during tick loops; the scheduler resumes it.
function M:_make_mission_coroutine(mission_cmd)
    local srv = self  -- capture for closure

    return coroutine.create(function()
        local robot_id = mission_cmd.robot_id
        local board    = mission_cmd.board
        local result

        -- Publish starting status
        srv:_publish_status(robot_id, { state = "planning" })

        -- Create planner
        local planner = global_planner.new({
            db_file    = srv.db_file,
            board_name = board,
            ltree_path = srv.ltree_path,
        })

        -- Create sequencer with yield-aware tick
        local seq = sequencer_mod.new({
            robot_id    = robot_id,
            db_file     = srv.db_file,
            hub_json    = srv.hub_json,
            nats_server = srv.nats_server,
            site        = mission_cmd.site or srv.site,
            tick_usleep = 0,  -- no sleep — scheduler controls timing
        })

        -- Build route
        local route, plan_info = mission_builder.build(mission_cmd, planner)
        if not route then
            result = {
                success = false,
                fault   = { reason = "planning_failed", detail = plan_info.error },
                replans = 0,
            }
            srv:_publish_status(robot_id, { state = "failed", error = plan_info.error })
            seq:shutdown()
            planner:close()
            return result
        end

        srv:_publish_status(robot_id, {
            state   = "executing",
            actions = #route,
            cost    = plan_info.total_cost,
        })

        -- Execute with yield-aware run
        seq:load_route(route)
        result = srv:_run_with_yield(seq, robot_id)
        local replans = 0

        -- Replan loop
        while result.needs_replan and replans < srv.max_replans do
            replans = replans + 1

            srv:_publish_status(robot_id, {
                state  = "replanning",
                replan = replans,
            })

            local current_node = planner:find_nearest_node(
                result.final_pose.x, result.final_pose.y)

            local remaining = srv:_get_remaining_stops(
                mission_cmd, result, plan_info)
            if #remaining == 0 then break end

            srv:_block_fault_edge(planner, result, plan_info)

            local new_route, new_info = mission_builder.rebuild(
                remaining, planner, current_node, result.final_pose.heading)
            if not new_route then
                result.success = false
                result.fault.detail = "replan failed"
                break
            end

            plan_info = new_info
            seq:load_route(new_route)
            result = srv:_run_with_yield(seq, robot_id)
        end

        if result.needs_replan and replans >= srv.max_replans then
            result.success = false
            result.fault = result.fault or {}
            result.fault.detail = "max replans exceeded"
        end

        if result.success or not result.needs_replan then
            seq:finish_mission(result)
        end

        result.replans = replans
        result.legs = plan_info.legs

        -- Publish final result
        local state = result.success and "completed" or "failed"
        srv:_publish_status(robot_id, { state = state, success = result.success })
        srv:_publish_result(robot_id, result)

        -- Close connections but don't send shutdown to remote
        -- (remote is shared; only send shutdown when all missions done)
        seq:get_hub_runtime():close()
        seq:get_mission():close()
        planner:close()

        return result
    end)
end

--- Run sequencer with coroutine yields between ticks.
-- Replaces the sequencer's internal usleep with coroutine.yield().
function M:_run_with_yield(seq, robot_id)
    local bb = seq:get_hub_runtime():get_blackboard()
    local hub_rt = seq:get_hub_runtime()
    local route = seq.route
    local mission = seq:get_mission()

    if not route then
        error("sequencer: no route loaded")
    end

    local start_time = os.clock()

    if seq.action_offset == 0 then
        mission:start()
    end

    local completed = 0
    local fault = nil

    for i, action in ipairs(route) do
        local action_index = seq.action_offset + i
        local kb_name = action.kb_name

        -- Build action JSON
        local action_json = {}
        if action.params then
            for k, v in pairs(action.params) do
                action_json[k] = v
            end
        end
        action_json.test_id   = action_index
        action_json.next_test = (i < #route) and (action_index + 1) or 0

        bb.current_test_json = json_util.encode(action_json)
        local activated = hub_rt:activate_kb(kb_name)
        if not activated then
            fault = { reason = "kb_not_found", action_index = action_index, kb_name = kb_name }
            break
        end

        mission:action_start(action_index, action, hub_rt:get_global_pose())

        -- Tick loop with yields
        local action_complete = false
        local max_ticks = seq.max_ticks_per_action
        for tick = 1, max_ticks do
            hub_rt:tick()

            if tick % 10 == 0 then
                mission:heartbeat(action_index, kb_name, hub_rt:get_global_pose())
            end

            if hub_rt:kb_is_complete(kb_name) then
                action_complete = true
                hub_rt:deactivate_kb(kb_name)
                break
            end

            if mission:is_abort_requested() then
                hub_rt:deactivate_kb(kb_name)
                fault = { reason = "abort_requested", action_index = action_index, kb_name = kb_name }
                break
            end

            -- Yield to scheduler instead of sleeping
            coroutine.yield()
        end

        if fault then
            mission:action_failed(action_index, action, fault.reason)
            break
        end

        if action_complete then
            local pose = hub_rt:get_global_pose()
            local success = bb.kb_done_success
            if success == true then
                mission:action_complete(action_index, action, pose)
                completed = completed + 1
            else
                fault = { reason = bb.fault_reason or "kb_done_failed",
                          action_index = action_index, kb_name = kb_name }
                mission:action_failed(action_index, action, fault.reason)
                break
            end
        else
            hub_rt:deactivate_kb(kb_name)
            fault = { reason = "timeout", action_index = action_index, kb_name = kb_name }
            mission:action_failed(action_index, action, fault.reason)
            break
        end
    end

    seq.action_offset = seq.action_offset + completed

    local final_pose = hub_rt:get_global_pose()
    local elapsed_ms = math.floor((os.clock() - start_time) * 1000)
    local success = (fault == nil) and (completed == #route)

    return {
        success      = success,
        needs_replan = (fault ~= nil) and (fault.reason ~= "abort_requested"),
        final_pose   = final_pose,
        completed    = seq.action_offset,
        total        = mission.route_length,
        elapsed_ms   = elapsed_ms,
        fault        = fault,
    }
end

---------------------------------------------------------------------------
-- Scheduler
---------------------------------------------------------------------------

--- Run all pending and queued missions concurrently via coroutines.
-- @param opts  optional: { drain_nats = bool, max_cycles = number }
function M:serve(opts)
    opts = opts or {}
    local drain_nats  = opts.drain_nats
    local max_cycles  = opts.max_cycles  -- nil = run until all done

    -- Launch coroutines for pending missions
    for _, cmd in ipairs(self.pending) do
        local co = self:_make_mission_coroutine(cmd)
        local robot_id = cmd.robot_id or "unknown"
        self.missions[robot_id] = {
            coroutine = co,
            result    = nil,
            state     = "active",
        }
        self.mission_count = self.mission_count + 1
    end
    self.pending = {}

    print(string.format("Scheduler: %d missions active", self.mission_count))

    local cycles = 0

    while self.mission_count > 0 do
        -- Drain NATS queue for new missions
        if drain_nats then
            self:_drain_nats_queue()
        end

        -- Resume all active coroutines
        for robot_id, m in pairs(self.missions) do
            if m.state == "active" then
                local ok, result = coroutine.resume(m.coroutine)
                if not ok then
                    -- Coroutine errored
                    print(string.format("Mission %s error: %s", robot_id, tostring(result)))
                    m.state = "error"
                    m.result = { success = false, fault = { reason = "error", detail = tostring(result) } }
                    self.mission_count = self.mission_count - 1
                elseif coroutine.status(m.coroutine) == "dead" then
                    -- Coroutine finished
                    m.state = "done"
                    m.result = result
                    self.mission_count = self.mission_count - 1
                end
            end
        end

        ffi.C.usleep(self.tick_usleep)

        if max_cycles then
            cycles = cycles + 1
            if cycles >= max_cycles then break end
        end
    end
end

--- Get results for all completed missions.
-- @return table { robot_id = result_table }
function M:get_results()
    local results = {}
    for robot_id, m in pairs(self.missions) do
        if m.state == "done" or m.state == "error" then
            results[robot_id] = m.result
        end
    end
    return results
end

---------------------------------------------------------------------------
-- NATS queue drain (for server mode)
---------------------------------------------------------------------------

function M:_drain_nats_queue()
    self:_ensure_nats()
    -- Claim up to 5 jobs per cycle
    for _ = 1, 5 do
        local job = self._jq:claim_job({"action_server.missions"})
        if not job then break end

        local ok, cmd = pcall(json_util.decode, job.payload_json)
        if ok and cmd.robot_id then
            local co = self:_make_mission_coroutine(cmd)
            self.missions[cmd.robot_id] = {
                coroutine = co,
                result    = nil,
                state     = "active",
                job_id    = job.id,
            }
            self.mission_count = self.mission_count + 1
            self._jq:complete_job(job.id, '{"status":"started"}')
        else
            self._jq:fail_job(job.id, "invalid mission JSON")
        end
    end
end

---------------------------------------------------------------------------
-- Status publishing
---------------------------------------------------------------------------

function M:_publish_status(robot_id, data)
    pcall(function()
        self:_ensure_nats()
        data.robot_id  = robot_id
        data.timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
        self._ks:put("action_server." .. robot_id .. ".status",
            json_util.encode(data))
    end)
end

function M:_publish_result(robot_id, result)
    pcall(function()
        self:_ensure_nats()
        self._ks:put("action_server." .. robot_id .. ".result",
            json_util.encode({
                success    = result.success,
                completed  = result.completed,
                total      = result.total,
                elapsed_ms = result.elapsed_ms,
                replans    = result.replans,
                fault      = result.fault,
                final_pose = result.final_pose,
            }))
    end)
end

---------------------------------------------------------------------------
-- Direct execution (single mission, no coroutine — for tests)
---------------------------------------------------------------------------

function M:execute_mission(mission_cmd)
    local robot_id = mission_cmd.robot_id or error("action_server: robot_id required")
    local board    = mission_cmd.board    or error("action_server: board required")

    local planner = global_planner.new({
        db_file    = self.db_file,
        board_name = board,
        ltree_path = self.ltree_path,
    })

    local seq = sequencer_mod.new({
        robot_id    = robot_id,
        db_file     = self.db_file,
        hub_json    = self.hub_json,
        nats_server = self.nats_server,
        site        = mission_cmd.site,
    })

    local route, plan_info = mission_builder.build(mission_cmd, planner)
    if not route then
        seq:shutdown()
        planner:close()
        return {
            success = false,
            fault   = { reason = "planning_failed", detail = plan_info.error },
            replans = 0,
        }
    end

    print(string.format("Mission: %s on %s, %d stops, %d actions (cost=%d)",
        robot_id, board, #mission_cmd.stops, #route, plan_info.total_cost))

    seq:load_route(route)
    local result = seq:run()
    local replans = 0

    while result.needs_replan and replans < self.max_replans do
        replans = replans + 1
        local current_node = planner:find_nearest_node(
            result.final_pose.x, result.final_pose.y)
        local remaining = self:_get_remaining_stops(mission_cmd, result, plan_info)
        if #remaining == 0 then break end
        self:_block_fault_edge(planner, result, plan_info)
        local new_route, new_info = mission_builder.rebuild(
            remaining, planner, current_node, result.final_pose.heading)
        if not new_route then
            result.success = false
            result.fault.detail = "replan failed"
            break
        end
        plan_info = new_info
        seq:load_route(new_route)
        result = seq:run()
    end

    if result.needs_replan and replans >= self.max_replans then
        result.success = false
        result.fault = result.fault or {}
        result.fault.detail = "max replans exceeded"
    end

    if result.success or not result.needs_replan then
        seq:finish_mission(result)
    end

    result.replans = replans
    result.legs = plan_info.legs

    seq:shutdown()
    planner:close()
    return result
end

---------------------------------------------------------------------------
-- Replan helpers
---------------------------------------------------------------------------

function M:_get_remaining_stops(mission_cmd, result, plan_info)
    local fault_action = result.fault and result.fault.action_index or math.huge
    local remaining = {}
    for _, leg in ipairs(plan_info.legs) do
        if leg.route_end >= fault_action then
            local stop = mission_cmd.stops[leg.index]
            if stop then
                remaining[#remaining + 1] = {
                    node = stop.node, action = stop.action, params = stop.params,
                }
            end
        end
    end
    return remaining
end

function M:_block_fault_edge(planner, result, plan_info)
    if not result.fault then return end
    local fault_idx = result.fault.action_index
    for _, leg in ipairs(plan_info.legs) do
        if fault_idx >= leg.route_start and fault_idx <= leg.route_end then
            if leg.from ~= leg.to then
                planner:mark_blocked(leg.from, leg.to)
            end
            return
        end
    end
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

function M:close()
    if self._jq then self._jq:destroy(); self._jq = nil end
    if self._ks then self._ks:disconnect(); self._ks:destroy(); self._ks = nil end
end

return M
