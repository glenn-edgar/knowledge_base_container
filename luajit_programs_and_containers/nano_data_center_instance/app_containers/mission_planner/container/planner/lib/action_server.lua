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
            pg_conn = { host=..., port=..., dbname=..., user=..., password=... },
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
local kb_query_mod    = require("kb_query")
local kb_runtime      = require("kb_runtime")
local link_manager_mod = require("link_manager")
local kv_writer_mod    = require("kv_writer")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Classify an error string from global_planner.new into a stable rejection
-- reason code. Pattern-matches the message text so the result fault.reason
-- is meaningful to JobQueue callers and kb_stream consumers without their
-- needing to parse free-form strings. Exposed on M for smoke testing.
---------------------------------------------------------------------------
function M.classify_board_error(err_str)
    err_str = tostring(err_str or "")
    if err_str:find("board not found", 1, true) then
        return "board_not_found"
    end
    if err_str:find("schema_version=", 1, true) then
        return "board_schema_unsupported"
    end
    if err_str:find("doc_get returned no content", 1, true) or
       err_str:find("not valid JSON", 1, true) or
       err_str:find("fs_node pointer null", 1, true) then
        return "board_load_failed"
    end
    return "board_load_failed"
end
local classify_board_error = M.classify_board_error

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    self.pg_conn     = opts.pg_conn     or error("action_server: pg_conn required")
    assert(type(self.pg_conn) == "table",
           "action_server: pg_conn must be a table {host,port,dbname,user,password}")
    self.hub_json    = opts.hub_json  -- legacy, no longer required
    self.nats_server = opts.nats_server or error("action_server: nats_server required")
    self.site            = opts.site            or error("action_server: site required")
    self.system_name     = opts.system_name     or error("action_server: system_name required (v3 kb_query positional arg)")
    self.own_instance_id = opts.own_instance_id or error("action_server: own_instance_id required (this container's name)")
    self.max_replans = opts.max_replans or 3
    self.tick_usleep = opts.tick_usleep or 2000

    -- drive_v2 is the only nav path. mission_builder emits drive_packet
    -- entries for nav (one packet per polyline edge, per-packet ACK
    -- matching). Bookends + per-stop operation entries still use the
    -- legacy {kb_name, params} shape; action_server's dispatch branches
    -- on entry.kind so both coexist in one route. The PLANNER_LEGACY_NAV
    -- env hatch + opts.use_drive_v2 flag were removed in the C5
    -- follow-up cleanup once cluster validation confirmed drive_v2 safe.

    -- Phase 5 C4: tenant identifier (planner_namespace). Multiple
    -- planner instances may run on one site; this field scopes which
    -- boards / robots / virtual nodes belong to this instance. Threaded
    -- through to kb_query (5th positional arg) and link_manager (opts)
    -- below. Defaults to own_instance_id when not supplied so existing
    -- single-tenant deployments behave unchanged.
    -- Sources (priority order): opts.planner_namespace, env
    -- PLANNER_NAMESPACE, fallback to own_instance_id.
    self.planner_namespace = opts.planner_namespace
                          or os.getenv("PLANNER_NAMESPACE")
                          or self.own_instance_id

    -- MQTT transport (optional — for MQTT-first architecture)
    self.mqtt_hub  = opts.mqtt_hub
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

    -- Discover initial board node (first node in first board — "node 0")
    -- + Phase 7 multi-tenant: enumerate the robot_ids this tenant owns so
    -- link_manager can drop link messages from robots that belong to a
    -- different planner. Both queries reuse the same kb_query instance.
    -- Phase 7 multi-tenant: enumerate the robot_ids this tenant owns so
    -- link_manager can drop link messages from robots that belong to a
    -- different planner. Standalone pcall (the board-discovery block
    -- below is independent so a failure on either side doesn't take out
    -- the other).
    self._init_node = nil
    self.allowed_robots = nil   -- nil = no filtering (single-tenant fallback)
    pcall(function()
        local q = kb_query_mod.new(self.pg_conn, self.system_name,
            self.site, self.own_instance_id, self.planner_namespace)
        local robots = q:list_tenant_robots()
        if robots and #robots > 0 then
            local set = {}
            for _, rid in ipairs(robots) do set[rid] = true end
            self.allowed_robots = set
            io.stderr:write(string.format(
                "action_server[%s]: tenant=%s owns %d robot(s)\n",
                tostring(self.own_instance_id),
                tostring(self.planner_namespace),
                #robots))
            io.stderr:flush()
        end
        q:close()
    end)
    -- Original board-discovery probe (kb_query has no list_boards today
    -- so this pcall has always been a silent no-op; _init_node stays nil
    -- and link_manager.on_link_change skips the write_position call).
    -- Left here as the agreed extension point when the boards listing
    -- helper lands.
    pcall(function()
        local q = kb_query_mod.new(self.pg_conn, self.system_name,
            self.site, self.own_instance_id, self.planner_namespace)
        if type(q.list_boards) == "function" then
            local boards = q:list_boards()
            if boards and boards[1] and type(q.get_board) == "function" then
                local board_data = q:get_board(boards[1])
                if board_data and board_data.nodes and board_data.nodes[1] then
                    self._init_node = board_data.nodes[1].name
                end
            end
        end
        q:close()
    end)

    -- Link manager: robot registration and liveness (MQTT-first only)
    if self.mqtt_hub then
        local kv_writer = opts.kv_writer or kv_writer_mod.new()
        self._link_kv_writer = kv_writer
        self.link_mgr = link_manager_mod.new(self.mqtt_hub, kv_writer, self.site, {
            planner_namespace = self.planner_namespace,
            allowed_robots    = self.allowed_robots,
            on_link_exception = function(robot_id, reason)
                self:_cancel_mission(robot_id, reason)
            end,
            on_link_change = function(robot_id, state)
                self:_publish_summary()
                -- Set initial position when robot goes live
                if state == "live" and self._init_node then
                    self.link_mgr:write_position(robot_id, self._init_node)
                end
            end,
        })

        -- Route link messages from MQTT hub to link_manager
        self.mqtt_hub:set_link_handler(function(robot_id, payload)
            local ok, data = pcall(json_util.decode, payload)
            if not ok or not data then return end
            local msg_type = data.type
            if msg_type == "link_announce" then
                self.link_mgr:on_announce(robot_id, payload)
            elseif msg_type == "link_heartbeat" then
                self.link_mgr:on_heartbeat(robot_id, payload)
            elseif msg_type == "link_confirm" then
                self.link_mgr:on_confirm(robot_id, payload)
            elseif msg_type == "link_disconnect" then
                self.link_mgr:on_disconnect(robot_id, payload)
            end
        end)
    end

    return self
end

---------------------------------------------------------------------------
-- NATS connection (lazy init)
---------------------------------------------------------------------------
function M:_ensure_nats()
    if self._ks then return end

    local ks_lib = require("lib.nats_key_store")
    local jq_lib = require("lib.nats_job_queue")

    -- Phase 7: per-tenant NATS resources. NATS bucket names disallow
    -- dots so both site and namespace get dots->underscores. Subjects
    -- and KV keys keep the dotted form (matches the KB ltree path
    -- scheme `system.<sys>.site.<S>.planner.<ns>.action_server.*`,
    -- minus the `system.<sys>.` prefix which is implicit per site).
    local site_bucket = self.site:gsub("%.", "_")
    local ns_bucket   = self.planner_namespace:gsub("%.", "_")
    local bucket_action_server = string.format(
        "%s_planner_%s_action_server", site_bucket, ns_bucket)
    local bucket_mission_log = string.format(
        "%s_planner_%s_mission_log", site_bucket, ns_bucket)

    -- Cache the subject/key prefix for this tenant; every
    -- _publish_*/get/claim helper reads from here.
    self._action_server_prefix = string.format(
        "%s.planner.%s.action_server", self.site, self.planner_namespace)

    self._ks = ks_lib.KeyStore.new({
        server        = self.nats_server,
        bucket        = bucket_action_server,
        description   = string.format(
            "Action server (tenant '%s'): status, results, summary, mission log",
            self.planner_namespace),
        create_bucket = true,
        history       = 1,
        client_name   = "action_server",
    })
    self._ks:connect()

    -- Separate bucket for mission log (higher history for rolling log)
    self._log_ks = ks_lib.KeyStore.new({
        server        = self.nats_server,
        bucket        = bucket_mission_log,
        description   = string.format(
            "Rolling mission log (tenant '%s'): last 50 missions",
            self.planner_namespace),
        create_bucket = true,
        history       = 50,
        client_name   = "action_server_log",
    })
    self._log_ks:connect()

    self._jq = jq_lib.JobQueue.new(self._ks:handle(), "action_server")

    -- Late-bind kv_writer's ks: link_manager's kv_writer was constructed
    -- in M.new (before _ensure_nats), so its self.ks was nil. Now that
    -- _ks exists, attach it so kv_writer:tick() can actually write KV
    -- keys. Without this, the first tick after a robot link goes "live"
    -- crashes the serve loop with "attempt to index field 'ks'
    -- (a nil value)" at kv_writer.lua:71. Surfaced ROBSIM C3
    -- gap-5/follow-on smoke 2026-05-10.
    if self._link_kv_writer and not self._link_kv_writer.ks then
        self._link_kv_writer.ks = self._ks
    end
end

---------------------------------------------------------------------------
-- Mission submission
---------------------------------------------------------------------------

--- Submit a mission for coroutine execution.
-- @param mission_cmd  table: { robot_id, board, start, stops[], bookend }
-- @return true on success, nil + reason on rejection
function M:submit(mission_cmd)
    local robot_id = mission_cmd.robot_id or error("action_server: robot_id required")

    -- Reject if robot already has an active mission
    local existing = self.missions[robot_id]
    if existing and existing.state == "active" then
        return nil, "robot " .. robot_id .. " already has an active mission"
    end

    self.pending[#self.pending + 1] = mission_cmd
    return true
end

--- Cancel an active mission for a robot.
-- The mission's sequencer will see the abort flag on its next tick.
-- @param robot_id  string
-- @return true on success, nil + reason if no active mission
function M:cancel(robot_id)
    local m = self.missions[robot_id]
    if not m or m.state ~= "active" then
        return nil, "no active mission for " .. robot_id
    end
    m.cancel_requested = true
    return true
end

--- Submit a mission to NATS JobQueue (for external clients).
-- @return job_id string
function M:submit_nats(mission_cmd)
    self:_ensure_nats()
    local payload = json_util.encode(mission_cmd)
    return self._jq:submit(payload, self._action_server_prefix .. ".missions", 5, 1, 600)
end

---------------------------------------------------------------------------
-- Status queries
---------------------------------------------------------------------------

--- Phase 5 C4: tenant identifier this action_server is scoped to.
-- Defaults to own_instance_id when no opt / env var supplied.
function M:get_planner_namespace()
    return self.planner_namespace
end

function M:get_mission_status(robot_id)
    self:_ensure_nats()
    local val = self._ks:get(self._action_server_prefix .. "." .. robot_id .. ".status")
    if val then
        local ok, decoded = pcall(json_util.decode, val)
        if ok then return decoded end
    end
    return nil
end

function M:get_mission_result(robot_id)
    self:_ensure_nats()
    local val = self._ks:get(self._action_server_prefix .. "." .. robot_id .. ".result")
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

        -- Get robot class config from link_manager + KB class
        local class_name = mission_cmd.class_name
        if srv.link_mgr then
            class_name = class_name or srv.link_mgr:get_class(robot_id)
        end

        -- Query capabilities, operation_types, and energy from KB class definition
        local capabilities = {}
        local operation_types = {}
        local energy_max = 0
        local energy_rate = 1.0
        local energy_infinite = false
        if class_name then
            local kb_q = kb_query_mod.new(srv.pg_conn, srv.system_name,
                srv.site, srv.own_instance_id, srv.planner_namespace)
            capabilities = kb_q:get_class_capabilities(class_name)
            operation_types = kb_q:get_class_operation_types(class_name)
            energy_max = kb_q:get_class_energy_max(class_name) or 0
            energy_rate = kb_q:get_class_energy_rate(class_name) or 1.0
            energy_infinite = kb_q:get_class_energy_infinite(class_name)
            kb_q:close()
        end

        -- Create planner. Wrapped in pcall so board_not_found /
        -- board_schema_unsupported / board_load_failed surface as
        -- specific rejection reasons rather than a generic
        -- coroutine-died exception.
        local mission_id_for_reject =
            mission_cmd.mission_id
            or string.format("direct_%s_%d", robot_id, math.floor(os.time() * 1000))

        local ok_planner, planner_or_err = pcall(global_planner.new, {
            pg_conn           = srv.pg_conn,
            board_name        = board,
            site              = srv.site,
            system_name       = srv.system_name,
            own_instance_id   = srv.own_instance_id,
            planner_namespace = srv.planner_namespace,
        })
        if not ok_planner then
            local err_str = tostring(planner_or_err)
            local reason  = classify_board_error(err_str)
            result = {
                success = false,
                fault   = { reason = reason, detail = err_str },
                replans = 0,
            }
            srv:_publish_status(robot_id, {
                state  = "failed",
                error  = err_str,
                reason = reason,
            })
            -- Durable rejection record (fail-soft: never let kb_stream
            -- pg hiccup take down the mission rejection path).
            pcall(kb_runtime.push_rejection, {
                pg_conn        = srv.pg_conn,
                system_name    = srv.system_name,
                site           = srv.site,
                container_name = srv.own_instance_id,
                robot_id       = robot_id,
                mission_id     = mission_id_for_reject,
                board_name     = board,
                reason         = reason,
                detail         = err_str,
            })
            return result
        end
        local planner = planner_or_err

        -- Build route with operation_types validation and energy budget.
        local route, plan_info = mission_builder.build(
            mission_cmd, planner, operation_types, energy_rate)
        if not route then
            local error_detail = plan_info.error
            if plan_info.unsupported then
                error_detail = error_detail .. ": " .. table.concat(plan_info.unsupported, "; ")
            end
            -- Surface the specific mission_builder error as the rejection
            -- reason ("transit_node_stops" / "unsupported_operation" /
            -- "no_path_found" / "no_stops") so consumers don't have to
            -- parse the free-form detail string.
            local reason = plan_info.error or "planning_failed"
            if reason:find("^no path") then reason = "no_path_found" end
            if reason == "no stops in mission" then reason = "no_stops" end
            result = {
                success     = false,
                fault       = { reason = reason, detail = error_detail },
                replans     = 0,
                unsupported = plan_info.unsupported,
            }
            srv:_publish_status(robot_id, {
                state = "failed",
                error = error_detail,
                reason = reason,
                unsupported = plan_info.unsupported,
            })
            pcall(kb_runtime.push_rejection, {
                pg_conn        = srv.pg_conn,
                system_name    = srv.system_name,
                site           = srv.site,
                container_name = srv.own_instance_id,
                robot_id       = robot_id,
                mission_id     = mission_id_for_reject,
                board_name     = board,
                board_sha256   = planner:get_board_sha256(),
                reason         = reason,
                detail         = error_detail,
                unsupported    = plan_info.unsupported,
            })
            planner:close()
            return result
        end

        -- Check energy budget: prefer link_manager (live robot state)
        -- Fall back to NATS KV only in direct execution mode (no link_manager)
        local energy_remaining = energy_max
        if srv.link_mgr and srv.link_mgr:is_live(robot_id) then
            local link_energy = srv.link_mgr:get_energy(robot_id)
            if link_energy then
                energy_remaining = link_energy
            end
        else
            local energy_data = srv:_read_robot_energy(robot_id)
            if energy_data then
                energy_remaining = energy_data.energy_remaining or energy_max
            end
        end

        if not energy_infinite and plan_info.total_cost > energy_remaining then
            local detail = string.format(
                "insufficient energy: need %d, have %d",
                plan_info.total_cost, energy_remaining)
            result = {
                success          = false,
                fault            = { reason = "insufficient_energy", detail = detail },
                replans          = 0,
                route            = route,
                legs             = plan_info.legs,
                energy_required  = plan_info.total_cost,
                energy_remaining = energy_remaining,
            }
            srv:_publish_status(robot_id, {
                state = "failed",
                error = detail,
                reason = "insufficient_energy",
                energy_required  = plan_info.total_cost,
                energy_remaining = energy_remaining,
            })
            pcall(kb_runtime.push_rejection, {
                pg_conn          = srv.pg_conn,
                system_name      = srv.system_name,
                site             = srv.site,
                container_name   = srv.own_instance_id,
                robot_id         = robot_id,
                mission_id       = mission_id_for_reject,
                board_name       = board,
                board_sha256     = planner:get_board_sha256(),
                reason           = "insufficient_energy",
                detail           = detail,
                energy_required  = plan_info.total_cost,
                energy_remaining = energy_remaining,
            })
            planner:close()
            return result
        end

        -- Create sequencer with yield-aware tick. mission_id is the
        -- JobQueue job.id (set by _drain_nats_queue when claimed). For
        -- direct in-process :submit callers, fall back to a synthetic id
        -- composed from robot_id + epoch ms so kb_runtime always has a
        -- non-empty mission_id assertion target.
        local mission_id = mission_cmd.mission_id
            or string.format("direct_%s_%d", robot_id, math.floor(os.time() * 1000))
        local seq = sequencer_mod.new({
            robot_id          = robot_id,
            pg_conn           = srv.pg_conn,
            nats_server       = srv.nats_server,
            site              = srv.site,
            system_name       = srv.system_name,
            own_instance_id   = srv.own_instance_id,
            planner_namespace = srv.planner_namespace,
            mission_id        = mission_id,
            board_name        = board,
            board_sha256      = planner:get_board_sha256(),
            capabilities      = capabilities,
            tick_usleep       = 0,  -- no sleep — scheduler controls timing
            energy_max        = energy_max,
            energy_remaining  = energy_remaining,
            mqtt_hub          = srv.mqtt_hub,
        })

        srv:_publish_status(robot_id, {
            state   = "executing",
            actions = #route,
            cost    = plan_info.total_cost,
            energy_remaining = energy_remaining,
        })

        -- Execute with yield-aware run
        seq:load_route(route)
        result = srv:_run_with_yield(seq, robot_id, plan_info)
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

            -- Heading is the robot's responsibility (locked contract
            -- 2026-05-10); we only pass the topology start node.
            local new_route, new_info = mission_builder.rebuild(
                remaining, planner, current_node)
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
        result.route = route
        result.energy_required  = plan_info.total_cost
        result.energy_remaining = energy_remaining

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
function M:_run_with_yield(seq, robot_id, plan_info)
    local bb = seq:get_hub_runtime():get_blackboard()
    local hub_rt = seq:get_hub_runtime()
    local route = seq.route
    local mission = seq:get_mission()

    -- Build action_index → leg destination lookup for position tracking
    local action_to_dest = {}
    if plan_info and plan_info.legs then
        for _, leg in ipairs(plan_info.legs) do
            if leg.route_end then
                action_to_dest[leg.route_end] = leg.to
            end
        end
    end

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

        if action.kind == "drive_packet" then
            -- Phase 5 C3b: drive-packet dispatch (one ACK per packet,
            -- one done per packet, no per-segment KB activation).
            local packet = action.packet
            -- send_drive_packet validates eagerly and errors on a
            -- duplicate-in-flight; both surface as the action's fault.
            local ok_send, err = pcall(hub_rt.send_drive_packet, hub_rt, packet)
            if not ok_send then
                fault = { reason = "drive_send_failed",
                          action_index = action_index,
                          kb_name      = "drive_packet",
                          detail       = tostring(err) }
                mission:action_failed(action_index, action, fault.reason)
                break
            end

            mission:action_start(action_index, action, hub_rt:get_global_pose())

            local action_complete = false
            local max_ticks = seq.max_ticks_per_action
            for tick = 1, max_ticks do
                hub_rt:tick()

                if tick % 10 == 0 then
                    mission:heartbeat(action_index, "drive_packet",
                        hub_rt:get_global_pose())
                end

                if hub_rt:drive_is_complete() then
                    action_complete = true
                    break
                end

                if mission:is_abort_requested() or
                   (self.missions[robot_id] and self.missions[robot_id].cancel_requested) then
                    fault = { reason = "cancelled",
                              action_index = action_index,
                              kb_name      = "drive_packet" }
                    break
                end

                coroutine.yield()
            end

            if fault then
                mission:action_failed(action_index, action, fault.reason)
                hub_rt:drive_clear()
                break
            end

            if action_complete then
                local s, _, drive_fault = hub_rt:drive_state_get()
                local pose = hub_rt:get_global_pose()
                if s == "drive_done" then
                    mission:action_complete(action_index, action, pose)
                    completed = completed + 1

                    local dest = action_to_dest[action_index]
                    if dest and self.link_mgr then
                        self.link_mgr:write_position(robot_id, dest)
                    end
                    hub_rt:drive_clear()
                else
                    -- drive_error
                    fault = { reason       = drive_fault or "drive_error",
                              action_index = action_index,
                              kb_name      = "drive_packet" }
                    mission:action_failed(action_index, action, fault.reason)
                    hub_rt:drive_clear()
                    break
                end
            else
                fault = { reason       = "timeout",
                          action_index = action_index,
                          kb_name      = "drive_packet" }
                mission:action_failed(action_index, action, fault.reason)
                hub_rt:drive_clear()
                break
            end

        else
            -- Legacy activate_kb dispatch (unchanged).
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

                if mission:is_abort_requested() or
                   (self.missions[robot_id] and self.missions[robot_id].cancel_requested) then
                    hub_rt:deactivate_kb(kb_name)
                    fault = { reason = "cancelled", action_index = action_index, kb_name = kb_name }
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

                    -- Update robot position if this action completed a navigation leg
                    local dest = action_to_dest[action_index]
                    if dest and self.link_mgr then
                        self.link_mgr:write_position(robot_id, dest)
                    end
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
    end

    seq.action_offset = seq.action_offset + completed

    local final_pose = hub_rt:get_global_pose()
    local elapsed_ms = math.floor((os.clock() - start_time) * 1000)
    local success = (fault == nil) and (completed == #route)

    return {
        success      = success,
        needs_replan = (fault ~= nil) and (fault.reason ~= "abort_requested") and (fault.reason ~= "cancelled"),
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
-- @param opts  optional: {
--                drain_nats = bool,
--                max_cycles = number,
--                on_tick    = function(cycle_idx)  -- per-cycle hook
--              }
-- on_tick fires once per scheduler cycle, BEFORE link_mgr/drain/resume,
-- and receives the 1-based cycle index. Use for heartbeats, watchdogs,
-- or anything that needs to share the scheduler thread without blocking
-- it. The hook is called inside pcall; errors are logged via print and
-- do not stop the loop (so a transient pg blip in a heartbeat handler
-- doesn't take down mission execution).
function M:serve(opts)
    opts = opts or {}
    local drain_nats  = opts.drain_nats
    local max_cycles  = opts.max_cycles  -- nil = run until all done
    local on_tick     = opts.on_tick

    -- Launch coroutines for pending missions
    for _, cmd in ipairs(self.pending) do
        local co = self:_make_mission_coroutine(cmd)
        local robot_id = cmd.robot_id or "unknown"
        self.missions[robot_id] = {
            coroutine = co,
            result    = nil,
            state     = "active",
            board     = cmd.board,
        }
        self.mission_count = self.mission_count + 1
        self:_publish_summary()
    end
    self.pending = {}

    print(string.format("Scheduler: %d missions active", self.mission_count))

    local cycles = 0

    -- When drain_nats is true, keep running even with 0 missions (persistent server).
    -- Otherwise exit when all missions complete.
    while self.mission_count > 0 or drain_nats do
        cycles = cycles + 1

        -- Per-cycle host hook (heartbeats, watchdogs). Errors are
        -- caught so a transient pg blip can't take down mission
        -- execution; the hook owner is responsible for its own
        -- recovery (e.g., reconnecting pg).
        if on_tick then
            local hook_ok, hook_err = pcall(on_tick, cycles)
            if not hook_ok then
                print(string.format("on_tick error (cycle %d): %s",
                    cycles, tostring(hook_err)))
            end
        end

        -- Tick link manager (processes link messages via mqtt_hub poll_and_route)
        if self.link_mgr then
            self.link_mgr:tick()
        end

        -- Drain queued KV writes (link status, energy, bitmask)
        if self._link_kv_writer then
            self._link_kv_writer:tick()
        end

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
                    self:_publish_summary()
                    self:_publish_mission_log(robot_id, m.result, m.board)
                elseif coroutine.status(m.coroutine) == "dead" then
                    -- Coroutine finished
                    m.state = "done"
                    m.result = result
                    self.mission_count = self.mission_count - 1
                    self:_publish_summary()
                    self:_publish_mission_log(robot_id, m.result, m.board)
                end
            end
        end

        -- Always poll MQTT for link messages each cycle. mission_count
        -- is now accurate (synchronous_fail path no longer inflates it)
        -- but polling every cycle stays the right call: link traffic is
        -- sparse, 1ms timeout is cheap, and gating on mission_count would
        -- silence link_announce reception when no missions are running --
        -- which is exactly when fresh robots need to register.
        if self.mqtt_hub then
            self.mqtt_hub:poll_and_route(1)
        end

        -- Idle sleep: longer when no missions, shorter when active
        if self.mission_count > 0 then
            ffi.C.usleep(self.tick_usleep)
        else
            ffi.C.usleep(50000)  -- 50ms idle poll
        end

        if max_cycles and cycles >= max_cycles then break end
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
        local job = self._jq:claim_job({self._action_server_prefix .. ".missions"})
        if not job then break end

        local ok, cmd = pcall(json_util.decode, job.payload_json)
        if not ok or not cmd.robot_id then
            self._jq:fail_job(job.id, "invalid mission JSON")
        elseif self.missions[cmd.robot_id] and self.missions[cmd.robot_id].state == "active" then
            self._jq:fail_job(job.id, "robot " .. cmd.robot_id .. " already has an active mission")
        else
            -- Tag the mission_cmd with the JobQueue job id so the sequencer
            -- can thread it down to kb_runtime as the durable mission_id.
            cmd.mission_id = job.id
            local co = self:_make_mission_coroutine(cmd)

            -- Late-complete JobQueue ack: step the coroutine ONCE so we
            -- can distinguish "planning failed and the mission rejected
            -- itself synchronously" from "planning succeeded and the
            -- mission is now executing (yielded)". This makes the
            -- JobQueue verdict a faithful signal of whether the planner
            -- accepted the mission.
            local resume_ok, first_yield = coroutine.resume(co)
            local co_status = coroutine.status(co)

            if not resume_ok then
                -- Coroutine threw during planning (e.g. unhandled pg
                -- error). Rare; classify_board_error pcalls cover the
                -- expected cases. fail_job + record the body for ops.
                io.stderr:write(string.format(
                    "ACTION_SERVER: planning_error for %s job=%s: %s\n",
                    tostring(cmd.robot_id), tostring(job.id),
                    tostring(first_yield)))
                io.stderr:flush()
                self._jq:fail_job(job.id,
                    "planning_error: " .. tostring(first_yield))
                -- Mission never enters self.missions; nothing to track.
            elseif co_status == "dead" then
                -- Coroutine ran to completion without yielding. Either
                -- a synchronous rejection (fault.reason set) or a
                -- successful empty-route mission (rare). Treat fault
                -- as fail_job; treat success as complete_job.
                local res = first_yield
                if res and res.success == false then
                    local reason = (res.fault and res.fault.reason) or "planning_failed"
                    local detail = (res.fault and res.fault.detail) or "no detail"
                    io.stderr:write(string.format(
                        "ACTION_SERVER: synchronous_fail %s job=%s reason=%s detail=%s\n",
                        tostring(cmd.robot_id), tostring(job.id),
                        tostring(reason), tostring(detail)))
                    io.stderr:flush()
                    self._jq:fail_job(job.id, reason .. ": " .. tostring(detail))
                else
                    self._jq:complete_job(job.id,
                        '{"status":"completed_immediately"}')
                end
                -- Track the mission as done so get_results / mission_log
                -- publishing pick it up in the main loop. Do NOT inc
                -- mission_count: the mission was never active. The
                -- active→done decrement on line 957 only fires for
                -- missions that started "active", so an INC here had
                -- no matching DEC and was the source of the
                -- /api/missions active_missions overcount.
                self.missions[cmd.robot_id] = {
                    coroutine = co,
                    result    = res,
                    state     = "done",
                    board     = cmd.board,
                    job_id    = job.id,
                }
                self:_publish_summary()
                -- Publish mission_log here (since main loop only handles
                -- the active→done transition; this mission was already
                -- done at first resume).
                self:_publish_mission_log(cmd.robot_id, res or
                    { success = false, fault = { reason = "unknown" } },
                    cmd.board)
            else
                -- Suspended (yielded) → planning succeeded; mission is
                -- now in execution. complete_job is the right ack.
                self._jq:complete_job(job.id, '{"status":"started"}')
                self.missions[cmd.robot_id] = {
                    coroutine = co,
                    result    = nil,
                    state     = "active",
                    board     = cmd.board,
                    job_id    = job.id,
                }
                self.mission_count = self.mission_count + 1
                self:_publish_summary()
            end
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
        self._ks:put(self._action_server_prefix .. "." .. robot_id .. ".status",
            json_util.encode(data))
    end)
end

function M:_read_robot_energy(robot_id)
    -- TODO Phase 7 follow-up: when robot_status bucket migrates to the
    -- per-tenant scheme (planner.<ns>.robots.<id>.status.energy), update
    -- both the bucket name and key here to mirror _action_server_prefix.
    -- Today no robot rows live in pg yet (per Phase 7 design memo) so
    -- this code path is a no-op until a per-robot publisher exists.
    local ok, result = pcall(function()
        self:_ensure_nats()
        local ks_lib = require("lib.nats_key_store")
        local site_bucket = self.site:gsub("%.", "_")
        local ks = ks_lib.KeyStore.new({
            server  = self.nats_server,
            bucket  = site_bucket .. "_robot_status",
            history = 1,
            client_name = "action_server_energy_reader",
        })
        ks:connect()
        local key = self.site .. ".robots." .. robot_id .. ".status.energy"
        local val = ks:get(key)
        ks:disconnect()
        ks:destroy()
        if val then
            return json_util.decode(val)
        end
        return nil
    end)
    if ok then return result end
    return nil
end

function M:_publish_result(robot_id, result)
    pcall(function()
        self:_ensure_nats()
        self._ks:put(self._action_server_prefix .. "." .. robot_id .. ".result",
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

--- Publish active mission summary for fleet manager / dashboard.
-- Called when missions start, complete, cancel, or error.
function M:_publish_summary()
    pcall(function()
        self:_ensure_nats()
        local robots = {}
        for robot_id, m in pairs(self.missions) do
            robots[robot_id] = {
                state = m.state,
                board = m.board,
            }
        end
        local registered = {}
        if self.link_mgr then
            registered = self.link_mgr:list_live()
        end
        self._ks:put(self._action_server_prefix .. ".summary",
            json_util.encode({
                active_missions   = self.mission_count,
                missions          = robots,
                registered_robots = registered,
                timestamp         = os.date("!%Y-%m-%dT%H:%M:%SZ"),
            }))
    end)
end

--- Compact a single route action into a {kb_name, detail} pair for the log.
-- Detail is a short human-readable summary so the UI can show it without
-- needing to know every action's param schema.
local function _summarize_action(action)
    local kb = action.kb_name or "?"
    local p  = action.params or {}
    local detail
    if kb == "path_rotate" then
        detail = string.format("%d° → %d°",
            math.floor((p.from_heading or 0) + 0.5),
            math.floor((p.to_heading or 0) + 0.5))
    elseif kb == "path_spline" or kb == "path_line" or kb == "path_wall" then
        detail = string.format("d=%d @ %d",
            p.distance or 0, p.speed or 0)
    elseif kb == "init_check" then
        detail = "robot self-test"
    elseif kb == "idle" then
        detail = "park"
    else
        -- Generic: pick a few common params if present
        local parts = {}
        for _, k in ipairs({ "target", "duration", "speed", "distance" }) do
            if p[k] ~= nil then parts[#parts + 1] = k .. "=" .. tostring(p[k]) end
        end
        detail = #parts > 0 and table.concat(parts, " ") or ""
    end
    return { kb_name = kb, detail = detail }
end

local function _summarize_route(route)
    if not route then return nil end
    local out = {}
    for i, action in ipairs(route) do
        out[i] = _summarize_action(action)
    end
    return out
end

local function _summarize_legs(legs)
    if not legs then return nil end
    local out = {}
    for i, leg in ipairs(legs) do
        out[i] = {
            from        = leg.from,
            to          = leg.to,
            cost        = leg.cost,
            action      = leg.action,
            route_start = leg.route_start,
            route_end   = leg.route_end,
        }
    end
    return out
end

--- Append completed mission to rolling log (NATS KV history).
-- External consumers read history() on this key to get last N missions.
-- Stores rich data: route, legs, fault details so the UI can render
-- per-action breakdown with the failed action highlighted.
function M:_publish_mission_log(robot_id, result, board)
    pcall(function()
        self:_ensure_nats()
        local fault_obj = nil
        if result.fault then
            fault_obj = {
                reason       = result.fault.reason,
                detail       = result.fault.detail,
                action_index = result.fault.action_index,
                kb_name      = result.fault.kb_name,
            }
        end
        self._log_ks:put(self._action_server_prefix .. ".mission_log",
            json_util.encode({
                robot_id         = robot_id,
                board            = board or "",
                success          = result.success,
                completed        = result.completed,
                total            = result.total,
                elapsed_ms       = result.elapsed_ms,
                fault            = fault_obj,
                -- Backwards-compat: short reason string for older UI
                fault_reason     = result.fault and result.fault.reason or nil,
                route            = _summarize_route(result.route),
                legs             = _summarize_legs(result.legs),
                unsupported      = result.unsupported,
                energy_required  = result.energy_required,
                energy_remaining = result.energy_remaining,
                timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
            }))
    end)
end

---------------------------------------------------------------------------
-- Direct execution (single mission, no coroutine — for tests)
---------------------------------------------------------------------------

function M:execute_mission(mission_cmd)
    local robot_id = mission_cmd.robot_id or error("action_server: robot_id required")
    local board    = mission_cmd.board    or error("action_server: board required")

    -- Get robot class config
    local class_name = mission_cmd.class_name
    if self.link_mgr then
        class_name = class_name or self.link_mgr:get_class(robot_id)
    end

    -- Query capabilities, operation_types, and energy from KB class definition
    local capabilities = {}
    local operation_types = {}
    local energy_max = 0
    local energy_rate = 1.0
    local energy_infinite = false
    if class_name then
        local kb_q = kb_query_mod.new(self.pg_conn, self.system_name,
            self.site, self.own_instance_id, self.planner_namespace)
        capabilities = kb_q:get_class_capabilities(class_name)
        operation_types = kb_q:get_class_operation_types(class_name)
        energy_max = kb_q:get_class_energy_max(class_name) or 0
        energy_rate = kb_q:get_class_energy_rate(class_name) or 1.0
        energy_infinite = kb_q:get_class_energy_infinite(class_name)
        kb_q:close()
    end

    local planner = global_planner.new({
        pg_conn           = self.pg_conn,
        board_name        = board,
        site              = self.site,
        system_name       = self.system_name,
        own_instance_id   = self.own_instance_id,
        planner_namespace = self.planner_namespace,
    })

    local route, plan_info = mission_builder.build(
        mission_cmd, planner, operation_types, energy_rate)
    if not route then
        local error_detail = plan_info.error
        if plan_info.unsupported then
            error_detail = error_detail .. ": " .. table.concat(plan_info.unsupported, "; ")
        end
        self:_publish_status(robot_id, {
            state = "failed",
            error = error_detail,
            unsupported = plan_info.unsupported,
        })
        planner:close()
        return {
            success = false,
            fault   = { reason = "planning_failed", detail = error_detail },
            replans = 0,
        }
    end

    -- Check energy budget
    local energy_remaining = energy_max
    local energy_data = self:_read_robot_energy(robot_id)
    if energy_data then
        energy_remaining = energy_data.energy_remaining or energy_max
    end

    if not energy_infinite and plan_info.total_cost > energy_remaining then
        local detail = string.format(
            "insufficient energy: need %d, have %d",
            plan_info.total_cost, energy_remaining)
        self:_publish_status(robot_id, {
            state = "failed",
            error = detail,
            energy_required  = plan_info.total_cost,
            energy_remaining = energy_remaining,
        })
        planner:close()
        return {
            success = false,
            fault   = { reason = "insufficient_energy", detail = detail },
            replans = 0,
        }
    end

    -- Direct (non-NATS) submit path: same fallback shape as the
    -- coroutine path above.
    local mission_id = mission_cmd.mission_id
        or string.format("direct_%s_%d", robot_id, math.floor(os.time() * 1000))
    local seq = sequencer_mod.new({
        robot_id          = robot_id,
        pg_conn           = self.pg_conn,
        nats_server       = self.nats_server,
        site              = self.site,
        system_name       = self.system_name,
        own_instance_id   = self.own_instance_id,
        planner_namespace = self.planner_namespace,
        mission_id        = mission_id,
        board_name        = board,
        board_sha256      = planner:get_board_sha256(),
        capabilities      = capabilities,
        energy_max        = energy_max,
        energy_remaining  = energy_remaining,
        mqtt_hub          = self.mqtt_hub,
        kv_writer         = self.kv_writer,
    })

    print(string.format("Mission: %s on %s, %d stops, %d actions (cost=%d, energy=%d/%d)",
        robot_id, board, #mission_cmd.stops, #route, plan_info.total_cost,
        energy_remaining, energy_max))

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
            remaining, planner, current_node)
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
-- Link exception: cancel active mission for a robot
---------------------------------------------------------------------------

function M:_cancel_mission(robot_id, reason)
    local m = self.missions[robot_id]
    if not m or m.state ~= "active" then
        -- No active mission, but still refresh summary (registered_robots changed)
        self:_publish_summary()
        return
    end

    io.stderr:write(string.format(
        "ACTION_SERVER: cancelling mission for %s (%s)\n", robot_id, reason))

    m.state = "cancelled"
    m.result = {
        success = false,
        fault   = { reason = "link_exception", detail = reason },
    }
    self.mission_count = self.mission_count - 1

    -- Publish failure to NATS KV
    self:_publish_status(robot_id, {
        state = "cancelled",
        error = "link_exception: " .. reason,
    })
    self:_publish_result(robot_id, m.result)
    self:_publish_summary()
    self:_publish_mission_log(robot_id, m.result, m.board)
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

function M:close()
    if self.link_mgr then self.link_mgr:shutdown() end
    if self._jq then self._jq:destroy(); self._jq = nil end
    if self._log_ks then self._log_ks:disconnect(); self._log_ks:destroy(); self._log_ks = nil end
    if self._ks then self._ks:disconnect(); self._ks:destroy(); self._ks = nil end
end

return M
