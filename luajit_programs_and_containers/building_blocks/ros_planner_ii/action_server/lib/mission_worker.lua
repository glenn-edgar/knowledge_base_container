--[[
    mission_worker.lua -- Standalone worker process for mission execution.

    Spawned as a child process by the action server. Each worker gets its
    own LuaJIT state, creates its own NATS connections, and executes one
    mission independently.

    Usage (spawned by action server):
        luajit mission_worker.lua <job_id>

    Environment:
        NATS_SERVER     — NATS broker URL
        WORKER_DB_FILE  — path to surface_ops.db
        WORKER_HUB_JSON — path to hub.json
        WORKER_SITE     — KB site name
        WORKER_LTREE    — path to ltree extension

    The worker:
      1. Connects to NATS, gets the job by ID
      2. Decodes mission command from job payload
      3. Executes the mission (plan + sequencer + replan loop)
      4. Publishes result to KeyStore
      5. Completes the job
      6. Exits
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
local sequencer       = require("sequencer")
local mission_builder = require("mission_builder")
local ks_lib          = require("lib.nats_key_store")
local jq_lib          = require("lib.nats_job_queue")

---------------------------------------------------------------------------
-- Configuration from environment
---------------------------------------------------------------------------
local job_id      = arg[1] or error("mission_worker: job_id argument required")
local nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local db_file     = os.getenv("WORKER_DB_FILE") or error("WORKER_DB_FILE required")
local hub_json    = os.getenv("WORKER_HUB_JSON") or error("WORKER_HUB_JSON required")
local site        = os.getenv("WORKER_SITE") or "moonbase.alpha.surface_ops"
local ltree_path  = os.getenv("WORKER_LTREE") or "/usr/local/lib/ltree"
local max_replans = tonumber(os.getenv("WORKER_MAX_REPLANS") or "3")

---------------------------------------------------------------------------
-- Connect to NATS
---------------------------------------------------------------------------
local ks = ks_lib.KeyStore.new({
    server        = nats_server,
    bucket        = "action_server",
    description   = "Action server mission status",
    create_bucket = true,
    history       = 1,
    client_name   = "worker_" .. job_id,
})
ks:connect()

local jq = jq_lib.JobQueue.new(ks:handle(), "worker_" .. job_id)

---------------------------------------------------------------------------
-- Get the job
---------------------------------------------------------------------------
local job = jq:get_job(job_id)
if not job then
    io.stderr:write("WORKER: job " .. job_id .. " not found\n")
    jq:destroy()
    ks:disconnect()
    ks:destroy()
    os.exit(1)
end

local ok, mission_cmd = pcall(json_util.decode, job.payload_json)
if not ok then
    io.stderr:write("WORKER: invalid mission JSON\n")
    jq:fail_job(job_id, "invalid mission JSON")
    jq:destroy()
    ks:disconnect()
    ks:destroy()
    os.exit(1)
end

local robot_id = mission_cmd.robot_id or "unknown"
local status_key = "action_server." .. robot_id .. ".status"
local result_key = "action_server." .. robot_id .. ".result"

---------------------------------------------------------------------------
-- Status helper
---------------------------------------------------------------------------
local function publish_status(status_data)
    status_data.robot_id  = robot_id
    status_data.job_id    = job_id
    status_data.timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    pcall(function()
        ks:put(status_key, json_util.encode(status_data))
    end)
end

---------------------------------------------------------------------------
-- Execute mission
---------------------------------------------------------------------------
publish_status({ state = "starting", board = mission_cmd.board })

io.write(string.format("WORKER [%s]: starting mission on %s\n",
    robot_id, mission_cmd.board))

-- Create global planner
local planner = global_planner.new({
    db_file    = db_file,
    board_name = mission_cmd.board,
    ltree_path = ltree_path,
})

-- Create sequencer
local seq = sequencer.new({
    robot_id    = robot_id,
    db_file     = db_file,
    hub_json    = hub_json,
    nats_server = nats_server,
    site        = mission_cmd.site or site,
})

-- Build route
local route, plan_info = mission_builder.build(mission_cmd, planner)
if not route then
    local err = plan_info.error or "planning failed"
    publish_status({ state = "failed", error = err })
    jq:fail_job(job_id, err)
    seq:shutdown()
    planner:close()
    jq:destroy()
    ks:disconnect()
    ks:destroy()
    io.write(string.format("WORKER [%s]: planning failed — %s\n", robot_id, err))
    os.exit(1)
end

publish_status({
    state       = "executing",
    actions     = #route,
    cost        = plan_info.total_cost,
    stops       = #mission_cmd.stops,
})

io.write(string.format("WORKER [%s]: %d stops, %d actions (cost=%d)\n",
    robot_id, #mission_cmd.stops, #route, plan_info.total_cost))

-- Execute with replan loop
seq:load_route(route)
local result = seq:run()
local replans = 0

while result.needs_replan and replans < max_replans do
    replans = replans + 1

    publish_status({
        state    = "replanning",
        replan   = replans,
        fault    = result.fault,
    })

    io.write(string.format("WORKER [%s]: replan %d — %s at action %d\n",
        robot_id, replans, result.fault.reason, result.fault.action_index))

    local current_node = planner:find_nearest_node(
        result.final_pose.x, result.final_pose.y)

    local remaining = {}
    local fault_action = result.fault.action_index
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

    if #remaining == 0 then break end

    -- Block fault edge
    for _, leg in ipairs(plan_info.legs) do
        if fault_action >= leg.route_start and fault_action <= leg.route_end then
            if leg.from ~= leg.to then
                planner:mark_blocked(leg.from, leg.to)
            end
            break
        end
    end

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

-- Finalize
if result.needs_replan and replans >= max_replans then
    result.success = false
    result.fault = result.fault or {}
    result.fault.detail = "max replans exceeded"
end

if result.success or not result.needs_replan then
    seq:finish_mission(result)
end

result.replans = replans
result.legs = plan_info.legs

---------------------------------------------------------------------------
-- Publish result and complete job
---------------------------------------------------------------------------
local result_json = json_util.encode({
    success    = result.success,
    completed  = result.completed,
    total      = result.total,
    elapsed_ms = result.elapsed_ms,
    replans    = result.replans,
    fault      = result.fault,
    final_pose = result.final_pose,
})

publish_status({
    state   = result.success and "completed" or "failed",
    success = result.success,
})

pcall(function() ks:put(result_key, result_json) end)

if result.success then
    jq:complete_job(job_id, result_json)
else
    jq:fail_job(job_id, result.fault and result.fault.reason or "unknown")
end

io.write(string.format("WORKER [%s]: %s (completed=%d/%d, replans=%d)\n",
    robot_id, result.success and "SUCCESS" or "FAILED",
    result.completed or 0, result.total or 0, replans))

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
seq:shutdown()
planner:close()
jq:destroy()
ks:disconnect()
ks:destroy()
