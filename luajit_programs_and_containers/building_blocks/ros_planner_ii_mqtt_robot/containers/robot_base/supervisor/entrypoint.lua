#!/usr/bin/env luajit
-- =============================================================================
-- entrypoint.lua -- robot_base supervisor main.
--
-- Invoked as:
--   luajit entrypoint.lua [/opt/robot_base/supervisor/robot_supervisor.json]
--
-- Lifecycle: see dsl.lua. Fail-stop on any child death
-- (feedback_no_soft_faults). Exits non-zero on terminate; Docker restart
-- policy decides whether to re-launch.
-- =============================================================================

local cfl_rt   = require("cfl_runtime")
local loader   = require("cfl_json_loader")
local builtins = require("cfl_builtins")
local sm       = require("cfl_state_machine")

local user_functions = require("user_functions")
local ptime = require("posix_time")
local pp    = require("process_primitives")
local er    = require("env_validate")

local M = {}

local function make_logger()
    return function(half, msg)
        io.stderr:write(string.format("%s [%s] %s\n",
            os.date("!%Y-%m-%dT%H:%M:%SZ"), half, msg))
        io.stderr:flush()
    end
end

local function default_context()
    return {
        env               = {},
        children          = {},
        log               = make_logger(),
        shutdown_requested_getter = nil,
        cfl_rt            = cfl_rt,
        chain_tree        = { flash = nil, handle = nil, kb_indexes = {} },
        settings = {
            tick_interval_s = 0.5,
            cfl_delta_time  = 0.5,
            cfl_max_ticks   = 1,
        },
    }
end

local function init_chain_tree(json_path, user_fns, settings)
    local flash = loader.load(json_path)
    loader.register_functions(flash, builtins, sm, user_fns)
    local ok, missing = loader.validate(flash)
    if not ok then
        local list = {}
        for _, m in ipairs(missing) do
            list[#list + 1] = "  " .. m.kind .. ": " .. m.name
        end
        error("chain-tree has unresolved user functions:\n" ..
              table.concat(list, "\n"))
    end
    local kb_indexes = {}
    for i, kb in ipairs(flash.kb_table) do
        kb_indexes[kb.name] = i - 1
    end
    local handle = cfl_rt.create({
        delta_time = settings.cfl_delta_time,
        max_ticks  = settings.cfl_max_ticks,
    }, flash)
    cfl_rt.reset(handle)
    return flash, handle, kb_indexes
end

local function any_active(handle)
    for _ in pairs(handle.active_tests) do return true end
    return false
end

local function run_loop(ctx)
    local interval = ctx.settings.tick_interval_s
    local handle   = ctx.chain_tree.handle
    local burst    = 0
    local deadline = ptime.now_sec()
    ctx.log("robot", string.format(
        "entering tick loop interval=%.3fs", interval))
    while true do
        burst = burst + 1
        local t0 = ptime.now_sec()
        cfl_rt.run(handle)
        local run_ms = (ptime.now_sec() - t0) * 1000.0
        if burst % 20 == 0 then
            local alive = 0
            local total = 0
            for _, c in pairs(ctx.children or {}) do
                total = total + 1
                if c.pid then alive = alive + 1 end
            end
            ctx.log("robot", string.format(
                "burst=%d ticks=%d run=%.1fms children=%d/%d",
                burst, handle.tick_count or 0, run_ms, alive, total))
        end
        if not any_active(handle) then
            ctx.log("robot", "exiting: " ..
                (ctx.terminate_reason or "no active tests"))
            os.exit(1)
        end
        deadline = deadline + interval
        local now = ptime.now_sec()
        if deadline < now then deadline = now end
        ptime.sleep_until(deadline)
    end
end

function M.main(args)
    local ctx = default_context()
    local env, err = er.gather()
    if not env then
        ctx.log("robot", "FATAL: " .. err)
        os.exit(1)
    end
    ctx.env = env
    ctx.log("robot", string.format(
        "robot_base supervisor: id=%s class=%s dongle=%s",
        env.ROBOT_ID, env.ROBOT_CLASS, env.DONGLE_INSTANCE))

    ctx.shutdown_requested_getter =
        pp.sigaction_any_flag({ pp.signals.SIGTERM, pp.signals.SIGINT })

    local json_path = args[1]
        or "/opt/robot_base/supervisor/robot_supervisor.json"
    ctx.log("robot", "loading chain-tree IR: " .. json_path)

    local user_fns = user_functions.build(ctx)
    local flash, handle, kb_idx =
        init_chain_tree(json_path, user_fns, ctx.settings)
    ctx.chain_tree.flash      = flash
    ctx.chain_tree.handle     = handle
    ctx.chain_tree.kb_indexes = kb_idx

    cfl_rt.add_test(handle, kb_idx.robot_supervisor or 0)
    run_loop(ctx)
end

if arg and (arg[0] or ""):match("entrypoint%.lua$") then
    M.main(arg)
end

return M
