#!/usr/bin/env luajit
-- =============================================================================
-- entrypoint.lua -- luajit-base supervisor main.
--
-- Invoked as:
--   luajit entrypoint.lua /opt/luajit_base/controller.db
--
-- Lifecycle:
--   1. Snapshot env vars into ctx.env
--   2. Open controller.db (read-only sqlite) -> ctx.ctrl_db
--   3. Install SIGTERM/SIGINT flag handlers (drain via ctx.shutdown_getter)
--   4. Load controller chain-tree JSON IR
--   5. Register user functions + validate
--   6. Activate controller KB, enter tick loop
--
-- Exits on CFL_TERMINATE_SYSTEM_EVENT (os.exit(1)). Docker restart=always
-- brings the supervisor back up clean.
-- =============================================================================

local ffi = require("ffi")
local M = {}

local cfl_rt   = require("cfl_runtime")
local loader   = require("cfl_json_loader")
local builtins = require("cfl_builtins")
local sm       = require("cfl_state_machine")

local user_functions = require("user_functions")
local ptime     = require("posix_time")
local pp        = require("process_primitives")
local h         = require("sqlite3_helpers")
local ndc_paths = require("ndc_paths")

local ENV_KEYS = {
    "CONTAINER_NAME", "APP_SYSTEM", "APP_SITE", "APP_CPU_ID",
    "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD",
    "NATS_URL", "MQTT_HOST",
}

local function make_logger()
    return function(half, msg)
        io.stderr:write(string.format("%s [%s] %s\n",
            os.date("!%Y-%m-%dT%H:%M:%SZ"), half, msg))
        io.stderr:flush()
    end
end

local function snapshot_env()
    local e = {}
    for _, k in ipairs(ENV_KEYS) do e[k] = os.getenv(k) or "" end
    return e
end

local function open_controller_db_ro(path)
    local sl = h.sqlite3_lib
    pcall(ffi.cdef, [[
        int sqlite3_open_v2(const char *filename, void **ppDb,
                            int flags, const char *zVfs);
    ]])
    local SQLITE_OPEN_READONLY = 0x00000001
    local pp_ = ffi.new("void*[1]")
    if sl.sqlite3_open_v2(path, pp_, SQLITE_OPEN_READONLY, nil) ~= 0 then
        error("cannot open " .. path)
    end
    return pp_[0]
end

local function default_context()
    return {
        env               = {},
        ctrl_db_path      = nil,
        ctrl_db           = nil,
        connectors        = { pg = nil },
        apps              = {},
        log               = make_logger(),
        shutdown_requested_getter = nil,
        cfl_rt            = cfl_rt,
        chain_tree        = { flash = nil, handle = nil, kb_indexes = {} },
        last_heartbeat_ts = 0,
        settings = {
            tick_interval_s = 1.0,
            cfl_delta_time  = 1.0,
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
    ctx.log("ctrl", string.format(
        "entering tick loop interval=%.3fs", interval))
    while true do
        burst = burst + 1
        local t0 = ptime.now_sec()
        cfl_rt.run(handle)
        local run_ms = (ptime.now_sec() - t0) * 1000.0
        local app_summary = "no-apps"
        if ctx.apps and #ctx.apps > 0 then
            local alive = 0
            for _, a in ipairs(ctx.apps) do
                if a.pid then alive = alive + 1 end
            end
            app_summary = string.format("apps=%d/%d", alive, #ctx.apps)
        end
        ctx.log("ctrl", string.format(
            "burst=%d ticks=%d run=%.1fms %s",
            burst, handle.tick_count or 0, run_ms, app_summary))
        if not any_active(handle) then
            local reason = ctx.terminate_reason or "no active tests"
            ctx.log("ctrl", "exiting: " .. reason)
            os.exit(1)
        end
        deadline = deadline + interval
        local now = ptime.now_sec()
        if deadline < now then deadline = now end
        ptime.sleep_until(deadline)
    end
end

---------------------------------------------------------------------------
-- main
---------------------------------------------------------------------------

function M.main(args)
    local ctrl_path = args[1]
        or error("missing argv[1] (controller.db path)")

    local ctx = default_context()
    ctx.env          = snapshot_env()
    ctx.ctrl_db_path = ctrl_path
    ctx.ctrl_db      = open_controller_db_ro(ctrl_path)

    ctx.log("ctrl", "opened controller.db: " .. ctrl_path)
    ctx.log("ctrl", string.format("identity: container=%s system=%s site=%s cpu=%s",
        ctx.env.CONTAINER_NAME, ctx.env.APP_SYSTEM, ctx.env.APP_SITE, ctx.env.APP_CPU_ID))

    if not ctx.env.APP_SYSTEM or ctx.env.APP_SYSTEM == "" then
        error("APP_SYSTEM env var missing -- node_control must inject it at container launch")
    end
    ndc_paths.configure{ system_name = ctx.env.APP_SYSTEM }

    -- Block SIGTERM+SIGINT and poll for pending deliveries each tick.
    -- No async callback into Lua (luajit ffi.cast as signal handler is
    -- unsafe). sigtimedwait with a zero-timespec drains any pending.
    ctx.shutdown_requested_getter =
        pp.sigaction_any_flag({ pp.signals.SIGTERM, pp.signals.SIGINT })

    -- Resolve chain-tree JSON: prefer explicit arg, then controller dir,
    -- then baked-in supervisor dir.
    local json_path = args[2]
        or ctrl_path:gsub("[^/]+$", "controller.json")
    if not io.open(json_path, "r") then
        json_path = "/opt/luajit_base/supervisor/controller.json"
    end
    ctx.log("ctrl", "loading chain-tree IR: " .. json_path)

    local user_fns = user_functions.build(ctx)
    local flash, handle, kb_idx =
        init_chain_tree(json_path, user_fns, ctx.settings)
    ctx.chain_tree.flash      = flash
    ctx.chain_tree.handle     = handle
    ctx.chain_tree.kb_indexes = kb_idx

    ctx.log("ctrl", "activating controller KB")
    cfl_rt.add_test(handle, kb_idx.controller or 0)

    run_loop(ctx)
end

if arg and (arg[0] or ""):match("entrypoint%.lua$") then
    M.main(arg)
end

return M
