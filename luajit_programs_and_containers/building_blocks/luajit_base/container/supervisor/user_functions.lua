-- =============================================================================
-- user_functions.lua -- luajit-base controller user-function registry.
--
-- M.build(ctx) returns a registered-name -> function map consumed by
-- the chain-tree loader. ctx holds shared state:
--   ctx.env               environ snapshot (CONTAINER_NAME, APP_SITE, ...)
--   ctx.ctrl_db_path      path to controller sqlite (argv[1])
--   ctx.ctrl_db           open sqlite handle (read-only)
--   ctx.connectors.pg     DBI pg connection (set by VERIFY_PG)
--   ctx.apps              [ { name, argv, start_order, restart_policy,
--                              pid, started_ts, restart_count,
--                              last_error, health } ]
--   ctx.shutdown_requested_getter   closure from process_primitives.sigaction_flag
--   ctx.log               stderr logger
--   ctx.cfl_rt            chain-tree runtime module (for terminate events)
--   ctx.chain_tree.handle runtime handle
-- =============================================================================

local eq_mod = require("cfl_event_queue")
local defs   = require("cfl_definitions")
local sm_mod = require("cfl_state_machine")

local ptime   = require("posix_time")
local pp      = require("process_primitives")
local pg_conn = require("pg_connector")
local kb_exc  = require("kb_exception")
local kb_stat = require("kb_status")
local dkjson  = require("dkjson")

local M = {}

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function now_ns() return math.floor(ptime.now_sec() * 1e9) end

local function container_path(ctx)
    return string.format("system.site.%s.cpu.%s.container.%s",
                         ctx.env.APP_SITE, ctx.env.APP_CPU_ID,
                         ctx.env.CONTAINER_NAME)
end

local function app_path(ctx, app_name)
    return container_path(ctx) .. ".app." .. app_name
end

local function exc_path(ctx, name)
    return container_path(ctx) .. ".SYS_EXCEPTION." .. name
end

-- Trigger the CFL_TERMINATE_SYSTEM_EVENT; tick loop observes no active tests
-- and exits. Watchdog (docker restart=always) cycles us.
local function terminate_system(ctx, reason)
    ctx.terminate_reason = reason or "unspecified"
    local h = ctx.chain_tree.handle
    eq_mod.send_null(h.event_queue, defs.CFL_TERMINATE_SYSTEM_EVENT)
end

---------------------------------------------------------------------------
-- sqlite_read: pull command_map rows out of controller.db.
-- Schema (see bundler/construct_controller_kb.lua):
--   CREATE TABLE command_map (
--     name TEXT PRIMARY KEY, argv TEXT NOT NULL, start_order INTEGER,
--     restart_policy TEXT, kb_path TEXT )
---------------------------------------------------------------------------

local function sqlite_read_command_map(db)
    local h = require("sqlite3_helpers")
    return h.sql_query(db,
        "SELECT name, argv, start_order, restart_policy, kb_path " ..
        "FROM command_map ORDER BY start_order ASC, name ASC", {})
end

---------------------------------------------------------------------------
-- build(ctx) returns the registry
---------------------------------------------------------------------------

function M.build(ctx)
    local log = ctx.log
    local R = {}

    ------------------------------------------------------------------
    -- launch-time (before state machine enters sync)
    ------------------------------------------------------------------

    R.READ_ENVIRONS = function(_h, _n)
        local want = { "CONTAINER_NAME", "APP_SITE", "APP_CPU_ID",
                       "PG_HOST", "PG_PORT", "PG_DB", "PG_USER" }
        local missing = {}
        for _, k in ipairs(want) do
            if not ctx.env[k] or ctx.env[k] == "" then
                missing[#missing + 1] = k
            end
        end
        if #missing > 0 then
            log("ctrl", "missing required env: " .. table.concat(missing, ","))
            terminate_system(ctx, "missing env: " .. table.concat(missing, ","))
            return
        end
        log("ctrl", string.format(
            "identity: container=%s site=%s cpu=%s",
            ctx.env.CONTAINER_NAME, ctx.env.APP_SITE, ctx.env.APP_CPU_ID))
    end

    -- LOAD_CONTROLLER_KB is a no-op at runtime; the sqlite db is already
    -- opened by entrypoint.lua (so we can fail fast on bundler breakage).
    -- Kept as a hook for future schema-growth reads.
    R.LOAD_CONTROLLER_KB = function(_h, _n)
        log("ctrl", "controller.db opened: " .. ctx.ctrl_db_path)
    end

    ------------------------------------------------------------------
    -- sync: pg + command_map
    ------------------------------------------------------------------

    -- Verifies follow chain-tree convention: CFL_VERIFY's main fn is called
    -- on every event (INIT, TIMER, TERMINATE, ...). Only do real work on
    -- TIMER_EVENT; return true for INIT/TERMINATE/etc so ERR doesn't trip
    -- on lifecycle events.
    -- DBD-PostgreSQL's Lua wrapper accepts host/port only via a DSN
    -- string (not positional args). Inside a container the default
    -- unix-socket path doesn't exist, so we must force TCP via DSN.
    local DBI = require("DBI")
    R.VERIFY_PG = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if ctx.connectors.pg then return true end
        local dsn = string.format("dbname=%s host=%s port=%s",
            ctx.env.PG_DB, ctx.env.PG_HOST, ctx.env.PG_PORT)
        local conn, err = DBI.Connect("PostgreSQL", dsn,
            ctx.env.PG_USER, ctx.env.PG_PASSWORD)
        if not conn then
            log("ctrl", "VERIFY_PG not ready (dsn=" .. dsn ..
                        "): " .. tostring(err))
            return false
        end
        -- Sanity query.
        local ok, qerr = pcall(function()
            local s = conn:prepare("SELECT 1"); assert(s:execute()); s:close()
        end)
        if not ok then
            conn:close()
            log("ctrl", "VERIFY_PG sanity query failed: " .. tostring(qerr))
            return false
        end
        conn:autocommit(true)
        ctx.connectors.pg = conn
        log("ctrl", "pg connected (" .. dsn .. ")")
        return true
    end

    R.ERR_PG_UNREACHABLE = function(_h, _n)
        log("ctrl", "ERR_PG_UNREACHABLE -- cannot reach master pg; terminating")
        -- Can't log_exception to pg when pg itself is unreachable.
        terminate_system(ctx, "pg unreachable")
    end

    R.LOAD_COMMAND_MAP = function(_h, _n)
        local rows = sqlite_read_command_map(ctx.ctrl_db)
        ctx.apps = {}
        for _, r in ipairs(rows) do
            local argv = dkjson.decode(r.argv) or {}
            ctx.apps[#ctx.apps + 1] = {
                name           = r.name,
                argv           = argv,
                start_order    = tonumber(r.start_order) or 0,
                restart_policy = r.restart_policy or "always",
                kb_path        = r.kb_path or "",
                pid            = nil,
                started_ts     = 0,
                restart_count  = 0,
                last_error     = "",
                health         = false,
            }
        end
        log("ctrl", string.format("loaded %d app(s) from command_map",
                                  #ctx.apps))
    end

    ------------------------------------------------------------------
    -- setup: spawn apps
    ------------------------------------------------------------------

    local function app_env(ctx, app)
        -- Pass everything the app might need; apps pick what they use.
        return {
            CONTAINER_NAME = ctx.env.CONTAINER_NAME,
            APP_SITE       = ctx.env.APP_SITE,
            APP_CPU_ID     = ctx.env.APP_CPU_ID,
            APP_NAME       = app.name,
            APP_KB_PATH    = app.kb_path,
            PG_HOST        = ctx.env.PG_HOST,
            PG_PORT        = ctx.env.PG_PORT,
            PG_DB          = ctx.env.PG_DB,
            PG_USER        = ctx.env.PG_USER,
            PG_PASSWORD    = ctx.env.PG_PASSWORD,
            NATS_URL       = ctx.env.NATS_URL or "",
            MQTT_HOST      = ctx.env.MQTT_HOST or "",
            CONTAINER_NAMESPACE = container_path(ctx),
            APP_NAMESPACE  = app_path(ctx, app.name),
        }
    end

    R.SPAWN_ALL_APPS = function(_h, _n)
        if not ctx.apps or #ctx.apps == 0 then
            log("ctrl", "SPAWN_ALL_APPS: no apps declared; idle container")
            return
        end
        for _, app in ipairs(ctx.apps) do
            local pid, err = pp.spawn(app.argv, app_env(ctx, app))
            if not pid then
                app.last_error = tostring(err)
                log("ctrl", string.format("spawn %s FAILED: %s", app.name, err))
            else
                app.pid        = pid
                app.started_ts = now_ns()
                app.health     = true
                log("ctrl", string.format("spawn %s pid=%d argv=%s",
                    app.name, pid, table.concat(app.argv, " ")))
            end
        end
    end

    R.VERIFY_ALL_APPS_ALIVE = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if not ctx.apps or #ctx.apps == 0 then return true end
        for _, app in ipairs(ctx.apps) do
            if not app.pid then return false end
            -- kill(pid, 0) is the POSIX liveness probe.
            local ok = pp.kill(app.pid, 0)
            if not ok then return false end
        end
        return true
    end

    R.ERR_APPS_START_FAIL = function(_h, _n)
        log("ctrl", "ERR_APPS_START_FAIL -- one or more apps failed to start")
        if ctx.connectors.pg then
            kb_exc.log_exception(ctx.connectors.pg,
                                 exc_path(ctx, "apps_start_fail"),
                                 "one or more apps failed to start")
        end
        terminate_system(ctx, "apps start fail")
    end

    R.WRITE_CONTAINER_HEALTH_TRUE = function(_h, _n)
        if not ctx.connectors.pg then return end
        local ok = pcall(kb_stat.set_status_data, ctx.connectors.pg,
            container_path(ctx) .. ".health",
            { status = true, ts = now_ns() })
        if not ok then log("ctrl", "WRITE_CONTAINER_HEALTH_TRUE: no status row (schema not declared)") end
    end

    R.WRITE_CONTAINER_HEALTH_FALSE = function(_h, _n)
        if not ctx.connectors.pg then return end
        pcall(kb_stat.set_status_data, ctx.connectors.pg,
            container_path(ctx) .. ".health",
            { status = false, ts = now_ns() })
    end

    ------------------------------------------------------------------
    -- monitor: strobe heartbeat + reap/respawn + shutdown watch
    ------------------------------------------------------------------

    R.STROBE_HEARTBEAT = function(_h, _n)
        ctx.last_heartbeat_ts = now_ns()
        -- v1 heartbeat writes to process_globals-equivalent (in-mem) only.
        -- Wiring into bit_mask_table lands when topology grows a per-container
        -- heartbeat bit (deferred; DCS's sys_sm polls heartbeat_fresh_s via
        -- an in-mem path in the same VM, but across VMs we need the bit mask).
    end

    -- Reap any exited children, then apply restart_policy.
    R.REAP_AND_RESPAWN = function(_h, _n)
        if not ctx.apps then return end
        for _, app in ipairs(ctx.apps) do
            if app.pid then
                local r = pp.waitpid_nohang(app.pid)
                if r then
                    -- exited
                    app.health     = false
                    app.last_error = "exit_code=" .. tostring(r.exit_code)
                    log("ctrl", string.format(
                        "app %s exited: pid=%d code=%s",
                        app.name, app.pid, tostring(r.exit_code)))
                    if ctx.connectors.pg then
                        kb_exc.log_exception(ctx.connectors.pg,
                            exc_path(ctx, "app_died_" .. app.name),
                            string.format("%s exited with %s",
                                          app.name, tostring(r.exit_code)))
                    end
                    app.pid = nil

                    local policy = app.restart_policy or "always"
                    local should_restart =
                          (policy == "always")
                       or (policy == "on-failure" and (r.exit_code or 0) ~= 0)
                    if should_restart then
                        local pid, err = pp.spawn(app.argv, app_env(ctx, app))
                        if pid then
                            app.pid           = pid
                            app.started_ts    = now_ns()
                            app.restart_count = app.restart_count + 1
                            app.health        = true
                            log("ctrl", string.format(
                                "respawn %s pid=%d (restart_count=%d)",
                                app.name, pid, app.restart_count))
                        else
                            log("ctrl", string.format(
                                "respawn %s FAILED: %s", app.name, err))
                        end
                    else
                        log("ctrl", string.format(
                            "not respawning %s (policy=%s, code=%s)",
                            app.name, policy, tostring(r.exit_code)))
                    end
                end
            end
        end
    end

    -- Returns false when a SIGTERM/SIGINT was caught; triggers
    -- ERR_TEARDOWN_REQUESTED -> change_state(teardown path).
    R.VERIFY_NO_SHUTDOWN_SIGNAL = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if ctx.shutdown_requested_getter and ctx.shutdown_requested_getter() then
            log("ctrl", "SIGTERM/SIGINT caught -- requesting teardown")
            return false
        end
        return true
    end

    R.ERR_TEARDOWN_REQUESTED = function(handle, _n)
        log("ctrl", "ERR_TEARDOWN_REQUESTED -> change_state(request_shutdown)")
        local sm = handle.flash_handle.sm_by_name
                   and handle.flash_handle.sm_by_name["ctrl_sm"]
        if not sm then
            terminate_system(ctx, "teardown requested (no sm_by_name)")
            return
        end
        local idx = sm.states["request_shutdown"]
        if not idx then
            terminate_system(ctx, "teardown requested (no request_shutdown state)")
            return
        end
        sm_mod.change_state(handle, 0, sm.node_id, idx, nil)
    end

    ------------------------------------------------------------------
    -- request_shutdown
    ------------------------------------------------------------------

    R.SIGTERM_ALL_APPS = function(_h, _n)
        if not ctx.apps then return end
        for _, app in ipairs(ctx.apps) do
            if app.pid then
                pp.kill(app.pid, pp.signals.SIGTERM)
                log("ctrl", string.format("SIGTERM %s pid=%d",
                                          app.name, app.pid))
            end
        end
    end

    R.VERIFY_ALL_APPS_EXITED = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if not ctx.apps then return true end
        for _, app in ipairs(ctx.apps) do
            if app.pid then
                local r = pp.waitpid_nohang(app.pid)
                if r then
                    app.pid    = nil
                    app.health = false
                else
                    return false   -- still running
                end
            end
        end
        return true
    end

    R.ERR_FORCE_TEARDOWN = function(_h, _n)
        log("ctrl", "ERR_FORCE_TEARDOWN -- apps did not exit in grace window")
        -- state machine is already advancing to teardown via
        -- chain-tree's asm_wait timeout -> error handler chain. Nothing
        -- else to do here beyond logging.
    end

    ------------------------------------------------------------------
    -- teardown
    ------------------------------------------------------------------

    R.SIGKILL_ALL_APPS = function(_h, _n)
        if not ctx.apps then return end
        for _, app in ipairs(ctx.apps) do
            if app.pid then
                pp.kill(app.pid, pp.signals.SIGKILL)
                log("ctrl", string.format("SIGKILL %s pid=%d",
                                          app.name, app.pid))
                -- Reap to avoid leaving zombies when the controller exits.
                for _ = 1, 20 do
                    if pp.waitpid_nohang(app.pid) then break end
                    ptime.sleep_for(0.05)
                end
                app.pid    = nil
                app.health = false
            end
        end
    end

    R.CLOSE_CONNECTIONS = function(_h, _n)
        if ctx.connectors.pg then
            pcall(function() ctx.connectors.pg:close() end)
            ctx.connectors.pg = nil
        end
        if ctx.ctrl_db then
            local sl = require("sqlite3_helpers").sqlite3_lib
            pcall(function() sl.sqlite3_close(ctx.ctrl_db) end)
            ctx.ctrl_db = nil
        end
        log("ctrl", "connections closed")
    end

    return R
end

return M
