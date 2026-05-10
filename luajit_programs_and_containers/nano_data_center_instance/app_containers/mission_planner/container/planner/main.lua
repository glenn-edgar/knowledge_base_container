#!/usr/bin/env luajit
-- =============================================================================
-- planner/main.lua -- planner worker entry point.
--
-- Phase 5b worker hookup (2026-05-10): the loop now drives
-- action_server:serve({drain_nats=true, on_tick=heartbeat}). Missions
-- enqueued by planner_ui (or any external client) into the NATS
-- JobQueue at <site>.action_server.missions are claimed, dispatched
-- as coroutines, and produce status keys that planner_ui's C6
-- dashboard polls. The on_tick callback handles the runtime.heartbeat
-- pg row + pg reconnect on failure -- separation kept inside main.lua.
--
-- Heartbeat path (snapshot row pre-allocated by apps_builder at
-- commission; this loop only UPDATEs):
--   system.<sys>.site.<S>.app_containers.<name>.runtime.heartbeat.
--     KB_STATUS_FIELD.snapshot
-- = { value = { at = unix_ms, host, cpu, ui_port, tick } }
--
-- Fallback: when action_server fails to instantiate (e.g., NATS
-- unreachable at startup), the loop degrades to heartbeat-only so
-- node_control's HTTP watchdog still sees the container as live.
-- The degraded loop continues to log + restart-on-pg-failure.
-- =============================================================================

local ffi = require("ffi")
pcall(ffi.cdef, [[
    typedef struct { long tv_sec; long tv_nsec; } ts_t;
    int nanosleep(const ts_t *req, ts_t *rem);
]])

-- Planner package libraries: imported from building_blocks/ros_planner_ii/
-- starting Phase B.2.A.2. The path stack is:
--   /opt/apps/planner/lib/?.lua            -- runtime/ files + action_server,
--                                             global_planner, sequencer, kb_query, KBM
--   /opt/apps/planner/?.lua                -- vendored upstream sub-trees (hub_dsl)
--   /opt/apps/planner/hub_dsl/?.lua        -- hub_dsl top (hub_dsl, build.sh; rare)
--   /opt/apps/planner/hub_dsl/hub_functions/?.lua  -- hub_control, event_handlers, ...
--   /opt/apps/planner/hub_dsl/protocol/?.lua       -- event_ids, command_packets, packet_mapper
--   /opt/apps/planner/hub_dsl/kb_construct/?.lua   -- kb_runtime, kb_exporter, board_builder
--   /opt/apps/planner/hub_dsl/kb/?.lua             -- common_tree, init_check, idle, etc.
--   chain_tree/lua_dsl/luajit_pipeline/?.lua
--                                          -- json_util reach for ct_loader_pure
-- nats_*.lua wrappers live under lib/lib/ so action_server's
-- require("lib.nats_key_store") resolves with the standard `?` -> `?/?` substitution.
package.path = "/opt/apps/planner/lib/?.lua;" ..
               "/opt/apps/planner/?.lua;" ..
               "/opt/apps/planner/hub_dsl/?.lua;" ..
               "/opt/apps/planner/hub_dsl/hub_functions/?.lua;" ..
               "/opt/apps/planner/hub_dsl/protocol/?.lua;" ..
               "/opt/apps/planner/hub_dsl/kb_construct/?.lua;" ..
               "/opt/apps/planner/hub_dsl/kb/?.lua;" ..
               "/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua;" ..
               package.path

local pg_connector    = require("pg_connector")
local infra_discovery = require("infra_discovery")
local kb_status       = require("kb_status")
local ndc_paths       = require("ndc_paths")
local fn_registry     = require("fn_registry")          -- runtime/ smoke load
local kv_writer       = require("kv_writer")            -- runtime/ smoke load
local nats_ks         = require("lib.nats_key_store")   -- A.3.2 NATS smoke load
local nats_jq         = require("lib.nats_job_queue")   -- A.3.2 NATS smoke load
local ct_loader_pure  = require("ct_loader_pure")       -- A.3.3 chain-tree IR loader
local ks_blackboard   = require("ks_blackboard")        -- A.3.3 NATS-KV blackboard
local mqtt_pubsub     = require("lib.mqtt_pubsub")      -- A.3.3b MQTT FFI wrapper
local lua_cbor        = require("lib.lua_cbor")         -- A.3.3b CBOR FFI wrapper
local mqtt_transport  = require("mqtt_transport")       -- A.3.3b uses lib.mqtt_pubsub + lib.lua_cbor

-- action_server + its require chain (kb_query, global_planner,
-- sequencer, link_manager, mission_builder, ...). Instantiated below
-- after pg connect + NATS infra discovery; serve() drives the main
-- loop in the action-server-available branch (Phase 5b worker hookup).
local ok_as, action_server = pcall(require, "action_server")
local action_server_status = ok_as and "ok" or ("FAIL: " .. tostring(action_server))

---------------------------------------------------------------------------
-- env
---------------------------------------------------------------------------

local function env(k) return os.getenv(k) or "" end

local CONTAINER_NAME = env("CONTAINER_NAME")
local APP_SYSTEM     = env("APP_SYSTEM")
local APP_SITE       = env("APP_SITE")
local APP_CPU_ID     = env("APP_CPU_ID")
local PG_HOST        = env("PG_HOST")
local PG_PORT        = tonumber(env("PG_PORT")) or 5432
local PG_DB          = env("PG_DB")
local PG_USER        = env("PG_USER")
local PG_PASSWORD    = env("PG_PASSWORD")

assert(APP_SYSTEM ~= "", "APP_SYSTEM env missing")
assert(APP_SITE   ~= "", "APP_SITE env missing")
assert(CONTAINER_NAME ~= "", "CONTAINER_NAME env missing")
assert(PG_HOST    ~= "", "PG_HOST env missing")

ndc_paths.configure{ system_name = APP_SYSTEM }

local function logf(fmt, ...)
    io.stderr:write(string.format("planner[%s]: " .. fmt .. "\n",
        CONTAINER_NAME, ...))
    io.stderr:flush()
end

logf("started system=%s site=%s cpu=%s pg=%s:%d/%s",
    APP_SYSTEM, APP_SITE, APP_CPU_ID, PG_HOST, PG_PORT, PG_DB)
logf("planner libs loaded: fn_registry=%s kv_writer=%s nats_ks=%s nats_jq=%s ct_loader=%s ks_bb=%s mqtt_ps=%s lua_cbor=%s mqtt_tx=%s action_server=%s",
    type(fn_registry.register_functions) == "function" and "ok" or "missing",
    type(kv_writer.new)                  == "function" and "ok" or "missing",
    type(nats_ks.KeyStore)               == "table"    and "ok" or "missing",
    type(nats_jq.JobQueue)               == "table"    and "ok" or "missing",
    (type(ct_loader_pure) == "table" or type(ct_loader_pure) == "function") and "ok" or "missing",
    (type(ks_blackboard) == "table" or type(ks_blackboard) == "function")   and "ok" or "missing",
    (type(mqtt_pubsub)   == "table" or type(mqtt_pubsub)   == "function")   and "ok" or "missing",
    (type(lua_cbor)      == "table" or type(lua_cbor)      == "function")   and "ok" or "missing",
    (type(mqtt_transport)== "table" or type(mqtt_transport)== "function")   and "ok" or "missing",
    action_server_status)

---------------------------------------------------------------------------
-- pg connect (retry until success; mirrors the dcs_host VERIFY_PG pattern)
---------------------------------------------------------------------------

local function connect_pg_until_ready()
    local cfg = { pg_host = PG_HOST, pg_port = PG_PORT,
                  pg_db   = PG_DB,   pg_user = PG_USER }
    local sleep_ts = ffi.new("ts_t"); sleep_ts.tv_sec = 1
    local attempts = 0
    while true do
        attempts = attempts + 1
        local conn, err = pg_connector.try_connect(cfg, PG_PASSWORD)
        if conn then
            logf("pg connected after %d attempt(s)", attempts)
            return conn
        end
        if attempts == 1 or attempts % 10 == 0 then
            logf("pg connect attempt %d failed: %s -- retry in 1s",
                attempts, tostring(err))
        end
        ffi.C.nanosleep(sleep_ts, nil)
    end
end

local pg = connect_pg_until_ready()

---------------------------------------------------------------------------
-- infra discovery (lookup-only; B.2.A.2 wires real clients)
---------------------------------------------------------------------------

local function discover(service_type)
    local r, err = infra_discovery.lookup(pg, APP_SITE, service_type,
        { require_healthy = false })
    if r then
        logf("infra %s host=%s port=%d healthy=%s age=%ds",
            service_type, r.host, r.port, tostring(r.healthy), r.age_s)
    else
        logf("infra %s lookup failed: %s", service_type, tostring(err))
    end
    return r
end

local nats_info = discover("nats")
local mqtt_info = discover("mqtt")

---------------------------------------------------------------------------
-- action_server instantiation. Phase 5b worker hookup turned dispatch
-- ON: serve() is now called below. Failure here drops the worker into
-- heartbeat-only fallback (NATS / pg may come back; supervisor handles
-- harder failures).
---------------------------------------------------------------------------

local pg_conn = {
    host     = PG_HOST,
    port     = PG_PORT,
    dbname   = PG_DB,
    user     = PG_USER,
    password = PG_PASSWORD,
}

-- Gap-5 fix (post-ROBSIM C3 smoke 2026-05-10): instantiate mqtt_hub
-- and pass to action_server. Without this, action_server doesn't create
-- link_manager (the `if self.mqtt_hub` gate at action_server.lua:161
-- short-circuits), so robot link_announce messages arrive at the broker
-- but are never delivered to the link bridge -- robots stay state=init,
-- planner stays state=planning. Discovered ROBSIM C3 smoke; fixed here.
local mqtt_hub = nil
if ok_as and mqtt_info then
    local mqtt_tx = require("mqtt_hub_transport")
    -- Phase 7 multi-tenant: pass planner_namespace so the MQTT client_id
    -- is unique across planners on a shared broker. Falls back to
    -- CONTAINER_NAME if PLANNER_NAMESPACE env isn't set.
    local hub_ns = os.getenv("PLANNER_NAMESPACE")
    if not hub_ns or hub_ns == "" then hub_ns = CONTAINER_NAME end
    local hub_ok, hub_or_err = pcall(mqtt_tx.new,
        mqtt_info.host, mqtt_info.port, APP_SITE, { namespace = hub_ns })
    if hub_ok then
        mqtt_hub = hub_or_err
        local conn_ok, cerr = pcall(function() mqtt_hub:connect() end)
        if conn_ok then
            logf("mqtt_hub connected: %s:%d site=%s",
                mqtt_info.host, mqtt_info.port, APP_SITE)
        else
            logf("mqtt_hub connect FAIL: %s -- link bridge disabled",
                tostring(cerr))
            mqtt_hub = nil
        end
    else
        logf("mqtt_hub_transport.new FAIL: %s -- link bridge disabled",
            tostring(hub_or_err))
    end
end

local action_srv = nil
if ok_as and nats_info then
    local nats_url = string.format("nats://%s:%d", nats_info.host, nats_info.port)
    -- Phase 5 C4: pass planner_namespace from env if set; otherwise
    -- action_server falls back to own_instance_id (single-tenant).
    local ok_inst, srv_or_err = pcall(action_server.new, {
        pg_conn           = pg_conn,
        site              = APP_SITE,
        system_name       = APP_SYSTEM,
        own_instance_id   = CONTAINER_NAME,
        nats_server       = nats_url,
        planner_namespace = os.getenv("PLANNER_NAMESPACE"),
        mqtt_hub          = mqtt_hub,
    })
    if ok_inst then
        action_srv = srv_or_err
        logf("action_server instantiated: nats=%s mqtt_hub=%s",
            nats_url, mqtt_hub and "wired" or "nil (link bridge disabled)")
    else
        logf("action_server instantiate FAIL: %s", tostring(srv_or_err))
    end
else
    logf("action_server instantiate SKIP: ok_as=%s nats_info=%s",
        tostring(ok_as), tostring(nats_info ~= nil))
end

---------------------------------------------------------------------------
-- runtime.heartbeat -- closure shared between the action_server-driven
-- loop and the heartbeat-only fallback. Wall-time gated so it fires
-- every TICK_SLEEP_S seconds regardless of the surrounding tick rate
-- (action_server's scheduler runs at 2-50ms per cycle when missions
-- are active; main.lua's old loop ran at 5s).
---------------------------------------------------------------------------

local hb_path = ndc_paths.app_runtime_heartbeat_path(APP_SITE, CONTAINER_NAME)
logf("heartbeat path = %s", hb_path)

local TICK_SLEEP_S = 5
local LOG_EVERY_N  = 12   -- one log line per minute (5s * 12)

local function now_ms() return os.time() * 1000 end

local hb_state = { tick = 0, last_at = 0 }

local function fire_heartbeat()
    hb_state.tick = hb_state.tick + 1
    local snapshot = {
        at      = now_ms(),
        host    = CONTAINER_NAME,
        cpu     = APP_CPU_ID,
        ui_port = 0,             -- TODO: surface external ui port
        tick    = hb_state.tick,
    }
    local ok, err = kb_status.set_status_data(pg, hb_path,
        { value = snapshot })
    if not ok then
        logf("heartbeat update failed: %s", tostring(err))
        if not pg_connector.is_alive(pg) then
            logf("pg conn dead, reconnecting")
            pcall(function() pg:close() end)
            pg = connect_pg_until_ready()
        end
    elseif hb_state.tick == 1 or hb_state.tick % LOG_EVERY_N == 0 then
        logf("heartbeat tick=%d at=%d", hb_state.tick, snapshot.at)
    end
    hb_state.last_at = os.time()
end

local function on_tick(_cycle_idx)
    -- action_server's scheduler calls this every 2-50ms; gate to
    -- TICK_SLEEP_S wall-time so we don't hammer pg.
    if os.time() - hb_state.last_at >= TICK_SLEEP_S then
        fire_heartbeat()
    end
end

---------------------------------------------------------------------------
-- Main entry: drive action_server:serve, or fall back to heartbeat-only.
---------------------------------------------------------------------------

if action_srv then
    fire_heartbeat()    -- one immediate heartbeat so the row updates
                        -- before the first 5s wall-time gate fires
    logf("entering action_server:serve(drain_nats=true) -- mission " ..
         "dispatch live; on_tick heartbeat every %ds", TICK_SLEEP_S)
    action_srv:serve({
        drain_nats = true,
        on_tick    = on_tick,
    })
    -- serve() returns only on shutdown / error in the loop body.
    logf("action_server:serve() returned -- worker exiting")
else
    -- Degraded mode: NATS unreachable or action_server failed to
    -- instantiate at startup. Keep the heartbeat row fresh so
    -- node_control's HTTP watchdog still considers us live; the
    -- container will be restarted by the supervisor if pg goes too.
    logf("action_server unavailable -- entering heartbeat-only fallback")
    local sleep_ts = ffi.new("ts_t"); sleep_ts.tv_sec = TICK_SLEEP_S
    while true do
        fire_heartbeat()
        ffi.C.nanosleep(sleep_ts, nil)
    end
end
