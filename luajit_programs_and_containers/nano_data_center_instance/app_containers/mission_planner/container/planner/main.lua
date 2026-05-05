#!/usr/bin/env luajit
-- =============================================================================
-- planner/main.lua -- Phase B.2.A.1 runtime skeleton.
--
-- Connects to pg, looks up NATS + MQTT addressing through infra_discovery,
-- and updates the runtime.heartbeat snapshot row every TICK_SLEEP_S
-- seconds. NO NATS subscription, NO mission processing yet -- those land
-- in B.2.A.2 (runtime libraries) and B.2.A.3 (action_server + hub_dsl).
--
-- Path written each tick:
--   system.<sys>.site.<S>.app_containers.<name>.runtime.heartbeat.
--     KB_STATUS_FIELD.snapshot
-- = { value = { at = unix_ms, host, cpu, ui_port, tick } }
--
-- Pre-allocated by apps_builder_framework's driver.lua at commission;
-- this loop only UPDATEs.
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

-- A.3.4: smoke-load action_server + its require chain (kb_query,
-- global_planner, sequencer, link_manager, mission_builder, etc.).
-- A.3.5: instantiate action_server with pg_conn (table) + site + nats_server.
-- See main loop further down for the instantiation step (after pg connect
-- and NATS infra discovery).
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
discover("mqtt")

---------------------------------------------------------------------------
-- A.3.5: instantiate action_server (smoke). Mission dispatch is gated on
-- the kb_runtime body port (deferred for V-heavy completion path).
---------------------------------------------------------------------------

local pg_conn = {
    host     = PG_HOST,
    port     = PG_PORT,
    dbname   = PG_DB,
    user     = PG_USER,
    password = PG_PASSWORD,
}

local action_srv = nil
if ok_as and nats_info then
    local nats_url = string.format("nats://%s:%d", nats_info.host, nats_info.port)
    local ok_inst, srv_or_err = pcall(action_server.new, {
        pg_conn         = pg_conn,
        site            = APP_SITE,
        system_name     = APP_SYSTEM,
        own_instance_id = CONTAINER_NAME,
        nats_server     = nats_url,
    })
    if ok_inst then
        action_srv = srv_or_err
        logf("action_server instantiated: nats_server=%s", nats_url)
    else
        logf("action_server instantiate FAIL: %s", tostring(srv_or_err))
    end
else
    logf("action_server instantiate SKIP: ok_as=%s nats_info=%s",
        tostring(ok_as), tostring(nats_info ~= nil))
end

---------------------------------------------------------------------------
-- A.3.6: JobQueue log-only observer.
--
-- Standalone subscribe to the same KV bucket + queue action_server uses
-- (per its _ensure_nats / _drain_nats_queue convention). We do NOT call
-- action_srv:serve(); dispatch is gated on the kb_runtime body port +
-- kb_query positional-arg fix (deferred to A.4 / A.4b).
--
-- Per-tick drain up to MAX_JOBS_PER_TICK; log payload, complete with
-- "logged" status. External submitter contract: a Lua client opens the
-- same bucket and calls jq:submit(payload_json, queue, priority, retries,
-- timeout). nats CLI 'pub' won't work -- this is JetStream KV-backed,
-- not subject pub/sub.
---------------------------------------------------------------------------

local json_util = require("json_util")

local jq_observer = nil
if action_srv and nats_info then
    local nats_url = string.format("nats://%s:%d", nats_info.host, nats_info.port)
    local site_bucket = APP_SITE:gsub("%.", "_")
    local ok_jq, err_jq = pcall(function()
        local ks = nats_ks.KeyStore.new({
            server        = nats_url,
            bucket        = site_bucket .. "_action_server",
            description   = "Action server: status, results, summary, mission log",
            create_bucket = true,
            history       = 1,
            client_name   = "planner_log_observer_ks",
        })
        ks:connect()
        jq_observer = {
            ks       = ks,
            jq       = nats_jq.JobQueue.new(ks:handle(), "planner_log_observer"),
            queue    = APP_SITE .. ".action_server.missions",
            seen     = 0,
        }
    end)
    if ok_jq and jq_observer then
        logf("jq observer subscribed: bucket=%s_action_server queue=%s",
            site_bucket, jq_observer.queue)
    else
        logf("jq observer subscribe FAIL: %s", tostring(err_jq))
        jq_observer = nil
    end
end

local MAX_JOBS_PER_TICK = 5

local function drain_observer()
    if not jq_observer then return end
    for _ = 1, MAX_JOBS_PER_TICK do
        local ok_claim, job_or_err = pcall(jq_observer.jq.claim_job,
            jq_observer.jq, { jq_observer.queue })
        if not ok_claim then
            logf("jq claim error: %s", tostring(job_or_err))
            return
        end
        local job = job_or_err
        if not job then return end
        jq_observer.seen = jq_observer.seen + 1
        logf("mission received #%d id=%s payload=%s",
            jq_observer.seen, job.id, tostring(job.payload_json))
        pcall(jq_observer.jq.complete_job, jq_observer.jq, job.id,
            '{"status":"logged_only","note":"A.3.6 observer; dispatch deferred"}')
    end
end

---------------------------------------------------------------------------
-- runtime.heartbeat tick loop
---------------------------------------------------------------------------

local hb_path = ndc_paths.app_runtime_heartbeat_path(APP_SITE, CONTAINER_NAME)
logf("heartbeat path = %s", hb_path)

local TICK_SLEEP_S = 5
local LOG_EVERY_N  = 12   -- one log line per minute (5s * 12)

local sleep_ts = ffi.new("ts_t"); sleep_ts.tv_sec = TICK_SLEEP_S

local function now_ms()
    -- coarse second-resolution; sufficient for liveness staleness checks
    return os.time() * 1000
end

local tick = 0
while true do
    tick = tick + 1
    local snapshot = {
        at      = now_ms(),
        host    = CONTAINER_NAME,
        cpu     = APP_CPU_ID,
        ui_port = 0,             -- TODO B.2.A.2+: surface external ui port
        tick    = tick,
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
    elseif tick == 1 or tick % LOG_EVERY_N == 0 then
        logf("heartbeat tick=%d at=%d", tick, snapshot.at)
    end
    drain_observer()
    ffi.C.nanosleep(sleep_ts, nil)
end
