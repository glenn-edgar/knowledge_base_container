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
--   /opt/apps/planner/lib/?.lua    -- runtime/ files (fn_registry, kv_writer, ...)
--   /opt/apps/planner/?.lua        -- vendored upstream sub-trees (action_server,
--                                     hub_dsl) keep their import paths verbatim
--   chain_tree/lua_dsl/luajit_pipeline/?.lua
--                                  -- json_util reach for ct_loader_pure
-- nats_*.lua wrappers live under lib/lib/ so action_server's
-- require("lib.nats_key_store") resolves with the standard `?` -> `?/?` substitution.
package.path = "/opt/apps/planner/lib/?.lua;" ..
               "/opt/apps/planner/?.lua;" ..
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

-- Other A.3.3 runtime files copied but not smoke-required yet:
-- link_client, link_manager, mqtt_transport, mqtt_hub_transport (need MQTT FFI -- A.3.3b)
-- queue_monitor (needs hub_dsl/protocol/event_ids + command_packets -- A.3.4)

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
logf("planner libs loaded: fn_registry=%s kv_writer=%s nats_ks=%s nats_jq=%s ct_loader=%s ks_bb=%s",
    type(fn_registry.register_functions) == "function" and "ok" or "missing",
    type(kv_writer.new)                  == "function" and "ok" or "missing",
    type(nats_ks.KeyStore)               == "table"    and "ok" or "missing",
    type(nats_jq.JobQueue)               == "table"    and "ok" or "missing",
    (type(ct_loader_pure)                == "table"
     or type(ct_loader_pure)             == "function") and "ok" or "missing",
    (type(ks_blackboard.new)             == "function"
     or type(ks_blackboard.create)       == "function"
     or type(ks_blackboard)               == "table")  and "ok" or "missing")

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

discover("nats")
discover("mqtt")

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
    ffi.C.nanosleep(sleep_ts, nil)
end
