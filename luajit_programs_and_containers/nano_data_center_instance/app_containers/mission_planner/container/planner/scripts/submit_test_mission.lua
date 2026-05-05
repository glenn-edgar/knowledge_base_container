#!/usr/bin/env luajit
-- =============================================================================
-- submit_test_mission.lua -- A.3.6 smoke helper.
--
-- Submits a mock mission to the action_server JobQueue (KV-backed,
-- NOT subject pub/sub). Run inside the planner container so the
-- package.path + .so deps already resolve:
--
--   docker exec mission_planner_01 luajit \
--     /opt/apps/planner/scripts/submit_test_mission.lua \
--     '{"robot_id":"rover_1","class_name":"drive_base","board":"landing_zone"}'
--
-- env (inherited from the container): APP_SITE, NATS host/port via
-- infra_discovery is NOT used here; we read NATS_URL directly from env
-- or fall back to nats-js-ram default.
-- =============================================================================

package.path = "/opt/apps/planner/lib/?.lua;" ..
               "/opt/apps/planner/?.lua;" ..
               "/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua;" ..
               package.path

local nats_ks = require("lib.nats_key_store")
local nats_jq = require("lib.nats_job_queue")

local APP_SITE = os.getenv("APP_SITE") or "moon_base_alpha"
local NATS_URL = os.getenv("NATS_URL")
                or ("nats://" .. (os.getenv("NATS_HOST") or "nats-js-ram")
                    .. ":" .. (os.getenv("NATS_PORT") or "4222"))

local payload = arg[1]
if not payload or payload == "" then
    payload = '{"robot_id":"rover_1","class_name":"drive_base","board":"landing_zone"}'
end

local site_bucket = APP_SITE:gsub("%.", "_")
local ks = nats_ks.KeyStore.new({
    server        = NATS_URL,
    bucket        = site_bucket .. "_action_server",
    description   = "Action server: status, results, summary, mission log",
    create_bucket = true,
    history       = 1,
    client_name   = "submit_test_mission",
})
ks:connect()

local jq = nats_jq.JobQueue.new(ks:handle(), "submit_test_mission")
local queue = APP_SITE .. ".action_server.missions"
local job_id = jq:submit(payload, queue, 5, 1, 600)

io.stdout:write(string.format("submitted: queue=%s job_id=%s payload=%s\n",
    queue, job_id, payload))
io.stdout:flush()

jq:destroy()
ks:disconnect()
ks:destroy()
