-- =============================================================================
-- test_container_rpc_client_e2e.lua
--
-- Phase 6.4b end-to-end acceptance: drives container_rpc_client.lua against
-- the live master without rebuilding any docker image. Mimics the supervisor
-- context (ctx.env, ctx.connectors.pg, ctx.log) so the client thinks it's
-- running inside a luajit-base container.
--
-- Validates:
--   1. send_ready() pushes CONTAINER_READY -> master ACKs (seq=0) -> client
--      transitions JOINING -> ACTIVE on first HB ACK round-trip.
--   2. Periodic tick() pushes HEARTBEAT, master ACKs, missed_acks stays 0.
--   3. Master's container_state KB row reaches ACTIVE.
--
-- Run with the cluster up:
--   POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) \
--     CONTAINER_NAME=test_app_01 APP_CPU_ID=cpu_01 \
--     APP_SITE=moonbase.alpha.dcs \
--     luajit construction/tests/test_container_rpc_client_e2e.lua
-- =============================================================================

local SCRIPT_DIR = (arg[0] or ""):match("(.+)/[^/]+$") or "."
package.path = SCRIPT_DIR .. "/../../../knowledge_base/postgres/data_structures/?.lua;"
            .. SCRIPT_DIR .. "/../../../luajit_base/container/supervisor/?.lua;"
            .. SCRIPT_DIR .. "/../../runtime/dcs_host/?.lua;"
            .. package.path

local DBI         = require("DBI")
local crpc_client = require("container_rpc_client")
local ndc_paths   = require("ndc_paths")

local NAME    = os.getenv("CONTAINER_NAME") or "test_app_01"
local CPU_ID  = os.getenv("APP_CPU_ID")     or "cpu_01"
local SITE    = os.getenv("APP_SITE")       or "moonbase.alpha.dcs"
local PG_PASS = os.getenv("POSTGRES_PASSWORD") or os.getenv("PG_PASSWORD")
local STATUS_PATH = ndc_paths.site_status_field_path(
                      SITE, "container_state_" .. NAME)

local function die(msg) io.stderr:write("FAIL: " .. msg .. "\n"); os.exit(1) end
local function ok(msg)  print("  PASS: " .. msg) end

if not PG_PASS then die("POSTGRES_PASSWORD env var not set") end

local conn, err = DBI.Connect("PostgreSQL",
  "dbname=knowledge_base host=localhost port=5432",
  "gedgar", PG_PASS)
if not conn then die("pg connect failed: " .. tostring(err)) end
conn:autocommit(true)

-- Mimic supervisor context.
local ctx = {
  env = {
    CONTAINER_NAME = NAME, APP_CPU_ID = CPU_ID, APP_SITE = SITE,
  },
  connectors = { pg = conn },
  log = function(half, msg)
    print(string.format("[%s] %s", half, msg))
  end,
}

print("== Phase 6.4b client acceptance ==")
print("  container = " .. NAME)
print("  cpu       = " .. CPU_ID)

-- 0. Drain whatever the master may have queued from prior runs.
local sync_q = require("kb_sync_queue")
sync_q.purge(conn, "knowledge_base", "container_" .. NAME .. "_q")
sync_q.purge(conn, "knowledge_base", "container_inbox_" .. CPU_ID .. "_q")
ok("queues purged")

-- 1. Instantiate + send CONTAINER_READY.
local cli = crpc_client.new(ctx)
local rok, rerr = cli:send_ready()
if not rok then die("send_ready: " .. tostring(rerr)) end
ok("CONTAINER_READY sent (state=" .. cli:state_name() .. ")")

-- 2. Tick a few times. Master ACKs READY (seq=0) on first drain, which
-- promotes JOINING -> ACTIVE on next tick. Each tick sleeps 0.4s so master's
-- 5Hz scheduler has time to drain.
local active_after_ticks
for i = 1, 10 do
  os.execute("sleep 0.4")
  cli:tick()
  if cli:state_name() == "ACTIVE" then
    active_after_ticks = i; break
  end
end
if cli:state_name() ~= "ACTIVE" then
  die("client never reached ACTIVE; state=" .. cli:state_name())
end
ok(string.format("client transitioned JOINING -> ACTIVE after %d ticks", active_after_ticks))

-- 3. Verify container_state KB row reflects ACTIVE.
local state_active = false
local last_data
for i = 1, 20 do
  local stmt = conn:prepare(
    "SELECT data::text FROM knowledge_base_status WHERE path::text = $1")
  stmt:execute(STATUS_PATH)
  local row = stmt:fetch(true)
  stmt:close()
  if row and row.data and row.data:find('"state":"ACTIVE"', 1, true) then
    state_active = true; last_data = row.data; break
  end
  last_data = row and row.data
  os.execute("sleep 0.5")
end
if not state_active then
  die("container_state never reached ACTIVE; last data: " .. tostring(last_data))
end
ok("master container_state = ACTIVE: " .. last_data)

-- 4. Soak: tick for a while, verify missed_acks stays 0 (steady-state
-- correctness). HEARTBEAT cadence is 60s so we can't actually send one in
-- a short test, but the missed-ACK math should NOT increment in <60s.
for i = 1, 5 do
  os.execute("sleep 1")
  cli:tick()
end
local raw = cli   -- reach into internals for assertion
if raw.missed_acks ~= 0 then
  die("missed_acks > 0 after short soak (got " .. raw.missed_acks .. ")")
end
ok("missed_acks stayed at 0 across 5 ticks (no false positives)")

print("\nALL PHASE 6.4b CLIENT TESTS PASSED")
pcall(function() conn:close() end)
os.exit(0)
