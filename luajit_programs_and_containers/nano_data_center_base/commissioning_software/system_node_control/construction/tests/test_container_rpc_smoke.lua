-- =============================================================================
-- test_container_rpc_smoke.lua
--
-- Phase 6.4 smoke test: master-side acceptance, no app-container changes.
--
-- Synthesizes a container by pushing CONTAINER_READY + HEARTBEAT into
-- container_inbox_cpu_01_q and reading HEARTBEAT_ACKs back from
-- container_test_app_01_q. Verifies:
--
--   1. Master promotes container UNKNOWN -> JOINING on CONTAINER_READY
--   2. Master ACKs the READY immediately (seq=0)
--   3. Master promotes JOINING -> ACTIVE on first HEARTBEAT round-trip
--   4. container_state_test_app_01 KB row reflects ACTIVE within 5s
--
-- Run with the cluster up. Requires POSTGRES_PASSWORD in env.
--
--   POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) \
--     luajit construction/tests/test_container_rpc_smoke.lua
-- =============================================================================

local SCRIPT_DIR = (arg[0] or ""):match("(.+)/[^/]+$") or "."
package.path = SCRIPT_DIR .. "/../../../knowledge_base/postgres/data_structures/?.lua;"
            .. SCRIPT_DIR .. "/../../../knowledge_base/postgres/?.lua;"
            .. SCRIPT_DIR .. "/../../runtime/dcs_host/?.lua;"
            .. package.path

local sync_q    = require("kb_sync_queue")
local DBI       = require("DBI")
local ndc_paths = require("ndc_paths")

ndc_paths.configure{ system_name = "moon_base" }

local DATABASE = "knowledge_base"
local SITE     = "moon_base_alpha"
local CPU_ID   = "cpu_01"
local NAME     = "test_app_01"
local INBOX_Q  = "container_inbox_" .. CPU_ID .. "_q"
local OUTBOX_Q = "container_" .. NAME .. "_q"
local STATUS_PATH = ndc_paths.site_status_field_path(
                      SITE, "container_state_" .. NAME)

local function die(msg)
  io.stderr:write("FAIL: " .. msg .. "\n")
  os.exit(1)
end

local function ok(msg)
  print("  PASS: " .. msg)
end

local pg_pass = os.getenv("POSTGRES_PASSWORD") or
                os.getenv("PG_PASSWORD") or
                die("POSTGRES_PASSWORD not set")

local conn, err = DBI.Connect("PostgreSQL",
  "dbname=" .. DATABASE .. " host=localhost port=5432",
  "gedgar", pg_pass)
if not conn then die("pg connect failed: " .. tostring(err)) end
conn:autocommit(true)

print("== Phase 6.4 smoke test ==")
print("  inbox_q  = " .. INBOX_Q)
print("  outbox_q = " .. OUTBOX_Q)

-- 0. Drain any pre-existing messages so we start clean.
sync_q.purge(conn, DATABASE, INBOX_Q)
sync_q.purge(conn, DATABASE, OUTBOX_Q)
ok("queues purged")

-- 1. Push CONTAINER_READY.
local epoch = os.time()
local _, perr = sync_q.push(conn, DATABASE, INBOX_Q, "CONTAINER_READY", {
  name  = NAME,
  slot  = 1,
  epoch = epoch,
})
if perr then die("push CONTAINER_READY: " .. perr) end
ok("CONTAINER_READY pushed (epoch=" .. epoch .. ")")

-- 2. Wait for the master to drain + ACK. Master scheduler runs at 5 Hz so
--    we should see an ACK within ~0.4s. Allow 3s.
local ack_seen = false
for i = 1, 30 do
  local rows, derr = sync_q.drain(conn, DATABASE, OUTBOX_Q, 5)
  if derr then die("drain outbox: " .. derr) end
  for _, r in ipairs(rows) do
    if r.verb == "HEARTBEAT_ACK" then ack_seen = true end
  end
  if ack_seen then break end
  os.execute("sleep 0.1")
end
if not ack_seen then die("master did not ACK CONTAINER_READY within 3s") end
ok("master ACKed CONTAINER_READY")

-- 3. Push HEARTBEAT (seq=1).
local _, perr2 = sync_q.push(conn, DATABASE, INBOX_Q, "HEARTBEAT", {
  name  = NAME,
  epoch = epoch,
  seq   = 1,
})
if perr2 then die("push HEARTBEAT: " .. perr2) end
ok("HEARTBEAT pushed (seq=1)")

-- 4. Wait for HEARTBEAT_ACK (seq=1).
local hb_ack_seen = false
for i = 1, 30 do
  local rows, derr = sync_q.drain(conn, DATABASE, OUTBOX_Q, 5)
  if derr then die("drain outbox: " .. derr) end
  for _, r in ipairs(rows) do
    if r.verb == "HEARTBEAT_ACK" and tonumber(r.payload.seq) == 1 then
      hb_ack_seen = true
    end
  end
  if hb_ack_seen then break end
  os.execute("sleep 0.1")
end
if not hb_ack_seen then die("master did not ACK HEARTBEAT(seq=1) within 3s") end
ok("master ACKed HEARTBEAT(seq=1)")

-- 5. Verify container_state_test_app_01 KB row reflects ACTIVE.
--    KB writeback runs at 5s cadence so allow up to 10s.
local active_seen = false
local last_data
for i = 1, 50 do
  local stmt, eerr = conn:prepare(
    "SELECT data::text FROM knowledge_base_status WHERE path::text = $1")
  if not stmt then die("prepare: " .. tostring(eerr)) end
  local _, exerr = stmt:execute(STATUS_PATH)
  if not _ then die("execute: " .. tostring(exerr)) end
  local row = stmt:fetch(true)
  stmt:close()
  if row and row.data then
    last_data = row.data
    if last_data:find('"state":"ACTIVE"', 1, true) then
      active_seen = true; break
    end
  end
  os.execute("sleep 0.2")
end
if not active_seen then
  die("container_state did not reach ACTIVE within 10s; last data: " ..
      tostring(last_data))
end
ok("container_state_" .. NAME .. " = ACTIVE: " .. last_data)

print("\nALL PHASE 6.4 SMOKE TESTS PASSED")
pcall(function() conn:close() end)  -- DBI variants vary; ignore close errors
os.exit(0)
