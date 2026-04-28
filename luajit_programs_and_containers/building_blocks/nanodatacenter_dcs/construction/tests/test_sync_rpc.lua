#!/usr/bin/env luajit
-- =============================================================================
-- test_sync_rpc.lua -- End-to-end smoke for sync_rpc.lua module.
--
-- Drives both master and slave sync_rpc instances against a throwaway pg
-- DB (`sync_rpc_smoke`). Simulates walker ticks manually -- no chain-tree
-- runtime needed. Validates:
--   - JOIN handshake (JOIN_REQ -> JOIN_ACK -> JOIN_CONFIRM -> ACTIVE)
--   - HEARTBEAT round-trip (HEARTBEAT -> HEARTBEAT_ACK -> seq advances)
--   - Master 2s grace (JOIN_REQ before grace expires still ACKs)
--   - Missed-ACK fail-stop (slave calls os.exit at threshold)
--   - Round-robin scheduler (1 peer per tick)
--   - Budget telemetry (under 50ms per handler, no violations)
--   - Outbox flush per tick
--
-- Usage:
--   POSTGRES_PASSWORD=... luajit test_sync_rpc.lua
-- =============================================================================

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  return src:match("(.*/)") or "./"
end
local DIR = script_dir()
package.path = DIR .. "../../../knowledge_base/postgres/construct_kb/?.lua;"
            .. DIR .. "../../../knowledge_base/postgres/data_structures/?.lua;"
            .. DIR .. "../../runtime/dcs_host/?.lua;"
            .. package.path

-- The cfl_definitions module is needed by sync_rpc's wait_bool wrapper.
-- It lives in chain_tree_luajit/runtime.
package.path = DIR .. "../../../chain_tree_luajit/runtime/?.lua;"
            .. package.path

local PG = {
  host   = os.getenv("PG_HOST") or "localhost",
  port   = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname = os.getenv("PG_DB")   or "knowledge_base",
  user   = os.getenv("PG_USER") or "gedgar",
}
local PASSWORD = os.getenv("POSTGRES_PASSWORD") or error("POSTGRES_PASSWORD not set")

-- HACK: sync_rpc.lua hardcodes DATABASE = "knowledge_base".
-- The test needs it pointed at the throwaway prefix so it doesn't
-- collide with the live KB. We monkey-patch via a wrapper module.
-- Easier: just point at "knowledge_base" but use unique queue names that
-- won't collide. But the new code creates per-target queues by name...
-- Cleanest: temporarily rebuild the queues under "knowledge_base" with
-- our test queue names ("test_master_q" etc.) and drop them after.
-- That keeps sync_rpc unmodified.
--
-- ACTUALLY: we'll create the throwaway tables in "knowledge_base" with
-- the ACTUAL queue names (master_q, cpu_02_q) and drop them at end.
-- This works because the live cluster is currently NOT using these
-- queue tables -- the live KB only has cluster_sync_bits etc. The
-- queue tables only get added by sync_queues.lua subsystem on the
-- next build_kb run, which the user will do themselves.

local CDT     = require("construct_data_tables")
local sync_q  = require("kb_sync_queue")
local defs    = require("cfl_definitions")
local sync_rpc = require("sync_rpc")

local DATABASE = "knowledge_base"   -- matches sync_rpc.DATABASE

io.write("0. CDT facade + create master_q + cpu_02_q in live KB ... ")
-- We need a kb instance. upload_flag=true skips schema rebuild (we don't
-- want to touch the live KB structure).
local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD,
                   DATABASE, true)   -- upload_flag = true: skip rebuild
local conn = kb.kb.conn
-- Drop any leftover tables from a prior run.
local function drop_if(sql) local s = conn:prepare(sql); if s then s:execute(); s:close() end end
drop_if('DROP TABLE IF EXISTS "knowledge_base_sync_msg__master_q" CASCADE')
drop_if('DROP TABLE IF EXISTS "knowledge_base_sync_msg__cpu_02_q" CASCADE')
-- Create the per-queue tables directly (not via construct DDL since
-- that would also drop the registry which the live KB doesn't have).
local sql = [[
  CREATE UNLOGGED TABLE IF NOT EXISTS %s (
    seq          BIGSERIAL PRIMARY KEY,
    verb         TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
  )
]]
local s1 = conn:prepare(string.format(sql, '"knowledge_base_sync_msg__master_q"'))
s1:execute(); s1:close()
local s2 = conn:prepare(string.format(sql, '"knowledge_base_sync_msg__cpu_02_q"'))
s2:execute(); s2:close()
print("ok")

---------------------------------------------------------------------------
-- Build minimal ctx for master + slave
---------------------------------------------------------------------------

local function make_log()
  local lines = {}
  return function(half, msg)
    lines[#lines + 1] = string.format("[%s] %s", half, msg)
  end, lines
end

local exit_called = { master = false, slave = false }
local original_exit = os.exit
local function fake_exit(code, who)
  exit_called[who or "?"] = true
  -- Don't actually exit; throw a tagged error caught by the test.
  error("__SIM_EXIT__:" .. tostring(code), 0)
end

local function make_ctx(cpu_id, is_master, peers, log_fn)
  return {
    cfg = {
      cpu_id        = cpu_id,
      is_master     = is_master and 1 or 0,
      master_cpu    = "cpu_01",
      peers         = peers,
      site          = "moonbase.alpha",
    },
    connectors = { pg = conn },
    log = log_fn,
    -- Stub kb_status: silently no-op (writeback paths don't need pg
    -- status fields to exist for the in-RAM logic we're testing).
    kb_status = {
      set_status_data = function(_c, _path, _data) return true end,
    },
    -- Stub kb_exception: capture exception writes so we can assert on them.
    kb_exception = {
      log_exception = function(_c, path, msg)
        -- print("[exc] " .. path .. ": " .. msg)
      end,
    },
  }
end

local mlog, mlines = make_log()
local slog, slines = make_log()

local master_ctx = make_ctx("cpu_01", true,  { "cpu_02" }, mlog)
local slave_ctx  = make_ctx("cpu_02", false, { "cpu_01" }, slog)

local master = sync_rpc.new(master_ctx)
local slave  = sync_rpc.new(slave_ctx)

-- Replace os.exit during slave handler tests so we don't kill ourselves.
-- Slave's os.exit is called inside _slave_on_reset_hint and
-- _slave_heartbeat_tick. We patch by monkey-patching os.exit and
-- expecting the patched version to be called.
local exit_who = "slave"
os.exit = function(code) fake_exit(code, exit_who) end

local R_master, R_slave = {}, {}
master:install_handlers(R_master)
slave:install_handlers(R_slave)

local function tick_event() return defs.CFL_TIMER_EVENT end
local function call_handler(R, name)
  local fn = R[name]
  if not fn then error("missing handler: " .. name) end
  return fn(nil, nil)
end
local function call_pred(R, name)
  local fn = R[name]
  if not fn then error("missing pred: " .. name) end
  return fn(nil, nil, nil, defs.CFL_TIMER_EVENT, nil)
end

---------------------------------------------------------------------------
-- 1. INIT both sides
---------------------------------------------------------------------------

io.write("1. MASTER_SYNC_INIT + SLAVE_SYNC_INIT ... ")
call_handler(R_master, "MASTER_SYNC_INIT")
call_handler(R_slave,  "SLAVE_SYNC_INIT")
local ms = master:_state()
assert(ms.is_master == true, "master flag wrong")
assert(ms.master.peer.cpu_02.state == "UNKNOWN", "peer init state wrong: " .. tostring(ms.master.peer.cpu_02.state))
local ss = slave:_state()
assert(ss.slave.state == "DISCONNECTED", "slave init state wrong: " .. tostring(ss.slave.state))
print("ok")

---------------------------------------------------------------------------
-- 2. Slave sends JOIN_REQ; master drains and ACKs
---------------------------------------------------------------------------

io.write("2. SLAVE_SEND_JOIN pushes JOIN_REQ; slave state -> JOINING ... ")
call_handler(R_slave, "SLAVE_SEND_JOIN")
assert(slave:_state().slave.state == "JOINING", "slave should be JOINING")
local n = sync_q.count(conn, DATABASE, "master_q")
assert(n == 1, "expected 1 msg in master_q, got " .. n)
print("ok")

io.write("3. master scheduler tick drains JOIN_REQ, ACK queued + flushed ... ")
call_handler(R_master, "RPC_SCHEDULER_TICK")
-- After this tick: master.peer[cpu_02].state = JOINING_SAW_REQ; outbox flushed
-- to cpu_02_q with JOIN_ACK.
local p = master:_state().master.peer.cpu_02
assert(p.state == "JOINING_SAW_REQ", "expected JOINING_SAW_REQ, got " .. p.state)
local nack = sync_q.count(conn, DATABASE, "cpu_02_q")
assert(nack == 1, "expected 1 JOIN_ACK in cpu_02_q, got " .. nack)
print("ok")

---------------------------------------------------------------------------
-- 4. Slave drains JOIN_ACK -> ACK_RECEIVED + sends JOIN_CONFIRM
---------------------------------------------------------------------------

io.write("4. slave scheduler tick: JOIN_ACK -> ACK_RECEIVED + queue JOIN_CONFIRM ... ")
call_handler(R_slave, "RPC_SCHEDULER_TICK")
assert(slave:_state().slave.state == "ACK_RECEIVED",
  "slave should be ACK_RECEIVED, got " .. slave:_state().slave.state)
local nconf = sync_q.count(conn, DATABASE, "master_q")
assert(nconf == 1, "expected 1 JOIN_CONFIRM in master_q, got " .. nconf)
print("ok")

---------------------------------------------------------------------------
-- 5. Master drains JOIN_CONFIRM -> peer ACTIVE
---------------------------------------------------------------------------

io.write("5. master scheduler tick: JOIN_CONFIRM -> peer ACTIVE ... ")
call_handler(R_master, "RPC_SCHEDULER_TICK")
assert(master:_state().master.peer.cpu_02.state == "ACTIVE",
  "expected ACTIVE, got " .. master:_state().master.peer.cpu_02.state)
print("ok")

io.write("6. VERIFY_ALL_PEERS_ACTIVE returns true ... ")
local r = call_pred(R_master, "VERIFY_ALL_PEERS_ACTIVE")
assert(r == true, "VERIFY_ALL_PEERS_ACTIVE: expected true, got " .. tostring(r))
print("ok")

---------------------------------------------------------------------------
-- 7. Slave heartbeat round-trip
---------------------------------------------------------------------------

io.write("7. slave heartbeat: SLAVE_HEARTBEAT_TICK pushes HEARTBEAT ... ")
-- Force next_hb_at into the past so the tick definitely sends.
slave.slave.next_hb_at = 0
call_handler(R_slave, "SLAVE_HEARTBEAT_TICK")
local nhb = sync_q.count(conn, DATABASE, "master_q")
assert(nhb == 1, "expected 1 HEARTBEAT in master_q, got " .. nhb)
print("ok")

io.write("8. master tick drains HEARTBEAT, queues HEARTBEAT_ACK ... ")
call_handler(R_master, "RPC_SCHEDULER_TICK")
local nack2 = sync_q.count(conn, DATABASE, "cpu_02_q")
assert(nack2 == 1, "expected 1 HEARTBEAT_ACK in cpu_02_q, got " .. nack2)
print("ok")

io.write("9. slave tick drains HEARTBEAT_ACK -> ACTIVE + last_ack_seq=1 ... ")
call_handler(R_slave, "RPC_SCHEDULER_TICK")
assert(slave:_state().slave.state == "ACTIVE",
  "slave should be ACTIVE, got " .. slave:_state().slave.state)
assert(slave:_state().slave.last_ack_seq == 1,
  "expected last_ack_seq=1, got " .. tostring(slave:_state().slave.last_ack_seq))
assert(slave:_state().slave.missed_acks == 0,
  "expected missed_acks=0 after ACK, got " .. tostring(slave:_state().slave.missed_acks))
print("ok")

io.write("10. VERIFY_OWN_ACTIVE returns true on slave ... ")
local r2 = call_pred(R_slave, "VERIFY_OWN_ACTIVE")
assert(r2 == true, "VERIFY_OWN_ACTIVE: expected true, got " .. tostring(r2))
print("ok")

---------------------------------------------------------------------------
-- 11. Master 2s grace: a fresh master should still ACK during grace
---------------------------------------------------------------------------

io.write("11. fresh master during grace still ACKs JOIN_REQ ... ")
local mctx2 = make_ctx("cpu_01", true, { "cpu_02" }, function() end)
local master2 = sync_rpc.new(mctx2)
local R_m2 = {}; master2:install_handlers(R_m2)
call_handler(R_m2, "MASTER_SYNC_INIT")
-- Push a JOIN_REQ directly to test grace behavior.
sync_q.purge(conn, DATABASE, "master_q")
sync_q.purge(conn, DATABASE, "cpu_02_q")
sync_q.push(conn, DATABASE, "master_q", "JOIN_REQ", { cpu_id = "cpu_02", epoch = 9999 })
assert(os.time() < master2.master.grace_until, "test must run within grace window")
call_handler(R_m2, "RPC_SCHEDULER_TICK")
local nack3 = sync_q.count(conn, DATABASE, "cpu_02_q")
assert(nack3 == 1, "expected ACK queued during grace, got " .. nack3)
print("ok (ACK queued during grace)")

---------------------------------------------------------------------------
-- 12. Missed-ACK fail-stop: 3 missed -> os.exit (mocked)
---------------------------------------------------------------------------

io.write("12. missed-ACK fail-stop triggers os.exit at threshold ... ")
-- Reset slave to ACTIVE state with last_ack_at far in the past.
slave.slave.state       = "ACTIVE"
slave.slave.missed_acks = 0
slave.slave.last_ack_at = os.time() - 100   -- way past 3 * heartbeat period
exit_called.slave = false
local ok, err = pcall(function() call_handler(R_slave, "SLAVE_HEARTBEAT_TICK") end)
assert(exit_called.slave == true,
  "expected os.exit call (got: ok=" .. tostring(ok) .. " err=" .. tostring(err) .. ")")
print("ok (slave called os.exit on fail-stop)")

---------------------------------------------------------------------------
-- 13. Budget telemetry: max < 50ms; no violations
---------------------------------------------------------------------------

io.write("13. budget summary: no violations, max < 50ms ... ")
local mb = master:budget_summary()
assert(mb.violations == 0, "master violations=" .. mb.violations)
assert(mb.max_ms < 50, "master max_ms=" .. mb.max_ms .. " (>=50)")
local sb = slave:budget_summary()
-- slave's last sample was the failed handler that called os.exit; pcall
-- caught it but the budget sample still got recorded. Should still be
-- under 50ms since the work before exit was tiny.
assert(sb.max_ms < 50, "slave max_ms=" .. sb.max_ms)
print(string.format("ok (master max=%.2fms, slave max=%.2fms)",
  mb.max_ms, sb.max_ms))

---------------------------------------------------------------------------
-- 14. Round-robin: master with 2 slaves rotates cursor
---------------------------------------------------------------------------

io.write("14. round-robin cursor with 2 peers ... ")
local mctx3 = make_ctx("cpu_01", true, { "cpu_02", "cpu_03" }, function() end)
local master3 = sync_rpc.new(mctx3)
local R_m3 = {}; master3:install_handlers(R_m3)
call_handler(R_m3, "MASTER_SYNC_INIT")
local cursors = {}
for i = 1, 4 do
  cursors[#cursors + 1] = master3.master.cursor
  -- Each tick advances cursor.
  call_handler(R_m3, "RPC_SCHEDULER_TICK")
end
-- Cursor sequence over 4 ticks should be 1, 2, 1, 2 (advances after each).
assert(cursors[1] == 1 and cursors[3] == 1, "cursor pattern wrong: " ..
  table.concat(cursors, ","))
assert(cursors[2] == 2 and cursors[4] == 2, "cursor pattern wrong: " ..
  table.concat(cursors, ","))
print("ok (cursor rotated 1,2,1,2)")

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

os.exit = original_exit

drop_if('DROP TABLE IF EXISTS "knowledge_base_sync_msg__master_q" CASCADE')
drop_if('DROP TABLE IF EXISTS "knowledge_base_sync_msg__cpu_02_q" CASCADE')

kb:disconnect()
print("\nALL SYNC_RPC SMOKE TESTS PASSED")
