#!/usr/bin/env luajit
-- =============================================================================
-- test_sync_queue.lua -- Smoke test for kb_sync_queue + construct_sync_queue.
--
-- Verifies push/drain/peek/count/purge + budget-relevant single-statement
-- behavior. Throwaway KB prefix: sync_smoke.
--
-- Usage:
--   POSTGRES_PASSWORD=... luajit test_sync_queue.lua
-- =============================================================================

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  return src:match("(.*/)") or "./"
end
local DIR = script_dir()
package.path = DIR .. "../../../knowledge_base/postgres/construct_kb/?.lua;"
            .. DIR .. "../../../knowledge_base/postgres/data_structures/?.lua;"
            .. package.path

local PG = {
  host   = os.getenv("PG_HOST") or "localhost",
  port   = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname = os.getenv("PG_DB")   or "knowledge_base",
  user   = os.getenv("PG_USER") or "gedgar",
}
local PASSWORD = os.getenv("POSTGRES_PASSWORD") or error("POSTGRES_PASSWORD not set")

local DATABASE = "sync_smoke"

local CDT     = require("construct_data_tables")
local sync_q  = require("kb_sync_queue")

io.write("1. CREATE Construct_Data_Tables facade ... ")
local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD, DATABASE, false)
local conn = kb.kb.conn
print("ok")

io.write("2. add_kb ('sync') ... ")
kb:add_kb("sync", "sync queue smoke kb")
kb:select_kb("sync")
print("ok")

io.write("3. add_sync_queue master_q + cpu_02_q ... ")
kb:add_header_node("CLASS", "sync_queues", {}, {}, "smoke queues")
kb:add_sync_queue({ queue_name = "master_q",  description = "master inbox" })
kb:add_sync_queue({ queue_name = "cpu_02_q",  description = "cpu_02 inbox" })
kb:leave_header_node("CLASS", "sync_queues")
print("ok")

---------------------------------------------------------------------------
-- Push / drain
---------------------------------------------------------------------------

io.write("4. push 3 verbs into master_q (simulating cpu_02 -> master) ... ")
local s1 = assert(sync_q.push(conn, DATABASE, "master_q", "JOIN_REQ",   { cpu_id = "cpu_02", epoch = 1700 }))
local s2 = assert(sync_q.push(conn, DATABASE, "master_q", "HEARTBEAT",  { cpu_id = "cpu_02", epoch = 1700, seq = 1 }))
local s3 = assert(sync_q.push(conn, DATABASE, "master_q", "HEARTBEAT",  { cpu_id = "cpu_02", epoch = 1700, seq = 2 }))
assert(s1 < s2 and s2 < s3, "seq not monotonic: " .. s1 .. " " .. s2 .. " " .. s3)
print(string.format("ok (seqs %d %d %d)", s1, s2, s3))

io.write("5. count master_q == 3 ... ")
local c = assert(sync_q.count(conn, DATABASE, "master_q"))
assert(c == 3, "expected count=3, got " .. c)
print("ok")

io.write("6. peek 5 returns 3 oldest-first; payload decoded ... ")
local rows = assert(sync_q.peek(conn, DATABASE, "master_q", 5))
assert(#rows == 3, "expected 3 rows, got " .. #rows)
assert(rows[1].verb == "JOIN_REQ", "row 1 verb mismatch: " .. rows[1].verb)
assert(rows[1].payload.cpu_id == "cpu_02", "payload not decoded")
assert(rows[2].verb == "HEARTBEAT" and rows[2].payload.seq == 1)
assert(rows[3].verb == "HEARTBEAT" and rows[3].payload.seq == 2)
-- peek does NOT delete.
assert(sync_q.count(conn, DATABASE, "master_q") == 3, "peek deleted rows!")
print("ok")

io.write("7. drain max=2 returns 2 oldest, leaves 1 ... ")
local drained = assert(sync_q.drain(conn, DATABASE, "master_q", 2))
assert(#drained == 2, "expected 2 drained, got " .. #drained)
assert(drained[1].verb == "JOIN_REQ", "drain order: " .. drained[1].verb)
assert(drained[2].verb == "HEARTBEAT" and drained[2].payload.seq == 1)
local remaining = assert(sync_q.count(conn, DATABASE, "master_q"))
assert(remaining == 1, "expected 1 remaining, got " .. remaining)
print("ok")

io.write("8. drain max=10 returns last 1; queue empty ... ")
local d2 = assert(sync_q.drain(conn, DATABASE, "master_q", 10))
assert(#d2 == 1 and d2[1].payload.seq == 2, "tail row mismatch")
assert(sync_q.count(conn, DATABASE, "master_q") == 0, "queue not empty after drain")
print("ok")

io.write("9. drain on empty queue returns 0 rows, no error ... ")
local d3 = assert(sync_q.drain(conn, DATABASE, "master_q", 5))
assert(#d3 == 0, "expected 0 rows, got " .. #d3)
print("ok")

---------------------------------------------------------------------------
-- Isolation between queues
---------------------------------------------------------------------------

io.write("10. push to cpu_02_q does NOT touch master_q ... ")
assert(sync_q.push(conn, DATABASE, "cpu_02_q", "JOIN_ACK", { master_epoch = 9000 }))
assert(sync_q.push(conn, DATABASE, "cpu_02_q", "HEARTBEAT_ACK", { seq = 1 }))
assert(sync_q.count(conn, DATABASE, "master_q") == 0, "master_q got a stray row")
assert(sync_q.count(conn, DATABASE, "cpu_02_q") == 2, "cpu_02_q count wrong")
print("ok")

io.write("11. purge cpu_02_q returns 2; queue empty ... ")
local n_purged = assert(sync_q.purge(conn, DATABASE, "cpu_02_q"))
assert(n_purged == 2, "expected purge=2, got " .. n_purged)
assert(sync_q.count(conn, DATABASE, "cpu_02_q") == 0)
print("ok")

---------------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------------

io.write("12. push rejects invalid queue_name ... ")
local s, e = sync_q.push(conn, DATABASE, "Bad-Name", "X", {})
assert(s == nil and e:find("invalid queue_name"), "expected validation reject, got: " .. tostring(e))
print("ok (got expected reject)")

io.write("13. push rejects empty verb ... ")
local s2, e2 = sync_q.push(conn, DATABASE, "master_q", "", {})
assert(s2 == nil and e2:find("verb must be"), "expected verb reject, got: " .. tostring(e2))
print("ok")

---------------------------------------------------------------------------
-- Budget sanity (advisory)
---------------------------------------------------------------------------

io.write("14. 100 push/drain pairs fit within 5s budget ... ")
local t0 = os.clock()
for i = 1, 100 do
  assert(sync_q.push(conn, DATABASE, "master_q", "HEARTBEAT", { seq = i }))
end
assert(sync_q.count(conn, DATABASE, "master_q") == 100)
local drained_all = assert(sync_q.drain(conn, DATABASE, "master_q", 200))
assert(#drained_all == 100)
local elapsed = (os.clock() - t0) * 1000
assert(elapsed < 5000, "100 push+drain took " .. elapsed .. "ms (>5s budget)")
print(string.format("ok (100 push + 1 drain in %.1fms; per-push avg %.2fms)",
  elapsed, elapsed / 101))

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

local function drop_if(sql) local s = conn:prepare(sql); if s then s:execute(); s:close() end end
drop_if('DROP TABLE IF EXISTS "sync_smoke_sync_msg__master_q" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_sync_msg__cpu_02_q" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_sync_queue_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_info" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_rollups" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_status" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_job" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_stream" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_stream_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_stream_inst" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_rpc_client" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_rpc_server" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_jsonb" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_bit_mask" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_document" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_link" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_link_mount" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_doc_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_fs_blob" CASCADE')
drop_if('DROP TABLE IF EXISTS "sync_smoke_fs_node" CASCADE')

kb:disconnect()
print("\nALL SYNC QUEUE SMOKE TESTS PASSED")
