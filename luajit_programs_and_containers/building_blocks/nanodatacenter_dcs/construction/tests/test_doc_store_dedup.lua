#!/usr/bin/env luajit
-- =============================================================================
-- test_doc_store_dedup.lua -- Supplemental file-store smoke test.
--
-- Covers the three acceptance criteria from observability/continue.md
-- "Smoke test queued" that test_new_drivers.lua does NOT exercise:
--
--   (4) Re-load same dir -- verify fs_blob count doesn't grow (sha256 dedup).
--   (5) Modify one file, re-load -- fs_node updates, old blob orphaned.
--   (6) fs_blob_sweep orphan reclamation.
--
-- Throwaway KB prefix: dedup_smoke (independent of test_new_drivers.lua).
--
-- Usage:
--   POSTGRES_PASSWORD=... luajit test_doc_store_dedup.lua
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

local DATABASE = "dedup_smoke"

---------------------------------------------------------------------------
-- Setup
---------------------------------------------------------------------------

local CDT          = require("construct_data_tables")
local kb_doc_store = require("kb_doc_store")
local kb_commiss   = require("kb_doc_commissioning")

io.write("1. CREATE Construct_Data_Tables facade ... ")
local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD, DATABASE, false)
local conn = kb.kb.conn
print("ok")

io.write("2. add_kb ('dedup') ... ")
kb:add_kb("dedup", "dedup smoke kb")
kb:select_kb("dedup")
print("ok")

io.write("3. add_doc_class dedup.tree (commissioning_only) ... ")
kb:add_header_node("CLASS", "dedup_class", {}, {}, "dedup smoke class")
kb:add_doc_class({
  namespace  = "dedup.tree",
  writer     = "commissioning_only",
  source_dir = "/tmp/will-be-set",
})
kb:leave_header_node("CLASS", "dedup_class")
print("ok")

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function sh(cmd)
  local ok = os.execute(cmd)
  assert(ok == 0 or ok == true, "shell failed: " .. cmd)
end
local function write_file(path, bytes)
  local f = assert(io.open(path, "wb")); f:write(bytes); f:close()
end

local function blob_count()
  local s = conn:prepare(string.format("SELECT COUNT(*)::int AS n FROM %s",
    '"' .. DATABASE .. '_fs_blob"'))
  s:execute(); local r = s:fetch(true); s:close(); return r.n
end
local function node_count()
  local s = conn:prepare(string.format("SELECT COUNT(*)::int AS n FROM %s WHERE kind='file'",
    '"' .. DATABASE .. '_fs_node"'))
  s:execute(); local r = s:fetch(true); s:close(); return r.n
end
local function sha_for_path(path)
  local row = kb_doc_store.doc_get(conn, DATABASE, path)
  return row and row.sha256
end

---------------------------------------------------------------------------
-- Build a small fixture: 3 files, 2 of them identical content (dedup test).
---------------------------------------------------------------------------

local SRC = "/tmp/dedup_smoke_src_" .. os.time()
sh("rm -rf '" .. SRC .. "'")
sh("mkdir -p '" .. SRC .. "/sub'")
write_file(SRC .. "/a.txt",     "alpha\n")
write_file(SRC .. "/b.txt",     "alpha\n")  -- identical to a.txt -> same sha
write_file(SRC .. "/sub/c.txt", "gamma\n")

io.write("4. initial load_dir (3 files, 2 share content) ... ")
local n, err = kb_commiss.load_dir(conn, DATABASE, "dedup.tree", SRC,
  { entity_key = "dedup_entity" })
assert(n, "load_dir: " .. tostring(err))
assert(n == 3, "expected 3 files loaded, got " .. n)
local nodes_after_load = node_count()
local blobs_after_load = blob_count()
assert(nodes_after_load == 3, "expected 3 file nodes, got " .. nodes_after_load)
assert(blobs_after_load == 2,
  "expected 2 blobs after dedup (a.txt = b.txt), got " .. blobs_after_load)
print(string.format("ok (nodes=%d blobs=%d)", nodes_after_load, blobs_after_load))

---------------------------------------------------------------------------
-- (4) Re-load same dir -- blob count must NOT grow.
---------------------------------------------------------------------------

io.write("5. re-load same dir -- blob count unchanged (sha256 dedup) ... ")
local n2 = assert(kb_commiss.load_dir(conn, DATABASE, "dedup.tree", SRC,
  { entity_key = "dedup_entity" }))
assert(n2 == 3, "expected 3 files re-loaded, got " .. n2)
local blobs_after_reload = blob_count()
local nodes_after_reload = node_count()
assert(blobs_after_reload == blobs_after_load,
  string.format("blob count grew on re-load: was %d, now %d",
    blobs_after_load, blobs_after_reload))
assert(nodes_after_reload == nodes_after_load,
  string.format("node count grew on re-load: was %d, now %d",
    nodes_after_load, nodes_after_reload))
print(string.format("ok (still nodes=%d blobs=%d)", nodes_after_reload, blobs_after_reload))

---------------------------------------------------------------------------
-- (5) Modify one file, re-load -- fs_node sha changes, old blob orphaned.
---------------------------------------------------------------------------

io.write("6. capture sha for a.txt before modify ... ")
local sha_before = assert(sha_for_path("dedup.tree.a"), "missing dedup.tree.a")
print("ok (sha=" .. (#sha_before == 32 and "32 bytes" or "?") .. ")")

io.write("7. modify a.txt content + re-load ... ")
write_file(SRC .. "/a.txt", "alpha-modified\n")
local n3 = assert(kb_commiss.load_dir(conn, DATABASE, "dedup.tree", SRC,
  { entity_key = "dedup_entity" }))
assert(n3 == 3, "expected 3 files re-loaded, got " .. n3)

local sha_after = assert(sha_for_path("dedup.tree.a"), "missing dedup.tree.a after modify")
assert(sha_after ~= sha_before, "fs_node sha did not change after modify")

-- After the modify: original "alpha\n" blob is now ONLY referenced by b.txt
-- (which still has "alpha\n"). So total blobs should now be 3:
--   - "alpha\n"          (b.txt)
--   - "alpha-modified\n" (a.txt, new)
--   - "gamma\n"          (sub/c.txt)
local blobs_after_modify = blob_count()
assert(blobs_after_modify == 3,
  "expected 3 blobs after modify (no orphan because b.txt still references original), got " .. blobs_after_modify)
print(string.format("ok (sha rotated, blobs=%d, no orphan yet)", blobs_after_modify))

io.write("8. delete b.txt + re-load -- now a.txt's old blob is orphaned ... ")
sh("rm '" .. SRC .. "/b.txt'")
-- load_dir does NOT delete missing files (it's an upsert, not a sync).
-- We must remove the b.txt fs_node manually to orphan its blob.
assert(kb_doc_store.doc_delete(conn, DATABASE, "dedup.tree.b"))
-- b.txt's blob ("alpha\n") now has zero referencing nodes.
-- Blob count is still 3 -- sweep hasn't run.
local blobs_pre_sweep = blob_count()
assert(blobs_pre_sweep == 3,
  "expected 3 blobs pre-sweep (orphan still resident), got " .. blobs_pre_sweep)
print(string.format("ok (orphan present; blobs=%d pre-sweep)", blobs_pre_sweep))

---------------------------------------------------------------------------
-- (6) fs_blob_sweep orphan reclamation.
---------------------------------------------------------------------------

io.write("9. fs_blob_sweep reclaims exactly 1 orphan ... ")
local n_swept = kb_doc_store.fs_blob_sweep(conn, DATABASE)
assert(n_swept == 1, "expected 1 orphan swept, got " .. tostring(n_swept))
local blobs_post_sweep = blob_count()
assert(blobs_post_sweep == 2,
  "expected 2 blobs post-sweep, got " .. blobs_post_sweep)
print(string.format("ok (swept=%d, blobs=%d)", n_swept, blobs_post_sweep))

io.write("10. second fs_blob_sweep is no-op (idempotent) ... ")
local n_swept_again = kb_doc_store.fs_blob_sweep(conn, DATABASE)
assert(n_swept_again == 0, "expected 0 orphans on second sweep, got " .. n_swept_again)
print("ok")

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

sh("rm -rf '" .. SRC .. "'")

local function drop_if(sql) local s = conn:prepare(sql); if s then s:execute(); s:close() end end
drop_if('DROP TABLE IF EXISTS "dedup_smoke_fs_node" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_fs_blob" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_doc_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_info" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_rollups" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_status" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_job" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_stream" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_stream_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_stream_inst" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_rpc_client" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_rpc_server" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_jsonb" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_bit_mask" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_document" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_link" CASCADE')
drop_if('DROP TABLE IF EXISTS "dedup_smoke_link_mount" CASCADE')

kb:disconnect()
print("\nALL DEDUP SMOKE TESTS PASSED")
