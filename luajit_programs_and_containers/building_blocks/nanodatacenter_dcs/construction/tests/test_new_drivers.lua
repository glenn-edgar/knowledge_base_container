#!/usr/bin/env luajit
-- =============================================================================
-- test_new_drivers.lua -- Smoke test for the class/instance stream + doc drivers.
--
-- Creates a throwaway KB database (`drivers_smoke`) in the dev Postgres,
-- exercises both drivers, verifies round-trip, drops the KB.
--
-- Usage:
--   POSTGRES_PASSWORD=... luajit test_new_drivers.lua
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

-- Defaults match topology.lua (dev pg-vector container on localhost:5432,
-- db 'knowledge_base', user 'gedgar'). Override via PG_* env if needed.
local PG = {
  host   = os.getenv("PG_HOST") or "localhost",
  port   = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname = os.getenv("PG_DB")   or "knowledge_base",
  user   = os.getenv("PG_USER") or "gedgar",
}
local PASSWORD = os.getenv("POSTGRES_PASSWORD") or error("POSTGRES_PASSWORD not set")

local DATABASE = "drivers_smoke"

---------------------------------------------------------------------------
-- Setup
---------------------------------------------------------------------------

local CDT          = require("construct_data_tables")
local kb_doc_store = require("kb_doc_store")
local kb_stream    = require("kb_stream_store")

io.write("1. CREATE Construct_Data_Tables facade ... ")
local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD, DATABASE, false)
print("ok")

io.write("2. add_kb ('smoke') ... ")
kb:add_kb("smoke", "driver smoke-test kb")
kb:select_kb("smoke")
print("ok")

---------------------------------------------------------------------------
-- Doc store round-trip
---------------------------------------------------------------------------

io.write("3. add_doc_class telemetry.doc_ring ... ")
kb:add_header_node("CLASS", "doc_ring_class", {}, {}, "doc smoke class")
kb:add_doc_class({
  namespace    = "smoke.doc_ring",
  writer       = "runtime",
  content_type = "application/octet-stream",
  description  = "doc smoke class",
})
kb:leave_header_node("CLASS", "doc_ring_class")
print("ok")

io.write("4. doc_put hello.txt ... ")
local conn    = kb.kb.conn
local content = "hello, world\n"
-- Compute sha256 via sha256sum for now (same helper the commissioning tool uses).
local tmp = os.tmpname()
local f = io.open(tmp, "wb"); f:write(content); f:close()
local h = io.popen("sha256sum -b '" .. tmp .. "' | cut -c1-64")
local hex = h:read("*l"); h:close(); os.remove(tmp)
local sha = {}
for i = 1, 64, 2 do sha[#sha + 1] = string.char(tonumber(hex:sub(i, i + 1), 16)) end
sha = table.concat(sha)

local ok, err = kb_doc_store.doc_put(conn, DATABASE, "smoke.doc_ring.hello", {
  sha256     = sha,
  content    = content,
  filename   = "hello.txt",
  ext        = "txt",
  entity_key = "smoke_entity_01",
})
assert(ok, "doc_put: " .. tostring(err))
print("ok")

io.write("5. doc_get round-trip ... ")
local got = kb_doc_store.doc_get(conn, DATABASE, "smoke.doc_ring.hello")
assert(got.content == content, "content mismatch: got " .. tostring(got.content))
assert(got.filename == "hello.txt", "filename mismatch")
assert(got.ext == "txt", "ext mismatch")
assert(got.entity_key == "smoke_entity_01", "entity_key mismatch")
print("ok")

io.write("6. doc_put commissioning_only rejects runtime ... ")
kb:add_header_node("CLASS", "doc_conf", {}, {}, "conf class")
kb:add_doc_class({
  namespace = "smoke.doc_conf",
  writer    = "commissioning_only",
})
kb:leave_header_node("CLASS", "doc_conf")
local ok2, err2 = kb_doc_store.doc_put(conn, DATABASE, "smoke.doc_conf.app", {
  sha256 = sha, content = content, filename = "app.conf", writer = "runtime",
})
assert(not ok2 and err2:find("commissioning_only"), "expected commissioning_only rejection, got: " .. tostring(err2))
print("ok (got expected reject: " .. err2 .. ")")

---------------------------------------------------------------------------
-- Stream store round-trip
---------------------------------------------------------------------------

io.write("7. add_stream_class telemetry.heartbeat ... ")
kb:add_header_node("CLASS", "hb_class", {}, {}, "stream smoke class")
kb:add_stream_class({
  namespace   = "smoke.heartbeat",
  cap         = 4,
  unlogged    = true,
  description = "stream smoke class",
})
kb:leave_header_node("CLASS", "hb_class")
print("ok")

io.write("8. stream_open smoke_entity_01 ... ")
local path = kb_stream.stream_open(conn, DATABASE, "smoke.heartbeat", "smoke_entity_01")
assert(path == "smoke.heartbeat.smoke_entity_01", "path mismatch: " .. tostring(path))
print("ok path=" .. path)

io.write("9. push 6 messages into cap=4 ring ... ")
for i = 1, 6 do
  local seq, perr = kb_stream.stream_push(conn, DATABASE, path, "msg_" .. i)
  assert(seq == i, "seq mismatch at push " .. i .. ": " .. tostring(seq))
end
print("ok (seqs 1..6)")

io.write("10. tail(10) should return only last 4 (cap enforcement) ... ")
local tail = kb_stream.stream_tail(conn, DATABASE, path, 10)
assert(#tail == 4, "expected 4 messages after cap, got " .. #tail)
assert(tail[1].seq == 6 and tail[4].seq == 3, "tail order wrong: " ..
  tail[1].seq .. ".." .. tail[4].seq)
assert(tail[1].payload == "msg_6", "payload mismatch: " .. tostring(tail[1].payload))
print("ok (seqs 6..3, payloads survived)")

io.write("11. since(seq>4) should return seqs 5,6 ... ")
local rows = kb_stream.stream_since(conn, DATABASE, path, 4)
assert(#rows == 2 and rows[1].seq == 5 and rows[2].seq == 6,
  "since mismatch: got " .. #rows .. " rows")
print("ok")

---------------------------------------------------------------------------
-- Cross-driver entity purge
---------------------------------------------------------------------------

io.write("12. purge_entity smoke_entity_01 sweeps both drivers ... ")
local n_doc    = kb_doc_store.doc_purge_entity(conn, DATABASE, "smoke_entity_01")
local n_stream = kb_stream.stream_purge_entity(conn, DATABASE, "smoke_entity_01")
assert(n_doc == 1, "expected 1 doc purged, got " .. tostring(n_doc))
assert(n_stream == 1, "expected 1 stream purged, got " .. tostring(n_stream))
print(string.format("ok (docs=%d streams=%d)", n_doc, n_stream))

io.write("13. post-purge tail returns nil ... ")
local t2, terr = kb_stream.stream_tail(conn, DATABASE, path, 10)
assert(not t2 and terr and terr:find("no such instance"),
  "expected 'no such instance', got: " .. tostring(terr))
print("ok")

---------------------------------------------------------------------------
-- Commissioning: load_dir / extract_dir round-trip with tricky filenames
---------------------------------------------------------------------------

local kb_commiss = require("kb_doc_commissioning")

-- Build a source tree with edge-case filenames.
local function sh(cmd)
  local ok = os.execute(cmd)
  assert(ok == 0 or ok == true, "shell failed: " .. cmd)
end
local function write_file(path, bytes)
  local f = assert(io.open(path, "wb")); f:write(bytes); f:close()
end
local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local b = f:read("*a"); f:close(); return b
end

local SRC = "/tmp/drivers_smoke_src_" .. os.time()
local DST = "/tmp/drivers_smoke_dst_" .. os.time()
sh("rm -rf '" .. SRC .. "' '" .. DST .. "'")
sh("mkdir -p '" .. SRC .. "/sub'")
write_file(SRC .. "/hello.txt",        "hello body\n")
write_file(SRC .. "/my-report.v2.pdf", "pdf body\n")
write_file(SRC .. "/README",           "readme body\n")
write_file(SRC .. "/.bashrc",          "bashrc body\n")
write_file(SRC .. "/sub/file.json",    '{"k":1}\n')

io.write("14. add_doc_class smoke.loaded (commissioning_only, source_dir) ... ")
kb:add_header_node("CLASS", "loaded_class", {}, {}, "load_dir smoke")
kb:add_doc_class({
  namespace  = "smoke.loaded",
  writer     = "commissioning_only",
  source_dir = SRC,
})
kb:leave_header_node("CLASS", "loaded_class")
print("ok")

io.write("15. load_dir walks source tree ... ")
local n_loaded, lerr = kb_commiss.load_dir(conn, DATABASE, "smoke.loaded", SRC,
                                            { entity_key = "commiss_entity" })
assert(n_loaded, "load_dir: " .. tostring(lerr))
assert(n_loaded == 5, "expected 5 files loaded, got " .. n_loaded)
print("ok (5 files)")

io.write("16. verify (filename, ext, label) for each edge case ... ")
local function check(path, want_filename, want_ext)
  local row = kb_doc_store.doc_get(conn, DATABASE, path)
  assert(row, "missing row at " .. path)
  assert(row.filename == want_filename,
    string.format("%s: filename want=%q got=%q", path, want_filename, tostring(row.filename)))
  assert(row.ext == want_ext,
    string.format("%s: ext want=%s got=%s", path,
                  tostring(want_ext), tostring(row.ext)))
end
-- hello.txt: straightforward
check("smoke.loaded.hello",         "hello.txt",        "txt")
-- my-report.v2.pdf: hyphen -> _, dots in basename -> _, ext = last suffix
check("smoke.loaded.my_report_v2",  "my-report.v2.pdf", "pdf")
-- README: no extension
check("smoke.loaded.readme",        "README",           nil)
-- .bashrc: leading dot means no usable extension; label becomes _bashrc
check("smoke.loaded._bashrc",       ".bashrc",          nil)
-- sub/file.json: nested
check("smoke.loaded.sub.file",      "file.json",        "json")
print("ok (all 5 edge cases)")

io.write("17. writer=commissioning_only accepted via load_dir (writer='commissioning') ... ")
-- Already proved by test 15 succeeding on a commissioning_only class. Re-affirm:
local cls_row = (function()
  local stmt = conn:prepare("SELECT writer FROM drivers_smoke_doc_class WHERE namespace = 'smoke.loaded'::ltree")
  stmt:execute(); local r = stmt:fetch(true); stmt:close(); return r
end)()
assert(cls_row.writer == "commissioning_only",
  "class writer mismatch: " .. tostring(cls_row.writer))
print("ok")

io.write("18. extract_dir reconstructs tree into DST ... ")
local n_written, eerr = kb_commiss.extract_dir(conn, DATABASE, "smoke.loaded", DST)
assert(n_written, "extract_dir: " .. tostring(eerr))
assert(n_written == 5, "expected 5 files written, got " .. n_written)
print("ok (5 files)")

io.write("19. diff src vs extracted tree (content + filenames) ... ")
local function files_under(root)
  local h = io.popen("cd '" .. root .. "' && find . -type f | sort")
  local list = {}
  for line in h:lines() do list[#list + 1] = line end
  h:close()
  return list
end
local src_files = files_under(SRC)
local dst_files = files_under(DST)
assert(#src_files == #dst_files,
  string.format("file count mismatch: src=%d dst=%d", #src_files, #dst_files))
for i, rel in ipairs(src_files) do
  assert(rel == dst_files[i],
    string.format("file name mismatch at %d: src=%s dst=%s", i, rel, dst_files[i]))
  local sb = read_file(SRC .. "/" .. rel:sub(3))  -- strip './'
  local db = read_file(DST .. "/" .. rel:sub(3))
  assert(sb == db,
    string.format("content mismatch at %s: src=%q dst=%q", rel, tostring(sb), tostring(db)))
end
print("ok (5 files match byte-for-byte)")

io.write("20. purge_entity commiss_entity sweeps loaded docs ... ")
local n_p = kb_doc_store.doc_purge_entity(conn, DATABASE, "commiss_entity")
-- Only file rows carry entity_key (load_dir sets it only on files, not dirs).
assert(n_p == 5, "expected 5 file purged, got " .. tostring(n_p))
print("ok (" .. n_p .. " purged)")

-- Clean up temp dirs
sh("rm -rf '" .. SRC .. "' '" .. DST .. "'")

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

kb:check_installation()

-- Drop smoke-test tables so we don't pollute the real knowledge_base DB.
-- Use raw DBI on the same connection.
local function drop_if(sql) local s = conn:prepare(sql); if s then s:execute(); s:close() end end
drop_if('DROP TABLE IF EXISTS "drivers_smoke_fs_node" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_fs_blob" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_doc_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_stream_inst" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_stream_class" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_stream_msg__smoke_heartbeat" CASCADE')
drop_if('DROP FUNCTION IF EXISTS "drivers_smoke_stream_push__smoke_heartbeat"(ltree, bytea)')
-- Baseline KB tables created by KnowledgeBaseManager / satellites:
drop_if('DROP TABLE IF EXISTS "drivers_smoke" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_stream" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_status" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_job" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_rpc_client" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_rpc_server" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_jsonb" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_bit_mask" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_document" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_link" CASCADE')
drop_if('DROP TABLE IF EXISTS "drivers_smoke_link_mount" CASCADE')

kb:disconnect()
print("\nALL DRIVER SMOKE TESTS PASSED")
