--[[
  kb_doc_store.lua

  Runtime client for the content-addressable document store (see
  construct_doc_store.lua for schema + DDL).

  Low-level API: the caller supplies sha256 on writes (we do not pull in
  a crypto library here). A helper in the commissioning tool (or a
  small openresty-side wrapper using resty.openssl.digest) computes
  sha256 and calls put().

  Three operations container code needs:
    - doc_get(path)           -> row or nil
    - doc_list(namespace)     -> list of rows
    - doc_stat(path)          -> { sha256, mtime, size } or nil  (for poll-on-change)

  Two operations only used by admin UI / runtime writers:
    - doc_put(path, opts)
    - doc_delete(path)

  Cross-class teardown (commissioning + admin):
    - doc_purge_entity(entity_key)

  Design-memory: project_dcs_doc_driver.md.
]]

local dkjson = require("dkjson")

local M = {}

---------------------------------------------------------------------------
-- SQL helpers (kept local; no dependency on construct_driver_common
-- since runtime may run in environments where construct_kb isn't loaded).
---------------------------------------------------------------------------

local function quote_ident(name)
  return '"' .. tostring(name):gsub('"', '""') .. '"'
end

local function quote_literal(val)
  if val == nil then return "NULL" end
  return "'" .. tostring(val):gsub("'", "''") .. "'"
end

-- Encode a Lua string as a Postgres bytea hex literal.
-- Input is expected to be a raw byte string (e.g. 32 bytes of sha256).
local function bytea_hex(bytes)
  if bytes == nil then return "NULL" end
  local s = "\\x"
  for i = 1, #bytes do
    s = s .. string.format("%02x", bytes:byte(i))
  end
  return "'" .. s .. "'::bytea"
end

-- Decode a Postgres bytea value (returned by DBI as a hex-prefixed string
-- "\xAABB..." or as raw bytes depending on driver config) back to raw.
local function bytea_decode(v)
  if v == nil then return nil end
  if type(v) ~= "string" then return v end
  if v:sub(1, 2) == "\\x" then
    local out = {}
    for i = 3, #v, 2 do
      out[#out + 1] = string.char(tonumber(v:sub(i, i + 1), 16))
    end
    return table.concat(out)
  end
  return v
end

local function exec(conn, sql)
  local stmt, err = conn:prepare(sql)
  if not stmt then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = stmt:execute()
  if not ok then stmt:close(); return nil, "execute: " .. tostring(eerr) end
  stmt:close()
  return true
end

local function query_one(conn, sql)
  local stmt, err = conn:prepare(sql)
  if not stmt then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = stmt:execute()
  if not ok then stmt:close(); return nil, "execute: " .. tostring(eerr) end
  local row = stmt:fetch(true)
  stmt:close()
  return row
end

local function query_all(conn, sql)
  local stmt, err = conn:prepare(sql)
  if not stmt then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = stmt:execute()
  if not ok then stmt:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local r = stmt:fetch(true)
    if not r then break end
    rows[#rows + 1] = r
  end
  stmt:close()
  return rows
end

---------------------------------------------------------------------------
-- Class lookup
---------------------------------------------------------------------------

local function class_for_path(conn, database, path)
  -- Find the longest registered class namespace that is an ancestor of path.
  local row, err = query_one(conn, string.format([[
    SELECT namespace::text AS namespace, writer, content_type
      FROM %s
     WHERE namespace @> %s::ltree
     ORDER BY nlevel(namespace) DESC
     LIMIT 1
  ]], quote_ident(database .. "_doc_class"), quote_literal(path)))
  if err then return nil, err end
  return row
end

---------------------------------------------------------------------------
-- Read
---------------------------------------------------------------------------

--- Fetch a file node with its content. Returns row or nil.
--- Row fields: path, kind, sha256 (raw bytes), content (raw bytes),
--- filename, ext, entity_key, meta (decoded table), mtime, size, mime.
function M.doc_get(conn, database, path)
  local tn = quote_ident(database .. "_fs_node")
  local tb = quote_ident(database .. "_fs_blob")
  local sql = string.format([[
    SELECT n.path::text AS path, n.kind, n.filename, n.ext, n.entity_key,
           n.meta::text AS meta, n.mtime, n.updated_at,
           b.sha256, b.content, b.size, b.mime
      FROM %s n
 LEFT JOIN %s b ON b.sha256 = n.sha256
     WHERE n.path = %s::ltree
  ]], tn, tb, quote_literal(path))
  local row, err = query_one(conn, sql)
  if err then return nil, err end
  if not row then return nil end
  row.meta    = row.meta and dkjson.decode(row.meta) or {}
  row.sha256  = bytea_decode(row.sha256)
  row.content = bytea_decode(row.content)
  return row
end

--- Stat a file node without fetching content. Cheap for poll-on-change.
function M.doc_stat(conn, database, path)
  local tn = quote_ident(database .. "_fs_node")
  local tb = quote_ident(database .. "_fs_blob")
  local sql = string.format([[
    SELECT n.sha256, n.mtime, n.updated_at, b.size, b.mime
      FROM %s n
 LEFT JOIN %s b ON b.sha256 = n.sha256
     WHERE n.path = %s::ltree
  ]], tn, tb, quote_literal(path))
  local row, err = query_one(conn, sql)
  if err then return nil, err end
  if not row then return nil end
  row.sha256 = bytea_decode(row.sha256)
  return row
end

--- Enumerate nodes under a namespace (subtree query).
function M.doc_list(conn, database, namespace, opts)
  opts = opts or {}
  local tn = quote_ident(database .. "_fs_node")
  local clause_kind = ""
  if opts.files_only then clause_kind = "AND kind = 'file'" end
  if opts.dirs_only  then clause_kind = "AND kind = 'dir'"  end
  local sql = string.format([[
    SELECT path::text AS path, kind, filename, ext, entity_key, mtime, updated_at
      FROM %s
     WHERE path <@ %s::ltree %s
     ORDER BY path
  ]], tn, quote_literal(namespace), clause_kind)
  return query_all(conn, sql)
end

---------------------------------------------------------------------------
-- Write
---------------------------------------------------------------------------

--- Upsert a file node + its blob. Idempotent on sha256 — re-running with
--- unchanged sha256 only bumps mtime/meta.
---
--- opts fields:
---   sha256      string  required, 32 raw bytes
---   content     string  required on first put (insert into fs_blob)
---   filename    string  required, original filename (not the ltree label)
---   ext         string  optional, file extension (no dot, lowercased)
---   mime        string  optional, MIME type stored on the blob
---   entity_key  string  optional, for cross-class purge grouping
---   meta        table   optional, merged into existing meta
---   mtime       string  optional ISO-8601 timestamptz (defaults NOW())
---   writer      string  'runtime' (default) | 'commissioning'
---                       Only 'commissioning' may write to a class
---                       whose writer='commissioning_only'.
function M.doc_put(conn, database, path, opts)
  assert(type(opts) == "table" and opts.sha256, "opts.sha256 required")
  assert(type(opts.filename) == "string", "opts.filename required")

  -- Writer policy check.
  local cls, err = class_for_path(conn, database, path)
  if err then return nil, err end
  if not cls then
    return nil, "no doc class registered for path: " .. path
  end
  local writer = opts.writer or "runtime"
  if cls.writer == "commissioning_only" and writer ~= "commissioning" then
    return nil, string.format(
      "class '%s' is commissioning_only; runtime write rejected", cls.namespace)
  end

  -- 1. Insert blob if new (dedup on sha256).
  if opts.content then
    local blob_sql = string.format([[
      INSERT INTO %s (sha256, size, content, mime)
      VALUES (%s, %d, %s, %s)
      ON CONFLICT (sha256) DO NOTHING
    ]],
      quote_ident(database .. "_fs_blob"),
      bytea_hex(opts.sha256),
      #opts.content,
      bytea_hex(opts.content),
      opts.mime and quote_literal(opts.mime) or "NULL")
    local ok, berr = exec(conn, blob_sql)
    if not ok then return nil, "blob upsert: " .. tostring(berr) end
  end

  -- 2. Upsert node row. Merge meta (don't clobber existing tags).
  local meta_json = dkjson.encode(opts.meta or {})
  local node_sql = string.format([[
    INSERT INTO %s (path, class_namespace, kind, sha256, filename, ext,
                    entity_key, meta, mtime)
    VALUES (%s::ltree, %s::ltree, 'file', %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (path) DO UPDATE SET
      sha256     = EXCLUDED.sha256,
      filename   = EXCLUDED.filename,
      ext        = EXCLUDED.ext,
      entity_key = EXCLUDED.entity_key,
      meta       = %s.meta || EXCLUDED.meta,
      mtime      = EXCLUDED.mtime,
      updated_at = NOW()
  ]],
    quote_ident(database .. "_fs_node"),
    quote_literal(path),
    quote_literal(cls.namespace),
    bytea_hex(opts.sha256),
    quote_literal(opts.filename),
    opts.ext and quote_literal(opts.ext) or "NULL",
    opts.entity_key and quote_literal(opts.entity_key) or "NULL",
    quote_literal(meta_json),
    opts.mtime and quote_literal(opts.mtime) or "NOW()",
    quote_ident(database .. "_fs_node"))

  local ok, nerr = exec(conn, node_sql)
  if not ok then return nil, "node upsert: " .. tostring(nerr) end
  return true
end

---------------------------------------------------------------------------
-- Directory row (for subtree metadata).
---------------------------------------------------------------------------

function M.doc_put_dir(conn, database, path, opts)
  opts = opts or {}
  local cls, err = class_for_path(conn, database, path)
  if err then return nil, err end
  if not cls then
    return nil, "no doc class registered for path: " .. path
  end
  local meta_json = dkjson.encode(opts.meta or {})
  local sql = string.format([[
    INSERT INTO %s (path, class_namespace, kind, filename, meta)
    VALUES (%s::ltree, %s::ltree, 'dir', %s, %s::jsonb)
    ON CONFLICT (path) DO UPDATE SET
      meta       = %s.meta || EXCLUDED.meta,
      updated_at = NOW()
  ]],
    quote_ident(database .. "_fs_node"),
    quote_literal(path),
    quote_literal(cls.namespace),
    quote_literal(opts.filename or path:match("([^.]+)$") or path),
    quote_literal(meta_json),
    quote_ident(database .. "_fs_node"))
  return exec(conn, sql)
end

---------------------------------------------------------------------------
-- Delete
---------------------------------------------------------------------------

--- Delete a single node. Blob may become orphan; sweep separately.
function M.doc_delete(conn, database, path)
  return exec(conn, string.format(
    "DELETE FROM %s WHERE path = %s::ltree",
    quote_ident(database .. "_fs_node"),
    quote_literal(path)))
end

--- Delete a whole subtree. Blobs may become orphans; sweep separately.
function M.doc_delete_subtree(conn, database, path)
  return exec(conn, string.format(
    "DELETE FROM %s WHERE path <@ %s::ltree",
    quote_ident(database .. "_fs_node"),
    quote_literal(path)))
end

--- Purge every doc associated with an entity_key, across all classes.
--- Mirrors the stream driver's purge_entity — one indexed lookup deletes
--- every row regardless of which class it lived in.
function M.doc_purge_entity(conn, database, entity_key)
  local n_deleted, err = query_one(conn, string.format([[
    WITH d AS (
      DELETE FROM %s WHERE entity_key = %s RETURNING 1
    ) SELECT COUNT(*)::int AS n FROM d
  ]], quote_ident(database .. "_fs_node"), quote_literal(entity_key)))
  if err then return nil, err end
  return n_deleted and n_deleted.n or 0
end

---------------------------------------------------------------------------
-- Orphan blob sweep
---------------------------------------------------------------------------

--- Remove blob rows no longer referenced by any fs_node. Safe to run on
--- a cron (nightly from log_manager) or after bulk deletes.
function M.fs_blob_sweep(conn, database)
  local row, err = query_one(conn, string.format([[
    WITH d AS (
      DELETE FROM %s b
      WHERE NOT EXISTS (SELECT 1 FROM %s n WHERE n.sha256 = b.sha256)
      RETURNING 1
    ) SELECT COUNT(*)::int AS n FROM d
  ]], quote_ident(database .. "_fs_blob"), quote_ident(database .. "_fs_node")))
  if err then return nil, err end
  return row and row.n or 0
end

return M
