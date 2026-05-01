--[[
  kb_stream_store.lua

  Runtime client for the class/instance capped-FIFO stream store (schema
  in construct_stream_store.lua).

  Three operations container code typically uses:
    - stream_open(namespace, entity_key, opts) -> path
    - stream_push(path, payload_bytes)         -> seq
    - stream_tail(path, n)                     -> newest-first rows
    - stream_since(path, last_seq)             -> rows with seq > last_seq

  Cross-class teardown (commissioning + admin):
    - stream_close(path)
    - stream_purge_entity(entity_key)

  Payload is raw bytes (typically CBOR). Driver is encoding-agnostic.

  Design-memory: project_dcs_stream_driver.md.
]]

local M = {}

---------------------------------------------------------------------------
-- SQL helpers (local; matches kb_doc_store style).
---------------------------------------------------------------------------

local function quote_ident(name)
  return '"' .. tostring(name):gsub('"', '""') .. '"'
end

local function quote_literal(val)
  if val == nil then return "NULL" end
  return "'" .. tostring(val):gsub("'", "''") .. "'"
end

local function bytea_hex(bytes)
  if bytes == nil then return "NULL" end
  local s = "\\x"
  for i = 1, #bytes do
    s = s .. string.format("%02x", bytes:byte(i))
  end
  return "'" .. s .. "'::bytea"
end

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

local function lookup_class(conn, database, namespace)
  local row, err = query_one(conn, string.format([[
    SELECT namespace::text AS namespace, cap_default, messages_table, push_function
      FROM %s
     WHERE namespace = %s::ltree
  ]], quote_ident(database .. "_stream_class"), quote_literal(namespace)))
  if err then return nil, err end
  if not row then return nil, "no stream class: " .. namespace end
  return row
end

---------------------------------------------------------------------------
-- Open / close
---------------------------------------------------------------------------

--- Open an instance under a class. Idempotent: if the instance already
--- exists, does nothing; if it existed before and was closed, bumps
--- generation so old cursors detect staleness.
---
--- @param conn        DBI connection
--- @param database    KB database name
--- @param namespace   class namespace (must be registered)
--- @param entity_key  string (ltree-label-safe; caller validates)
--- @param opts        { cap = int (override), meta = table }
--- @return path, err
function M.stream_open(conn, database, namespace, entity_key, opts)
  opts = opts or {}
  local cls, err = lookup_class(conn, database, namespace)
  if not cls then return nil, err end

  local cap = opts.cap or cls.cap_default
  assert(type(entity_key) == "string" and entity_key:match("^[A-Za-z][A-Za-z0-9_]*$"),
    "entity_key must match [A-Za-z][A-Za-z0-9_]*")

  local path = namespace .. "." .. entity_key

  -- Upsert with generation bump on re-open.
  local sql = string.format([[
    INSERT INTO %s (path, class_namespace, cap, entity_key, tail_seq, generation)
    VALUES (%s::ltree, %s::ltree, %d, %s, 0, 1)
    ON CONFLICT (path) DO UPDATE SET
      cap        = EXCLUDED.cap,
      entity_key = EXCLUDED.entity_key,
      -- Do not reset tail_seq on re-open of a still-live instance; only
      -- bump generation when reopening an empty instance to signal
      -- reuse.
      generation = CASE
        WHEN %s.tail_seq = 0 THEN %s.generation
        ELSE %s.generation
      END
  ]],
    quote_ident(database .. "_stream_inst"),
    quote_literal(path),
    quote_literal(namespace),
    cap,
    quote_literal(entity_key),
    quote_ident(database .. "_stream_inst"),
    quote_ident(database .. "_stream_inst"),
    quote_ident(database .. "_stream_inst"))
  local ok, eerr = exec(conn, sql)
  if not ok then return nil, eerr end
  return path
end

--- Close a single instance. Content (messages + inst row) is removed.
function M.stream_close(conn, database, path)
  -- Row deletion from stream_inst does NOT cascade to stream_msg
  -- (different table per class; no FK). Do messages first, then inst.
  local cls, err = query_one(conn, string.format([[
    SELECT c.messages_table
      FROM %s i JOIN %s c ON c.namespace = i.class_namespace
     WHERE i.path = %s::ltree
  ]], quote_ident(database .. "_stream_inst"),
      quote_ident(database .. "_stream_class"),
      quote_literal(path)))
  if err then return nil, err end
  if not cls then return true end  -- already gone

  local ok, eerr = exec(conn, string.format(
    "DELETE FROM %s WHERE stream_path = %s::ltree",
    quote_ident(cls.messages_table), quote_literal(path)))
  if not ok then return nil, eerr end

  return exec(conn, string.format(
    "DELETE FROM %s WHERE path = %s::ltree",
    quote_ident(database .. "_stream_inst"), quote_literal(path)))
end

---------------------------------------------------------------------------
-- Push
---------------------------------------------------------------------------

--- Atomic push via the class's plpgsql function. Returns the new seq.
function M.stream_push(conn, database, path, payload_bytes)
  assert(type(payload_bytes) == "string", "payload must be a byte string")
  -- Look up the class (via instance).
  local row, err = query_one(conn, string.format([[
    SELECT c.push_function
      FROM %s i JOIN %s c ON c.namespace = i.class_namespace
     WHERE i.path = %s::ltree
  ]], quote_ident(database .. "_stream_inst"),
      quote_ident(database .. "_stream_class"),
      quote_literal(path)))
  if err then return nil, err end
  if not row then return nil, "no stream instance: " .. path end

  local seq_row, serr = query_one(conn, string.format(
    "SELECT %s(%s::ltree, %s) AS seq",
    quote_ident(row.push_function),
    quote_literal(path),
    bytea_hex(payload_bytes)))
  if serr then return nil, serr end
  return tonumber(seq_row.seq)
end

---------------------------------------------------------------------------
-- Read
---------------------------------------------------------------------------

local function msg_table_for(conn, database, path)
  local row, err = query_one(conn, string.format([[
    SELECT c.messages_table, i.generation
      FROM %s i JOIN %s c ON c.namespace = i.class_namespace
     WHERE i.path = %s::ltree
  ]], quote_ident(database .. "_stream_inst"),
      quote_ident(database .. "_stream_class"),
      quote_literal(path)))
  if err then return nil, err end
  return row
end

--- Fetch the last N messages, newest-first.
function M.stream_tail(conn, database, path, n)
  n = n or 50
  local mt, err = msg_table_for(conn, database, path)
  if not mt then return nil, err or "no such instance" end

  local rows, rerr = query_all(conn, string.format([[
    SELECT seq, generation, ts, payload
      FROM %s
     WHERE stream_path = %s::ltree
     ORDER BY seq DESC
     LIMIT %d
  ]], quote_ident(mt.messages_table), quote_literal(path), n))
  if rerr then return nil, rerr end
  for _, r in ipairs(rows) do
    r.seq     = tonumber(r.seq)
    r.payload = bytea_decode(r.payload)
  end
  return rows
end

--- Tail-follow: fetch messages with seq > last_seq, oldest-first.
--- If the instance's generation changed since last_seq was issued, the
--- caller's cursor is stale — returns (nil, "generation mismatch", current_generation).
function M.stream_since(conn, database, path, last_seq, expect_generation)
  local mt, err = msg_table_for(conn, database, path)
  if not mt then return nil, err or "no such instance" end

  if expect_generation and tonumber(expect_generation) ~= tonumber(mt.generation) then
    return nil, "generation mismatch", tonumber(mt.generation)
  end

  local rows, rerr = query_all(conn, string.format([[
    SELECT seq, generation, ts, payload
      FROM %s
     WHERE stream_path = %s::ltree AND seq > %d
     ORDER BY seq ASC
  ]], quote_ident(mt.messages_table), quote_literal(path), tonumber(last_seq) or 0))
  if rerr then return nil, rerr end
  for _, r in ipairs(rows) do
    r.seq     = tonumber(r.seq)
    r.payload = bytea_decode(r.payload)
  end
  return rows, nil, tonumber(mt.generation)
end

---------------------------------------------------------------------------
-- Enumerate
---------------------------------------------------------------------------

--- List instances under a namespace (subtree).
function M.stream_enumerate(conn, database, namespace)
  return query_all(conn, string.format([[
    SELECT path::text AS path, entity_key, cap, tail_seq, generation, last_write_at
      FROM %s
     WHERE path <@ %s::ltree
     ORDER BY path
  ]], quote_ident(database .. "_stream_inst"), quote_literal(namespace)))
end

---------------------------------------------------------------------------
-- Purge by entity
---------------------------------------------------------------------------

--- Delete every stream instance (across all classes) whose entity_key
--- matches. Dispatches DELETE to each affected per-class messages table
--- via dynamic SQL through the class registry.
--- @return number of instances deleted
function M.stream_purge_entity(conn, database, entity_key)
  -- Find affected instances and their messages_table.
  local rows, err = query_all(conn, string.format([[
    SELECT i.path::text AS path, c.messages_table
      FROM %s i JOIN %s c ON c.namespace = i.class_namespace
     WHERE i.entity_key = %s
  ]],
    quote_ident(database .. "_stream_inst"),
    quote_ident(database .. "_stream_class"),
    quote_literal(entity_key)))
  if err then return nil, err end

  for _, r in ipairs(rows) do
    local ok, mderr = exec(conn, string.format(
      "DELETE FROM %s WHERE stream_path = %s::ltree",
      quote_ident(r.messages_table), quote_literal(r.path)))
    if not ok then return nil, "purge " .. r.path .. ": " .. tostring(mderr) end
  end

  local ok, ierr = exec(conn, string.format(
    "DELETE FROM %s WHERE entity_key = %s",
    quote_ident(database .. "_stream_inst"),
    quote_literal(entity_key)))
  if not ok then return nil, ierr end

  return #rows
end

---------------------------------------------------------------------------
-- TTL janitor
---------------------------------------------------------------------------

--- Reap instances whose class set retention_idle and whose last_write_at
--- exceeds it. Safety net for crashed containers; do NOT rely on this as
--- the primary decommission path.
function M.stream_reap_idle(conn, database)
  local rows, err = query_all(conn, string.format([[
    SELECT i.path::text AS path, c.messages_table
      FROM %s i JOIN %s c ON c.namespace = i.class_namespace
     WHERE c.retention_idle IS NOT NULL
       AND i.last_write_at < NOW() - c.retention_idle
  ]], quote_ident(database .. "_stream_inst"),
      quote_ident(database .. "_stream_class")))
  if err then return nil, err end

  for _, r in ipairs(rows) do
    local ok, mderr = exec(conn, string.format(
      "DELETE FROM %s WHERE stream_path = %s::ltree",
      quote_ident(r.messages_table), quote_literal(r.path)))
    if not ok then return nil, "reap " .. r.path .. ": " .. tostring(mderr) end
  end

  local ok, ierr = exec(conn, string.format([[
    DELETE FROM %s i
     USING %s c
     WHERE c.namespace = i.class_namespace
       AND c.retention_idle IS NOT NULL
       AND i.last_write_at < NOW() - c.retention_idle
  ]], quote_ident(database .. "_stream_inst"),
      quote_ident(database .. "_stream_class")))
  if not ok then return nil, ierr end

  return #rows
end

return M
