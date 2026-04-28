--[[
  kb_sync_queue.lua

  Runtime client for the per-queue sync message log (schema in
  construct_sync_queue.lua).

  Purpose: pg-backed message transport for inter-CPU sync verbs in
  Phase 6 (HEARTBEAT, JOIN_REQ, JOIN_ACK, etc.). NOT for async work-job
  RPC -- that uses kb_rpc_*.

  API:
    push(conn, db, queue_name, verb, payload_table)  -> seq, err
    drain(conn, db, queue_name, max_n)               -> rows, err
    peek(conn, db, queue_name, max_n)                -> rows, err   (no delete)
    count(conn, db, queue_name)                      -> n, err
    purge(conn, db, queue_name)                      -> n, err

  Each row in returned `rows`: { seq, verb, payload (decoded table), inserted_at }.

  Constraints (per feedback_phase6_handler_budget):
    - No SERIALIZABLE, no advisory lock, no retries-with-sleep.
    - Single statement per call. push ~= 1-3ms. drain ~= 2-5ms.
    - Caller is responsible for handler budget; this module does not
      enforce a deadline.

  Design memory: project_kb_sync_queue.md
]]

local dkjson = require("dkjson")

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
-- Validation
---------------------------------------------------------------------------

local function valid_qname(s)
  return type(s) == "string" and s:match("^[a-z][a-z0-9_]*$") ~= nil
end

local function table_for(database, qname)
  return database .. "_sync_msg__" .. qname
end

local function decode_row(r)
  if r.payload and type(r.payload) == "string" then
    r.payload = dkjson.decode(r.payload) or {}
  elseif r.payload == nil then
    r.payload = {}
  end
  if r.seq then r.seq = tonumber(r.seq) end
  return r
end

---------------------------------------------------------------------------
-- push
---------------------------------------------------------------------------

--- Append one verb message to a queue. Single INSERT.
--- @param conn       DBI connection
--- @param database   KB database name
--- @param queue_name target queue (matches [a-z][a-z0-9_]*)
--- @param verb       string verb name (e.g. "JOIN_REQ", "HEARTBEAT")
--- @param payload    table; encoded as JSONB. nil -> {}
--- @return seq, err
function M.push(conn, database, queue_name, verb, payload)
  if not valid_qname(queue_name) then
    return nil, "invalid queue_name: " .. tostring(queue_name)
  end
  if type(verb) ~= "string" or verb == "" then
    return nil, "verb must be non-empty string"
  end
  local payload_json = dkjson.encode(payload or {})
  local sql = string.format([[
    INSERT INTO %s (verb, payload) VALUES (%s, %s::jsonb)
    RETURNING seq
  ]], quote_ident(table_for(database, queue_name)),
      quote_literal(verb),
      quote_literal(payload_json))
  local row, err = query_one(conn, sql)
  if err then return nil, err end
  return tonumber(row.seq)
end

---------------------------------------------------------------------------
-- drain
---------------------------------------------------------------------------

--- Atomically claim and remove up to max_n oldest messages.
--- Single DELETE...USING (SELECT...FOR UPDATE SKIP LOCKED) statement.
--- @return list of rows (oldest-first), or nil + err
function M.drain(conn, database, queue_name, max_n)
  if not valid_qname(queue_name) then
    return nil, "invalid queue_name: " .. tostring(queue_name)
  end
  max_n = max_n or 5
  local tn = quote_ident(table_for(database, queue_name))
  local sql = string.format([[
    WITH next_msgs AS (
      SELECT seq FROM %s
       ORDER BY seq ASC
       LIMIT %d
       FOR UPDATE SKIP LOCKED
    )
    DELETE FROM %s t
     USING next_msgs n
     WHERE t.seq = n.seq
    RETURNING t.seq, t.verb, t.payload::text AS payload, t.inserted_at
  ]], tn, max_n, tn)
  local rows, err = query_all(conn, sql)
  if err then return nil, err end
  -- DELETE...RETURNING returns rows in arbitrary order; sort by seq.
  table.sort(rows, function(a, b) return tonumber(a.seq) < tonumber(b.seq) end)
  for _, r in ipairs(rows) do decode_row(r) end
  return rows
end

---------------------------------------------------------------------------
-- peek (read without delete; for observability + tests)
---------------------------------------------------------------------------

function M.peek(conn, database, queue_name, max_n)
  if not valid_qname(queue_name) then
    return nil, "invalid queue_name: " .. tostring(queue_name)
  end
  max_n = max_n or 5
  local sql = string.format([[
    SELECT seq, verb, payload::text AS payload, inserted_at
      FROM %s
     ORDER BY seq ASC
     LIMIT %d
  ]], quote_ident(table_for(database, queue_name)), max_n)
  local rows, err = query_all(conn, sql)
  if err then return nil, err end
  for _, r in ipairs(rows) do decode_row(r) end
  return rows
end

---------------------------------------------------------------------------
-- count + purge (for observability + tests)
---------------------------------------------------------------------------

function M.count(conn, database, queue_name)
  if not valid_qname(queue_name) then
    return nil, "invalid queue_name: " .. tostring(queue_name)
  end
  local row, err = query_one(conn, string.format(
    "SELECT COUNT(*)::bigint AS n FROM %s",
    quote_ident(table_for(database, queue_name))))
  if err then return nil, err end
  return tonumber(row.n)
end

function M.purge(conn, database, queue_name)
  if not valid_qname(queue_name) then
    return nil, "invalid queue_name: " .. tostring(queue_name)
  end
  local row, err = query_one(conn, string.format([[
    WITH d AS (DELETE FROM %s RETURNING 1)
    SELECT COUNT(*)::int AS n FROM d
  ]], quote_ident(table_for(database, queue_name))))
  if err then return nil, err end
  return tonumber(row.n) or 0
end

return M
