-- =============================================================================
-- kb_stream.lua -- Direct SQL stream-table push for host process.
--
-- Mirrors KB_Stream:push_stream_data but skips the KB_Search dependency
-- so it can use ctx.connectors.pg directly. Standard circular-buffer
-- semantics: overwrite the oldest row (smallest recorded_at) at the
-- given path with the new payload + NOW().
--
-- The slots must be pre-allocated by the construct script via
-- add_stream_field(name, length, ...). Push errors loudly if no slots
-- exist for the path (means the schema add was forgotten).
-- =============================================================================

local dkjson = require("dkjson")

local M = {}

local function escape(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

local function query_one(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local row = sth:fetch(true)
  sth:close()
  return row
end

local function query_all(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local r = sth:fetch(true)
    if not r then break end
    rows[#rows + 1] = r
  end
  sth:close()
  return rows
end

---------------------------------------------------------------------------
-- push(conn, path, data) -> ok, err
---------------------------------------------------------------------------
function M.push(conn, path, data)
  if type(data) ~= "table" then return nil, "data must be a table" end
  local json = dkjson.encode(data)

  local oldest, err = query_one(conn, string.format([[
    SELECT id FROM knowledge_base_stream
     WHERE path = '%s'::ltree
     ORDER BY recorded_at ASC
     LIMIT 1
  ]], escape(path)))
  if err then return nil, err end
  if not oldest then
    return nil, string.format("no slots pre-allocated for path '%s'", path)
  end

  return exec(conn, string.format([[
    UPDATE knowledge_base_stream
       SET data = '%s'::jsonb,
           recorded_at = NOW(),
           valid = TRUE
     WHERE id = %s
  ]], escape(json), tostring(oldest.id)))
end

---------------------------------------------------------------------------
-- read_recent(conn, path, limit) -> array of {recorded_at, data}, err
-- Newest-first. Used by trend computation to read its own history.
---------------------------------------------------------------------------
function M.read_recent(conn, path, limit)
  limit = limit or 360
  local rows, err = query_all(conn, string.format([[
    SELECT recorded_at, data
      FROM knowledge_base_stream
     WHERE path = '%s'::ltree AND valid = TRUE
     ORDER BY recorded_at DESC
     LIMIT %d
  ]], escape(path), limit))
  if err then return nil, err end
  local out = {}
  for _, r in ipairs(rows or {}) do
    local d = r.data
    if type(d) == "string" then d = dkjson.decode(d) end
    out[#out + 1] = { recorded_at = r.recorded_at, data = d }
  end
  return out
end

return M
