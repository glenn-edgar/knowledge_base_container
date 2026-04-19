-- =============================================================================
-- kb_container_registry.lua -- Runtime library for the container registry.
--
-- The registry is the pg-backed mapping from (site, cpu, container_name)
-- -> (host, image, ports[], description, category, registered_at) that
-- the gateway polls to route /ui/<container>/<slot>/ URLs to the right
-- upstream host:port. node_control writes rows on spawn, deletes them on
-- teardown. Apps do NOT self-register.
--
-- Storage model (mirrors SYS_EXCEPTION but fully dynamic):
--   * Schema row in `knowledge_base` at path
--     `system.site.<S>.cpu.<cpu>.CONTAINER_REGISTRY.<name>`:
--       knowledge_base = "system"
--       label          = "CONTAINER_REGISTRY"
--       name           = <container_name>
--       properties     = { cpu_id, definition, category }
--       data           = {}                -- always empty; dynamic data
--                                             lives in the status row.
--   * Runtime row in `knowledge_base_status` at the same path:
--       data = { host, image, ports = [...], description, registered_at }
--
--   ports is a JSON array of records:
--     { slot, internal, external, protocol, purpose, description }
--
-- Gateway polls with:
--   SELECT k.path, k.name, k.properties, s.data
--   FROM knowledge_base k
--   JOIN knowledge_base_status s ON s.path = k.path
--   WHERE k.label = 'CONTAINER_REGISTRY'
--
-- All operations take a DBI conn (autocommit on); all return (ok, err)
-- for writes or (result, err) for reads.
-- =============================================================================

local dkjson = require("dkjson")

local M = {}

M.LABEL = "CONTAINER_REGISTRY"
M.KB    = "system"   -- which KB namespace the rows live in

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function escape_sql(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

local function fetch_all(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local row = sth:fetch(true)
    if not row then break end
    local copy = {}
    for k, v in pairs(row) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  sth:close()
  return rows
end

local function decode_json(s)
  if s == nil or s == "" then return {} end
  if type(s) ~= "string" then return s end
  local t, err = dkjson.decode(s)
  if not t then return nil, "decode: " .. tostring(err) end
  return t
end

-- Unix epoch seconds. ptime.now_sec is CLOCK_MONOTONIC (uptime), which
-- is useless for a gateway-displayable "registered at" field -- use
-- wall-clock so `date -d @<ts>` works and entries are comparable across
-- reboots.
local function now_sec() return os.time() end

M.decode_json = decode_json

---------------------------------------------------------------------------
-- Path builder. Keeps the convention in ONE place; node_control and any
-- gateway-side pg-reader should use this rather than hand-roll strings.
---------------------------------------------------------------------------

function M.path(site, cpu_id, container_name)
  if type(site) ~= "string" or site == "" then
    error("M.path: site required")
  end
  if type(cpu_id) ~= "string" or cpu_id == "" then
    error("M.path: cpu_id required")
  end
  if type(container_name) ~= "string" or container_name == "" then
    error("M.path: container_name required")
  end
  return string.format("system.site.%s.cpu.%s.%s.%s",
                       site, cpu_id, M.LABEL, container_name)
end

---------------------------------------------------------------------------
-- register(conn, site, cpu_id, name, properties, data) -- idempotent upsert.
--
-- properties: { definition = "test_app", category = "application" }
--             cpu_id is auto-added; caller shouldn't duplicate it.
-- data:       { host = "cpu_01", image = "...", ports = [...],
--               description = "...", registered_at = <unix_sec> }
--             registered_at is filled if caller omits it.
--
-- Both rows are UPSERTed so a re-register (restart) refreshes cleanly
-- without a DELETE+INSERT race with the gateway poller.
---------------------------------------------------------------------------

function M.register(conn, site, cpu_id, name, properties, data)
  local path = M.path(site, cpu_id, name)
  local props = {}
  for k, v in pairs(properties or {}) do props[k] = v end
  props.cpu_id = cpu_id                       -- authoritative, not caller-supplied

  local row = {}
  for k, v in pairs(data or {}) do row[k] = v end
  row.registered_at = row.registered_at or now_sec()

  local schema_sql = string.format(
    "INSERT INTO knowledge_base (knowledge_base, label, name, properties, data, path) " ..
    "VALUES ('%s', '%s', '%s', '%s'::json, '{}'::json, '%s'::ltree) " ..
    "ON CONFLICT (path) DO UPDATE SET " ..
    "  knowledge_base = EXCLUDED.knowledge_base, " ..
    "  label          = EXCLUDED.label, " ..
    "  name           = EXCLUDED.name, " ..
    "  properties     = EXCLUDED.properties",
    escape_sql(M.KB), escape_sql(M.LABEL), escape_sql(name),
    escape_sql(dkjson.encode(props)), escape_sql(path))
  local ok, err = exec(conn, schema_sql)
  if not ok then return nil, "schema insert: " .. tostring(err) end

  local status_sql = string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path), escape_sql(dkjson.encode(row)))
  ok, err = exec(conn, status_sql)
  if not ok then return nil, "status insert: " .. tostring(err) end

  return true
end

---------------------------------------------------------------------------
-- deregister(conn, site, cpu_id, name) -- delete both rows. No-op if absent.
---------------------------------------------------------------------------

function M.deregister(conn, site, cpu_id, name)
  local path = M.path(site, cpu_id, name)
  local ok, err = exec(conn, string.format(
    "DELETE FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(path)))
  if not ok then return nil, "status delete: " .. tostring(err) end

  ok, err = exec(conn, string.format(
    "DELETE FROM knowledge_base WHERE path = '%s'::ltree",
    escape_sql(path)))
  if not ok then return nil, "schema delete: " .. tostring(err) end

  return true
end

---------------------------------------------------------------------------
-- list_by_cpu(conn, site, cpu_id) -- every registered container on this CPU.
-- Returns array of { path, name, properties, data } records with the JSON
-- columns decoded to Lua tables. Used by node_control's RECONCILE pass.
---------------------------------------------------------------------------

function M.list_by_cpu(conn, site, cpu_id)
  local prefix = string.format("system.site.%s.cpu.%s.%s",
                               escape_sql(site), escape_sql(cpu_id), M.LABEL)
  local rows, err = fetch_all(conn, string.format(
    "SELECT k.path, k.name, k.properties AS properties, s.data AS data " ..
    "FROM knowledge_base k " ..
    "LEFT JOIN knowledge_base_status s ON s.path = k.path " ..
    "WHERE k.label = '%s' AND k.path <@ '%s'::ltree",
    escape_sql(M.LABEL), prefix))
  if not rows then return nil, err end

  local out = {}
  for _, r in ipairs(rows) do
    local props, perr = decode_json(r.properties)
    if not props then return nil, "row " .. tostring(r.path) .. ": " .. perr end
    local data, derr = decode_json(r.data)
    if not data then return nil, "row " .. tostring(r.path) .. ": " .. derr end
    out[#out + 1] = {
      path       = tostring(r.path),
      name       = tostring(r.name),
      properties = props,
      data       = data,
    }
  end
  return out
end

---------------------------------------------------------------------------
-- list_all(conn) -- every registered container site-wide. What the
-- gateway reads on each 2s poll cycle.
---------------------------------------------------------------------------

function M.list_all(conn)
  local rows, err = fetch_all(conn, string.format(
    "SELECT k.path, k.name, k.properties AS properties, s.data AS data " ..
    "FROM knowledge_base k " ..
    "LEFT JOIN knowledge_base_status s ON s.path = k.path " ..
    "WHERE k.label = '%s'", escape_sql(M.LABEL)))
  if not rows then return nil, err end

  local out = {}
  for _, r in ipairs(rows) do
    local props = decode_json(r.properties) or {}
    local data  = decode_json(r.data)       or {}
    out[#out + 1] = {
      path       = tostring(r.path),
      name       = tostring(r.name),
      properties = props,
      data       = data,
    }
  end
  return out
end

---------------------------------------------------------------------------
-- reconcile(conn, site, cpu_id, expected_names_set) -- garbage-collect
-- stale registrations. expected_names_set is a table keyed by container
-- name with any truthy value. Returns count of rows deleted, or nil+err.
--
-- Typical call shape from node_control at boot / after scan:
--   local expected = {}
--   for _, c in ipairs(docker_ps_output) do expected[c.name] = true end
--   kbcr.reconcile(conn, site, cpu_id, expected)
---------------------------------------------------------------------------

function M.reconcile(conn, site, cpu_id, expected_names_set)
  local existing, err = M.list_by_cpu(conn, site, cpu_id)
  if not existing then return nil, err end

  local deleted = 0
  for _, r in ipairs(existing) do
    if not expected_names_set[r.name] then
      local ok, derr = M.deregister(conn, site, cpu_id, r.name)
      if not ok then return nil, "reconcile delete " .. r.name .. ": " .. derr end
      deleted = deleted + 1
    end
  end
  return deleted
end

return M
