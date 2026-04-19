-- =============================================================================
-- kb_assignments.lua -- Query helper for node_control.
--
-- Lists every container on a given CPU whose `properties.managed_by` is
-- "node_control" (i.e. application kind), joined with its `service.main`
-- info_node data. This is how node_control learns what docker runs to
-- issue at boot and what to stop at teardown.
--
-- Returned shape (one per assignment):
--   { name        = "test_app_01",
--     definition  = "test_app",
--     managed_by  = "node_control",
--     service     = {
--       host  = "test_app_01",           -- docker DNS name
--       ports = { {slot, internal, external, protocol, purpose, description}, ... },
--       cfg   = { ... default_cfg ... },
--     },
--   }
--
-- Build spec (image, env_defaults, env_required, volumes, labels,
-- restart_policy, etc.) is NOT in the returned struct -- callers look it
-- up in ctx.system_control_globals.build_specs[def_name]. Combining the
-- two gives a full docker-run spec.
-- =============================================================================

local dkjson = require("dkjson")

local M = {}

local function escape_sql(s) return tostring(s):gsub("'", "''") end

local function fetch_all(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local row = sth:fetch(true); if not row then break end
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
  return dkjson.decode(s) or {}
end

---------------------------------------------------------------------------
-- list_node_managed(conn, site, cpu_id)
--
-- Single JOIN that pulls the container header (for properties) plus the
-- service.main info_node (for resolved ports/host). The `<@` ltree test
-- scopes to descendants of `.container`, and `nlevel = nlevel+1` limits
-- to direct children (so we don't accidentally pick up
-- container.<inst>.service.main etc. as "container" rows).
---------------------------------------------------------------------------

function M.list_node_managed(conn, site, cpu_id)
  local prefix = string.format("system.site.%s.cpu.%s.container",
                               escape_sql(site), escape_sql(cpu_id))
  local sql = string.format([[
    SELECT
      c.name       AS name,
      c.properties AS props,
      s.data       AS service_data
    FROM knowledge_base c
    LEFT JOIN knowledge_base s
      ON s.path  = c.path || 'service.main'::ltree
     AND s.label = 'service'
    WHERE c.knowledge_base = 'system'
      AND c.label          = 'container'
      AND c.path <@ '%s'::ltree
      AND nlevel(c.path) = nlevel('%s'::ltree) + 1
  ]], prefix, prefix)

  local rows, err = fetch_all(conn, sql)
  if not rows then return nil, err end

  local out = {}
  for _, r in ipairs(rows) do
    local props   = decode_json(r.props)
    local service = decode_json(r.service_data)
    if props.managed_by == "node_control" then
      out[#out + 1] = {
        name       = tostring(r.name),
        definition = props.definition,
        managed_by = props.managed_by,
        service    = service,   -- { host, ports = [...], cfg }
      }
    end
  end
  return out
end

return M
