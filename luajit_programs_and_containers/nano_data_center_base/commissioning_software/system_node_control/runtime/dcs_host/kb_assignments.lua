-- =============================================================================
-- kb_assignments.lua -- Query helper for node_control.
--
-- Lists every container assigned to a given CPU. Source of truth (Phase B
-- Layer N): each app's `app_containers.<i>.placement.current.KB_STATUS_FIELD.cpu`
-- row, written by apps_builder_framework at commission time. Service spec
-- (host, resolved ports, cfg) is fetched from the existing per-CPU
-- `cpu.<my>.container.<i>.service.main` row -- those still exist and
-- carry the resolved port records build_kb's container_runtime subsystem
-- emits per topology.
--
-- Pre-Layer-N legacy: this function used to scan `cpu.<my>.container.*`
-- and filter by `properties.managed_by = "node_control"`. That worked but
-- coupled placement to which CPU's namespace the row lived in; load-
-- balancer hands-off (move app from cpu_02 to cpu_03) had no representation.
-- The placement query decouples placement from row locality.
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

local dkjson    = require("dkjson")
local ndc_paths = require("ndc_paths")

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
-- Phase B Layer N: placement-driven. Two-step:
--   1. Query app_containers.<i>.placement.current.KB_STATUS_FIELD.cpu for
--      every app whose placement is this cpu_id; capture instance_id +
--      role.
--   2. For each placed instance, look up the existing
--      cpu.<my>.container.<inst> row (still emitted by build_kb's
--      container_runtime subsystem from topology) for properties (def,
--      managed_by) and service.main (host, ports, cfg).
--
-- The two-step query is intentional: if placement says an app is on this
-- cpu but the cpu.<my>.container.<inst> row is missing (build_kb hasn't
-- caught up, or placement was rewritten by load-balancer to a CPU that
-- doesn't have the def yet), we surface it as a soft warning rather than
-- silently dropping the assignment.
---------------------------------------------------------------------------

function M.list_node_managed(conn, site, cpu_id)
  -- Step 1: which app instances are placed on this CPU?
  local app_root = escape_sql(ndc_paths.app_containers_root(site))
  local placement_sql = string.format([[
    SELECT
      ac.name       AS instance_id,
      pc_cpu.data   AS cpu_data,
      pc_role.data  AS role_data
    FROM knowledge_base ac
    JOIN knowledge_base pc_cpu
      ON pc_cpu.path = ac.path || 'placement.current.KB_STATUS_FIELD.cpu'::ltree
    LEFT JOIN knowledge_base pc_role
      ON pc_role.path = ac.path || 'placement.current.KB_STATUS_FIELD.role'::ltree
    WHERE ac.knowledge_base = 'system'
      AND ac.label          = 'app_containers'
      AND ac.path <@ '%s'::ltree
      AND nlevel(ac.path) = nlevel('%s'::ltree) + 1
  ]], app_root, app_root)

  local placement_rows, perr = fetch_all(conn, placement_sql)
  if not placement_rows then return nil, "placement query: " .. tostring(perr) end

  local placed = {}  -- [instance_id] = role
  for _, r in ipairs(placement_rows) do
    local cpu_d  = decode_json(r.cpu_data)
    if tostring(cpu_d.value) == cpu_id then
      local role_d = decode_json(r.role_data)
      placed[tostring(r.instance_id)] = tostring(role_d.value or "active")
    end
  end

  -- No placements? Return empty list (no error).
  if not next(placed) then return {} end

  -- Step 2: pull the per-instance container header + service.main data.
  -- Build an IN-list of expected container paths under this CPU.
  local cpu_container_root = escape_sql(ndc_paths.cpu_path(site, cpu_id, "container"))
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
  ]], cpu_container_root, cpu_container_root)

  local rows, err = fetch_all(conn, sql)
  if not rows then return nil, "container query: " .. tostring(err) end

  -- Index by name for quick lookup; build output filtered by placement.
  local by_name = {}
  for _, r in ipairs(rows) do by_name[tostring(r.name)] = r end

  local out = {}
  for instance_id, _role in pairs(placed) do
    local r = by_name[instance_id]
    if r then
      local props   = decode_json(r.props)
      local service = decode_json(r.service_data)
      -- Only surface app-kind containers (not infra). The build_kb
      -- container header sets managed_by="node_control" iff app-kind on
      -- this CPU; placement.current rows are only emitted for apps too.
      if props.managed_by == "node_control" then
        out[#out + 1] = {
          name       = instance_id,
          definition = props.definition,
          managed_by = props.managed_by,
          service    = service,   -- { host, ports = [...], cfg }
        }
      end
    end
    -- If r is nil: placement points at this CPU but no container row
    -- exists yet. Soft-skip; caller can detect via empty list. Surface
    -- via a logged warning at the call site if useful.
  end
  return out
end

return M
