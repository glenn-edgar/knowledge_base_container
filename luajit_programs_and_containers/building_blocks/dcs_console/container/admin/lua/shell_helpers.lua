-- shell_helpers.lua -- reusable pieces for admin view/sse modules.
--
-- * M.pg_connect(): pgmoon-over-cosocket connection using the env vars
--   injected by node_control. Resolves host.docker.internal to its IP
--   via /etc/hosts because openresty cosockets don't consult /etc/hosts
--   and Docker Desktop's 127.0.0.11 DNS is unreliable on bridge nets.
-- * M.set_context(ctx): writes HX-Trigger-After-Settle with shell:context.
-- * M.active_exception_count(pg): the number the alarm badge shows.
-- * HTML helpers: traffic-light pills, UTC <time> elements, empty-state
--   lines.
-- * M.escape(s): HTML-escape a string.

local pgmoon = require("pgmoon")
local cjson  = require("cjson.safe")

local M = {}

------------------------------------------------------------------------
-- pg connect
------------------------------------------------------------------------

local function resolve_from_hosts(name)
  local f = io.open("/etc/hosts", "r")
  if not f then return nil end
  for line in f:lines() do
    local stripped = line:gsub("#.*$", "")
    local ip, rest = stripped:match("^%s*([%da-fA-F:%.]+)%s+(.*)$")
    if ip and rest and not ip:find(":") then           -- ipv4 only
      for tok in rest:gmatch("%S+") do
        if tok == name then f:close() return ip end
      end
    end
  end
  f:close()
  return nil
end

local CACHED_HOST_IP

local function host_ip()
  if CACHED_HOST_IP then return CACHED_HOST_IP end
  local h = os.getenv("PG_HOST") or "host.docker.internal"
  if h:match("^%d+%.%d+%.%d+%.%d+$") then
    CACHED_HOST_IP = h
    return h
  end
  local ip = resolve_from_hosts(h)
  if not ip then
    error("shell_helpers: cannot resolve PG_HOST='" .. h ..
          "' from /etc/hosts (container missing --add-host?)")
  end
  CACHED_HOST_IP = ip
  return ip
end

function M.pg_connect()
  local pg = pgmoon.new({
    host        = host_ip(),
    port        = tonumber(os.getenv("PG_PORT") or "5432"),
    database    = os.getenv("PG_DB")       or "knowledge_base",
    user        = os.getenv("PG_USER")     or "gedgar",
    password    = os.getenv("PG_PASSWORD") or "",
    socket_type = "nginx",
  })
  local ok, err = pg:connect()
  if not ok then return nil, "pg connect: " .. tostring(err) end
  return pg
end

------------------------------------------------------------------------
-- shell:context header
------------------------------------------------------------------------

function M.set_context(ctx)
  ngx.header["HX-Trigger-After-Settle"] =
    cjson.encode({ ["shell:context"] = ctx })
end

------------------------------------------------------------------------
-- SQL helpers
------------------------------------------------------------------------

-- Count of active unacknowledged SYS_EXCEPTION rows site-wide. Drives
-- the alarm badge on every view. Returns 0 on any error (badge stays
-- hidden rather than flashing a misleading value).
function M.active_exception_count(pg)
  local rs, err = pg:query([[
    SELECT COUNT(*) AS n
    FROM knowledge_base_status s
    JOIN knowledge_base        k ON k.path = s.path
    WHERE k.label = 'SYS_EXCEPTION'
      AND (s.data->>'status')::bool = true
      AND COALESCE((s.data->>'acknowledged')::bool, false) = false
  ]])
  if not rs or not rs[1] then return 0 end
  return tonumber(rs[1].n) or 0
end

local SITE
local function site()
  if not SITE then
    SITE = os.getenv("APP_SITE") or "moonbase.alpha.dcs"
  end
  return SITE
end

-- Site-level status field: system_ready, cluster_go, etc. Returns
-- the numeric `value` from the JSON column (runtime row in
-- knowledge_base_status); falls back to the schema default if the
-- runtime row is empty (first boot). Returns nil on any error.
function M.site_status_value(pg, name)
  local path = string.format("system.site.%s.KB_STATUS_FIELD.%s",
                             site(), name)
  local rs, err = pg:query(string.format([[
    SELECT COALESCE(
      NULLIF(s.data->>'value', ''),
      k.data->>'value'
    ) AS v
    FROM knowledge_base        k
    LEFT JOIN knowledge_base_status s ON s.path = k.path
    WHERE k.path = '%s'::ltree
  ]], path:gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  return tonumber(rs[1].v)
end

-- Read a site-level bit_mask_table value (ready_bits, cluster_sync_bits).
function M.site_bit_mask(pg, name)
  local node_id = (string.format("system.site.%s.KB_BIT_MASK.%s",
                                 site(), name)):gsub("%.", "_"):lower()
  local rs, err = pg:query(string.format(
    "SELECT bit_mask FROM bit_mask_table WHERE node_id = '%s'",
    node_id:gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  return tonumber(rs[1].bit_mask)
end

-- Count of CPUs in the topology (used to build the expected mask).
-- Derived from any bootstrap.config that names expected_cpu_count --
-- which every CPU's bootstrap shares. Falls back to COUNT(DISTINCT cpu)
-- from the container hierarchy if bootstrap rows aren't addressable.
function M.expected_cpu_count(pg)
  -- bootstrap.config lives under cpu.<id>.bootstrap.config and its data
  -- column carries expected_cpu_count. Pull any one.
  local rs, err = pg:query([[
    SELECT (data->>'expected_cpu_count')::int AS n
    FROM knowledge_base
    WHERE label = 'bootstrap' AND name = 'config'
    LIMIT 1
  ]])
  if rs and rs[1] and rs[1].n then return tonumber(rs[1].n) end
  return nil
end

-- List every CPU in the topology, ordered by bit_index. Each row:
--   { cpu_id="cpu_01", hostname="localhost", role="master",
--     is_master=true, bit_index=0 }
-- Pulled from the cpu header rows construct_dcs_kb plants with
-- `kb:add_header_node("cpu", cpu_id, cpu_props, ...)`; properties
-- carries hostname + role + bit_index + is_master.
function M.list_cpus(pg)
  local rs, err = pg:query([[
    SELECT name AS cpu_id,
           properties->>'hostname' AS hostname,
           properties->>'role'     AS role,
           (properties->>'is_master')::int AS is_master,
           (properties->>'bit_index')::int AS bit_index
    FROM knowledge_base
    WHERE label = 'cpu'
    ORDER BY (properties->>'bit_index')::int
  ]])
  if not rs then return nil, err end
  local out = {}
  for _, r in ipairs(rs) do
    out[#out + 1] = {
      cpu_id    = r.cpu_id,
      hostname  = r.hostname,
      role      = r.role,
      is_master = (tonumber(r.is_master) == 1),
      bit_index = tonumber(r.bit_index) or 0,
    }
  end
  return out
end

-- List every registered container with its assigned CPU + image +
-- description. Null-safe on the status-row side (freshly-constructed
-- KB has the schema row but an empty status.data until REGISTER writes).
function M.list_containers(pg)
  local rs, err = pg:query([[
    SELECT k.name,
           k.properties->>'cpu_id'      AS cpu_id,
           k.properties->>'definition'  AS definition,
           s.data->>'image'             AS image,
           s.data->>'description'       AS description,
           s.data->>'registered_at'     AS registered_at
    FROM knowledge_base k
    LEFT JOIN knowledge_base_status s ON s.path = k.path
    WHERE k.label = 'CONTAINER_REGISTRY'
    ORDER BY k.properties->>'cpu_id', k.name
  ]])
  if not rs then return nil, err end
  local out = {}
  for _, r in ipairs(rs) do
    out[#out + 1] = {
      name          = r.name,
      cpu_id        = r.cpu_id,
      definition    = r.definition,
      image         = r.image,
      description   = r.description,
      registered_at = tonumber(r.registered_at),
    }
  end
  return out
end

-- One CPU's full record (as returned by list_cpus) or nil if unknown.
-- Convenience for views that only need a single CPU's metadata.
function M.get_cpu(pg, cpu_id)
  local rs, err = pg:query(string.format([[
    SELECT name AS cpu_id,
           properties->>'hostname' AS hostname,
           properties->>'role'     AS role,
           (properties->>'is_master')::int AS is_master,
           (properties->>'bit_index')::int AS bit_index
    FROM knowledge_base
    WHERE label = 'cpu' AND name = '%s'
    LIMIT 1
  ]], (cpu_id or ""):gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  local r = rs[1]
  return {
    cpu_id    = r.cpu_id,
    hostname  = r.hostname,
    role      = r.role,
    is_master = (tonumber(r.is_master) == 1),
    bit_index = tonumber(r.bit_index) or 0,
  }
end

-- Read a CPU's heartbeat timestamp (epoch seconds) from the
-- bit_mask_table. Returns nil if the CPU hasn't written yet (bit_mask=0).
function M.read_cpu_heartbeat_epoch(pg, cpu_id)
  local node_id = (string.format(
    "system.site.%s.cpu.%s.KB_BIT_MASK.heartbeat", site(), cpu_id))
    :gsub("%.", "_"):lower()
  local rs, err = pg:query(string.format(
    "SELECT bit_mask FROM bit_mask_table WHERE node_id = '%s'",
    node_id:gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  local ns = tonumber(rs[1].bit_mask)
  if not ns or ns == 0 then return nil end
  return math.floor(ns / 1000000000)
end

-- Containers assigned to a specific CPU, drawn from CONTAINER_REGISTRY.
-- Used by the Assignments leaf and the Summary count.
function M.containers_on(pg, cpu_id)
  local list, err = M.list_containers(pg)
  if not list then return nil, err end
  local out = {}
  for _, c in ipairs(list) do
    if c.cpu_id == cpu_id then out[#out + 1] = c end
  end
  return out
end

-- Full details of one CONTAINER_REGISTRY row -- schema-row properties
-- (cpu_id, definition, category) + status-row data (host, image,
-- ports[], description, registered_at). Ports come back as a Lua
-- table of records, each with slot/external/internal/protocol/
-- purpose/description. Returns nil if no such container.
function M.get_container(pg, name)
  if not name or name == "" then return nil end
  local rs, err = pg:query(string.format([[
    SELECT k.name,
           k.properties AS properties,
           s.data       AS data
    FROM knowledge_base k
    LEFT JOIN knowledge_base_status s ON s.path = k.path
    WHERE k.label = 'CONTAINER_REGISTRY' AND k.name = '%s'
    LIMIT 1
  ]], name:gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  local r = rs[1]
  local props = r.properties
  if type(props) == "string" then props = cjson.decode(props) or {} end
  local data = r.data
  if type(data) == "string" then data = cjson.decode(data) or {} end
  return {
    name          = r.name,
    cpu_id        = props and props.cpu_id,
    definition    = props and props.definition,
    category      = props and props.category,
    host          = data  and data.host,
    image         = data  and data.image,
    description   = data  and data.description,
    registered_at = data  and tonumber(data.registered_at),
    ports         = data  and data.ports or {},
  }
end

-- Read a CPU's operational flag (written by node_control once its
-- assigned containers are healthy). Path:
--   system.site.<S>.cpu.<id>.container.node_control...  -- NO, we use
-- the process_globals-derived KB_STATUS_FIELD under cpu.<id> if
-- planted. For v1 we just infer operational from:
--   heartbeat fresh AND ready_bit set.
-- Returned value lets the Summary view render a "Operational" pill.
function M.cpu_is_operational(ready_bits, bit_index, hb_epoch, hb_stale_s)
  if not ready_bits or not bit_index then return nil end
  local mask_bit = math.floor(ready_bits / (2 ^ bit_index)) % 2
  if mask_bit ~= 1 then return false end
  if hb_epoch and hb_stale_s then
    local age = os.time() - hb_epoch
    if age > hb_stale_s then return false end
  end
  return true
end

------------------------------------------------------------------------
-- HTML helpers
------------------------------------------------------------------------

function M.escape(s)
  if s == nil then return "" end
  return (tostring(s)
    :gsub("&", "&amp;")
    :gsub("<", "&lt;")
    :gsub(">", "&gt;")
    :gsub('"', "&quot;")
    :gsub("'", "&#39;"))
end

-- Traffic-light pill. kind ∈ {"ok", "warn", "fail", "unknown"}. The
-- CSS in shell.css styles these; the icon is rendered via ::before.
function M.pill(kind, text)
  return string.format(
    '<span class="status %s"><span class="icon"></span>%s</span>',
    M.escape(kind), M.escape(text))
end

-- <time> element that the client-side stale-ticker decorates. Pass a
-- Unix epoch seconds number; we render ISO-8601 UTC into datetime=.
function M.time_el(epoch_seconds, stale_after_seconds)
  if not epoch_seconds or epoch_seconds == 0 then
    return '<span class="empty">never</span>'
  end
  local utc = os.date("!%Y-%m-%dT%H:%M:%SZ", epoch_seconds)
  return string.format('<time datetime="%s" data-stale-after="%d"></time>',
    utc, stale_after_seconds or 30)
end

function M.now_utc_iso()
  return os.date("!%Y-%m-%dT%H:%M:%SZ")
end

------------------------------------------------------------------------
-- Exception listing + mutations (Phase 5)
------------------------------------------------------------------------

-- Full list of SYS_EXCEPTION rows in the requested state.
-- filter ∈ { "active", "acknowledged", "history" }.
--   active      : status=true  AND acknowledged≠true
--   acknowledged: status=true  AND acknowledged=true
--   history     : status=false  (i.e. cleared)
--
-- Returns array of:
--   { path, name, cpu_id, agent_instance, type, description,
--     ts, last_error, trace_b64,
--     acknowledged, ack_by, ack_at,
--     cleared_by, cleared_at, note }
function M.list_exceptions(pg, filter)
  local where
  if filter == "active" then
    where = "(s.data->>'status')::bool = true AND COALESCE((s.data->>'acknowledged')::bool, false) = false"
  elseif filter == "acknowledged" then
    where = "(s.data->>'status')::bool = true AND COALESCE((s.data->>'acknowledged')::bool, false) = true"
  else
    where = "COALESCE((s.data->>'status')::bool, false) = false"
  end
  local rs, err = pg:query(string.format([[
    SELECT k.path AS path, k.name AS name,
           k.properties AS props, s.data AS data
    FROM knowledge_base k
    JOIN knowledge_base_status s ON s.path = k.path
    WHERE k.label = 'SYS_EXCEPTION' AND %s
    ORDER BY COALESCE((s.data->>'ts')::bigint, 0) DESC
    LIMIT 200
  ]], where))
  if not rs then return nil, err end
  local out = {}
  for _, r in ipairs(rs) do
    local props = r.props;  if type(props) == "string" then props = cjson.decode(props) or {} end
    local data  = r.data;   if type(data)  == "string" then data  = cjson.decode(data)  or {} end
    local path_s = tostring(r.path or "")
    local cpu_id = path_s:match("%.cpu%.([^%.]+)%.SYS_EXCEPTION")
    out[#out + 1] = {
      path            = path_s,
      name            = r.name,
      cpu_id          = cpu_id,
      agent_instance  = props.instance,
      type            = props.type,
      description     = props.description,
      ts              = tonumber(data.ts),
      last_error      = data.last_error,
      trace_b64       = data.trace_b64,
      acknowledged    = (data.acknowledged == true or data.acknowledged == "true"),
      ack_by          = data.ack_by,
      ack_at          = tonumber(data.ack_at),
      cleared_by      = data.cleared_by,
      cleared_at      = tonumber(data.cleared_at),
      note            = data.note,
    }
  end
  return out
end

-- Lazy CREATE TABLE IF NOT EXISTS for the ops audit log. Memoised per
-- worker process so we only pay the round-trip once.
local _audit_log_ready

function M.ensure_audit_log_table(pg)
  if _audit_log_ready then return true end
  local ok, err = pg:query([[
    CREATE TABLE IF NOT EXISTS audit_log (
      id        SERIAL PRIMARY KEY,
      ts        TIMESTAMPTZ DEFAULT now(),
      operator  TEXT NOT NULL,
      action    TEXT NOT NULL,
      target    TEXT,
      note      TEXT,
      result    TEXT
    )
  ]])
  if not ok then return nil, err end
  _audit_log_ready = true
  return true
end

-- Append one audit row. Never throws; any pg error is logged but the
-- caller's mutation result is what matters to the operator.
function M.audit_log_append(pg, operator, action, target, note, result)
  if not _audit_log_ready then
    local ok = M.ensure_audit_log_table(pg)
    if not ok then return end
  end
  local function esc(s) return (tostring(s or "")):gsub("'", "''") end
  pg:query(string.format(
    "INSERT INTO audit_log (operator, action, target, note, result) " ..
    "VALUES ('%s', '%s', '%s', '%s', '%s')",
    esc(operator), esc(action), esc(target), esc(note), esc(result)))
end

-- Ack: mark status row acknowledged=true + ack_by + ack_at. Does NOT
-- touch the status field; the exception is still "live" (count stays
-- in ready_bits etc.), operator has just silenced the alarm.
--
-- knowledge_base_status.data is `json` (not `jsonb`), so we cast the
-- column to jsonb for jsonb_set then back to json for the UPDATE.
function M.ack_exception(pg, path, operator, note)
  local function esc(s) return (tostring(s or "")):gsub("'", "''") end
  local sql = string.format([[
    UPDATE knowledge_base_status
    SET data = (jsonb_set(
                  jsonb_set(
                    jsonb_set(
                      jsonb_set(data::jsonb, '{acknowledged}', 'true'::jsonb),
                      '{ack_by}', to_jsonb('%s'::text)),
                    '{ack_at}', to_jsonb(extract(epoch FROM now())::int)),
                  '{note}', to_jsonb('%s'::text)))::json
    WHERE path = '%s'::ltree
  ]], esc(operator), esc(note or ""), esc(path))
  local ok, err = pg:query(sql)
  return ok and true or nil, err
end

-- Clear: mark status=false + cleared_by + cleared_at. Also keeps
-- acknowledged=true for the history record (cleared implies acked).
function M.clear_exception(pg, path, operator, note)
  local function esc(s) return (tostring(s or "")):gsub("'", "''") end
  local sql = string.format([[
    UPDATE knowledge_base_status
    SET data = (jsonb_set(
                  jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        jsonb_set(data::jsonb, '{status}', 'false'::jsonb),
                        '{acknowledged}', 'true'::jsonb),
                      '{cleared_by}', to_jsonb('%s'::text)),
                    '{cleared_at}', to_jsonb(extract(epoch FROM now())::int)),
                  '{note}', to_jsonb('%s'::text)))::json
    WHERE path = '%s'::ltree
  ]], esc(operator), esc(note or ""), esc(path))
  local ok, err = pg:query(sql)
  return ok and true or nil, err
end

-- Build an ltree path under the current site prefix. Variadic args
-- become trailing labels. Empty/nil args are skipped so callers can
-- always pass in optional segments without branching.
--   kb_path()                           -> "system.site.<SITE>"
--   kb_path("cpu", "cpu_01")            -> "system.site.<SITE>.cpu.cpu_01"
--   kb_path("cpu", cpu_id, "CONTAINER_REGISTRY", name)
function M.kb_path(...)
  local parts = { "system", "site", site() }
  for _, seg in ipairs({ ... }) do
    if seg ~= nil and seg ~= "" then
      parts[#parts + 1] = tostring(seg)
    end
  end
  return table.concat(parts, ".")
end

-- Render a `<span class="kb-path">` wrapper. Convenience for view authors.
function M.kb_path_span(...)
  return string.format('<span class="kb-path">%s</span>',
                       M.escape(M.kb_path(...)))
end

return M
