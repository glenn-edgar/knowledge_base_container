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

local pgmoon    = require("pgmoon")
local cjson     = require("cjson.safe")
local ndc_paths = require("ndc_paths")

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
  local path = ndc_paths.site_status_field_path(site(), name)
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
  local node_id = (ndc_paths.site_path(site(), "KB_BIT_MASK." .. name))
                    :gsub("%.", "_"):lower()
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

-- List every PLACED application container (from the `container`
-- header rows construct_dcs_kb plants), not just registered ones.
-- CONTAINER_REGISTRY rows come and go with node_control's REGISTER/
-- DEREGISTER dance (Phase 7b maintenance takes a container out of
-- the registry while its header row stays). We want the tree to
-- show all placements so the operator can navigate to a paused
-- container and bring it back.
--
-- Result rows include a `registered` boolean derived from the left
-- join; UI can dim/annotate off-registry entries.
function M.list_containers(pg)
  local rs, err = pg:query([[
    SELECT k.name                              AS name,
           k.properties->>'definition'         AS definition,
           subpath(k.path, -3, 1)::text        AS cpu_id,
           rs.data->>'image'                   AS image,
           rs.data->>'description'             AS description,
           rs.data->>'registered_at'           AS registered_at,
           (kr.name IS NOT NULL)               AS registered
    FROM knowledge_base k
    LEFT JOIN knowledge_base        kr
           ON kr.label = 'CONTAINER_REGISTRY' AND kr.name = k.name
    LEFT JOIN knowledge_base_status rs ON rs.path = kr.path
    WHERE k.label = 'container'
      AND k.properties->>'kind' = 'application'
    ORDER BY subpath(k.path, -3, 1)::text, k.name
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
      registered    = (r.registered == true or r.registered == "t"),
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
  local node_id = ndc_paths.heartbeat_path(site(), cpu_id)
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
-- Maintenance lease (Phase 7b, X7)
------------------------------------------------------------------------

local function maintenance_path(cpu_id, container_name)
  return ndc_paths.container_status_field_path(
    site(), cpu_id, container_name, "maintenance_until")
end

-- Returns epoch seconds the lease expires (0 = not in maintenance).
function M.read_maintenance_until(pg, cpu_id, container_name)
  local path = maintenance_path(cpu_id, container_name)
  local rs, err = pg:query(string.format(
    "SELECT COALESCE((data->>'value')::bigint, 0) AS v " ..
    "FROM knowledge_base_status WHERE path = '%s'::ltree",
    path:gsub("'", "''")))
  if not rs or not rs[1] then return 0 end
  return tonumber(rs[1].v) or 0
end

-- Write the lease. Caller is responsible for computing the expiry epoch
-- (now + lease_default, or 0 to end the lease).
function M.write_maintenance_until(pg, cpu_id, container_name, epoch_seconds)
  local n = math.floor(tonumber(epoch_seconds) or 0)
  local path = maintenance_path(cpu_id, container_name)
  local rs, err = pg:query(string.format([[
    UPDATE knowledge_base_status
    SET data = jsonb_build_object('value', %d)::json
    WHERE path = '%s'::ltree
  ]], n, path:gsub("'", "''")))
  if not rs then return nil, err end
  return true
end

-- Default lease duration from the site-level setpoint (phase 7a
-- tunable), with a 900s fallback if the setpoint somehow isn't
-- planted.
function M.maintenance_lease_default(pg)
  local v = M.site_status_value(pg, "unmonitor_lease_default_s")
  return tonumber(v) or 900
end

-- Recent audit_log entries, optionally filtered to a specific target.
-- Pass target=nil (or empty) to get site-wide recent activity.
-- Returns array of rows; each row has string fields:
--   id, ts, operator, action, target, note, result
function M.recent_audit(pg, target, limit)
  limit = tonumber(limit) or 10
  local filter = ""
  if target and target ~= "" then
    filter = "WHERE target = '" .. tostring(target):gsub("'", "''") .. "'"
  end
  local rs = pg:query(string.format([[
    SELECT id,
           to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts,
           extract(epoch FROM ts)::bigint AS epoch,
           operator, action, target, note, result
    FROM audit_log
    %s
    ORDER BY id DESC
    LIMIT %d
  ]], filter, limit))
  if not rs then return {} end
  local out = {}
  for _, r in ipairs(rs) do
    out[#out + 1] = {
      id = r.id, ts = r.ts, epoch = tonumber(r.epoch),
      operator = r.operator, action = r.action, target = r.target,
      note = r.note, result = r.result,
    }
  end
  return out
end

-- Render a compact audit-log table (HTML fragment). Reused across
-- per-target views (container_status, cpu_summary, system_overview).
function M.audit_table_html(rows, empty_msg)
  if not rows or #rows == 0 then
    return '<p class="placeholder">' .. (empty_msg or "No activity recorded.") .. '</p>'
  end
  local out = { '<table style="width:100%;border-collapse:collapse">',
    '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">When</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Operator</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Action</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Note</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Result</th>' ..
    '</tr></thead><tbody>' }
  for _, r in ipairs(rows) do
    local result_cell = r.result or ""
    local result_style = "color:#8f8"
    if result_cell:find("^error") then result_style = "color:#f88" end
    local when_html = r.epoch and M.time_el(r.epoch, 86400)
                   or M.escape(r.ts or "")
    out[#out + 1] = string.format(
      '<tr>' ..
      '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222;font-size:0.88em">%s</td>' ..
      '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222;font-size:0.88em">%s</td>' ..
      '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222;font-size:0.88em"><code>%s</code></td>' ..
      '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222;font-size:0.85em;color:#aaa">%s</td>' ..
      '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222;font-size:0.85em;%s">%s</td>' ..
      '</tr>',
      when_html,
      M.escape(r.operator or ""),
      M.escape(r.action or ""),
      M.escape(r.note or ""),
      result_style,
      M.escape(result_cell))
  end
  out[#out + 1] = '</tbody></table>'
  return table.concat(out)
end

-- Guard rail: containers whose *definition* is `dcs_console` host
-- both the gateway (the operator's browser entry point) and the
-- admin UI itself. Stopping or restarting them from the UI kicks
-- the operator off and leaves them no way back except shell access.
-- Actions that would pause or cycle the container short-circuit
-- with this check first. An operator who really wants to stop
-- dcs_console can do it via docker CLI or a direct pg UPDATE.
local PROTECTED_DEFS = { dcs_console = true }

function M.is_protected_container(c)
  if not c or not c.definition then return false end
  return PROTECTED_DEFS[c.definition] == true
end

-- CPU-wide maintenance lease (X4). 0 = CPU is live; >0 = epoch
-- seconds when the whole-CPU lease expires.
local function cpu_maintenance_path(cpu_id)
  return ndc_paths.cpu_status_field_path(
    site(), cpu_id, "cpu_maintenance_until")
end
function M.read_cpu_maintenance_until(pg, cpu_id)
  local path = cpu_maintenance_path(cpu_id)
  local rs = pg:query(string.format(
    "SELECT COALESCE((data->>'value')::bigint, 0) AS v " ..
    "FROM knowledge_base_status WHERE path = '%s'::ltree",
    path:gsub("'", "''")))
  if not rs or not rs[1] then return 0 end
  return tonumber(rs[1].v) or 0
end
function M.write_cpu_maintenance_until(pg, cpu_id, epoch_seconds)
  local n = math.floor(tonumber(epoch_seconds) or 0)
  local path = cpu_maintenance_path(cpu_id)
  local rs, err = pg:query(string.format([[
    UPDATE knowledge_base_status
    SET data = jsonb_build_object('value', %d)::json
    WHERE path = '%s'::ltree
  ]], n, path:gsub("'", "''")))
  if not rs then return nil, err end
  return true
end

------------------------------------------------------------------------
-- Setpoints (Phase 7a) -- operator-tunable site-level status fields
------------------------------------------------------------------------

-- Read both the runtime value and the schema default for one setpoint.
-- Returns { current, default, description }; current is nil when the
-- status row data is empty (so the effective value is the default).
function M.read_setpoint(pg, name)
  local path = ndc_paths.site_status_field_path(site(), name)
  local rs, err = pg:query(string.format([[
    SELECT k.properties->>'description' AS description,
           k.data->>'value' AS default_value,
           NULLIF(s.data->>'value', '') AS current_value
    FROM knowledge_base k
    LEFT JOIN knowledge_base_status s ON s.path = k.path
    WHERE k.path = '%s'::ltree
  ]], (path):gsub("'", "''")))
  if not rs or not rs[1] then return nil, err or "setpoint not found" end
  local r = rs[1]
  return {
    current     = tonumber(r.current_value),
    default     = tonumber(r.default_value),
    description = r.description,
  }
end

-- Atomic write of a setpoint's runtime value. Full overwrite of the
-- status row's data blob -- setpoint rows only ever have the
-- {"value": N} shape so there's nothing to preserve. Caller must have
-- already validated the range.
function M.write_setpoint(pg, name, value)
  local n = tonumber(value)
  if n == nil then return nil, "value must be a number" end
  local path = ndc_paths.site_status_field_path(site(), name)
  local rs, err = pg:query(string.format([[
    UPDATE knowledge_base_status
    SET data = jsonb_build_object('value', %d)::json
    WHERE path = '%s'::ltree
  ]], n, (path):gsub("'", "''")))
  if not rs then return nil, err end
  return true
end

------------------------------------------------------------------------
-- TCP reachability probe for the Infra menu (Phase 6)
------------------------------------------------------------------------

-- Cached host IP for infra probes. Infra containers are published on
-- host.docker.internal, which openresty cosockets can't resolve via
-- /etc/hosts -- so we reuse the pre-cached IP already discovered in
-- host_ip() above (private to pg_connect but identical target).
local function infra_host_ip()
  return host_ip()
end

-- Open a TCP connection to (host, port), close immediately, report
-- outcome + latency in milliseconds. Uses openresty cosockets so the
-- probe is fully non-blocking. Returns:
--   true,  nil,       latency_ms  on success
--   false, "msg...",  latency_ms  on failure (timeout / refused / etc)
function M.tcp_probe(host, port, timeout_ms)
  timeout_ms = timeout_ms or 2000
  local sock = ngx.socket.tcp()
  sock:settimeout(timeout_ms)
  local t0 = ngx.now()
  local ok, err = sock:connect(host, port)
  local elapsed = math.floor((ngx.now() - t0) * 1000 + 0.5)
  if not ok then
    pcall(function() sock:close() end)
    return false, err or "connect failed", elapsed
  end
  sock:close()
  return true, nil, elapsed
end

-- Probe helper exposed for views -- same signature as tcp_probe but
-- internally uses the cached host.docker.internal IP.
function M.probe_infra(port, timeout_ms)
  return M.tcp_probe(infra_host_ip(), port, timeout_ms)
end

-- Read the `container` header row for an infra container (different
-- schema from CONTAINER_REGISTRY; infra containers are pre-placed by
-- laptop scripts and DCS only start/stops them).
-- Returns { path, name, cpu_id, image, hostname (via join with cpu row) }
-- or nil if not found.
function M.get_infra_container(pg, name)
  if not name or name == "" then return nil end
  local rs, err = pg:query(string.format([[
    SELECT path, name, properties
    FROM knowledge_base
    WHERE label = 'container' AND name = '%s'
    LIMIT 1
  ]], name:gsub("'", "''")))
  if not rs or not rs[1] then return nil end
  local r = rs[1]
  local props = r.properties
  if type(props) == "string" then props = cjson.decode(props) or {} end
  local path_s = tostring(r.path or "")
  local cpu_id = path_s:match("%.cpu%.([^%.]+)%.container%.")
  return {
    path       = path_s,
    name       = r.name,
    cpu_id     = cpu_id,
    definition = props.definition,
    kind       = props.kind,
    managed_by = props.managed_by,
  }
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
