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

return M
