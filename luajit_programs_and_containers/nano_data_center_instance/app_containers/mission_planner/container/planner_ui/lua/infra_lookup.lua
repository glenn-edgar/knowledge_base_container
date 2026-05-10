-- planner_ui :: infra_discovery (pgmoon flavor) for OpenResty cosockets.
--
-- Mirrors the planner worker's infra_discovery.lookup (app_lib/) but
-- uses pgmoon's :query() instead of the DBI-style :prepare/:execute
-- chain, because OpenResty workers need cosocket-friendly I/O.
--
-- Falls back gracefully: if pg is unreachable or the registry row is
-- missing, returns nil + err and the caller drops back to env / the
-- legacy hardcoded default. That keeps a broken pg from making the
-- /api/submit_mission endpoint fail outright.
--
-- Reads (per service_type) the rows at:
--   system.<sys>.site.<S>.infrastructure.registry.service.<type>.
--     KB_STATUS_FIELD.{ host, port, healthy, last_seen }
-- and stitches them into { host, port, healthy, age_s }.

local cjson = require("cjson.safe")

local M = {}

local function env(k) return os.getenv(k) or "" end

local function escape(s) return tostring(s):gsub("'", "''") end

local function read_field(pg, base_path, field)
  local sql = string.format(
    "SELECT data::text AS data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape(base_path .. "." .. field))
  local rs, err = pg:query(sql)
  if not rs then return nil, "query: " .. tostring(err) end
  if type(rs) ~= "table" or #rs == 0 then
    return nil, "row not found at " .. base_path .. "." .. field
  end
  local raw = rs[1].data
  if type(raw) == "string" then
    local decoded = cjson.decode(raw)
    if not decoded then return nil, "json decode failed at " .. base_path .. "." .. field end
    return decoded.value
  end
  if type(raw) == "table" then return raw.value end
  return raw
end

--- Look up a site-wide infra service via pg.
-- @param pg            pgmoon connection (already :connect()'d).
-- @param service_type  "nats" | "mqtt" | "postgres" | "kv_bridge"
-- @param opts          { system?, site?, require_healthy?, max_age_s? }
-- @return { host, port, healthy, age_s } on success
-- @return nil, err     on failure
function M.lookup(pg, service_type, opts)
  if not pg then return nil, "pg required" end
  if type(service_type) ~= "string" or service_type == "" then
    return nil, "service_type required"
  end
  opts = opts or {}
  local sys  = opts.system  or env("APP_SYSTEM")
  local site = opts.site    or env("APP_SITE")
  if sys == "" or site == "" then
    return nil, "APP_SYSTEM / APP_SITE not set"
  end
  local require_healthy = (opts.require_healthy ~= false)
  local max_age_s = tonumber(opts.max_age_s) or 30

  local base = string.format(
    "system.%s.site.%s.infrastructure.registry.service.%s.KB_STATUS_FIELD",
    sys, site, service_type)

  local host, err = read_field(pg, base, "host")
  if err then return nil, err end
  local port, e2 = read_field(pg, base, "port")
  if e2 then return nil, e2 end
  local healthy, e3 = read_field(pg, base, "healthy")
  if e3 then return nil, e3 end
  local last_seen, e4 = read_field(pg, base, "last_seen")
  if e4 then return nil, e4 end

  local age_s = os.time() - (tonumber(last_seen) or 0)
  local out = {
    host    = host or "",
    port    = tonumber(port) or 0,
    healthy = (healthy == true),
    age_s   = age_s,
  }

  if require_healthy and not out.healthy then
    return nil, "not healthy"
  end
  if require_healthy and age_s > max_age_s then
    return nil, "stale"
  end
  if require_healthy and (out.host == "" or out.port == 0) then
    return nil, "host/port empty"
  end
  return out
end

-- One-time-per-worker log so cluster smoke can verify infra_lookup is
-- actually being preferred over the NATS_URL env fallback. ngx.log is
-- always present under OpenResty; gate on type to keep this module
-- usable from a plain luajit shell.
local _logged_once = false
local function log_first_success(url)
  if _logged_once then return end
  _logged_once = true
  if type(ngx) == "table" and type(ngx.log) == "function" then
    ngx.log(ngx.NOTICE, "infra_lookup: NATS via registry -> ", url)
  else
    io.stderr:write("infra_lookup: NATS via registry -> " .. url .. "\n")
  end
end

--- Convenience: NATS URL string from registry, or nil + err.
-- Opens its own pg connection via the supplied db module so callers
-- don't need to thread one through. Connection is returned to the
-- cosocket pool via :keepalive() on success.
function M.nats_url(db, opts)
  local pg, perr = db.connect()
  if not pg then return nil, perr end
  local r, err = M.lookup(pg, "nats", opts)
  pcall(function() pg:keepalive() end)
  if not r then return nil, err end
  local url = string.format("nats://%s:%d", r.host, r.port)
  log_first_success(url)
  return url
end

return M
