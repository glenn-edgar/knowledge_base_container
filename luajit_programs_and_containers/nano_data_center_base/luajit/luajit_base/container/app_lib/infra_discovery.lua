-- =============================================================================
-- infra_discovery.lua -- App-side helper: look up runtime addressing for
-- a site-wide infrastructure service (NATS, MQTT, postgres, kv-bridge, ...).
--
-- system_control writes the runtime addressing on each tick into:
--   system.<sys>.site.<S>.infrastructure.registry.service.<service_type>.
--     KB_STATUS_FIELD.{ host, port, healthy, last_seen }
-- (See subsystems/infrastructure_registry.lua + dcs_host/infra_publisher.lua.)
--
-- This module is the read side. App containers call it once at startup
-- (or on reconnect) to discover where their infra dependencies live.
--
-- Caller is responsible for connecting to pg first (PG_HOST etc. are in env).
-- pg is the only rendez-vous point in the v3 architecture.
--
-- Returns a table { host, port, protocol, healthy, last_seen, age_s }.
-- Errors out if the service was never registered (schema row missing).
-- =============================================================================

local cjson     = require("dkjson")
local ndc_paths = require("ndc_paths")

local M = {}

local function escape(s) return tostring(s):gsub("'", "''") end

-- Read one KB_STATUS_FIELD row's `value` from pg.
-- Returns the decoded value or nil + err.
local function read_field(conn, base_path, field)
  local path = base_path .. "." .. field
  local sql = string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape(path))
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare " .. path .. ": " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local row = sth:fetch(true)
  sth:close()
  if not row then return nil, "row not found at " .. path end
  local raw = row.data
  if type(raw) == "string" then
    local decoded = cjson.decode(raw)
    if not decoded then return nil, "json decode failed at " .. path end
    return decoded.value
  end
  if type(raw) == "table" then return raw.value end
  return raw
end

---------------------------------------------------------------------------
-- Public: lookup(pg, site, service_type, opts) -> table | nil, err
---------------------------------------------------------------------------
--
-- @param pg            pg connection (DBI handle, pgmoon, ... — must
--                       support :prepare()/:execute()/:fetch(true)).
-- @param site          string — the site name (e.g. "moon_base_alpha").
-- @param service_type  string — abstract name from definitions.lua
--                       service_contract (e.g. "nats", "mqtt", "postgres",
--                       "kv_bridge").
-- @param opts          { require_healthy = true|false (default true),
--                        max_age_s       = number (default 30) }
--
-- @return { host, port, protocol, healthy, last_seen, age_s } on success.
-- @return nil, err on failure (row missing, stale, unhealthy).
---------------------------------------------------------------------------
function M.lookup(pg, site, service_type, opts)
  assert(pg,           "infra_discovery.lookup: pg conn required")
  assert(type(site) == "string" and site ~= "",
         "infra_discovery.lookup: site required")
  assert(type(service_type) == "string" and service_type ~= "",
         "infra_discovery.lookup: service_type required")
  opts = opts or {}
  local require_healthy = (opts.require_healthy ~= false)
  local max_age_s       = tonumber(opts.max_age_s) or 30

  -- ndc_paths.configure() must already have been called by the caller's
  -- module-load setup (mirrors how observability/dcs_console do it).
  local base = ndc_paths.site_path(site,
    "infrastructure.registry.service." .. service_type
    .. ".KB_STATUS_FIELD")

  local host, err = read_field(pg, base, "host")
  if err then return nil, err end
  local port, e2 = read_field(pg, base, "port")
  if e2  then return nil, e2 end
  local proto, e3 = read_field(pg, base, "protocol")
  if e3  then return nil, e3 end
  local healthy, e4 = read_field(pg, base, "healthy")
  if e4  then return nil, e4 end
  local last_seen, e5 = read_field(pg, base, "last_seen")
  if e5  then return nil, e5 end

  local age_s = os.time() - (tonumber(last_seen) or 0)
  local out = {
    host       = host or "",
    port       = tonumber(port) or 0,
    protocol   = proto or "tcp",
    healthy    = (healthy == true),
    last_seen  = tonumber(last_seen) or 0,
    age_s      = age_s,
  }

  if require_healthy and not out.healthy then
    return nil, string.format(
      "service %q not healthy (host=%q age=%ds)",
      service_type, tostring(out.host), age_s)
  end
  if require_healthy and age_s > max_age_s then
    return nil, string.format(
      "service %q stale (age=%ds > max=%ds)",
      service_type, age_s, max_age_s)
  end
  if require_healthy and (out.host == "" or out.port == 0) then
    return nil, string.format(
      "service %q advertised but host/port empty (registry not yet populated?)",
      service_type)
  end

  return out
end

---------------------------------------------------------------------------
-- Convenience: build a NATS URL string from a lookup result.
-- Returns "nats://<host>:<port>" or nil + err.
---------------------------------------------------------------------------
function M.nats_url(pg, site, opts)
  local r, err = M.lookup(pg, site, "nats", opts)
  if not r then return nil, err end
  return string.format("nats://%s:%d", r.host, r.port)
end

---------------------------------------------------------------------------
-- Convenience: MQTT host:port pair.
---------------------------------------------------------------------------
function M.mqtt_addr(pg, site, opts)
  local r, err = M.lookup(pg, site, "mqtt", opts)
  if not r then return nil, err end
  return r.host, r.port
end

return M
