-- poller.lua -- pg -> shared_dict ui_registry pump.
--
-- Runs under init_worker_by_lua_file (once per worker; worker_processes=1
-- so exactly once per container). Schedules ngx.timer.at(0, ...) to do
-- the actual network IO because cosockets are not available inside the
-- init_worker phase itself.
--
-- Each tick:
--   1. Connect to pg (env-injected PG_HOST/PORT/DB/USER/PASSWORD).
--   2. Read gateway_poll_interval_sec from knowledge_base_status (fallback
--      to the schema default in knowledge_base, then to 15).
--   3. Read every CONTAINER_REGISTRY row.
--   4. Build { routes, menu, poll_sec } and JSON-encode into shared_dict.
--   5. Re-arm the timer with the new poll_sec.
--
-- Any failure is fatal: SIGTERM the nginx master (so the luajit-base
-- supervisor sees the process die and system_control reboots), then
-- os.exit the worker. Design decision #5b -- no stale-cache mode.
--
-- Filters:
--   * Skips rows where the port record's slot == "gateway" (prevents
--     the gateway from routing back to itself -- design #7).
--   * Only records with purpose == "ui" enter the route table.

local pgmoon    = require("pgmoon")
local cjson     = require("cjson.safe")
local ndc_paths = require("ndc_paths")

local DEFAULT_POLL_SEC = 15
local PIDFILE          = "/tmp/nginx_dcs_console_gateway.pid"
local ui_registry      = ngx.shared.ui_registry

-- Openresty cosockets do NOT consult /etc/hosts; they only use nginx's
-- resolver directive. Docker Desktop's embedded DNS (127.0.0.11) is not
-- consistently reachable from bridge-networked containers, so we resolve
-- host.docker.internal ourselves from /etc/hosts (populated by docker
-- --add-host host.docker.internal:host-gateway) and hand IPs to pgmoon +
-- to the route table. No nginx resolver is then required.
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

local function env_host()
  local h = os.getenv("PG_HOST") or "host.docker.internal"
  if h:match("^%d+%.%d+%.%d+%.%d+$") then return h end
  local ip = resolve_from_hosts(h)
  if not ip then
    error("poller: cannot resolve PG_HOST='" .. h ..
          "' from /etc/hosts (container missing --add-host?)")
  end
  return ip
end

local HOST_IP = env_host()

local PG = {
  host        = HOST_IP,
  port        = tonumber(os.getenv("PG_PORT") or "5432"),
  database    = os.getenv("PG_DB") or "knowledge_base",
  user        = os.getenv("PG_USER") or "gedgar",
  password    = os.getenv("PG_PASSWORD") or "",
  socket_type = "nginx",
}

-- Configure ndc_paths for this worker. Idempotent if route.lua / etc
-- have already done it.
ndc_paths.configure{ system_name = os.getenv("APP_SYSTEM") or "moon_base" }

local SITE = os.getenv("APP_SITE") or "moon_base_alpha"

-- Site-level KB status field path. Mirrors construct_dcs_kb.lua's
-- kb:add_status_field at site scope (path =
-- system.<system_name>.site.<SITE>.KB_STATUS_FIELD.<name>). The schema row
-- (knowledge_base) holds the default {"value":15}; the status row
-- (knowledge_base_status) is empty {} until an operator writes a new value.
local POLL_CONFIG_PATH = ndc_paths.site_status_field_path(
  SITE, "gateway_poll_interval_sec")

local REGISTRY_SQL = [[
  SELECT k.name AS name, k.properties AS properties, s.data AS data
  FROM knowledge_base k
  LEFT JOIN knowledge_base_status s ON s.path = k.path
  WHERE k.label = 'CONTAINER_REGISTRY'
]]

local POLL_INTERVAL_SQL = string.format([[
  SELECT COALESCE(NULLIF(s.data->>'value', ''), k.data->>'value', '%d')::int AS val
  FROM knowledge_base k
  LEFT JOIN knowledge_base_status s ON s.path = k.path
  WHERE k.path = '%s'::ltree
]], DEFAULT_POLL_SEC, POLL_CONFIG_PATH)

-- SIGTERM the nginx master. The pid file was written by nginx at startup
-- (pid directive in nginx.conf). Killing master triggers a graceful
-- shutdown: workers drain, master exits, the supervisor sees the process
-- die and raises an exception; system_control then reboots the container.
local function fatal(msg)
  ngx.log(ngx.ERR, "gateway poller FATAL: ", msg)
  os.execute(string.format(
    "kill -TERM $(cat %s 2>/dev/null) 2>/dev/null", PIDFILE))
  os.exit(1)
end

local function as_table(v)
  if type(v) == "table" then return v end
  if type(v) == "string" and v ~= "" then
    local t = cjson.decode(v)
    if type(t) == "table" then return t end
  end
  return {}
end

local function build_routes(pg)
  local rs, err = pg:query(POLL_INTERVAL_SQL)
  if not rs then return nil, "poll_interval query: " .. tostring(err) end
  local poll_sec = DEFAULT_POLL_SEC
  if rs[1] and rs[1].val then
    poll_sec = tonumber(rs[1].val) or DEFAULT_POLL_SEC
  end
  if poll_sec < 1 then poll_sec = DEFAULT_POLL_SEC end

  local rows, qerr = pg:query(REGISTRY_SQL)
  if not rows then return nil, "registry query: " .. tostring(qerr) end

  local routes, menu = {}, {}
  for _, r in ipairs(rows) do
    local data  = as_table(r.data)
    local ports = data.ports or {}
    for _, p in ipairs(ports) do
      if p.slot ~= "gateway" and p.purpose == "ui" and p.external then
        local key = r.name .. "/" .. p.slot
        routes[key] = {
          upstream  = HOST_IP .. ":" .. tostring(p.external),
          container = r.name,
          slot      = p.slot,
          external  = p.external,
          description = p.description or "",
        }
        menu[#menu + 1] = routes[key]
      end
    end
  end
  table.sort(menu, function(a, b)
    if a.container ~= b.container then return a.container < b.container end
    return a.slot < b.slot
  end)
  return { poll_sec = poll_sec, routes = routes, menu = menu }
end

local function poll_once()
  local pg = pgmoon.new(PG)
  local ok, err = pg:connect()
  if not ok then return nil, "pg connect: " .. tostring(err) end

  local tab, terr = build_routes(pg)
  pg:disconnect()
  if not tab then return nil, terr end

  local blob = cjson.encode(tab)
  local sok, serr = ui_registry:set("_blob", blob)
  if not sok then return nil, "shared_dict set: " .. tostring(serr) end
  ui_registry:set("_ready", "1")
  return tab.poll_sec
end

local function schedule(delay)
  local ok, err = ngx.timer.at(delay, function(premature)
    if premature then return end
    local next_sec, ferr = poll_once()
    if not next_sec then fatal(ferr) end
    ngx.log(ngx.INFO, "gateway poll ok; next in ", next_sec, "s")
    schedule(next_sec)
  end)
  if not ok then fatal("timer.at: " .. tostring(err)) end
end

schedule(0)
