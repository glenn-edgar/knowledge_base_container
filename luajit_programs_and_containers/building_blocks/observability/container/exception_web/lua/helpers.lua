-- helpers.lua -- pg connect + html helpers + query primitives for
-- exception_web views.
--
-- Design: every request opens a fresh pgmoon connection, queries, renders,
-- closes. Matches the browser-refresh model (no shared_dict caching, no
-- live polling). Short-lived connections keep the analyzer-side pool
-- unobstructed.

local pgmoon = require("pgmoon")

local M = {}

---------------------------------------------------------------------------
-- /etc/hosts resolution (openresty cosockets don't consult /etc/hosts)
---------------------------------------------------------------------------

local function resolve_from_hosts(name)
  local f = io.open("/etc/hosts", "r")
  if not f then return nil end
  for line in f:lines() do
    local stripped = line:gsub("#.*$", "")
    local ip, rest = stripped:match("^%s*([%da-fA-F:%.]+)%s+(.*)$")
    if ip and rest and not ip:find(":") then
      for tok in rest:gmatch("%S+") do
        if tok == name then f:close(); return ip end
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
  if h:match("^%d+%.%d+%.%d+%.%d+$") then CACHED_HOST_IP = h; return h end
  local ip = resolve_from_hosts(h)
  if not ip then
    error("cannot resolve PG_HOST='" .. h .. "' from /etc/hosts")
  end
  CACHED_HOST_IP = ip
  return ip
end

---------------------------------------------------------------------------
-- pg_connect
---------------------------------------------------------------------------

function M.pg_connect()
  local pg = pgmoon.new({
    host        = host_ip(),
    port        = tonumber(os.getenv("PG_PORT") or "5432"),
    database    = os.getenv("PG_DB")       or "knowledge_base",
    user        = os.getenv("PG_USER")     or "gedgar",
    password    = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or "",
    socket_type = "nginx",
  })
  local ok, err = pg:connect()
  if not ok then return nil, "pg connect: " .. tostring(err) end
  return pg
end

---------------------------------------------------------------------------
-- HTML / URL helpers
---------------------------------------------------------------------------

function M.escape(s)
  if s == nil then return "" end
  return tostring(s)
    :gsub("&", "&amp;")
    :gsub("<", "&lt;")
    :gsub(">", "&gt;")
    :gsub('"', "&quot;")
    :gsub("'", "&#39;")
end

--- Trim a SYS_EXCEPTION ltree path for UI display:
--- "system.site.X.cpu.cpu_01.SYS_EXCEPTION.container_died" ->
---   cpu_01 / container_died
--- "system.site.X.SYS_EXCEPTION.cluster_not_ready" ->
---   site / cluster_not_ready
function M.short_path(p)
  if not p then return "" end
  local cpu = p:match("%.cpu%.([^%.]+)%.")
  local name = p:match("%.SYS_EXCEPTION%.(.+)$") or p
  if cpu then return cpu .. " / " .. name end
  return "site / " .. name
end

--- Render epoch-seconds as YYYY-MM-DD HH:MM:SS UTC or "never" if 0.
function M.fmt_ts(ts)
  ts = tonumber(ts) or 0
  if ts == 0 then return "never" end
  return os.date("!%Y-%m-%d %H:%M:%S", ts) .. "Z"
end

--- "2m 14s ago" from an epoch-seconds timestamp.
function M.fmt_age(ts)
  ts = tonumber(ts) or 0
  if ts == 0 then return "—" end
  local now = os.time()
  local age = now - ts
  if age < 0 then return "future" end
  if age < 60   then return string.format("%ds ago", age) end
  if age < 3600 then return string.format("%dm %ds ago", math.floor(age/60), age%60) end
  if age < 86400 then return string.format("%dh ago", math.floor(age/3600)) end
  return string.format("%dd ago", math.floor(age/86400))
end

---------------------------------------------------------------------------
-- Query helpers
---------------------------------------------------------------------------

--- Run a SQL query, return rows table. On error: nil, err.
function M.query(pg, sql)
  local rows, err = pg:query(sql)
  if not rows then return nil, err end
  return rows
end

---------------------------------------------------------------------------
-- Priority class helper (for CSS colouring)
---------------------------------------------------------------------------

function M.pri_name(pri)
  pri = tonumber(pri) or 3
  return ({[1]="Emergency",[2]="High",[3]="Medium",[4]="Low"})[pri] or "Medium"
end

function M.pri_class(pri)
  pri = tonumber(pri) or 3
  return "pri-" .. pri
end

function M.state_class(state)
  return "st-" .. (state or "NORMAL"):gsub("_", "-"):lower()
end

return M
