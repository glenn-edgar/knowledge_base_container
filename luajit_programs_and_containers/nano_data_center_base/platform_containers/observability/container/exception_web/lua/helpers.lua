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

---------------------------------------------------------------------------
-- SYS_EXCEPTION child read/write (pgmoon-native, mirrors kb_exception.lua)
---------------------------------------------------------------------------

local cjson = require("cjson.safe")

local function quote_literal(s)
  return "'" .. tostring(s):gsub("'", "''") .. "'"
end

--- Read a scalar KB_STATUS_FIELD child of a SYS_EXCEPTION.
--- Returns the `value` field from data jsonb, or nil.
function M.read_status(pg, exc_path, field)
  local sql = string.format(
    "SELECT data FROM knowledge_base_status " ..
    "WHERE path = %s::ltree",
    quote_literal(exc_path .. ".KB_STATUS_FIELD." .. field))
  local rows = pg:query(sql)
  if not rows or #rows == 0 then return nil end
  local d = rows[1].data
  if type(d) == "string" then d = cjson.decode(d) or {} end
  return (d or {}).value
end

--- Read the jsonb signatures array under a SYS_EXCEPTION.
--- KB_JSONB_FIELD data lives in knowledge_base_document (column ltree),
--- not knowledge_base_status. The data column holds the array directly
--- (not wrapped in {value: ...}).
--- Returns an array of signature records or an empty array.
function M.read_signatures(pg, exc_path)
  local sql = string.format(
    "SELECT data FROM knowledge_base_document " ..
    "WHERE ltree = %s::ltree",
    quote_literal(exc_path .. ".KB_JSONB_FIELD.signatures"))
  local rows = pg:query(sql)
  if not rows or #rows == 0 then return {} end
  local d = rows[1].data
  if type(d) == "string" then d = cjson.decode(d) or {} end
  -- Either stored as a bare array or wrapped in {value: array}.
  -- kb_exception.lua currently wraps; construct defaults to bare.
  if type(d) == "table" then
    if type(d.value) == "table" then return d.value end
    if #d > 0 or next(d) == nil then return d end
  end
  return {}
end

--- Read the KB header properties (type, priority, description, ...).
function M.read_props(pg, exc_path)
  local sql = string.format(
    "SELECT properties FROM knowledge_base " ..
    "WHERE label = 'SYS_EXCEPTION' AND path = %s::ltree",
    quote_literal(exc_path))
  local rows = pg:query(sql)
  if not rows or #rows == 0 then return nil end
  local p = rows[1].properties
  if type(p) == "string" then p = cjson.decode(p) or {} end
  return p or {}
end

--- Write a scalar status child. value can be string / number / bool.
function M.write_status(pg, exc_path, field, value)
  local path = exc_path .. ".KB_STATUS_FIELD." .. field
  local json = cjson.encode({ value = value })
  local sql = string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES (%s::ltree, %s::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    quote_literal(path), quote_literal(json))
  return pg:query(sql)
end

---------------------------------------------------------------------------
-- Lifecycle operations (mirrors kb_exception.lua's SCADA API)
---------------------------------------------------------------------------

local function now_s() return os.time() end

--- Operator ack. UNACK_ACTIVE -> ACK_ACTIVE. RTN_UNACK -> NORMAL.
function M.ack(pg, exc_path, operator, comment)
  local state = M.read_status(pg, exc_path, "state") or "NORMAL"
  local new_state = state
  if state == "UNACK_ACTIVE" then new_state = "ACK_ACTIVE" end
  if state == "RTN_UNACK"    then new_state = "NORMAL"     end
  if new_state ~= state then M.write_status(pg, exc_path, "state", new_state) end
  M.write_status(pg, exc_path, "last_ack_ts", now_s())
  M.write_status(pg, exc_path, "last_ack_by", tostring(operator or "ops"))
  if comment and #comment > 0 then
    M.write_status(pg, exc_path, "last_comment", tostring(comment))
  end
  return true
end

--- Clear (condition gone). Transitions per SCADA semantics.
function M.clear(pg, exc_path)
  local state = M.read_status(pg, exc_path, "state") or "NORMAL"
  local new_state = state
  if state == "UNACK_ACTIVE" then new_state = "RTN_UNACK" end
  if state == "ACK_ACTIVE"   then new_state = "NORMAL"    end
  if state == "SHELVED"      then new_state = "NORMAL"    end
  if new_state ~= state then
    M.write_status(pg, exc_path, "state", new_state)
    M.write_status(pg, exc_path, "last_rtn_ts", now_s())
  end
  return true
end

--- Shelve for a duration (seconds). 0 = suppress (no auto-unshelve).
function M.shelve(pg, exc_path, duration_s, operator, reason)
  local now   = now_s()
  local until_ = (tonumber(duration_s) or 0) > 0
                 and (now + tonumber(duration_s)) or 0
  M.write_status(pg, exc_path, "state",          "SHELVED")
  M.write_status(pg, exc_path, "last_shelve_ts", now)
  M.write_status(pg, exc_path, "last_shelve_by", tostring(operator or "ops"))
  M.write_status(pg, exc_path, "shelve_until",   until_)
  if reason and #reason > 0 then
    M.write_status(pg, exc_path, "last_comment", tostring(reason))
  end
  return true
end

function M.unshelve(pg, exc_path, operator)
  local state = M.read_status(pg, exc_path, "state") or "NORMAL"
  if state ~= "SHELVED" then return true end
  M.write_status(pg, exc_path, "state",        "NORMAL")
  M.write_status(pg, exc_path, "shelve_until", 0)
  if operator then
    M.write_status(pg, exc_path, "last_ack_by", tostring(operator))
  end
  return true
end

---------------------------------------------------------------------------
-- Form / URL helpers
---------------------------------------------------------------------------

--- Read POST body as form-encoded key/value pairs.
function M.read_post_args()
  ngx.req.read_body()
  local args = ngx.req.get_post_args() or {}
  return args
end

--- URL-encode a raw string (dots preserved for ltree paths).
function M.urlencode(s)
  if not s then return "" end
  return (tostring(s):gsub("[^%w%-_%.~]", function(c)
    return string.format("%%%02X", c:byte())
  end))
end

---------------------------------------------------------------------------
-- Reverse-proxy prefix awareness
--
-- When this app is reached via the dcs_console gateway, the gateway sets
-- X-Forwarded-Prefix to the namespaced path (e.g. /ui/observability_01/
-- exception_web). Every internal href, form action, and static-asset URL
-- must be prepended with this prefix so the browser stays inside the
-- iframe namespace when it follows the link. Direct-port access has no
-- header set; mk_url() then no-ops and the path is used as-is, so the
-- app remains independently testable.
---------------------------------------------------------------------------

--- Read the X-Forwarded-Prefix request header, normalized.
--- Returns "" when not set (direct-port access) or the prefix WITHOUT a
--- trailing slash so concatenation with a leading-slash path is exact.
function M.gateway_prefix()
  local v = ngx.var.http_x_forwarded_prefix
  if not v or v == "" then return "" end
  -- Strip any trailing slash; mk_url always supplies its own.
  return (v:gsub("/+$", ""))
end

--- Build a URL inside this app, prefix-aware.
---  mk_url("/detail?path=foo")
---    direct port -> "/detail?path=foo"
---    via gateway -> "/ui/observability_01/exception_web/detail?path=foo"
--- Always pass a path that begins with "/"; that's the unambiguous root.
function M.mk_url(path)
  if not path or path == "" then return M.gateway_prefix() .. "/" end
  if path:sub(1, 1) ~= "/" then path = "/" .. path end
  return M.gateway_prefix() .. path
end

return M
