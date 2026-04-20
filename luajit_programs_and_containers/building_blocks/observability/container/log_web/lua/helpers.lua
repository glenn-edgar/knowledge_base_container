-- log_web/lua/helpers.lua -- pg connect + html helpers + KB_LOG queries.

local pgmoon = require("pgmoon")
local cjson  = require("cjson.safe")

local M = {}

---------------------------------------------------------------------------
-- /etc/hosts resolution (same pattern as exception_web)
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
    :gsub("&", "&amp;"):gsub("<", "&lt;"):gsub(">", "&gt;")
    :gsub('"', "&quot;"):gsub("'", "&#39;")
end

function M.urlencode(s)
  if not s then return "" end
  return (tostring(s):gsub("[^%w%-_%.~]", function(c)
    return string.format("%%%02X", c:byte())
  end))
end

function M.fmt_ts(ts)
  ts = tonumber(ts) or 0
  if ts == 0 then return "never" end
  return os.date("!%Y-%m-%d %H:%M:%S", ts) .. "Z"
end

function M.fmt_age(ts)
  ts = tonumber(ts) or 0
  if ts == 0 then return "—" end
  local age = os.time() - ts
  if age < 0    then return "future" end
  if age < 60   then return string.format("%ds ago", age) end
  if age < 3600 then return string.format("%dm ago", math.floor(age/60)) end
  if age < 86400 then return string.format("%dh ago", math.floor(age/3600)) end
  return string.format("%dd ago", math.floor(age/86400))
end

function M.short_log_path(p)
  if not p then return "" end
  -- Example: system.site.X.cpu.cpu_01.KB_LOG.host_cpu_pct
  -- -> cpu_01 / host_cpu_pct
  -- Example: system.site.X.cpu.cpu_01.container.foo.KB_LOG.x
  -- -> cpu_01 / foo / x
  local name   = p:match("%.KB_LOG%.([^%.]+)$") or p
  local cont   = p:match("%.container%.([^%.]+)%.KB_LOG%.")
  local cpu    = p:match("%.cpu%.([^%.]+)%.")
  if cpu and cont then return cpu .. " / " .. cont .. " / " .. name end
  if cpu then return cpu .. " / " .. name end
  return "site / " .. name
end

---------------------------------------------------------------------------
-- KB_LOG queries
---------------------------------------------------------------------------

local function quote_literal(s)
  return "'" .. tostring(s):gsub("'", "''") .. "'"
end

--- Read a KB_LOG header's properties.
function M.read_log_props(pg, log_path)
  local sql = string.format(
    "SELECT properties FROM knowledge_base " ..
    "WHERE label = 'KB_LOG' AND path = %s::ltree",
    quote_literal(log_path))
  local rows = pg:query(sql)
  if not rows or #rows == 0 then return nil end
  local p = rows[1].properties
  if type(p) == "string" then p = cjson.decode(p) or {} end
  return p or {}
end

--- Read the live_stats jsonb blob (stored in knowledge_base_document).
function M.read_live_stats(pg, log_path)
  local sql = string.format(
    "SELECT data FROM knowledge_base_document " ..
    "WHERE ltree = %s::ltree",
    quote_literal(log_path .. ".KB_JSONB_FIELD.live_stats"))
  local rows = pg:query(sql)
  if not rows or #rows == 0 then return {} end
  local d = rows[1].data
  if type(d) == "string" then d = cjson.decode(d) or {} end
  if type(d) ~= "table" then return {} end
  -- live_stats may be stored bare or wrapped in {value: ...}
  if type(d.value) == "table" then return d.value end
  return d
end

--- Read all raw samples for a log, newest-first. Returns rows with
--- recorded_at + decoded data { ts, value }.
function M.read_raw_samples(pg, log_path, limit)
  limit = tonumber(limit) or 512
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local sql = string.format([[
    SELECT EXTRACT(EPOCH FROM recorded_at)::float AS rec_epoch, data
      FROM knowledge_base_stream
     WHERE path = %s::ltree AND valid = TRUE
     ORDER BY recorded_at DESC
     LIMIT %d
  ]], quote_literal(stream_path), limit)
  local rows = pg:query(sql) or {}
  for _, r in ipairs(rows) do
    if type(r.data) == "string" then r.data = cjson.decode(r.data) or {} end
  end
  return rows
end

--- Read rollup rows for a log. tier ∈ {'1min', '1hour', '1day'}.
function M.read_rollups(pg, log_path, tier, window_s)
  local sql = string.format([[
    SELECT EXTRACT(EPOCH FROM bucket_start)::float AS bucket_epoch,
           count, sum, sumsq, min_val, max_val
      FROM knowledge_base_rollups
     WHERE tier = %s
       AND source_path = %s::ltree
       AND bucket_start >= NOW() - INTERVAL '%d seconds'
     ORDER BY bucket_start ASC
  ]], quote_literal(tier), quote_literal(log_path), tonumber(window_s) or 3600)
  return pg:query(sql) or {}
end

--- Read all KB_RULE children of a log + their state.
function M.read_rules_for_log(pg, log_path)
  local sql = string.format([[
    SELECT k.path::text       AS path,
           k.properties        AS props,
           COALESCE(s_en.data->>'value', 'true')  AS enabled,
           COALESCE(s_sup.data->>'value', 'false') AS suppressed,
           CASE WHEN (s_fc.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_fc.data->>'value')::bigint ELSE 0 END AS fire_count,
           CASE WHEN (s_ft.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_ft.data->>'value')::bigint ELSE 0 END AS last_fired_ts
      FROM knowledge_base k
      LEFT JOIN knowledge_base_status s_en
        ON s_en.path = (k.path::text || '.KB_STATUS_FIELD.enabled')::ltree
      LEFT JOIN knowledge_base_status s_sup
        ON s_sup.path = (k.path::text || '.KB_STATUS_FIELD.suppressed')::ltree
      LEFT JOIN knowledge_base_status s_fc
        ON s_fc.path = (k.path::text || '.KB_STATUS_FIELD.fire_count')::ltree
      LEFT JOIN knowledge_base_status s_ft
        ON s_ft.path = (k.path::text || '.KB_STATUS_FIELD.last_fired_ts')::ltree
     WHERE k.label = 'KB_RULE' AND k.path <@ %s::ltree
     ORDER BY k.path
  ]], quote_literal(log_path))
  local rows = pg:query(sql) or {}
  for _, r in ipairs(rows) do
    if type(r.props) == "string" then r.props = cjson.decode(r.props) or {} end
  end
  return rows
end

--- Enumerate all KB_LOGs. Returns list with { path, properties }.
function M.list_logs(pg)
  local rows = pg:query(
    "SELECT path::text AS path, properties FROM knowledge_base " ..
    "WHERE label = 'KB_LOG' ORDER BY path") or {}
  for _, r in ipairs(rows) do
    if type(r.properties) == "string" then r.properties = cjson.decode(r.properties) or {} end
  end
  return rows
end

--- List all KB_LOGs filtered by kind ('operational' | 'archival' | 'diagnostic').
--- Includes a joined summary: last_value + last_sample_ts + live_stats (for slope / ma).
function M.list_logs_with_summary(pg, kind_filter)
  local filter = ""
  if kind_filter then
    filter = string.format("AND k.properties->>'kind' = %s", quote_literal(kind_filter))
  end
  local sql = string.format([[
    SELECT k.path::text AS path,
           k.properties AS props,
           COALESCE(s_lv.data->>'value', '') AS last_value,
           CASE WHEN (s_ts.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_ts.data->>'value')::bigint ELSE 0 END AS last_sample_ts,
           CASE WHEN (s_cnt.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_cnt.data->>'value')::bigint ELSE 0 END AS sample_count_total,
           d.data AS live_stats
      FROM knowledge_base k
      LEFT JOIN knowledge_base_status s_lv
        ON s_lv.path = (k.path::text || '.KB_STATUS_FIELD.last_value')::ltree
      LEFT JOIN knowledge_base_status s_ts
        ON s_ts.path = (k.path::text || '.KB_STATUS_FIELD.last_sample_ts')::ltree
      LEFT JOIN knowledge_base_status s_cnt
        ON s_cnt.path = (k.path::text || '.KB_STATUS_FIELD.sample_count_total')::ltree
      LEFT JOIN knowledge_base_document d
        ON d.ltree = (k.path::text || '.KB_JSONB_FIELD.live_stats')::ltree
     WHERE k.label = 'KB_LOG' %s
     ORDER BY k.path
  ]], filter)
  local rows = pg:query(sql) or {}
  for _, r in ipairs(rows) do
    if type(r.props) == "string"      then r.props = cjson.decode(r.props) or {} end
    if type(r.live_stats) == "string" then r.live_stats = cjson.decode(r.live_stats) or {} end
    -- live_stats stored bare or wrapped
    if r.live_stats and type(r.live_stats) == "table" and type(r.live_stats.value) == "table" then
      r.live_stats = r.live_stats.value
    end
  end
  return rows
end

--- List all KB_RULE paths site-wide with parent log, props + state.
function M.list_all_rules(pg)
  local sql = [[
    SELECT k.path::text AS path,
           k.properties AS props,
           COALESCE(s_en.data->>'value', 'true')  AS enabled,
           COALESCE(s_sup.data->>'value', 'false') AS suppressed,
           CASE WHEN (s_su.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_su.data->>'value')::bigint ELSE 0 END AS suppressed_until,
           CASE WHEN (s_fc.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_fc.data->>'value')::bigint ELSE 0 END AS fire_count,
           CASE WHEN (s_ft.data->>'value') ~ '^-?[0-9]+$'
                THEN (s_ft.data->>'value')::bigint ELSE 0 END AS last_fired_ts,
           COALESCE(s_fv.data->>'value', '') AS last_fired_value
      FROM knowledge_base k
      LEFT JOIN knowledge_base_status s_en
        ON s_en.path = (k.path::text || '.KB_STATUS_FIELD.enabled')::ltree
      LEFT JOIN knowledge_base_status s_sup
        ON s_sup.path = (k.path::text || '.KB_STATUS_FIELD.suppressed')::ltree
      LEFT JOIN knowledge_base_status s_su
        ON s_su.path = (k.path::text || '.KB_STATUS_FIELD.suppressed_until')::ltree
      LEFT JOIN knowledge_base_status s_fc
        ON s_fc.path = (k.path::text || '.KB_STATUS_FIELD.fire_count')::ltree
      LEFT JOIN knowledge_base_status s_ft
        ON s_ft.path = (k.path::text || '.KB_STATUS_FIELD.last_fired_ts')::ltree
      LEFT JOIN knowledge_base_status s_fv
        ON s_fv.path = (k.path::text || '.KB_STATUS_FIELD.last_fired_value')::ltree
     WHERE k.label = 'KB_RULE'
     ORDER BY fire_count DESC, k.path
  ]]
  local rows = pg:query(sql) or {}
  for _, r in ipairs(rows) do
    if type(r.props) == "string" then r.props = cjson.decode(r.props) or {} end
  end
  return rows
end

---------------------------------------------------------------------------
-- Rule state writes (pgmoon-native)
---------------------------------------------------------------------------

local function write_status(pg, path, field, value)
  local p = path .. ".KB_STATUS_FIELD." .. field
  local json = cjson.encode({ value = value })
  local sql = string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES (%s::ltree, %s::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    quote_literal(p), quote_literal(json))
  return pg:query(sql)
end

function M.rule_set_enabled(pg, rule_path, on)
  return write_status(pg, rule_path, "enabled", on and true or false)
end

function M.rule_shelve(pg, rule_path, duration_s)
  local now   = os.time()
  local until_ = (tonumber(duration_s) or 0) > 0
                 and (now + tonumber(duration_s)) or 0
  write_status(pg, rule_path, "suppressed", true)
  write_status(pg, rule_path, "suppressed_until", until_)
  return true
end

function M.rule_unshelve(pg, rule_path)
  write_status(pg, rule_path, "suppressed", false)
  write_status(pg, rule_path, "suppressed_until", 0)
  return true
end

---------------------------------------------------------------------------
-- Form helpers
---------------------------------------------------------------------------

function M.read_post_args()
  ngx.req.read_body()
  return ngx.req.get_post_args() or {}
end

return M
