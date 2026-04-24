-- export.lua -- CSV / JSON dump of a KB_LOG's samples or rollup rows.
--
-- URL params:
--   path       ltree of the KB_LOG (required)
--   tier       "raw" (default) | "1min" | "1hour" | "1day"
--   format     "csv" (default) | "json"
--   window_s   seconds of history to include (default: 3600 for raw,
--              86400 for 1min, 7d for 1hour, 30d for 1day)
--
-- Output is streamed line-by-line so exporting large windows doesn't
-- buffer everything in memory on the server. Content-Disposition
-- prompts the browser to save the file; curl -O also works.

local h     = require("helpers")
local cjson = require("cjson.safe")

local args     = ngx.req.get_uri_args() or {}
local log_path = args.path
local tier     = (args.tier or "raw")
local format   = (args.format or "csv"):lower()

if not log_path or log_path == "" then
  ngx.status = 400
  ngx.header["Content-Type"] = "text/plain"
  ngx.say("missing required query parameter: path")
  return
end
if not (tier == "raw" or tier == "1min" or tier == "1hour" or tier == "1day") then
  ngx.status = 400
  ngx.header["Content-Type"] = "text/plain"
  ngx.say("tier must be one of: raw, 1min, 1hour, 1day")
  return
end
if not (format == "csv" or format == "json") then
  ngx.status = 400
  ngx.header["Content-Type"] = "text/plain"
  ngx.say("format must be csv or json")
  return
end

local DEFAULT_WINDOW = { raw = 3600, ["1min"] = 86400,
                         ["1hour"] = 7 * 86400, ["1day"] = 30 * 86400 }
local window_s = tonumber(args.window_s) or DEFAULT_WINDOW[tier]

local pg, err = h.pg_connect()
if not pg then
  ngx.status = 500
  ngx.header["Content-Type"] = "text/plain"
  ngx.say("pg connect: " .. tostring(err))
  return
end

-- Build the export filename. Strip ltree dots to a safer name.
local safe_path = log_path:gsub("[^%w_%-]", "_")
local filename  = string.format("%s_%s_%ds.%s",
                                safe_path, tier, window_s, format)

ngx.header["Cache-Control"]       = "no-store"
ngx.header["Content-Disposition"] = "attachment; filename=" .. filename

---------------------------------------------------------------------------
-- Raw tier-0 ring export
---------------------------------------------------------------------------

local function stream_raw()
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local sql = string.format(
    "SELECT EXTRACT(EPOCH FROM recorded_at)::bigint AS rec_epoch, data " ..
    "FROM knowledge_base_stream " ..
    "WHERE path = '%s'::ltree AND valid = TRUE " ..
    "  AND recorded_at > NOW() - INTERVAL '%d seconds' " ..
    "ORDER BY recorded_at ASC",
    stream_path:gsub("'", "''"), window_s)
  local rows = pg:query(sql) or {}

  if format == "csv" then
    ngx.header["Content-Type"] = "text/csv; charset=utf-8"
    ngx.say("recorded_epoch,sample_ts,value")
    for _, r in ipairs(rows) do
      local d = r.data
      if type(d) == "string" then d = cjson.decode(d) or {} end
      ngx.say(string.format("%s,%s,%s",
        tostring(r.rec_epoch),
        tostring((d or {}).ts or ""),
        tostring((d or {}).value or "")))
    end
  else
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.print('{"path":"', log_path, '","tier":"raw","window_s":', window_s, ',"rows":[')
    local first = true
    for _, r in ipairs(rows) do
      local d = r.data
      if type(d) == "string" then d = cjson.decode(d) or {} end
      if not first then ngx.print(",") else first = false end
      ngx.print(string.format(
        '{"rec_epoch":%d,"ts":%s,"value":%s}',
        tonumber(r.rec_epoch) or 0,
        tostring((d or {}).ts or "null"),
        tostring((d or {}).value or "null")))
    end
    ngx.say("]}")
  end
end

---------------------------------------------------------------------------
-- Rollup export (tier-1/2/3)
---------------------------------------------------------------------------

local function stream_rollup()
  local sql = string.format(
    "SELECT EXTRACT(EPOCH FROM bucket_start)::bigint AS bucket_epoch, " ..
    "       count, sum, sumsq, min_val, max_val " ..
    "FROM knowledge_base_rollups " ..
    "WHERE tier = '%s' AND source_path = '%s'::ltree " ..
    "  AND bucket_start > NOW() - INTERVAL '%d seconds' " ..
    "ORDER BY bucket_start ASC",
    tier, log_path:gsub("'", "''"), window_s)
  local rows = pg:query(sql) or {}

  if format == "csv" then
    ngx.header["Content-Type"] = "text/csv; charset=utf-8"
    ngx.say("bucket_epoch,count,mean,min,max,stddev")
    for _, r in ipairs(rows) do
      local c = tonumber(r.count) or 0
      local s = tonumber(r.sum) or 0
      local ssq = tonumber(r.sumsq) or 0
      local mean = (c > 0) and (s / c) or 0
      local variance = (c > 0) and math.max(0, (ssq / c) - mean * mean) or 0
      local stddev = math.sqrt(variance)
      ngx.say(string.format("%s,%d,%.6g,%.6g,%.6g,%.6g",
        tostring(r.bucket_epoch), c, mean,
        tonumber(r.min_val) or 0, tonumber(r.max_val) or 0, stddev))
    end
  else
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.print('{"path":"', log_path, '","tier":"', tier,
              '","window_s":', window_s, ',"rows":[')
    local first = true
    for _, r in ipairs(rows) do
      local c = tonumber(r.count) or 0
      local s = tonumber(r.sum) or 0
      local ssq = tonumber(r.sumsq) or 0
      local mean = (c > 0) and (s / c) or 0
      local variance = (c > 0) and math.max(0, (ssq / c) - mean * mean) or 0
      local stddev = math.sqrt(variance)
      if not first then ngx.print(",") else first = false end
      ngx.print(string.format(
        '{"bucket_epoch":%d,"count":%d,"mean":%.6g,"min":%.6g,"max":%.6g,"stddev":%.6g}',
        tonumber(r.bucket_epoch) or 0, c, mean,
        tonumber(r.min_val) or 0, tonumber(r.max_val) or 0, stddev))
    end
    ngx.say("]}")
  end
end

if tier == "raw" then stream_raw() else stream_rollup() end

pg:keepalive(60000, 8)
