#!/usr/bin/env luajit
-- log_analyzer/main.lua -- Phase 6 real logic.
--
-- Per-tick orchestrator:
--   1. Discover all KB_LOG paths (refresh every 30s)
--   2. For each log: read new samples since cursor, update live_stats
--      in-memory, evaluate KB_RULE children, fire SYS_EXCEPTIONs on trip,
--      checkpoint live_stats back to PG
--   3. Periodically: tier-1 rollup (60s), tier-2 (1h), tier-3 (1d)
--   4. Periodically: sample_gap check (10s), rule-suppression sweep (10s)
--
-- Modules:
--   stats.lua   -- welford + MA + envelope + slope + cusum update
--   rules.lua   -- 7 rule-kind evaluators + fire dispatcher
--   rollups.lua -- tier-0 -> tier-1 -> tier-2 -> tier-3 SQL compaction
--
-- DCS libs staged into /opt/apps/lib/ by docker_build.sh:
--   kb_log, kb_rule, kb_exception, posix_time, pg_connector

package.path = "/opt/apps/lib/?.lua;/opt/apps/log_analyzer/?.lua;" .. package.path

local DBI    = require("DBI")
local ptime  = require("posix_time")

local kb_log   = require("kb_log")
local kb_rule  = require("kb_rule")
local kb_exc   = require("kb_exception")
local stats    = require("stats")
local rules    = require("rules")
local rollups  = require("rollups")

---------------------------------------------------------------------------
-- Logging
---------------------------------------------------------------------------

local function log(msg)
  io.stderr:write("log_analyzer: " .. msg .. "\n")
  io.stderr:flush()
end

---------------------------------------------------------------------------
-- host.docker.internal resolution (no system resolver available in
-- vanilla containers). Reads /etc/hosts for the entry Docker injects.
---------------------------------------------------------------------------

local function resolve_host(name)
  if not name or name == "" then return "127.0.0.1" end
  if not name:match("^host%.docker%.internal") then return name end
  local f = io.open("/etc/hosts", "r")
  if not f then return name end
  for line in f:lines() do
    local ip = line:match("^(%d+%.%d+%.%d+%.%d+)%s+host%.docker%.internal")
    if ip then f:close(); return ip end
  end
  f:close()
  return name
end

---------------------------------------------------------------------------
-- Connection
---------------------------------------------------------------------------

local function pg_connect()
  local host = resolve_host(os.getenv("PG_HOST") or "host.docker.internal")
  local port = os.getenv("PG_PORT") or "5432"
  local db   = os.getenv("PG_DB")   or "knowledge_base"
  local user = os.getenv("PG_USER") or "gedgar"
  local pass = os.getenv("POSTGRES_PASSWORD")
  if not pass or pass == "" then
    error("POSTGRES_PASSWORD not in env")
  end
  local conn, err = DBI.Connect("PostgreSQL", db, user, pass, host, tostring(port))
  if not conn then error("pg connect failed: " .. tostring(err)) end
  conn:autocommit(true)

  -- Sanity query: ensures the server is actually reachable + queryable.
  local sth, perr = conn:prepare("SELECT 1")
  if not sth then error("sanity prepare: " .. tostring(perr)) end
  assert(sth:execute(), "sanity execute failed")
  sth:close()

  return conn, { host = host, port = port, db = db, user = user }
end

---------------------------------------------------------------------------
-- Per-log in-memory state
---------------------------------------------------------------------------

-- analyzer_state[log_path] = {
--   props         = { kind, expected_hz, ma_short_s, ma_long_s, ... }
--   live_stats    = table written/read from live_stats jsonb
--   rules         = [ { path, properties }, ... ]
--   last_cursor   = timestamptz string of last-processed sample
-- }
local analyzer_state = {}

local function count(t)
  local n = 0 for _ in pairs(t) do n = n + 1 end return n
end

--- Load or refresh the KB_LOG inventory. Cheap enough to run every 30s.
--- Rules are refreshed on every discovery pass so operator edits
--- (enabled/suppressed) propagate quickly without restart.
local function rediscover(conn)
  local all_logs, err = kb_log.list_all(conn)
  if not all_logs then log("list_all failed: " .. tostring(err)); return end

  for _, lrow in ipairs(all_logs) do
    local path = lrow.path
    local s = analyzer_state[path]
    if not s then
      -- First time: prime from pg's live_stats (survives analyzer restart)
      s = {
        path        = path,
        props       = lrow.properties or {},
        live_stats  = kb_log.read_live_stats(conn, path) or {},
        rules       = {},
        last_cursor = nil,
      }
      analyzer_state[path] = s
    else
      s.props = lrow.properties or s.props
    end
    local rule_rows, rerr = kb_rule.list_for_log(conn, path)
    s.rules = rule_rows or {}
    if rerr then log("rules list failed for " .. path .. ": " .. tostring(rerr)) end
  end
end

---------------------------------------------------------------------------
-- Per-tick: ingest new samples, update stats, evaluate rules
---------------------------------------------------------------------------

local function tick(conn)
  for path, s in pairs(analyzer_state) do
    local new_samples, err = kb_log.read_samples_since(
      conn, path, s.last_cursor, 500)
    if not new_samples then
      log("read_samples " .. path .. ": " .. tostring(err))
      goto continue
    end
    if #new_samples == 0 then goto continue end

    for _, row in ipairs(new_samples) do
      local d   = row.data or {}
      local ts  = tonumber(d.ts)    or 0
      local val = tonumber(d.value) or 0
      stats.update(s.live_stats, ts, val, s.props)
      rules.evaluate(conn, path, s.rules, s.live_stats, val, ts, log)
      s.last_cursor = row.recorded_at
    end

    -- Checkpoint live_stats after batch (one write per log per tick with
    -- any activity, not per-sample).
    kb_log.write_live_stats(conn, path, s.live_stats)
    ::continue::
  end
end

---------------------------------------------------------------------------
-- Periodic: sample_gap rules (time-driven, not sample-driven)
---------------------------------------------------------------------------

local function check_all_sample_gaps(conn, now)
  for path, s in pairs(analyzer_state) do
    rules.check_sample_gaps(conn, path, s.rules, s.live_stats, now, log)
  end
end

---------------------------------------------------------------------------
-- Periodic: rollup compaction, kind-aware
---------------------------------------------------------------------------

local ROLLUPS_TABLE = "knowledge_base_rollups"

local function do_rollups(conn, phase)
  -- phase ∈ {"1min", "1hour", "1day"} depending on which boundary hit
  for path, s in pairs(analyzer_state) do
    local kind = (s.props or {}).kind or "operational"
    if kind == "diagnostic" then goto continue end

    if phase == "1min" and kind == "operational" then
      local ok, err = rollups.compact_tier1(conn, ROLLUPS_TABLE, path)
      if not ok then log("tier-1 " .. path .. ": " .. tostring(err)) end

    elseif phase == "1hour" and kind == "operational" then
      local ok, err = rollups.compact_tier2(conn, ROLLUPS_TABLE, path)
      if not ok then log("tier-2 " .. path .. ": " .. tostring(err)) end

    elseif phase == "1day" then
      if kind == "operational" then
        local ok, err = rollups.compact_tier3(conn, ROLLUPS_TABLE, path)
        if not ok then log("tier-3 " .. path .. ": " .. tostring(err)) end
      elseif kind == "archival" then
        local ok, err = rollups.compact_tier3_from_raw(conn, ROLLUPS_TABLE, path)
        if not ok then log("tier-3(arch) " .. path .. ": " .. tostring(err)) end
      end
    end
    ::continue::
  end

  if phase == "1day" then
    -- Retention trim runs once per day. Cheap compared to the aggregates.
    rollups.trim(conn, ROLLUPS_TABLE)
  end
end

---------------------------------------------------------------------------
-- Main loop
---------------------------------------------------------------------------

local function main()
  log("starting (Phase 6 — real logic)")
  local conn, cfg = pg_connect()
  log(string.format("connected %s@%s:%s/%s", cfg.user, cfg.host, cfg.port, cfg.db))

  rediscover(conn)
  log(string.format("discovered %d KB_LOGs", count(analyzer_state)))

  local last_rediscover = ptime.now_sec()
  local last_gap_check  = 0
  local last_suppr_sweep = 0
  local last_min_bucket  = math.floor(ptime.now_sec() / 60)
  local last_hour_bucket = math.floor(ptime.now_sec() / 3600)
  local last_day_bucket  = math.floor(ptime.now_sec() / 86400)

  local tick_count = 0
  while true do
    local now = ptime.now_sec()
    tick_count = tick_count + 1

    -- Core: sample ingest + rule eval
    local ok, err = pcall(tick, conn)
    if not ok then log("tick error: " .. tostring(err)) end

    -- Rediscover every 30s
    if now - last_rediscover > 30 then
      pcall(rediscover, conn)
      last_rediscover = now
    end

    -- sample_gap check every 10s
    if now - last_gap_check > 10 then
      pcall(check_all_sample_gaps, conn, now)
      last_gap_check = now
    end

    -- rule suppression sweep every 10s
    if now - last_suppr_sweep > 10 then
      pcall(kb_rule.sweep_expired_suppressions, conn)
      last_suppr_sweep = now
    end

    -- Rollup boundaries: when the floor(now / bucket_s) advances, compact.
    local min_bucket = math.floor(now / 60)
    if min_bucket ~= last_min_bucket then
      pcall(do_rollups, conn, "1min")
      last_min_bucket = min_bucket
    end

    local hour_bucket = math.floor(now / 3600)
    if hour_bucket ~= last_hour_bucket then
      pcall(do_rollups, conn, "1hour")
      last_hour_bucket = hour_bucket
    end

    local day_bucket = math.floor(now / 86400)
    if day_bucket ~= last_day_bucket then
      pcall(do_rollups, conn, "1day")
      last_day_bucket = day_bucket
    end

    -- Heartbeat every 30 ticks (~30s)
    if tick_count % 30 == 0 then
      log(string.format("tick=%d logs=%d",
                        tick_count, count(analyzer_state)))
    end

    ptime.sleep_for(1.0)
  end
end

local ok, err = pcall(main)
if not ok then
  log("FATAL: " .. tostring(err))
  os.exit(1)
end
