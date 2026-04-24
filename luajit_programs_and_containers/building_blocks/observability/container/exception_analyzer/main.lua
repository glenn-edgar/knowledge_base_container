#!/usr/bin/env luajit
-- exception_analyzer/main.lua -- Phase 7 real logic.
--
-- SCADA alarm-lifecycle janitor. Does NOT raise exceptions (log_analyzer
-- does that); it READS + TRANSITIONS them through the state machine:
--
--   1. Sweep expired shelves: SHELVED with shelve_until in the past ->
--      NORMAL (operator set a lease, it expired).
--   2. Compute flap rates: per-exception hit_count delta over a rolling
--      5-minute window -> flap_rate_5min status child.
--   3. Flood detect: if flap_rate_5min > threshold, auto-shelve the
--      exception for 5 min (alarm rationalization).
--
-- Deferred for v2: alarm_flood_detected meta-exception; RTN_UNACK aging.
--
-- DCS libs staged into /opt/apps/lib/ by docker_build.sh.

package.path = "/opt/apps/lib/?.lua;/opt/apps/exception_analyzer/?.lua;"
             .. package.path

local dkjson = require("dkjson")
local DBI    = require("DBI")
local ptime  = require("posix_time")
local kb_exc = require("kb_exception")
local kb_log = require("kb_log")

---------------------------------------------------------------------------
-- Logging
---------------------------------------------------------------------------

local function log(msg)
  io.stderr:write("exception_analyzer: " .. msg .. "\n")
  io.stderr:flush()
end

---------------------------------------------------------------------------
-- Connect (same pattern as log_analyzer)
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

local function pg_connect()
  local host = resolve_host(os.getenv("PG_HOST") or "host.docker.internal")
  local port = os.getenv("PG_PORT") or "5432"
  local db   = os.getenv("PG_DB")   or "knowledge_base"
  local user = os.getenv("PG_USER") or "gedgar"
  local pass = os.getenv("POSTGRES_PASSWORD")
  if not pass or pass == "" then error("POSTGRES_PASSWORD not in env") end
  local conn, err = DBI.Connect("PostgreSQL", db, user, pass, host, tostring(port))
  if not conn then error("pg connect failed: " .. tostring(err)) end
  conn:autocommit(true)
  local sth = assert(conn:prepare("SELECT 1"))
  assert(sth:execute()); sth:close()
  return conn, { host = host, port = port, db = db, user = user }
end

--- Liveness probe (mirrors log_analyzer/main.lua). Returns true if a
--- SELECT 1 round-trips successfully. Main loop uses this to detect a
--- dead connection and reconnect before queries silently fail.
local function conn_alive(conn)
  if not conn then return false end
  local ok_prep, sth = pcall(function() return conn:prepare("SELECT 1") end)
  if not ok_prep or not sth then return false end
  local ok_exec = pcall(function()
    assert(sth:execute()); sth:close()
  end)
  return ok_exec == true
end

---------------------------------------------------------------------------
-- SQL helpers
---------------------------------------------------------------------------

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

local function fetch_all(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local r = sth:fetch(true)
    if not r then break end
    local copy = {}; for k, v in pairs(r) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  sth:close()
  return rows
end

local function escape_sql(s) return tostring(s):gsub("'", "''") end

local function set_status_child(conn, exc_path, field, value)
  local path = exc_path .. ".KB_STATUS_FIELD." .. field
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path), escape_sql(dkjson.encode({ value = value }))))
end

---------------------------------------------------------------------------
-- Sweep expired shelves
---------------------------------------------------------------------------

--- Find SYS_EXCEPTIONs whose shelve_until has elapsed and unshelve them.
--- Operator-set leases expire automatically via this path.
local function sweep_expired_shelves(conn, now)
  -- Guard the ::int cast with a regex check: PG's planner may reorder
  -- the cast ahead of the JOIN filter, evaluating it on non-shelve_until
  -- rows whose `value` is a text string (like "jane.doe" for last_shelve_by).
  -- The regex guard short-circuits the cast for non-numeric values.
  local sql = string.format([[
    SELECT k.path::text AS path
      FROM knowledge_base k
      JOIN knowledge_base_status s
        ON s.path = (k.path::text || '.KB_STATUS_FIELD.shelve_until')::ltree
     WHERE k.label = 'SYS_EXCEPTION'
       AND (s.data->>'value') ~ '^-?[0-9]+$'
       AND (s.data->>'value')::bigint > 0
       AND (s.data->>'value')::bigint <= %d
  ]], now)
  local rows, err = fetch_all(conn, sql)
  if not rows then
    log("sweep_expired_shelves query failed: " .. tostring(err))
    return 0
  end
  for _, r in ipairs(rows) do
    kb_exc.unshelve(conn, r.path, "auto_expire")
    log("unshelved " .. r.path .. " (lease expired)")
  end
  return #rows
end

---------------------------------------------------------------------------
-- Flap-rate tracking
---------------------------------------------------------------------------

-- In-memory: path -> array of { ts, hit_count } snapshots.
-- Rolling 5-minute window; oldest snapshots evicted.
local flap_snapshots = {}
local FLAP_WINDOW_S      = 300       -- 5 min
local FLOOD_THRESHOLD    = 0.1       -- 0.1 raises/s ≈ 6/min ≈ 360/hr
local FLOOD_SHELVE_S     = 300       -- auto-shelve duration

local function update_flap_snapshots(conn, now)
  -- Same regex guard as sweep_expired_shelves: PG may cast pre-JOIN.
  local rows, err = fetch_all(conn, [[
    SELECT k.path::text AS path,
           CASE WHEN (s.data->>'value') ~ '^-?[0-9]+$'
                THEN (s.data->>'value')::bigint ELSE 0 END AS hit_count
      FROM knowledge_base k
      JOIN knowledge_base_status s
        ON s.path = (k.path::text || '.KB_STATUS_FIELD.hit_count')::ltree
     WHERE k.label = 'SYS_EXCEPTION'
  ]])
  if not rows then
    log("flap query failed: " .. tostring(err))
    return
  end
  for _, r in ipairs(rows) do
    local hit = tonumber(r.hit_count) or 0
    local snaps = flap_snapshots[r.path] or {}
    snaps[#snaps + 1] = { ts = now, hit = hit }
    -- Evict snapshots older than window
    while #snaps > 0 and (now - snaps[1].ts) > FLAP_WINDOW_S do
      table.remove(snaps, 1)
    end
    flap_snapshots[r.path] = snaps

    -- Compute rate and checkpoint to PG
    if #snaps >= 2 then
      local newest = snaps[#snaps]
      local oldest = snaps[1]
      local elapsed = newest.ts - oldest.ts
      local delta   = newest.hit - oldest.hit
      local rate    = (elapsed > 0) and (delta / elapsed) or 0
      set_status_child(conn, r.path, "flap_rate_5min", rate)
    end
  end
end

---------------------------------------------------------------------------
-- Flood detection: auto-shelve exceptions that are flapping
---------------------------------------------------------------------------

local function check_flap_floods(conn, now)
  local shelved = 0
  for path, snaps in pairs(flap_snapshots) do
    if #snaps >= 2 then
      local newest = snaps[#snaps]
      local oldest = snaps[1]
      local elapsed = newest.ts - oldest.ts
      local delta   = newest.hit - oldest.hit
      local rate    = (elapsed > 0) and (delta / elapsed) or 0

      if rate > FLOOD_THRESHOLD then
        -- Check current state — don't re-shelve if already shelved
        local sth = conn:prepare(string.format(
          "SELECT (data->>'value') AS v FROM knowledge_base_status " ..
          "WHERE path = '%s.KB_STATUS_FIELD.state'::ltree",
          escape_sql(path)))
        if sth then
          sth:execute()
          local r = sth:fetch(true)
          sth:close()
          if r and r.v ~= "SHELVED" then
            kb_exc.shelve(conn, path, FLOOD_SHELVE_S, "flap_detector",
              string.format("auto-shelve: rate=%.3f/s > %.3f/s (5-min window)",
                            rate, FLOOD_THRESHOLD))
            log(string.format("FLOOD: shelved %s (rate=%.3f/s)", path, rate))
            shelved = shelved + 1
          end
        end
      end
    end
  end
  return shelved
end

---------------------------------------------------------------------------
-- Main loop
---------------------------------------------------------------------------

local function main()
  log("starting (Phase 7 — real logic)")
  local conn, cfg = pg_connect()
  log(string.format("connected %s@%s:%s/%s", cfg.user, cfg.host, cfg.port, cfg.db))

  local last_sweep   = 0
  local last_flap    = 0
  local last_conn_check = 0
  local tick_count   = 0

  while true do
    local now = math.floor(ptime.now_sec())
    tick_count = tick_count + 1

    -- Liveness + self-heal every 10s. Same pattern as log_analyzer:
    -- without this, a severed pg socket (from build_kb DROP TABLE or
    -- an idle timeout) makes every subsequent prepare fail silently.
    if now - last_conn_check > 10 then
      last_conn_check = now
      if not conn_alive(conn) then
        log("pg connection dead; reconnecting")
        pcall(function() if conn then conn:close() end end)
        local ok_pc, new_conn_or_err = pcall(pg_connect)
        if ok_pc and new_conn_or_err then
          conn = new_conn_or_err
          log("pg reconnected")
        else
          log("pg reconnect failed: " .. tostring(new_conn_or_err))
        end
      end
    end

    -- Every 10s: sweep expired shelves
    if now - last_sweep > 10 then
      local ok, n = pcall(sweep_expired_shelves, conn, now)
      if ok and n and n > 0 then
        log(string.format("sweep: unshelved %d", n))
      elseif not ok then
        log("sweep error: " .. tostring(n))
      end
      last_sweep = now
    end

    -- Every 60s: update flap snapshots + check for floods
    if now - last_flap > 60 then
      pcall(update_flap_snapshots, conn, now)
      pcall(check_flap_floods, conn, now)
      last_flap = now
    end

    -- KB heartbeat: push wall-clock epoch into the site-level
    -- exception_analyzer_heartbeat ring every tick. sample_gap rule
    -- (60s) raises SYS_EXCEPTION.exception_analyzer_stalled on silence.
    pcall(kb_log.push_sample, conn,
      "system.site." .. (os.getenv("APP_SITE") or "moonbase.alpha.dcs") ..
        ".KB_LOG.exception_analyzer_heartbeat",
      os.time())

    -- Heartbeat log every 30 ticks (~30s)
    if tick_count % 30 == 0 then
      local tracked = 0
      for _ in pairs(flap_snapshots) do tracked = tracked + 1 end
      log(string.format("tick=%d tracked=%d", tick_count, tracked))
    end

    ptime.sleep_for(1.0)
  end
end

local ok, err = pcall(main)
if not ok then
  log("FATAL: " .. tostring(err))
  os.exit(1)
end
