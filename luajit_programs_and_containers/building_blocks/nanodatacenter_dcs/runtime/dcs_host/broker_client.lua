-- =============================================================================
-- broker_client.lua -- read docker_host_broker state via knowledge_base_status.
--
-- The broker (`building_blocks/docker_host_broker/`) is a Go container that
-- owns all docker daemon access on the host. It publishes its state to NATS
-- and ALSO mirrors it into knowledge_base_status under the same ltree paths
-- the NATS subjects use. Bare-LuaJIT dcs.lua has no NATS client, so reads
-- happen via this module against the pg row the broker writes.
--
-- Why pg, not NATS: see `building_blocks/docker_host_broker/WIRE_PROTOCOL.md`
-- § "Pg mirror". dcs.lua already polls pg every tick for KB state — adding
-- one more SELECT (cached for ~0.5s) is in the noise. NATS pub/sub is still
-- the primary path for any consumer that has a NATS client.
--
-- Read freshness:
--   * Broker publishes containers snapshot every 5s.
--   * Broker heartbeat every 1s.
--   * This module caches reads for `cache_ttl_s` (default 0.5s) so a chain-tree
--     verify_col with N verifies in one tick does at most one pg read per tick.
--   * `is_fresh(window_s)` exposes data-freshness so verifies fail closed when
--     the broker is dead (stale data ≠ container is fine).
-- =============================================================================

local dkjson      = require("dkjson")
local posix_time  = require("posix_time")
local http_client = require("http_client")

local M = {}

-- ---------------------------------------------------------------------------
-- Module config (mutable via M.configure)
-- ---------------------------------------------------------------------------

local cfg = {
  site               = nil,    -- set by M.configure
  cache_ttl_s        = 0.5,    -- skip re-read within this window
  fresh_window_s     = 15.0,   -- snapshot stale beyond this -> "broker down"
  hb_fresh_window_s  = 5.0,    -- heartbeat stale beyond this -> "broker down"
}

-- snapshot_path resolves the snapshot ltree path for the configured site.
local function snapshot_path()
  if not cfg.site then
    error("broker_client: not configured; call M.configure{ site = ... } first")
  end
  return string.format(
    "system.site.%s.docker_broker.containers.KB_STATUS_FIELD.snapshot",
    cfg.site)
end

local function heartbeat_path()
  if not cfg.site then
    error("broker_client: not configured; call M.configure{ site = ... } first")
  end
  return string.format(
    "system.site.%s.docker_broker.heartbeat.KB_STATUS_FIELD.last",
    cfg.site)
end

local function stats_path()
  if not cfg.site then
    error("broker_client: not configured; call M.configure{ site = ... } first")
  end
  return string.format(
    "system.site.%s.docker_broker.containers.KB_STATUS_FIELD.stats",
    cfg.site)
end

-- ---------------------------------------------------------------------------
-- In-memory cache (per dcs.lua process)
-- ---------------------------------------------------------------------------

local cache = {
  fetched_at_mono = 0,    -- posix_time.now_sec() of last successful read
  snapshot_ts     = 0,    -- broker's wall-clock ts (from JSON)
  by_name         = {},   -- name -> container info table
  hb_ts           = 0,    -- last heartbeat ts
  hb_ok_dock      = false,

  -- Container stats are fetched on-demand by get_stats(); they have a
  -- separate ts/cache because the broker publishes them on a different
  -- cadence (5s default) and many supervisor calls don't need them.
  stats_fetched_at_mono = 0,
  stats_ts              = 0,
  stats_by_name         = {},
}

-- Reset cache (testing or after a known broker bounce).
function M.reset_cache()
  cache.fetched_at_mono = 0
  cache.snapshot_ts     = 0
  cache.by_name         = {}
  cache.hb_ts           = 0
  cache.hb_ok_dock      = false
end

-- ---------------------------------------------------------------------------
-- pg helpers
-- ---------------------------------------------------------------------------

local function escape(s) return tostring(s):gsub("'", "''") end

-- Read JSON payload at an ltree path; returns decoded table or nil, err.
local function read_json_at(conn, path)
  local sql = string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape(path))
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local row = sth:fetch(true)
  sth:close()
  if not row then return nil, "row not found" end
  local raw = row.data
  if type(raw) == "string" then
    local decoded = dkjson.decode(raw)
    if not decoded then return nil, "json decode failed" end
    return decoded
  end
  return raw
end

-- ---------------------------------------------------------------------------
-- Refresh logic
-- ---------------------------------------------------------------------------

-- Force a re-read of the snapshot + heartbeat from pg. Returns true on
-- success, false + err on failure (cache untouched).
function M.refresh(conn)
  if not conn then return false, "no pg connection" end
  if not cfg.site then return false, "site not configured" end

  local sp = snapshot_path()
  local snap, err = read_json_at(conn, sp)
  if not snap then return false, "snapshot read at " .. sp .. ": " .. tostring(err) end

  local hb, herr = read_json_at(conn, heartbeat_path())
  -- Heartbeat is "best-effort"; missing hb downgrades to stale rather than
  -- failing the whole refresh.

  local by_name = {}
  if type(snap.containers) == "table" then
    for _, c in ipairs(snap.containers) do
      if c and c.name then by_name[c.name] = c end
    end
  end

  cache.fetched_at_mono = posix_time.now_sec()
  cache.snapshot_ts     = tonumber(snap.ts) or 0
  cache.by_name         = by_name
  if hb and not herr then
    cache.hb_ts      = tonumber(hb.ts) or 0
    cache.hb_ok_dock = hb.docker_socket_ok and true or false
  end
  return true
end

-- Internal: refresh if our cached snapshot is older than cache_ttl_s.
local function maybe_refresh(conn)
  local now = posix_time.now_sec()
  if (now - cache.fetched_at_mono) <= cfg.cache_ttl_s then
    return true
  end
  return M.refresh(conn)
end

-- ---------------------------------------------------------------------------
-- Public API
-- ---------------------------------------------------------------------------

-- Configure site + optional knobs. Idempotent; can be called repeatedly to
-- adjust freshness windows for testing.
function M.configure(opts)
  if type(opts) ~= "table" or not opts.site then
    error("broker_client.configure: opts.site is required")
  end
  cfg.site = opts.site
  if opts.cache_ttl_s       then cfg.cache_ttl_s       = opts.cache_ttl_s end
  if opts.fresh_window_s    then cfg.fresh_window_s    = opts.fresh_window_s end
  if opts.hb_fresh_window_s then cfg.hb_fresh_window_s = opts.hb_fresh_window_s end
  -- Mutation API: callers pass http_base_url to enable start/stop/run/rm.
  -- Reads work without it; mutations error out with "not configured".
  if opts.http_base_url then
    http_client.configure{
      base_url  = opts.http_base_url,
      timeout_s = opts.http_timeout_s,
    }
  end
end

-- Returns (true|false, reason). Snapshot is "fresh" only if the broker has
-- written within fresh_window_s of wall-clock now AND the heartbeat row is
-- within hb_fresh_window_s. Either-or staleness flips us to "broker down".
function M.is_fresh()
  if cache.fetched_at_mono == 0 then
    return false, "broker_client: never refreshed"
  end
  local now_wall = os.time()
  local snap_age = now_wall - cache.snapshot_ts
  if snap_age > cfg.fresh_window_s then
    return false, string.format("snapshot stale (%ds > %ds)", snap_age, cfg.fresh_window_s)
  end
  local hb_age = now_wall - cache.hb_ts
  if hb_age > cfg.hb_fresh_window_s then
    return false, string.format("heartbeat stale (%ds > %ds)", hb_age, cfg.hb_fresh_window_s)
  end
  return true
end

-- Returns the per-container info table or nil. Refreshes cache on demand.
function M.get_container(conn, name)
  local ok, _ = maybe_refresh(conn)
  if not ok then return nil end
  return cache.by_name[name]
end

-- Returns (true|false, reason). False also when broker is stale/down.
function M.is_running(conn, name)
  local fresh, ferr = (function()
    local ok = maybe_refresh(conn)
    if not ok then return false, "refresh failed" end
    return M.is_fresh()
  end)()
  if not fresh then return false, ferr end
  local ci = cache.by_name[name]
  if not ci then return false, "container not in snapshot" end
  if ci.state ~= "running" then
    return false, string.format("state=%s", tostring(ci.state))
  end
  return true
end

-- Health string ("healthy"/"unhealthy"/"starting"/"none") or nil if unknown.
function M.health_of(conn, name)
  local ci = M.get_container(conn, name)
  if not ci then return nil end
  return ci.health
end

-- Snapshot age in wall-clock seconds (negative if cache is empty).
function M.last_age_s()
  if cache.snapshot_ts == 0 then return -1 end
  return os.time() - cache.snapshot_ts
end

-- Force a re-read of the per-container stats row. Cached for cache_ttl_s.
function M.refresh_stats(conn)
  if not conn then return false, "no pg connection" end
  local sp = stats_path()
  local doc, err = read_json_at(conn, sp)
  if not doc then return false, "stats read at " .. sp .. ": " .. tostring(err) end
  local by_name = {}
  if type(doc.stats) == "table" then
    for name, s in pairs(doc.stats) do
      if type(s) == "table" then by_name[name] = s end
    end
  end
  cache.stats_fetched_at_mono = posix_time.now_sec()
  cache.stats_ts              = tonumber(doc.ts) or 0
  cache.stats_by_name         = by_name
  return true
end

-- Returns the full stats table {name -> stats} from the broker's most
-- recent publish. nil + err if the cache is empty and refresh fails.
-- Cache TTL is the same cache_ttl_s used for the containers snapshot.
function M.get_stats(conn)
  local now = posix_time.now_sec()
  if (now - cache.stats_fetched_at_mono) > cfg.cache_ttl_s then
    local ok, err = M.refresh_stats(conn)
    if not ok then return nil, err end
  end
  return cache.stats_by_name
end

-- Stats freshness check (parallel to is_fresh for snapshots).
function M.stats_fresh()
  if cache.stats_fetched_at_mono == 0 then
    return false, "stats: never refreshed"
  end
  local age = os.time() - cache.stats_ts
  -- Stats publish cadence default 5s; allow up to 4× before "stale".
  if age > 20 then
    return false, string.format("stats stale (%ds)", age)
  end
  return true
end

-- ---------------------------------------------------------------------------
-- Mutation API (POST /v1/cmd/* on the broker; see WIRE_PROTOCOL.md).
--
-- Return contract for chain-tree handlers:
--
--   ok=true,  info_table  -- container is in the requested target state.
--                            Counts wire-protocol idempotent collisions
--                            (e.g. start on an already-running container)
--                            as success: the supervisor wanted state X,
--                            and the system is in state X. info.idempotent
--                            is set when we got there via 409.
--
--   ok=false, err_string  -- target state NOT reached. Caller fail-stops
--                            (per feedback_no_soft_faults). Includes the
--                            broker's `error` code where present.
-- ---------------------------------------------------------------------------

local function map_response(status, body, err, idempotent_codes)
  if not status then
    return false, "broker unreachable: " .. tostring(err or "unknown")
  end
  if status == 202 then
    return true, body or {}
  end
  if status == 409 and body and idempotent_codes[body.error or ""] then
    return true, { idempotent = true, code = body.error, body = body }
  end
  local detail = (body and (body.error or body.message)) or ""
  return false, string.format("broker rejected: status=%d %s", status, detail)
end

function M.start(name)
  if type(name) ~= "string" or name == "" then return false, "name required" end
  local s, b, e = http_client.post_json("/v1/cmd/start", { name = name })
  return map_response(s, b, e, { already_running = true })
end

function M.stop(name, timeout_s)
  if type(name) ~= "string" or name == "" then return false, "name required" end
  local s, b, e = http_client.post_json("/v1/cmd/stop",
    { name = name, timeout_s = tonumber(timeout_s) or 0 })
  return map_response(s, b, e, { already_stopped = true })
end

function M.run(spec)
  if type(spec) ~= "table" or not spec.name or not spec.image then
    return false, "spec.name and spec.image required"
  end
  local s, b, e = http_client.post_json("/v1/cmd/run", spec)
  -- name_taken is NOT idempotent: caller must rm first (image/port spec
  -- may differ). Surface it as a fail with the existing id so the caller
  -- can decide.
  if s == 409 and b and b.error == "name_taken" then
    return false, string.format("name_taken (existing_id=%s)", tostring(b.existing_id or ""))
  end
  return map_response(s, b, e, {})
end

function M.rm(name, force)
  if type(name) ~= "string" or name == "" then return false, "name required" end
  local s, b, e = http_client.post_json("/v1/cmd/rm",
    { name = name, force = force and true or false })
  -- 404 is success-as-idempotent: container is already gone. We treat
  -- that the same way we'd treat a 202 (target state reached).
  if s == 404 then
    return true, { idempotent = true, code = "not_found" }
  end
  return map_response(s, b, e, {})
end

-- Returns the current cache as a read-only-ish view. For diagnostics.
function M.debug_view()
  local names = {}
  for k in pairs(cache.by_name) do names[#names + 1] = k end
  return {
    site             = cfg.site,
    fetched_at_mono  = cache.fetched_at_mono,
    snapshot_ts      = cache.snapshot_ts,
    snapshot_age_s   = (cache.snapshot_ts > 0) and (os.time() - cache.snapshot_ts) or -1,
    hb_ts            = cache.hb_ts,
    hb_age_s         = (cache.hb_ts > 0) and (os.time() - cache.hb_ts) or -1,
    docker_socket_ok = cache.hb_ok_dock,
    container_count  = #names,
    container_names  = names,
  }
end

return M
