-- planner_ui :: status.lua (Phase 5b C6).
--
-- Read-side companion to submit.lua. Same option-4 design (FFI direct
-- to lib/nats_key_store): planner_ui and planner worker share the
-- container, so libnats_key_store.so resolves through the system
-- loader and the lib/nats_key_store.lua wrapper is reachable via
-- lua_package_path. Reads are bounded (one key for the summary; one
-- get + one optional get for the detail) so the brief synchronous
-- FFI block is acceptable on a 2s polling cadence.
--
-- Keys read (action_server publishes these via lib/action_server.lua;
-- Phase 7: per-tenant under planner.<ns>):
--   <site>.planner.<ns>.action_server.summary
--     { active_missions, missions: {robot_id -> {state, board}},
--       registered_robots, timestamp }
--   <site>.planner.<ns>.action_server.<robot_id>.status
--     { state, robot_id, timestamp, ...mission-specific fields }
--   <site>.planner.<ns>.action_server.<robot_id>.result
--     { success, completed, total, elapsed_ms, replans, fault,
--       final_pose }       -- only set after mission ends

local M = {}

-- Worker-lifetime singleton. Separate from submit.lua's KS+JQ pair --
-- two connections per nginx worker; cheaper than passing a handle
-- around and avoids coupling the two modules.
local _ks

------------------------------------------------------------------------
-- Key builders
------------------------------------------------------------------------

function M.summary_key(site, ns)
  return string.format("%s.planner.%s.action_server.summary", site, ns)
end

function M.status_key(site, ns, robot_id)
  return string.format("%s.planner.%s.action_server.%s.status",
                       site, ns, robot_id)
end

function M.result_key(site, ns, robot_id)
  return string.format("%s.planner.%s.action_server.%s.result",
                       site, ns, robot_id)
end

------------------------------------------------------------------------
-- KeyStore lazy init (read-only client)
------------------------------------------------------------------------

local function ensure_ks(opts)
  if _ks then return _ks end
  local ks_lib    = opts.ks_lib    or require("lib.nats_key_store")
  local site      = opts.site
  local ns        = opts.planner_namespace
  local nats_url  = opts.nats_url
  local worker_id = opts.worker_id or "planner_ui_status"

  -- Phase 7: per-tenant bucket. NATS bucket names disallow dots so
  -- both site and namespace are normalized; matches action_server.lua
  -- + submit.lua exactly.
  local site_bucket = site:gsub("%.", "_")
  local ns_bucket   = ns:gsub("%.", "_")
  local bucket_name = string.format(
    "%s_planner_%s_action_server", site_bucket, ns_bucket)
  -- create_bucket=false: action_server creates it on its first publish.
  -- planner_ui is read-only here -- if the bucket doesn't exist yet
  -- (no missions ever launched), get() returns nil and the dashboard
  -- shows the empty state.
  local k = ks_lib.KeyStore.new({
    server        = nats_url,
    bucket        = bucket_name,
    create_bucket = false,
    history       = 1,
    client_name   = worker_id,
  })
  local connect_ok, kerr = pcall(function() k:connect() end)
  if not connect_ok then
    return nil, "ks connect: " .. tostring(kerr)
  end
  _ks = k
  return _ks
end

------------------------------------------------------------------------
-- Reads
------------------------------------------------------------------------

-- List missions (dashboard view). Returns a table:
--   { missions = {{robot_id, state, board}}, timestamp = "...",
--     active_missions = N, registered_robots = {...} }
--
-- If the summary key is missing (no missions ever launched), returns
-- an empty-but-shaped envelope so the UI can render the empty state.
--
-- opts test shim: { ks_lib, cjson, site, nats_url, planner_namespace, worker_id }.
function M.list_missions(opts)
  opts = opts or {}
  -- Env check first so a no-opts caller in a host shell without
  -- cjson.safe doesn't crash before reporting the real config issue.
  local site = opts.site or os.getenv("APP_SITE") or ""
  if site == "" then return nil, "APP_SITE not set" end
  local ns = opts.planner_namespace or os.getenv("PLANNER_NAMESPACE") or ""
  if ns == "" then return nil, "PLANNER_NAMESPACE not set" end
  local cjson    = opts.cjson    or require("cjson.safe")
  local nats_url = opts.nats_url or os.getenv("NATS_URL") or "nats://127.0.0.1:4222"

  opts.site = site; opts.nats_url = nats_url; opts.planner_namespace = ns
  local ks, kerr = ensure_ks(opts)
  if not ks then return nil, kerr end

  local empty = {
    missions          = {},
    active_missions   = 0,
    registered_robots = {},
    timestamp         = nil,
  }

  local get_ok, val = pcall(function()
    return ks:get(M.summary_key(site, ns))
  end)
  if not get_ok then return nil, "ks get: " .. tostring(val) end
  if not val then return empty end

  local decoded, derr = cjson.decode(val)
  if not decoded then
    return nil, "decode summary: " .. tostring(derr)
  end

  -- Convert the dict {robot_id -> {state, board}} into an array
  -- {{robot_id, state, board}} so the JSON is order-stable on the
  -- wire (htmx & JSON parsers both prefer arrays for lists).
  local rows = {}
  if type(decoded.missions) == "table" then
    for robot_id, m in pairs(decoded.missions) do
      rows[#rows + 1] = {
        robot_id = robot_id,
        state    = m.state,
        board    = m.board,
      }
    end
    table.sort(rows, function(a, b) return a.robot_id < b.robot_id end)
  end

  return {
    missions          = rows,
    active_missions   = decoded.active_missions or #rows,
    registered_robots = decoded.registered_robots or {},
    timestamp         = decoded.timestamp,
  }
end

-- Fetch detailed status for one robot. Returns a table:
--   { status = {...} | nil, result = {...} | nil }
-- Either field may be nil:
--   - status nil  = no mission has ever run for this robot
--   - result nil  = mission still running, or no mission ever ran
--
-- Caller (handler) maps {status=nil,result=nil} to 404.
function M.get_mission(robot_id, opts)
  opts = opts or {}
  if type(robot_id) ~= "string" or robot_id == "" then
    return nil, "robot_id required"
  end
  if not robot_id:match("^[%w_%-%.]+$") then
    return nil, "invalid robot_id"
  end

  local site = opts.site or os.getenv("APP_SITE") or ""
  if site == "" then return nil, "APP_SITE not set" end
  local ns = opts.planner_namespace or os.getenv("PLANNER_NAMESPACE") or ""
  if ns == "" then return nil, "PLANNER_NAMESPACE not set" end
  local cjson    = opts.cjson    or require("cjson.safe")
  local nats_url = opts.nats_url or os.getenv("NATS_URL") or "nats://127.0.0.1:4222"

  opts.site = site; opts.nats_url = nats_url; opts.planner_namespace = ns
  local ks, kerr = ensure_ks(opts)
  if not ks then return nil, kerr end

  local function read_json(key)
    local ok, raw = pcall(function() return ks:get(key) end)
    if not ok then return nil, "ks get " .. key .. ": " .. tostring(raw) end
    if not raw then return nil end
    local d, derr = cjson.decode(raw)
    if not d then return nil, "decode " .. key .. ": " .. tostring(derr) end
    return d
  end

  local status, serr = read_json(M.status_key(site, ns, robot_id))
  if serr then return nil, serr end
  local result, rerr = read_json(M.result_key(site, ns, robot_id))
  if rerr then return nil, rerr end

  return { status = status, result = result }
end

-- Test hook: drop singleton.
function M._reset() _ks = nil end

return M
