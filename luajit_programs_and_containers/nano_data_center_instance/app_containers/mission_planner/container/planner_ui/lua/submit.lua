-- planner_ui :: submit.lua (Phase 5b C5).
--
-- Pure-Lua submission helper. Validates a mission input, lazily creates
-- a worker-lifetime KeyStore + JobQueue pair, and submits via the same
-- libnats_job_queue.so the planner worker uses.
--
-- Design choice (option 4 in continue.md): planner_ui and the planner
-- worker live in the same container, share /usr/local/lib (ldconfig'd
-- in the Dockerfile), and ship the same lib/nats_*.lua FFI wrappers.
-- planner_ui can therefore call jq:submit() directly from inside an
-- nginx content_by_lua_file handler with no IPC layer.
--
-- Cost: jq:submit() is a synchronous FFI call into libnats; it briefly
-- blocks the nginx worker (~1-5ms on a localhost NATS). For a single-
-- operator admin UI this is acceptable. Rejected at design time:
--   - HTTP-to-worker (option 2): main.lua's 5s blocking nanosleep loop
--     has no poll structure today.
--   - lua-resty-nats (option 1): not vendored in openresty_base; full
--     vendor + audit + base rebuild for one button.
--   - kv-bridge MQTT (option 3): mqtt_pubsub absent from openresty_base
--     lualib (verified 2026-05-10).
--
-- Pure-Lua module: no ngx.* calls; the thin handler in
-- api_submit_mission.lua bridges ngx <-> this.

local M = {}

-- Treat empty-string env values as unset. The supervisor's app_env()
-- (luajit_base/.../user_functions.lua) injects NATS_URL="" when the
-- container env doesn't carry one, and Lua's `or` chain stops at "".
local function nonempty(s) return (s and s ~= "") and s or nil end

-- Worker-lifetime singletons. The KeyStore + JobQueue pair owns a
-- libnats connection; reusing it across requests avoids per-click
-- connect overhead (which would dominate the FFI cost).
local _ks, _jq

------------------------------------------------------------------------
-- Validation
------------------------------------------------------------------------

-- Required string fields on the submitted mission. board + source +
-- target are the launcher's three picks; robot_id identifies the
-- recipient. All four are restricted to a conservative character set
-- (alnum / underscore / hyphen / dot) so they round-trip safely
-- through JSON, NATS subject paths, and ltree labels downstream.
local REQUIRED = { "robot_id", "board", "source", "target" }
local NAME_RE  = "^[%w_%-%.]+$"

function M.validate(input)
  if type(input) ~= "table" then
    return nil, "body must be a JSON object"
  end
  for _, key in ipairs(REQUIRED) do
    local v = input[key]
    if type(v) ~= "string" or v == "" then
      return nil, "missing or empty field: " .. key
    end
    if not v:match(NAME_RE) then
      return nil, "invalid characters in field: " .. key
    end
  end
  if input.source == input.target then
    return nil, "source and target must differ"
  end
  return true
end

------------------------------------------------------------------------
-- Mission-cmd shape
------------------------------------------------------------------------

-- Mirror action_server's M:submit expected shape:
--   { robot_id, board, start, stops[], bookend? }
-- The launcher submits a single source -> target leg today; the stops
-- list carries just the target. Multi-stop missions are a future UI
-- enhancement (chained click sequence).
--
-- stops[i] must be {node=...} table (mission_builder.lua:144 requires
-- stop.node). Passing a bare string surfaces as "missing node" error.
function M.build_mission(input)
  return {
    robot_id = input.robot_id,
    board    = input.board,
    start    = input.source,
    stops    = { { node = input.target } },
  }
end

-- The queue action_server claims from. Must match action_server.lua's
-- submit_nats / _drain_nats_queue expectations. Phase 7 (per-tenant):
-- subject is `<site>.planner.<ns>.action_server.missions`.
function M.queue_name(site, ns)
  return string.format("%s.planner.%s.action_server.missions", site, ns)
end

------------------------------------------------------------------------
-- KS / JQ lazy init
------------------------------------------------------------------------

-- Test shim: opts.ks_lib / opts.jq_lib injectable. Production path
-- requires("lib.nats_*"), which resolves through lua_package_path
-- (planner's lib/ is added in nginx.conf so planner_ui can reach the
-- shared FFI wrappers without copying them).
local function ensure_jq(opts)
  if _jq then return _jq end
  local ks_lib    = opts.ks_lib    or require("lib.nats_key_store")
  local jq_lib    = opts.jq_lib    or require("lib.nats_job_queue")
  local site      = opts.site
  local nats_url  = opts.nats_url
  local ns        = opts.planner_namespace
  local worker_id = opts.worker_id or "planner_ui"

  -- Phase 7: per-tenant bucket. NATS bucket names disallow dots so
  -- both site and namespace are normalized; matches action_server.lua's
  -- _ensure_nats convention exactly.
  local site_bucket = site:gsub("%.", "_")
  local ns_bucket   = ns:gsub("%.", "_")
  local bucket_name = string.format(
    "%s_planner_%s_action_server", site_bucket, ns_bucket)
  local k = ks_lib.KeyStore.new({
    server        = nats_url,
    bucket        = bucket_name,
    description   = string.format(
      "planner_ui mission submit (tenant '%s')", ns),
    create_bucket = true,
    history       = 1,
    client_name   = worker_id,
  })
  local connect_ok, kerr = pcall(function() k:connect() end)
  if not connect_ok then
    return nil, "ks connect: " .. tostring(kerr)
  end
  _ks = k
  _jq = jq_lib.JobQueue.new(_ks:handle(), worker_id)
  return _jq
end

------------------------------------------------------------------------
-- Submit
------------------------------------------------------------------------

-- Submit a (validated) mission. Returns job_id on success, or
-- (nil, err) on failure. Caller is responsible for validate() first;
-- this function does not re-validate.
--
-- opts test shim:
--   ks_lib, jq_lib       -- replace the FFI modules
--   cjson                -- replace cjson.safe (dotted reload)
--   site                 -- override APP_SITE
--   nats_url             -- override NATS_URL
--   planner_namespace    -- override PLANNER_NAMESPACE (Phase 7)
--   worker_id            -- override default "planner_ui"
function M.do_submit(input, opts)
  opts = opts or {}
  -- Env / opt checks BEFORE require("cjson.safe") so a host shell
  -- without cjson doesn't crash on early-exit paths during testing
  -- (per feedback_openresty_cosocket_resolver.md / submit.lua history).
  local site = opts.site or os.getenv("APP_SITE") or ""
  if site == "" then return nil, "APP_SITE not set" end
  local ns = opts.planner_namespace or os.getenv("PLANNER_NAMESPACE") or ""
  if ns == "" then return nil, "PLANNER_NAMESPACE not set" end
  local cjson    = opts.cjson    or require("cjson.safe")
  -- Resolution order for the NATS URL:
  --   1. opts.nats_url           (test shim)
  --   2. infra_discovery via pg  (matches planner worker; pg-rendez-vous)
  --   3. NATS_URL env            (operational override)
  --   4. cluster planner-net default
  -- Each stage falls through silently on failure so a broken pg or a
  -- registry row that hasn't been written yet doesn't take down submit.
  local nats_url = opts.nats_url
  if not nats_url then
    local ok_il, infra_lookup = pcall(require, "infra_lookup")
    local ok_db, db           = pcall(require, "db")
    if ok_il and ok_db then
      local u = infra_lookup.nats_url(db, { require_healthy = false })
      if u then nats_url = u end
    end
  end
  nats_url = nats_url or nonempty(os.getenv("NATS_URL"))
                     or "nats://nats-js-ram:4222"

  local mission = M.build_mission(input)
  local payload = cjson.encode(mission)
  if not payload then return nil, "failed to encode mission as JSON" end

  opts.site              = site
  opts.nats_url          = nats_url
  opts.planner_namespace = ns
  local jq, jerr = ensure_jq(opts)
  if not jq then return nil, jerr end

  local id_ok, job_id_or_err = pcall(function()
    return jq:submit(payload, M.queue_name(site, ns), 5, 1, 600)
  end)
  if not id_ok then
    return nil, "submit: " .. tostring(job_id_or_err)
  end
  return job_id_or_err
end

-- Test hook: drop the singleton so the next do_submit() rebuilds.
-- Production code should NOT call this -- it leaks a NATS connection.
function M._reset() _ks, _jq = nil, nil end

return M
