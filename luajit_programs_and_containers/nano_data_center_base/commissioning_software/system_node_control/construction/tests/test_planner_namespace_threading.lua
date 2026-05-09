#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_namespace_threading.lua -- Phase 5 C4 acceptance for
-- planner_namespace threading through action_server / kb_query /
-- link_manager / sequencer / hub_runtime.
--
-- Coverage:
--   kb_query.new:
--     - 5th positional arg planner_namespace stored + accessible
--     - default fallback to own_instance_id when nil
--
--   link_manager.new:
--     - opts.planner_namespace stored + accessible
--     - nil when not supplied (link_manager has no own_instance_id
--       fallback; partition logic added in Phase 6/7 will assert
--       presence)
--
--   action_server.new:
--     - opts.planner_namespace stored + accessible via getter
--     - PLANNER_NAMESPACE env var picked up when opt not supplied
--     - opts > env > own_instance_id priority
--     - threads through to link_manager (when mqtt_hub provided)
--
--   hub_runtime.new:
--     - opts.planner_namespace stored
--     - default fallback to own_instance_id
--
-- Construction-only tests; no live pg / NATS / MQTT contact.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
local LUA_SHARE  = REPO_ROOT
    .. "nano_data_center_base/luajit/luajit_base/container/prebuilt_lua_share"

package.path = PLANNER   .. "/lib/?.lua;"
            .. PLANNER   .. "/hub_dsl/protocol/?.lua;"
            .. PLANNER   .. "/hub_dsl/hub_functions/?.lua;"
            .. PLANNER   .. "/hub_dsl/kb_construct/?.lua;"
            .. LUA_SHARE .. "/?.lua;"
            .. LUA_SHARE .. "/chain_tree/lua_dsl/luajit_pipeline/?.lua;"
            .. package.path

-- Stub knowledge_base_manager so kb_query.new doesn't try to connect
-- to a live postgres at construction. We only test field threading
-- here, not query behavior, so the KBM instance just needs to exist.
package.preload["knowledge_base_manager"] = function()
  return {
    new = function(_table, _conn, _ro)
      return { _stub = true,
               find_by_pattern = function() return {} end,
               close = function() end }
    end,
  }
end

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- Reset PLANNER_NAMESPACE between tests so env-var leakage doesn't
-- alter results. Lua doesn't expose unsetenv, so we set "" and the
-- code paths under test treat empty string as nil-ish (they use
-- truthy-or fallback chains). Where exact-nil matters, we verify by
-- pre-checking that os.getenv returns "" or nil.
------------------------------------------------------------------------
local function clear_env()
  -- Use os.execute to clear the env var for this process.
  -- Workaround: Lua can't unsetenv, but we can spawn a subshell.
  -- For our purposes, setting to empty is sufficient because the
  -- code uses `or` chains: "" is truthy in Lua (only nil/false fall
  -- through), so we have to actually unset. ffi route:
  local ok_ffi, ffi = pcall(require, "ffi")
  if ok_ffi then
    pcall(ffi.cdef, "int unsetenv(const char *name);")
    pcall(ffi.C.unsetenv, "PLANNER_NAMESPACE")
  end
end

local function set_env(val)
  local ok_ffi, ffi = pcall(require, "ffi")
  if ok_ffi then
    pcall(ffi.cdef, "int setenv(const char *name, const char *value, int overwrite);")
    pcall(ffi.C.setenv, "PLANNER_NAMESPACE", val, 1)
  end
end

clear_env()

------------------------------------------------------------------------
print("== kb_query.new: planner_namespace 5th arg ==")
------------------------------------------------------------------------

do
  -- kb_query.new doesn't need a real pg_conn at construction
  -- (KBM.new on read-only mode just stores params); we pass a stub
  -- that satisfies the type assertion.
  local kb_query = require("kb_query")

  local pg_stub = { host = "localhost", port = 5432, dbname = "x",
                    user = "y", password = "z" }

  local q1 = kb_query.new(pg_stub, "ros_planner_ii",
    "moonbase.alpha.surface_ops", "mission_planner_01", "surface_ops")
  ok("explicit planner_namespace stored",
     q1:get_planner_namespace() == "surface_ops")

  local q2 = kb_query.new(pg_stub, "ros_planner_ii",
    "moonbase.alpha.surface_ops", "mission_planner_01")
  ok("nil planner_namespace falls back to own_instance_id",
     q2:get_planner_namespace() == "mission_planner_01")

  local q3 = kb_query.new(pg_stub, "ros_planner_ii",
    "moonbase.alpha.surface_ops", "mission_planner_01", nil)
  ok("explicit nil planner_namespace falls back",
     q3:get_planner_namespace() == "mission_planner_01")
end

------------------------------------------------------------------------
print()
print("== link_manager.new: opts.planner_namespace ==")
------------------------------------------------------------------------

do
  local link_manager = require("link_manager")

  -- link_manager.new's args are (mqtt_hub, kv_writer, site, opts);
  -- mqtt_hub + kv_writer are stored but not used at construction.
  local lm1 = link_manager.new({}, {}, "moonbase.alpha.surface_ops", {
    planner_namespace = "surface_ops",
  })
  ok("link_manager stores planner_namespace",
     lm1:get_planner_namespace() == "surface_ops")

  local lm2 = link_manager.new({}, {}, "moonbase.alpha.surface_ops", {})
  ok("link_manager: missing opt -> nil",
     lm2:get_planner_namespace() == nil)

  local lm3 = link_manager.new({}, {}, "moonbase.alpha.surface_ops")
  ok("link_manager: missing opts table -> nil",
     lm3:get_planner_namespace() == nil)
end

------------------------------------------------------------------------
print()
print("== action_server.new: priority order opts > env > own_instance_id ==")
------------------------------------------------------------------------

do
  -- action_server.new requires pg_conn + site + system_name + own_instance_id +
  -- nats_server. It doesn't actually CONNECT until the first NATS-needing
  -- method, so a stub pg_conn + nats URL string is fine.
  local action_server = require("action_server")

  local function build(extra)
    local opts = {
      pg_conn         = { host = "h", port = 1, dbname = "x",
                          user = "y", password = "z" },
      site            = "moonbase.alpha.surface_ops",
      system_name     = "ros_planner_ii",
      own_instance_id = "mission_planner_01",
      nats_server     = "nats://localhost:4222",
    }
    if extra then for k, v in pairs(extra) do opts[k] = v end end
    return action_server.new(opts)
  end

  -- 1. No opt, no env -> fallback to own_instance_id
  clear_env()
  local s1 = build()
  ok("no opt + no env -> own_instance_id",
     s1:get_planner_namespace() == "mission_planner_01")

  -- 2. Env set, no opt -> env wins over own_instance_id
  set_env("surface_ops")
  local s2 = build()
  ok("env=surface_ops, no opt -> 'surface_ops'",
     s2:get_planner_namespace() == "surface_ops")

  -- 3. Opt set, env set -> opt wins
  set_env("surface_ops")
  local s3 = build({ planner_namespace = "tunnel_ops" })
  ok("opt=tunnel_ops + env=surface_ops -> 'tunnel_ops'",
     s3:get_planner_namespace() == "tunnel_ops")

  -- 4. Opt set, env clear -> opt wins (no fallback)
  clear_env()
  local s4 = build({ planner_namespace = "tunnel_ops" })
  ok("opt=tunnel_ops + no env -> 'tunnel_ops'",
     s4:get_planner_namespace() == "tunnel_ops")

  clear_env()
end

------------------------------------------------------------------------
print()
print("== action_server -> link_manager threading ==")
------------------------------------------------------------------------

do
  -- When mqtt_hub is provided, action_server constructs link_manager
  -- and should pass planner_namespace through. mqtt_hub here is a
  -- stub; only set_link_handler is called at construction.
  local action_server = require("action_server")
  local stub_hub = {
    set_link_handler = function(self, _) end,
  }

  local srv = action_server.new({
    pg_conn           = { host = "h", port = 1, dbname = "x",
                          user = "y", password = "z" },
    site              = "moonbase.alpha.surface_ops",
    system_name       = "ros_planner_ii",
    own_instance_id   = "mission_planner_01",
    nats_server       = "nats://localhost:4222",
    mqtt_hub          = stub_hub,
    planner_namespace = "surface_ops",
  })

  ok("action_server stored planner_namespace",
     srv:get_planner_namespace() == "surface_ops")
  ok("link_manager received planner_namespace via opts",
     srv.link_mgr ~= nil
       and srv.link_mgr:get_planner_namespace() == "surface_ops")
end

------------------------------------------------------------------------
print()
print("== hub_runtime: planner_namespace stored + fallback ==")
------------------------------------------------------------------------

do
  local hub_runtime = require("hub_runtime")
  local stub_tx = { send_rpc = function() end,
                    recv_stream = function() end,
                    close = function() end }

  local hub1 = hub_runtime.new({
    robot_id          = "rover_1",
    site              = "moonbase.alpha.surface_ops",
    system_name       = "ros_planner_ii",
    own_instance_id   = "mission_planner_01",
    planner_namespace = "surface_ops",
    transport         = stub_tx,
  })
  ok("hub_runtime stores planner_namespace",
     hub1.planner_namespace == "surface_ops")

  local hub2 = hub_runtime.new({
    robot_id        = "rover_1",
    site            = "moonbase.alpha.surface_ops",
    system_name     = "ros_planner_ii",
    own_instance_id = "mission_planner_01",
    transport       = stub_tx,
  })
  ok("hub_runtime falls back to own_instance_id",
     hub2.planner_namespace == "mission_planner_01")
end

------------------------------------------------------------------------
print()
print("== Phase 5 C5: action_server use_drive_v2 default + escape hatch ==")
------------------------------------------------------------------------

do
  local action_server = require("action_server")

  local function clear_drive_envs()
    local ok_ffi, ffi = pcall(require, "ffi")
    if ok_ffi then
      pcall(ffi.cdef, "int unsetenv(const char *name);")
      pcall(ffi.C.unsetenv, "PLANNER_DRIVE_V2")
      pcall(ffi.C.unsetenv, "PLANNER_LEGACY_NAV")
    end
  end
  local function set_env(name, val)
    local ok_ffi, ffi = pcall(require, "ffi")
    if ok_ffi then
      pcall(ffi.cdef, "int setenv(const char *name, const char *value, int overwrite);")
      pcall(ffi.C.setenv, name, val, 1)
    end
  end

  local function build(extra)
    local opts = {
      pg_conn         = { host = "h", port = 1, dbname = "x",
                          user = "y", password = "z" },
      site            = "moonbase.alpha.surface_ops",
      system_name     = "ros_planner_ii",
      own_instance_id = "mission_planner_01",
      nats_server     = "nats://localhost:4222",
    }
    if extra then for k, v in pairs(extra) do opts[k] = v end end
    return action_server.new(opts)
  end

  -- Default (no opt, no env): drive_v2 ON
  clear_drive_envs()
  local s1 = build()
  ok("default (no opt, no env): use_drive_v2 = true",
     s1.use_drive_v2 == true)

  -- Explicit opt false: explicit override beats default
  clear_drive_envs()
  local s2 = build({ use_drive_v2 = false })
  ok("opts.use_drive_v2 = false explicitly disables",
     s2.use_drive_v2 == false)

  -- Explicit opt true: matches default but verifies the explicit path
  clear_drive_envs()
  local s3 = build({ use_drive_v2 = true })
  ok("opts.use_drive_v2 = true explicitly enables",
     s3.use_drive_v2 == true)

  -- PLANNER_LEGACY_NAV env=1 (escape hatch): forces legacy when no opt
  clear_drive_envs()
  set_env("PLANNER_LEGACY_NAV", "1")
  local s4 = build()
  ok("PLANNER_LEGACY_NAV=1 (no opt) -> use_drive_v2 = false",
     s4.use_drive_v2 == false)

  -- Opt overrides env (priority order honored)
  clear_drive_envs()
  set_env("PLANNER_LEGACY_NAV", "1")
  local s5 = build({ use_drive_v2 = true })
  ok("opts.use_drive_v2=true beats PLANNER_LEGACY_NAV=1",
     s5.use_drive_v2 == true)

  -- Opt false beats env unset
  clear_drive_envs()
  local s6 = build({ use_drive_v2 = false })
  ok("opts.use_drive_v2=false beats env-default-on",
     s6.use_drive_v2 == false)

  clear_drive_envs()
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
