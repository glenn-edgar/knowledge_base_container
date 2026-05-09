#!/usr/bin/env luajit
-- =============================================================================
-- test_action_server_phase7.lua -- Phase 7 C3 acceptance for the
-- per-tenant NATS bucket + key/subject naming on action_server.
--
-- Per project_phase7_multitenant_design.md Q3 (Pattern 1, locked):
--   bucket: <site_bucket>_planner_<ns>_action_server   (and _mission_log)
--   keys:   <site>.planner.<ns>.action_server.<...>
--   subject for queue: <site>.planner.<ns>.action_server.missions
--
-- Stubs every external dependency (pg_conn, ks_lib, jq_lib, kb_query,
-- json_util) via package.preload so _ensure_nats and the publisher
-- helpers run without a live NATS / pg.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
local LUAJIT_BASE = REPO_ROOT .. "nano_data_center_base/luajit/luajit_base"
package.path = PLANNER .. "/lib/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/" ..
                  "chain_tree/lua_dsl/luajit_pipeline/?.lua;" ..
               package.path

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

------------------------------------------------------------------------
print("== source-level: action_server.lua uses per-tenant prefix ==")
------------------------------------------------------------------------

local AS_PATH = PLANNER .. "/lib/action_server.lua"
local src = read_file(AS_PATH)
ok("action_server.lua readable", src ~= nil)

if src then
  -- Per-tenant prefix is constructed in _ensure_nats
  ok("_action_server_prefix is built with planner_namespace",
     src:find('self%._action_server_prefix = string%.format%(') ~= nil and
     src:find('"%%s%.planner%.%%s%.action_server"', 1, false) ~= nil and
     src:find("self%.planner_namespace") ~= nil)

  -- Bucket names include _planner_<ns>_ segment (with underscore-
  -- normalized namespace for NATS bucket-name rules).
  ok("bucket name template includes _planner_<ns>_action_server",
     src:find('"%%s_planner_%%s_action_server"', 1, false) ~= nil)
  ok("bucket name template includes _planner_<ns>_mission_log",
     src:find('"%%s_planner_%%s_mission_log"', 1, false) ~= nil)
  ok("ns_bucket normalizes dots to underscores",
     src:find("ns_bucket   = self.planner_namespace:gsub", 1, true) ~= nil)

  -- Every publisher / consumer uses _action_server_prefix
  for _, suffix in ipairs({
    '"%.status"', '"%.result"', '"%.summary"',
    '"%.mission_log"', '"%.missions"',
  }) do
    ok("uses _action_server_prefix .. " .. suffix,
       src:find("self%._action_server_prefix %.%. " .. suffix) ~= nil
       or src:find('self%._action_server_prefix %.%. "%."') ~= nil)
  end

  -- The OLD site-level construction is gone everywhere it mattered.
  -- (Kept allow-listed: _read_robot_energy still uses self.site.robots.*
  -- pending Phase 7 robot publisher work — has a TODO comment. Don't
  -- false-fail on that one.)
  local site_action = "self.site .. \".action_server"
  ok("no remaining `self.site .. \".action_server` constructions",
     src:find(site_action, 1, true) == nil,
     "found stale site-level action_server reference")

  ok("_read_robot_energy carries TODO for Phase 7 robot publisher",
     src:find("TODO Phase 7 follow%-up", 1, false) ~= nil)
end

------------------------------------------------------------------------
print()
print("== runtime: stub-instantiate action_server + capture bucket+keys ==")
------------------------------------------------------------------------

-- Trace what gets passed to KeyStore.new + JobQueue.new + ks:put / get.
local trace = {
  ks_buckets = {},
  jq_subjects_submit = {},
  jq_subjects_claim = {},
  ks_puts = {},
  ks_gets = {},
}

local function reset_trace()
  trace.ks_buckets = {}
  trace.jq_subjects_submit = {}
  trace.jq_subjects_claim = {}
  trace.ks_puts = {}
  trace.ks_gets = {}
end

-- Stubs

package.preload["lib.nats_key_store"] = function()
  local KS = {}; KS.__index = KS
  function KS.new(opts)
    trace.ks_buckets[#trace.ks_buckets + 1] = opts.bucket
    return setmetatable({ _bucket = opts.bucket, _kv = {} }, KS)
  end
  function KS:connect() self._connected = true end
  function KS:handle()  return self end
  function KS:put(k, v)
    trace.ks_puts[#trace.ks_puts + 1] = { bucket = self._bucket, key = k }
    self._kv[k] = v
  end
  function KS:get(k)
    trace.ks_gets[#trace.ks_gets + 1] = { bucket = self._bucket, key = k }
    return self._kv[k]
  end
  return { KeyStore = KS }
end

package.preload["lib.nats_job_queue"] = function()
  local JQ = {}; JQ.__index = JQ
  function JQ.new(_handle, _worker_id)
    return setmetatable({}, JQ)
  end
  function JQ:submit(payload, subject, ...)
    trace.jq_subjects_submit[#trace.jq_subjects_submit + 1] = subject
    return "FAKE_JOB_ID"
  end
  function JQ:claim_job(subjects)
    trace.jq_subjects_claim[#trace.jq_subjects_claim + 1] = subjects[1]
    return nil   -- pretend no jobs available
  end
  return { JobQueue = JQ }
end

-- Minimal stubs for the rest of action_server's require chain so the
-- file can be loaded without pg / chain_tree / mqtt deps.
package.preload["json_util"] = function()
  return { encode = function(t) return "FAKE_JSON" end,
           decode = function(s) return {} end }
end
package.preload["kb_query"] = function()
  return { new = function(...)
    return { list_boards = function() return {} end,
             close = function() end } end }
end
package.preload["link_manager"] = function()
  return { new = function() return {
    list_live = function() return {} end,
    set_link_handler = function() end,
    tick = function() end,
  } end }
end
package.preload["mission_builder"] = function() return {
  new = function() return {} end,
  rebuild = function() return nil end,
} end
package.preload["sequencer"] = function() return {
  new = function() return {} end,
} end
package.preload["hub_runtime"] = function() return {
  new = function() return { activate_kb = function() end,
                            deactivate_kb = function() end,
                            get_global_pose = function() return {} end,
                            poll_and_route = function() end } end,
} end
package.preload["global_planner"] = function() return {
  new = function() return {} end,
} end
package.preload["kb_runtime"] = function() return {} end
package.preload["kv_writer"]  = function() return {
  new = function() return { tick = function() end } end,
} end

local action_server = require("action_server")

------------------------------------------------------------------------

do
  reset_trace()
  local srv = action_server.new({
    pg_conn           = { host = "x" },
    site              = "moon_base.alpha",
    system_name       = "moon_base",
    own_instance_id   = "mission_planner_01",
    nats_server       = "nats://stub:4222",
    planner_namespace = "surface_ops",
  })
  ok("action_server instantiated", srv ~= nil)
  ok("planner_namespace stored",
     srv:get_planner_namespace() == "surface_ops",
     srv:get_planner_namespace())

  -- Force NATS init (lazy until first publish/get/submit)
  srv:_ensure_nats()
  ok("two buckets created (action_server + mission_log)",
     #trace.ks_buckets == 2, "got " .. #trace.ks_buckets)

  ok("action_server bucket name = <site_bucket>_planner_<ns>_action_server",
     trace.ks_buckets[1] == "moon_base_alpha_planner_surface_ops_action_server",
     "got " .. tostring(trace.ks_buckets[1]))
  ok("mission_log bucket name = <site_bucket>_planner_<ns>_mission_log",
     trace.ks_buckets[2] == "moon_base_alpha_planner_surface_ops_mission_log",
     "got " .. tostring(trace.ks_buckets[2]))

  -- Exercise each publisher and confirm the keys carry planner.<ns>
  srv:_publish_status("rover_1", { state = "active" })
  ok("publish_status key = <site>.planner.<ns>.action_server.<robot>.status",
     trace.ks_puts[1] and trace.ks_puts[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.rover_1.status",
     trace.ks_puts[1] and trace.ks_puts[1].key)

  reset_trace()
  srv:_ensure_nats()  -- already inited, no new buckets
  srv:_publish_result("rover_1", { success = true, completed = 5,
                                    total = 5, elapsed_ms = 100,
                                    replans = 0 })
  ok("publish_result key = <site>.planner.<ns>.action_server.<robot>.result",
     trace.ks_puts[1] and trace.ks_puts[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.rover_1.result",
     trace.ks_puts[1] and trace.ks_puts[1].key)

  reset_trace()
  srv:_publish_summary()
  ok("publish_summary key = <site>.planner.<ns>.action_server.summary",
     trace.ks_puts[1] and trace.ks_puts[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.summary",
     trace.ks_puts[1] and trace.ks_puts[1].key)

  reset_trace()
  srv:_publish_mission_log("rover_1", { success = true, completed = 5,
                                         total = 5, elapsed_ms = 100,
                                         replans = 0 }, "landing_zone")
  ok("publish_mission_log key = <site>.planner.<ns>.action_server.mission_log",
     trace.ks_puts[1] and trace.ks_puts[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.mission_log",
     trace.ks_puts[1] and trace.ks_puts[1].key)

  reset_trace()
  srv:get_mission_status("rover_1")
  ok("get_mission_status reads <site>.planner.<ns>.action_server.<robot>.status",
     trace.ks_gets[1] and trace.ks_gets[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.rover_1.status",
     trace.ks_gets[1] and trace.ks_gets[1].key)

  reset_trace()
  srv:get_mission_result("rover_1")
  ok("get_mission_result reads <site>.planner.<ns>.action_server.<robot>.result",
     trace.ks_gets[1] and trace.ks_gets[1].key ==
     "moon_base.alpha.planner.surface_ops.action_server.rover_1.result",
     trace.ks_gets[1] and trace.ks_gets[1].key)

  reset_trace()
  srv:submit_nats({ robot_id = "rover_1", board = "x" })
  ok("submit_nats subject = <site>.planner.<ns>.action_server.missions",
     trace.jq_subjects_submit[1] ==
     "moon_base.alpha.planner.surface_ops.action_server.missions",
     trace.jq_subjects_submit[1])

  reset_trace()
  srv:_drain_nats_queue()
  ok("_drain_nats_queue subject = <site>.planner.<ns>.action_server.missions",
     trace.jq_subjects_claim[1] ==
     "moon_base.alpha.planner.surface_ops.action_server.missions",
     trace.jq_subjects_claim[1])
end

------------------------------------------------------------------------
print()
print("== bucket name normalizes dots in namespace ==")
------------------------------------------------------------------------

do
  reset_trace()
  -- Tenant name with dots (e.g. "team.alpha"); bucket must collapse
  -- them to underscores per NATS rules.
  local srv = action_server.new({
    pg_conn           = { host = "x" },
    site              = "moon.alpha",
    system_name       = "moon",
    own_instance_id   = "mp_01",
    nats_server       = "nats://stub",
    planner_namespace = "team.alpha",
  })
  srv:_ensure_nats()
  ok("dotted namespace -> underscored in bucket",
     trace.ks_buckets[1] == "moon_alpha_planner_team_alpha_action_server",
     "got " .. tostring(trace.ks_buckets[1]))
  -- but subjects/keys keep dots
  reset_trace()
  srv:_publish_summary()
  ok("dotted namespace kept in subject/key",
     trace.ks_puts[1] and trace.ks_puts[1].key ==
     "moon.alpha.planner.team.alpha.action_server.summary",
     trace.ks_puts[1] and trace.ks_puts[1].key)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
