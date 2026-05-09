#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_ui_status.lua -- Phase 5b C6 acceptance for the status
-- module + handlers.
--
-- Coverage:
--   status.summary_key / status_key / result_key      -- key shape
--   status.list_missions:
--     - APP_SITE missing rejection
--     - empty bucket (summary key missing) -> empty-but-shaped envelope
--     - populated summary -> sorted array, fields preserved
--     - decode failure -> error
--     - KS connect failure -> error
--     - lazy singleton reuse across calls
--   status.get_mission:
--     - input validation (nil, "", invalid chars)
--     - APP_SITE missing
--     - both keys missing -> {status=nil, result=nil} (handler maps to 404)
--     - status only present -> {status={...}, result=nil}
--     - both present -> {status={...}, result={...}}
--     - decode failure -> error
--   handler files + nginx routes
--
-- All KV access is stubbed via opts.ks_lib injection -- no real NATS,
-- no FFI .so required.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PUI        = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner_ui"
package.path = PUI .. "/lua/?.lua;" .. package.path

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
-- env helpers
------------------------------------------------------------------------
local ffi = require("ffi")
pcall(ffi.cdef, [[
  int setenv(const char *name, const char *value, int overwrite);
  int unsetenv(const char *name);
]])
local function set_env(n, v) pcall(ffi.C.setenv, n, v, 1) end
local function clear_env(n)  pcall(ffi.C.unsetenv, n) end

local status = require("status")

------------------------------------------------------------------------
-- KS stub: serves a configurable map of key->value, tracks calls.
------------------------------------------------------------------------

local function make_stubs(o)
  o = o or {}
  local trace = {
    ks_new_calls   = 0,
    connect_calls  = 0,
    get_calls      = {},
    last_ks_opts   = nil,
  }
  local kv = o.kv or {}
  local ks_obj = {
    connect = function(self)
      trace.connect_calls = trace.connect_calls + 1
      if o.connect_err then error(o.connect_err) end
    end,
    get = function(self, key)
      trace.get_calls[#trace.get_calls + 1] = key
      if o.get_err then error(o.get_err) end
      return kv[key]
    end,
  }
  local ks_lib = {
    KeyStore = { new = function(opts)
      trace.ks_new_calls = trace.ks_new_calls + 1
      trace.last_ks_opts = opts
      return ks_obj
    end },
  }
  local cjson_stub = {
    decode = function(s)
      if o.decode_err then return nil, o.decode_err end
      -- Toy decoder: stored values are Lua tables disguised as a
      -- "JSON:<id>" sentinel; the kv map carries the real table under
      -- a parallel _decoded map keyed by the same sentinel.
      local table_for = (o.decoded or {})[s]
      if table_for then return table_for end
      return nil, "stub decoder: unknown payload " .. tostring(s)
    end,
  }
  return ks_lib, cjson_stub, trace
end

------------------------------------------------------------------------
print("== status.summary_key / status_key / result_key ==")
------------------------------------------------------------------------

do
  ok("summary key shape (with planner.<ns>)",
     status.summary_key("siteA", "tA") ==
     "siteA.planner.tA.action_server.summary")
  ok("status key shape (with planner.<ns>)",
     status.status_key("siteA", "tA", "rover_1") ==
     "siteA.planner.tA.action_server.rover_1.status")
  ok("result key shape (with planner.<ns>)",
     status.result_key("siteA", "tA", "rover_1") ==
     "siteA.planner.tA.action_server.rover_1.result")
end

------------------------------------------------------------------------
print()
print("== status.list_missions ==")
------------------------------------------------------------------------

do
  status._reset()
  clear_env("APP_SITE")
  clear_env("PLANNER_NAMESPACE")
  local r, err = status.list_missions()
  ok("missing APP_SITE rejected",
     r == nil and err and err:find("APP_SITE not set"), err)

  -- APP_SITE present but PLANNER_NAMESPACE missing -> error (Phase 7).
  -- Pass a cjson stub so the host doesn't crash on require("cjson.safe")
  -- before the env-check fires.
  status._reset()
  set_env("APP_SITE", "siteA")
  local stub_ks_lib, stub_cjs = make_stubs({})
  r, err = status.list_missions({
    ks_lib = stub_ks_lib, cjson = stub_cjs, nats_url = "nats://x",
  })
  ok("missing PLANNER_NAMESPACE rejected (Phase 7)",
     r == nil and err and err:find("PLANNER_NAMESPACE not set"), err)
  clear_env("APP_SITE")

  -- empty bucket: summary key missing -> empty-but-shaped envelope
  status._reset()
  local ks_lib, cjs, trace = make_stubs({})  -- empty kv
  local payload, e = status.list_missions({
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("empty bucket -> envelope returned (no error)", payload ~= nil, e)
  ok("envelope.missions is empty array",
     type(payload.missions) == "table" and #payload.missions == 0)
  ok("envelope.active_missions = 0",
     payload.active_missions == 0)
  ok("envelope.timestamp is nil", payload.timestamp == nil)
  ok("KS opts: bucket = <site>_planner_<ns>_action_server",
     trace.last_ks_opts and trace.last_ks_opts.bucket ==
     "siteA_planner_tA_action_server",
     "got " .. tostring(trace.last_ks_opts and trace.last_ks_opts.bucket))
  ok("KS opts: create_bucket = false (read-only client)",
     trace.last_ks_opts and trace.last_ks_opts.create_bucket == false)
  ok("KS opts: worker_id default = planner_ui_status",
     trace.last_ks_opts and trace.last_ks_opts.client_name ==
     "planner_ui_status")
  ok("KS:get called with per-tenant summary key",
     trace.get_calls[1] == "siteA.planner.tA.action_server.summary",
     trace.get_calls[1])

  -- populated summary
  status._reset()
  local fake_summary = {
    active_missions = 2,
    missions = {
      rover_2 = { state = "active",   board = "habitat" },
      rover_1 = { state = "planning", board = "landing_zone" },
    },
    registered_robots = { "rover_1", "rover_2" },
    timestamp = "2026-05-10T12:00:00Z",
  }
  ks_lib, cjs, trace = make_stubs({
    kv = { ["siteA.planner.tA.action_server.summary"] = "JSON:summary" },
    decoded = { ["JSON:summary"] = fake_summary },
  })
  payload = status.list_missions({
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("populated summary -> 2 missions", #payload.missions == 2)
  ok("missions sorted by robot_id alpha (rover_1 first)",
     payload.missions[1].robot_id == "rover_1")
  ok("first mission state preserved",
     payload.missions[1].state == "planning")
  ok("first mission board preserved",
     payload.missions[1].board == "landing_zone")
  ok("active_missions copied", payload.active_missions == 2)
  ok("timestamp copied",
     payload.timestamp == "2026-05-10T12:00:00Z")
  ok("registered_robots copied",
     #payload.registered_robots == 2 and
     payload.registered_robots[1] == "rover_1")

  -- decode failure
  status._reset()
  ks_lib, cjs, trace = make_stubs({
    kv = { ["siteA.planner.tA.action_server.summary"] = "garbage" },
    -- no `decoded` entry; cjson.decode returns nil + err
  })
  r, err = status.list_missions({
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("decode failure -> error",
     r == nil and err and err:find("decode summary"), err)

  -- KS connect failure
  status._reset()
  ks_lib, cjs = make_stubs({ connect_err = "nats unreachable" })
  r, err = status.list_missions({
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("ks connect failure -> error",
     r == nil and err and err:find("ks connect"), err)
end

------------------------------------------------------------------------
print()
print("== status.list_missions: lazy singleton reuse ==")
------------------------------------------------------------------------

do
  status._reset()
  local ks_lib, cjs, trace = make_stubs({})
  local opts = { ks_lib = ks_lib, cjson = cjs,
                 site = "siteA", planner_namespace = "tA",
                 nats_url = "nats://x" }
  status.list_missions(opts)
  status.list_missions(opts)
  status.list_missions(opts)
  ok("KeyStore.new called once across 3 list calls",
     trace.ks_new_calls == 1, "called " .. trace.ks_new_calls)
  ok("connect called once (lazy init)",
     trace.connect_calls == 1)
  ok("get called 3 times (one per list_missions)",
     #trace.get_calls == 3)
end

------------------------------------------------------------------------
print()
print("== status.get_mission: input validation ==")
------------------------------------------------------------------------

do
  status._reset()
  local r, err = status.get_mission(nil)
  ok("nil robot_id rejected",
     r == nil and err == "robot_id required")

  r, err = status.get_mission("")
  ok("empty robot_id rejected",
     r == nil and err == "robot_id required")

  r, err = status.get_mission("rover/1")
  ok("invalid char rejected",
     r == nil and err == "invalid robot_id")

  r, err = status.get_mission("rover with space")
  ok("space rejected",
     r == nil and err == "invalid robot_id")
end

------------------------------------------------------------------------
print()
print("== status.get_mission: KV reads ==")
------------------------------------------------------------------------

do
  -- both keys missing
  status._reset()
  local ks_lib, cjs, trace = make_stubs({ kv = {} })
  local detail = status.get_mission("rover_1", {
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("both keys missing -> detail returned (handler maps to 404)",
     detail ~= nil)
  ok("status field nil",  detail and detail.status == nil)
  ok("result field nil",  detail and detail.result == nil)
  ok("two get calls (status then result)",
     #trace.get_calls == 2 and
     trace.get_calls[1] == "siteA.planner.tA.action_server.rover_1.status" and
     trace.get_calls[2] == "siteA.planner.tA.action_server.rover_1.result")

  -- status present, result missing (mission still running)
  status._reset()
  local fake_status = { state = "active", current_packet = 3,
                        robot_id = "rover_1",
                        timestamp = "2026-05-10T12:00:01Z" }
  ks_lib, cjs, trace = make_stubs({
    kv = { ["siteA.planner.tA.action_server.rover_1.status"] = "JSON:status" },
    decoded = { ["JSON:status"] = fake_status },
  })
  detail = status.get_mission("rover_1", {
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("status present -> field decoded",
     detail and detail.status and detail.status.state == "active")
  ok("result still nil (mission running)", detail and detail.result == nil)

  -- both present (mission complete)
  status._reset()
  local fake_status2 = { state = "complete", robot_id = "rover_1",
                         timestamp = "2026-05-10T12:01:00Z" }
  local fake_result  = { success = true, completed = 5, total = 5,
                         elapsed_ms = 4321 }
  ks_lib, cjs, trace = make_stubs({
    kv = {
      ["siteA.planner.tA.action_server.rover_1.status"] = "JSON:s2",
      ["siteA.planner.tA.action_server.rover_1.result"] = "JSON:r1",
    },
    decoded = { ["JSON:s2"] = fake_status2, ["JSON:r1"] = fake_result },
  })
  detail = status.get_mission("rover_1", {
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("status decoded",
     detail and detail.status and detail.status.state == "complete")
  ok("result decoded",
     detail and detail.result and detail.result.success == true and
     detail.result.completed == 5)

  -- decode failure
  status._reset()
  ks_lib, cjs, _ = make_stubs({
    kv = { ["siteA.planner.tA.action_server.rover_1.status"] = "garbage" },
  })
  local r, err = status.get_mission("rover_1", {
    ks_lib = ks_lib, cjson = cjs,
    site = "siteA", planner_namespace = "tA", nats_url = "nats://x",
  })
  ok("status decode failure -> error",
     r == nil and err and err:find("decode siteA%.planner%.tA%.action_server"),
     err)
end

------------------------------------------------------------------------
print()
print("== handler / chassis files ==")
------------------------------------------------------------------------

do
  for _, name in ipairs({
    "status.lua", "api_missions.lua", "api_mission.lua",
  }) do
    local chunk, err = loadfile(PUI .. "/lua/" .. name)
    ok(name .. " parses cleanly",
       chunk ~= nil, err and tostring(err) or "")
  end

  local nginx = read_file(PUI .. "/conf/nginx.conf")
  ok("nginx.conf has GET /api/missions location",
     nginx and nginx:find("location = /api/missions", 1, true) ~= nil)
  ok("nginx.conf has /api/mission/<robot> regex location",
     nginx and nginx:find("/api/mission/", 1, true) ~= nil)
  ok("nginx.conf wires api_missions.lua",
     nginx and nginx:find("api_missions.lua", 1, true) ~= nil)
  ok("nginx.conf wires api_mission.lua",
     nginx and nginx:find("api_mission.lua", 1, true) ~= nil)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
