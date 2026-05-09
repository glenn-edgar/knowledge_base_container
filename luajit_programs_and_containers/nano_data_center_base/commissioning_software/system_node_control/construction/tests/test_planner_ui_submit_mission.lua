#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_ui_submit_mission.lua -- Phase 5b C5 acceptance for the
-- mission launcher's submit module + handler parse.
--
-- Coverage:
--   submit.validate    -- type / missing / empty / invalid-char /
--                        source==target rejections; happy path
--   submit.build_mission -- shape mirrors action_server.M:submit's expectation
--   submit.queue_name  -- "<site>.action_server.missions"
--   submit.do_submit   -- env validation, lazy KS/JQ init, args passed
--                        to jq:submit, ks connect failure, submit raise,
--                        cjson.encode failure, singleton reuse
--   handler files      -- api_submit_mission.lua + submit.lua parse cleanly
--   nginx.conf         -- POST route + extended lua_package_path present
--   shell_page.lua     -- launcher elements injected
--
-- The submit module accepts an `opts` shim for every external dep
-- (ks_lib, jq_lib, cjson, site, nats_url), so we exercise the full
-- code path without needing a real NATS server or the FFI .so.
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
local function set_env(n, v)  pcall(ffi.C.setenv, n, v, 1) end
local function clear_env(n)   pcall(ffi.C.unsetenv, n) end

local submit = require("submit")

------------------------------------------------------------------------
-- Stub factories: KS + JQ + cjson
------------------------------------------------------------------------

-- Track every ks_lib.KeyStore.new + ks:connect + jq.submit call.
local function make_stubs(o)
  o = o or {}
  local trace = {
    ks_new_calls   = 0,
    jq_new_calls   = 0,
    submit_calls   = {},   -- { {payload, queue, prio, retries, timeout} }
    last_ks_opts   = nil,
    last_worker_id = nil,
  }
  local ks_obj = {
    connect = function(self)
      if o.ks_connect_err then error(o.ks_connect_err) end
      self._connected = true
    end,
    handle = function(self) return "FAKE_KS_HANDLE" end,
  }
  local ks_lib = {
    KeyStore = { new = function(opts)
      trace.ks_new_calls = trace.ks_new_calls + 1
      trace.last_ks_opts = opts
      return ks_obj
    end },
  }
  local jq_obj = {
    submit = function(self, payload, queue, prio, retries, timeout)
      trace.submit_calls[#trace.submit_calls + 1] = {
        payload  = payload, queue = queue,
        priority = prio,    max_retries = retries,
        timeout  = timeout,
      }
      if o.submit_err then error(o.submit_err) end
      return o.submit_id or "JOB_FAKE_001"
    end,
  }
  local jq_lib = {
    JobQueue = { new = function(handle, worker_id)
      trace.jq_new_calls   = trace.jq_new_calls + 1
      trace.last_worker_id = worker_id
      return jq_obj
    end },
  }
  local cjson_stub = {
    encode = function(t)
      -- `x and nil or y` short-circuits to y because (true and nil)
      -- is nil and `nil or y` is y. Use an explicit branch.
      if o.encode_err then return nil end
      return "ENCODED:" .. (t and t.robot_id or "?")
    end,
  }
  return ks_lib, jq_lib, cjson_stub, trace
end

------------------------------------------------------------------------
print("== submit.validate ==")
------------------------------------------------------------------------

do
  local r, err = submit.validate(nil)
  ok("nil rejected", r == nil and err and err:find("JSON object"), err)

  r, err = submit.validate("not a table")
  ok("string rejected", r == nil and err and err:find("JSON object"))

  for _, key in ipairs({"robot_id", "board", "source", "target"}) do
    local input = { robot_id = "r", board = "b", source = "s", target = "t" }
    input[key] = nil
    r, err = submit.validate(input)
    ok("missing " .. key .. " rejected",
       r == nil and err and err:find(key, 1, true))
    input[key] = ""
    r, err = submit.validate(input)
    ok("empty " .. key .. " rejected",
       r == nil and err and err:find(key, 1, true))
  end

  r, err = submit.validate({ robot_id = "rover/1", board = "b",
                             source = "s", target = "t" })
  ok("invalid char in robot_id rejected (slash)",
     r == nil and err and err:find("invalid characters"), err)

  r, err = submit.validate({ robot_id = "r", board = "b",
                             source = "x; DROP TABLE",
                             target = "t" })
  ok("invalid char in source rejected (semicolon + space)",
     r == nil and err and err:find("invalid characters"))

  r, err = submit.validate({ robot_id = "r", board = "b",
                             source = "same", target = "same" })
  ok("source == target rejected",
     r == nil and err and err:find("must differ"), err)

  -- Happy path: dotted + dashed + underscore allowed
  r = submit.validate({ robot_id = "rover_1",
                        board    = "landing.zone",
                        source   = "lander-pad",
                        target   = "habitat_site" })
  ok("happy path returns true", r == true)
end

------------------------------------------------------------------------
print()
print("== submit.build_mission ==")
------------------------------------------------------------------------

do
  local m = submit.build_mission({
    robot_id = "rover_1", board = "landing_zone",
    source = "lander_pad", target = "habitat_site",
  })
  ok("robot_id mirrored", m.robot_id == "rover_1")
  ok("board mirrored",    m.board    == "landing_zone")
  ok("source -> start",   m.start    == "lander_pad")
  ok("target -> stops[1]",
     type(m.stops) == "table" and m.stops[1] == "habitat_site"
     and #m.stops == 1)
end

------------------------------------------------------------------------
print()
print("== submit.queue_name ==")
------------------------------------------------------------------------

do
  ok("simple site",
     submit.queue_name("moonbase") == "moonbase.action_server.missions")
  ok("dotted site",
     submit.queue_name("ros_planner_ii.moonbase.alpha") ==
     "ros_planner_ii.moonbase.alpha.action_server.missions")
end

------------------------------------------------------------------------
print()
print("== submit.do_submit: env + happy path ==")
------------------------------------------------------------------------

do
  submit._reset()
  clear_env("APP_SITE")
  local input = { robot_id = "rover_1", board = "landing_zone",
                  source = "a", target = "b" }
  local ks_lib, jq_lib, cjs, trace = make_stubs()
  local id, err = submit.do_submit(input, {
    ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
  })
  ok("missing APP_SITE rejected",
     id == nil and err and err:find("APP_SITE not set"), err)

  -- Happy path
  submit._reset()
  ks_lib, jq_lib, cjs, trace = make_stubs()
  id, err = submit.do_submit(input, {
    ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
    site = "ros_planner_ii.moonbase.alpha", nats_url = "nats://stub:4222",
  })
  ok("happy path returns job_id",   id == "JOB_FAKE_001", err)
  ok("ks_lib.KeyStore.new called",  trace.ks_new_calls == 1)
  ok("jq_lib.JobQueue.new called",  trace.jq_new_calls == 1)
  ok("KeyStore opts carry NATS url",
     trace.last_ks_opts and
     trace.last_ks_opts.server == "nats://stub:4222")
  ok("KeyStore bucket name normalizes dots to underscores",
     trace.last_ks_opts and trace.last_ks_opts.bucket ==
     "ros_planner_ii_moonbase_alpha_action_server",
     "got " .. tostring(trace.last_ks_opts and trace.last_ks_opts.bucket))
  ok("KeyStore create_bucket=true",
     trace.last_ks_opts and trace.last_ks_opts.create_bucket == true)
  ok("worker_id defaults to planner_ui",
     trace.last_worker_id == "planner_ui")

  ok("submit called once", #trace.submit_calls == 1)
  local call = trace.submit_calls[1]
  ok("submit payload is JSON-encoded mission",
     call and call.payload == "ENCODED:rover_1", call and call.payload)
  ok("submit queue is <site>.action_server.missions",
     call and call.queue ==
     "ros_planner_ii.moonbase.alpha.action_server.missions",
     call and call.queue)
  ok("submit priority = 5",     call and call.priority == 5)
  ok("submit max_retries = 1",  call and call.max_retries == 1)
  ok("submit timeout = 600",    call and call.timeout == 600)
end

------------------------------------------------------------------------
print()
print("== submit.do_submit: lazy singleton reuse ==")
------------------------------------------------------------------------

do
  submit._reset()
  local ks_lib, jq_lib, cjs, trace = make_stubs()
  local opts = { ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
                 site = "siteX", nats_url = "nats://x" }
  local input = { robot_id = "r1", board = "b1", source = "s", target = "t" }
  submit.do_submit(input, opts)
  submit.do_submit(input, opts)
  submit.do_submit(input, opts)
  ok("KeyStore.new called once across 3 submits",
     trace.ks_new_calls == 1, "called " .. trace.ks_new_calls .. " times")
  ok("JobQueue.new called once across 3 submits",
     trace.jq_new_calls == 1, "called " .. trace.jq_new_calls .. " times")
  ok("submit invoked 3 times",
     #trace.submit_calls == 3,
     "called " .. #trace.submit_calls .. " times")
end

------------------------------------------------------------------------
print()
print("== submit.do_submit: error paths ==")
------------------------------------------------------------------------

do
  submit._reset()
  local ks_lib, jq_lib, cjs = make_stubs({ ks_connect_err = "boom" })
  local id, err = submit.do_submit(
    { robot_id = "r", board = "b", source = "s", target = "t" },
    { ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
      site = "siteX", nats_url = "nats://x" })
  ok("ks connect failure -> error",
     id == nil and err and err:find("ks connect"), err)

  submit._reset()
  ks_lib, jq_lib, cjs = make_stubs({ submit_err = "nats unreachable" })
  id, err = submit.do_submit(
    { robot_id = "r", board = "b", source = "s", target = "t" },
    { ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
      site = "siteX", nats_url = "nats://x" })
  ok("jq:submit raise -> error",
     id == nil and err and err:find("submit:") and err:find("nats unreachable"),
     err)

  submit._reset()
  ks_lib, jq_lib, cjs = make_stubs({ encode_err = true })
  id, err = submit.do_submit(
    { robot_id = "r", board = "b", source = "s", target = "t" },
    { ks_lib = ks_lib, jq_lib = jq_lib, cjson = cjs,
      site = "siteX", nats_url = "nats://x" })
  ok("cjson encode failure -> error",
     id == nil and err and err:find("encode"), err)
end

------------------------------------------------------------------------
print()
print("== handler / chassis files ==")
------------------------------------------------------------------------

do
  for _, name in ipairs({ "submit.lua", "api_submit_mission.lua" }) do
    local chunk, err = loadfile(PUI .. "/lua/" .. name)
    ok(name .. " parses cleanly",
       chunk ~= nil, err and tostring(err) or "")
  end

  local nginx = read_file(PUI .. "/conf/nginx.conf")
  ok("nginx.conf has POST /api/submit_mission location",
     nginx and nginx:find("/api/submit_mission", 1, true) ~= nil)
  ok("nginx.conf wires api_submit_mission.lua",
     nginx and nginx:find("api_submit_mission.lua", 1, true) ~= nil)
  ok("nginx.conf extended lua_package_path to planner/lib",
     nginx and nginx:find("/opt/apps/planner/lib/?.lua", 1, true) ~= nil)
  ok("nginx.conf caps body size for submit",
     nginx and nginx:find("client_max_body_size", 1, true) ~= nil)

  local shell = read_file(PUI .. "/lua/shell_page.lua")
  ok("shell_page has #launcher-bar",
     shell and shell:find('id="launcher%-bar"') ~= nil)
  ok("shell_page has #robot-input",
     shell and shell:find('id="robot%-input"') ~= nil)
  ok("shell_page has #launcher-mode-btn",
     shell and shell:find('id="launcher%-mode%-btn"') ~= nil)
  ok("shell_page has #submit-mission-btn",
     shell and shell:find('id="submit%-mission%-btn"') ~= nil)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
