#!/usr/bin/env luajit
-- =============================================================================
-- test_robot_sim_package.lua -- ROBSIM C2 acceptance for the
-- robot_sim app_containers package + topology + definitions changes.
--
-- Coverage:
--   definitions.lua: robot_sim def present with kind=application,
--     image=nanodatacenter/robot-sim:latest, networks=planner-net,
--     no port_spec (headless container)
--   topology.lua: robot_sim_rover_1 instance on cpu_02 with
--     def=robot_sim + params{robot_id, planner_namespace, capabilities}
--   app_containers/robot_sim/manifest.lua, container_spec.lua,
--     kb_build.lua all parse cleanly + return expected shapes
--   container/Dockerfile: FROM luajit-base, installs libmosquitto1,
--     vendors libmqtt_pubsub.so, runs bundle_controller
--   container/robot_sim/main.lua parses cleanly + has expected
--     behavior surface (env reads, MQTT bring-up, RPC echo handler,
--     heartbeat loop, no signal handler)
--   container/robot_sim/app.manifest.json declares the supervised
--     process correctly
--   vendored library files present (mock_mqtt_robot_lib.lua,
--     lib/mqtt_pubsub.lua, prebuilt_libs/libmqtt_pubsub.so)
--
-- Live-cluster smoke (image build + container boot + planner pipeline)
-- is ROBSIM C3 -- user-driven per feedback_user_driven_testing.md.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local INSTANCE   = REPO_ROOT
    .. "nano_data_center_instance/app_containers/robot_sim"
local SNC_BASE   = SCRIPT_DIR .. "../"

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

local function file_exists(path)
  local f = io.open(path, "r"); if f then f:close(); return true end
  return false
end

------------------------------------------------------------------------
print("== definitions.lua: robot_sim def ==")
------------------------------------------------------------------------

do
  local defs = dofile(SNC_BASE .. "catalogs/definitions.lua")
  ok("definitions table loads", type(defs) == "table")
  local r = defs and defs.robot_sim
  ok("robot_sim def exists", r ~= nil)
  if r then
    ok("kind = application",   r.kind == "application")
    ok("runtime = docker",     r.runtime == "docker")
    ok("image = robot-sim:latest",
       r.image == "nanodatacenter/robot-sim:latest")
    ok("restart_policy = unless-stopped",
       r.restart_policy == "unless-stopped")
    ok("networks includes planner-net",
       type(r.networks) == "table" and r.networks[1] == "planner-net")
    ok("port_spec is empty / nil (headless)",
       r.port_spec == nil or next(r.port_spec) == nil)
  end
end

------------------------------------------------------------------------
print()
print("== topology.lua: robot_sim_rover_1 instance ==")
------------------------------------------------------------------------

do
  local topo = dofile(SNC_BASE .. "catalogs/topology.lua")
  ok("topology table loads", type(topo) == "table")
  local cpu_02 = topo and topo.cpus and topo.cpus.cpu_02
  ok("cpu_02 present", cpu_02 ~= nil)
  if cpu_02 then
    local found
    for _, inst in ipairs(cpu_02.instances or {}) do
      if inst.name == "robot_sim_rover_1" then found = inst end
    end
    ok("robot_sim_rover_1 instance present", found ~= nil)
    if found then
      ok("def = robot_sim", found.def == "robot_sim")
      ok("ports empty (headless)",
         found.ports == nil or next(found.ports) == nil)
      ok("params present", type(found.params) == "table")
      if found.params then
        ok("params.robot_id = rover_1",
           found.params.robot_id == "rover_1")
        ok("params.planner_namespace = mission_planner_01",
           found.params.planner_namespace == "mission_planner_01")
        ok("params.capabilities is a list",
           type(found.params.capabilities) == "table"
           and #found.params.capabilities >= 1)
      end
    end
  end
end

------------------------------------------------------------------------
print()
print("== app_containers/robot_sim package files exist ==")
------------------------------------------------------------------------

for _, p in ipairs({
  "manifest.lua", "container_spec.lua", "kb_build.lua",
  "container/Dockerfile", "container/docker_build.sh",
  "container/robot_sim/main.lua",
  "container/robot_sim/app.manifest.json",
  "container/robot_sim/lib/mock_mqtt_robot_lib.lua",
  "container/robot_sim/lib/lib/mqtt_pubsub.lua",
  "container/prebuilt_libs/libmqtt_pubsub.so",
}) do
  ok(p .. " exists", file_exists(INSTANCE .. "/" .. p))
end

------------------------------------------------------------------------
print()
print("== manifest.lua + container_spec.lua + kb_build.lua shape ==")
------------------------------------------------------------------------

do
  local m = dofile(INSTANCE .. "/manifest.lua")
  ok("manifest is table", type(m) == "table")
  ok("manifest.status.class = robot_sim",
     m.status and m.status.class == "robot_sim")
  ok("manifest.jsonb.capabilities present",
     m.jsonb and type(m.jsonb.capabilities) == "table"
     and #m.jsonb.capabilities >= 1)
  ok("manifest.jsonb.mqtt_protocol present",
     m.jsonb and type(m.jsonb.mqtt_protocol) == "table"
     and m.jsonb.mqtt_protocol.port == 1883)
  ok("manifest.jsonb has no ui_protocol (headless)",
     m.jsonb.ui_protocol == nil)
  ok("manifest.jsonb has no nats_protocol (MQTT-only)",
     m.jsonb.nats_protocol == nil)
end

do
  local cs = dofile(INSTANCE .. "/container_spec.lua")
  ok("container_spec is table", type(cs) == "table")
  ok("container_spec.class = robot_sim", cs.class == "robot_sim")
  ok("container_spec.image = robot-sim:latest",
     cs.image == "nanodatacenter/robot-sim:latest")
  ok("container_spec.kind = application",
     cs.kind == "application")
  ok("container_spec.port_spec empty (headless)",
     cs.port_spec == nil or next(cs.port_spec) == nil)
  ok("env_required lists ROBOT_ID + PLANNER_NAMESPACE + MQTT_HOST + MQTT_PORT",
     (function()
       if type(cs.env_required) ~= "table" then return false end
       local has = {}
       for _, e in ipairs(cs.env_required) do has[e] = true end
       return has.ROBOT_ID and has.PLANNER_NAMESPACE
              and has.MQTT_HOST and has.MQTT_PORT
     end)())
end

do
  local kb_fn, perr = loadfile(INSTANCE .. "/kb_build.lua")
  ok("kb_build.lua parses cleanly", kb_fn ~= nil, perr and tostring(perr))
  if kb_fn then
    local fn = kb_fn()
    ok("kb_build returns a function", type(fn) == "function")
  end
end

------------------------------------------------------------------------
print()
print("== Dockerfile shape ==")
------------------------------------------------------------------------

do
  local df = read_file(INSTANCE .. "/container/Dockerfile")
  ok("Dockerfile readable", df ~= nil)
  if df then
    ok("FROM luajit-base",
       df:find("FROM nanodatacenter/luajit%-base:latest") ~= nil)
    ok("installs libmosquitto1 (libmqtt_pubsub runtime dep)",
       df:find("libmosquitto1", 1, true) ~= nil)
    ok("vendors libmqtt_pubsub.so to /usr/local/lib",
       df:find("COPY prebuilt_libs/libmqtt_pubsub.so /usr/local/lib/", 1, true)
       ~= nil)
    ok("runs ldconfig after .so vendor",
       df:find("ldconfig", 1, true) ~= nil)
    ok("COPYs robot_sim/ to /opt/apps/robot_sim/",
       df:find("COPY robot_sim/ /opt/apps/robot_sim/", 1, true) ~= nil)
    ok("runs bundle_controller",
       df:find("bundle_controller", 1, true) ~= nil)
  end
end

------------------------------------------------------------------------
print()
print("== docker_build.sh shape ==")
------------------------------------------------------------------------

do
  local sh = read_file(INSTANCE .. "/container/docker_build.sh")
  ok("docker_build.sh readable", sh ~= nil)
  if sh then
    ok("declares IMAGE_TAG default",
       sh:find("nanodatacenter/robot%-sim:latest") ~= nil)
    ok("checks luajit-base present",
       sh:find("luajit%-base:latest") ~= nil)
    ok("calls docker build with SCRIPT_DIR",
       sh:find('docker build %-t "$IMAGE_TAG"') ~= nil)
    ok("uses set -euo pipefail (strict)",
       sh:find("set %-euo pipefail") ~= nil)
  end
end

------------------------------------------------------------------------
print()
print("== robot_sim/main.lua: parse + behavior surface ==")
------------------------------------------------------------------------

do
  -- Don't actually require it (would try to load lib.mqtt_pubsub
  -- which needs the .so). Just parse-load + grep behavior surface.
  local chunk, err = loadfile(INSTANCE .. "/container/robot_sim/main.lua")
  ok("main.lua parses cleanly", chunk ~= nil, err and tostring(err) or "")

  local src = read_file(INSTANCE .. "/container/robot_sim/main.lua")
  if src then
    -- Required env reads (APP_SITE, ROBOT_ID, PLANNER_NAMESPACE,
    -- MQTT_HOST: required, no default, fail-fast). MQTT_PORT has
    -- a default of 1883 so it's read via env_or, not env, with
    -- a slightly different assert message.
    for _, var in ipairs({
      "APP_SITE", "ROBOT_ID", "PLANNER_NAMESPACE", "MQTT_HOST",
    }) do
      ok("reads + asserts required env " .. var,
         src:find("env%(\"" .. var .. "\"%)") ~= nil
         and src:find(var .. " env missing", 1, true) ~= nil)
    end
    ok("reads MQTT_PORT (with default + numeric assert)",
       src:find('env_or%("MQTT_PORT"', 1, false) ~= nil
       and src:find("MQTT_PORT env missing or invalid", 1, true) ~= nil)

    -- Module loads
    ok("requires mock_mqtt_robot_lib",
       src:find('require%("mock_mqtt_robot_lib"%)') ~= nil)
    ok("requires lib.mqtt_pubsub (FFI wrapper)",
       src:find('require%("lib.mqtt_pubsub"%)') ~= nil)
    ok("requires dkjson",
       src:find('require%("dkjson"%)') ~= nil)

    -- MQTT bring-up
    ok("creates LinkState",
       src:find("lib.LinkState.new", 1, true) ~= nil)
    ok("creates PubSub",
       src:find("pubsub.PubSub.new", 1, true) ~= nil)
    ok("subscribes to topics.rpc",
       src:find("ps:subscribe%(topics.rpc") ~= nil)
    ok("publishes initial link_announce",
       src:find("link:make_announce%(%)") ~= nil)
    ok("connect retry loop present",
       src:find("CONNECT_MAX_RETRIES", 1, true) ~= nil
       and src:find("connect_with_retry", 1, true) ~= nil)

    -- RPC echo handler
    ok("echoes ack via lib.make_ack",
       src:find("lib.make_ack%(cmd%)") ~= nil)
    ok("echoes kb_done_success via lib.make_kb_done_success",
       src:find("lib.make_kb_done_success", 1, true) ~= nil)
    ok("publishes responses to topics.stream_bus",
       src:find("topics.stream_bus", 1, true) ~= nil)

    -- Link verb handler
    ok("handles planner_glob via link:on_planner_verb",
       src:find("link:on_planner_verb", 1, true) ~= nil)

    -- Heartbeat loop
    ok("periodic heartbeat present",
       src:find("link:make_heartbeat%(%)") ~= nil
       and src:find("HB_PERIOD_S", 1, true) ~= nil)
    ok("heartbeat gated to registering/live state",
       src:find('link.state == "registering"', 1, true) ~= nil
       and src:find('link.state == "live"', 1, true) ~= nil)

    -- No signal handler (per feedback_luajit_signal_safety.md). Match
    -- ffi.cast( with the open paren to skip prose mentions in comments.
    ok("no ffi.cast(...) signal handler (docker SIGTERM kills cleanly)",
       src:find("ffi.cast(", 1, true) == nil)
    ok("no SIGINT_CAUGHT loop (was in mock; removed for container)",
       src:find("SIGINT_CAUGHT", 1, true) == nil)
  end
end

------------------------------------------------------------------------
print()
print("== app.manifest.json shape ==")
------------------------------------------------------------------------

do
  local mf = read_file(INSTANCE .. "/container/robot_sim/app.manifest.json")
  ok("app.manifest.json readable", mf ~= nil)
  if mf then
    ok('name = "robot_sim"', mf:find('"name": "robot_sim"', 1, true) ~= nil)
    ok("argv runs main.lua",
       mf:find("/opt/apps/robot_sim/main.lua", 1, true) ~= nil)
    ok('restart_policy = "always"',
       mf:find('"restart_policy": "always"', 1, true) ~= nil)
  end
end

------------------------------------------------------------------------
print()
print("== ctx.ROBOTS now non-empty when topology loaded ==")
------------------------------------------------------------------------

do
  -- Load topology fresh, exercise the same enumeration build_kb.lua
  -- does, confirm robot_sim_rover_1 surfaces with the right shape.
  local topo = dofile(SNC_BASE .. "catalogs/topology.lua")
  local ROBOTS = {}
  for _, cpu in pairs(topo.cpus or {}) do
    for _, inst in ipairs(cpu.instances or {}) do
      if inst.def == "robot_sim" then
        local p = inst.params or {}
        ROBOTS[#ROBOTS + 1] = {
          container_name    = inst.name,
          robot_id          = p.robot_id,
          planner_namespace = p.planner_namespace,
          capabilities      = p.capabilities or {},
        }
      end
    end
  end
  ok("ctx.ROBOTS enumerates exactly 1 robot from current topology",
     #ROBOTS == 1, "got " .. #ROBOTS)
  if ROBOTS[1] then
    ok("enumerated robot_id = rover_1", ROBOTS[1].robot_id == "rover_1")
    ok("enumerated planner_namespace = mission_planner_01",
       ROBOTS[1].planner_namespace == "mission_planner_01")
    ok("enumerated container_name = robot_sim_rover_1",
       ROBOTS[1].container_name == "robot_sim_rover_1")
    ok("enumerated capabilities >= 1",
       #ROBOTS[1].capabilities >= 1)
  end
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
