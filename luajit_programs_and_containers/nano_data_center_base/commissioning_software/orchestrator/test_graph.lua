--[[
  test_graph.lua — Unit tests for orchestrator graph module

  Tests topological sort, level grouping, service discovery,
  env resolution, and docker run command building.
  Uses mock container data matching site_config.lua dependency model.
]]

package.path = package.path .. ";./?.lua"

local graph = require("graph")

---------------------------------------------------------------------------
-- Test framework
---------------------------------------------------------------------------

local pass_count = 0
local fail_count = 0

local function assert_eq(actual, expected, msg)
  if actual == expected then
    pass_count = pass_count + 1
  else
    fail_count = fail_count + 1
    print(string.format("FAIL: %s — expected %s, got %s",
      msg, tostring(expected), tostring(actual)))
  end
end

local function assert_list_eq(actual, expected, msg)
  local a = table.concat(actual, ",")
  local e = table.concat(expected, ",")
  if a == e then
    pass_count = pass_count + 1
  else
    fail_count = fail_count + 1
    print(string.format("FAIL: %s — expected {%s}, got {%s}", msg, e, a))
  end
end

local function assert_contains(str, substr, msg)
  if str:find(substr, 1, true) then
    pass_count = pass_count + 1
  else
    fail_count = fail_count + 1
    print(string.format("FAIL: %s — '%s' not found in '%s'", msg, substr, str))
  end
end

local function assert_not_contains(str, substr, msg)
  if not str:find(substr, 1, true) then
    pass_count = pass_count + 1
  else
    fail_count = fail_count + 1
    print(string.format("FAIL: %s — '%s' should not be in '%s'", msg, substr, str))
  end
end

---------------------------------------------------------------------------
-- Mock container graph (matches site_config.lua)
---------------------------------------------------------------------------

local function make_containers()
  return {
    nats_server = {
      name = "nats_server", docker_name = "nats-js-ram",
      depends_on = {}, type = "infrastructure",
      image = "nanodatacenter/nats-js-ram:latest",
      ports = { "4222:4222", "8222:8222", "9222:9222" },
      tmpfs = { "/data:rw,size=50m" },
      env = {},
      services = {
        { name = "nats", protocol = "nats", data = { host = "127.0.0.1", port = 4222 } },
        { name = "monitoring", protocol = "http", data = { host = "127.0.0.1", port = 8222 } },
      },
    },
    mqtt_broker = {
      name = "mqtt_broker", docker_name = "mosquitto-ram-ws_main",
      depends_on = {}, type = "infrastructure",
      image = "nanodatacenter/mosquitto-ram-ws:latest",
      ports = { "1883:1883", "9001:9001" },
      env = {},
      services = {
        { name = "mqtt", protocol = "mqtt", data = { host = "127.0.0.1", port = 1883 } },
      },
    },
    postgres = {
      name = "postgres", docker_name = "pg-vector",
      depends_on = {}, type = "infrastructure",
      image = "pgvector/pgvector:pg17",
      ports = { "5432:5432" },
      volumes = { "/home/gedgar/Postgres_Data/vector:/var/lib/postgresql/data" },
      env = { POSTGRES_USER = "gedgar", POSTGRES_PASSWORD = "$POSTGRES_PASSWORD", POSTGRES_DB = "knowledge_base" },
      services = {
        { name = "postgres", protocol = "postgres", data = { host = "127.0.0.1", port = 5432, dbname = "knowledge_base" } },
      },
    },
    web_gateway = {
      name = "web_gateway", docker_name = "openresty-gateway",
      depends_on = { "nats_server", "postgres" }, type = "infrastructure",
      image = "nanodatacenter/openresty-gateway:latest",
      ports = { "8080:8080" },
      volumes = { "/srv/shell:/srv/shell:ro" },
      env = { POSTGRES_PASSWORD = "$POSTGRES_PASSWORD" },
      links = { "pg-vector:pg-vector" },
      services = {
        { name = "http", protocol = "http", data = { host = "127.0.0.1", port = 8080 } },
        { name = "pg_client", protocol = "postgres", data = { role = "client" } },
      },
    },
    kv_bridge = {
      name = "kv_bridge", docker_name = "kv-bridge",
      depends_on = { "nats_server", "mqtt_broker" }, type = "service",
      image = "nanodatacenter/kv-bridge:latest",
      network = "planner-net",
      restart = "unless-stopped",
      env = { MQTT_TOPIC = "kv_bridge/write", LOG_LEVEL = "info" },
      env_names = { nats = "NATS_URL" },
      services = {
        { name = "mqtt_client", protocol = "mqtt", data = { role = "subscriber" } },
        { name = "nats_client", protocol = "nats", data = { role = "client" } },
      },
    },
    surface_ops_planner = {
      name = "surface_ops_planner", docker_name = "surface_ops-planner",
      depends_on = { "nats_server", "mqtt_broker" }, type = "action_server",
      image = "nanodatacenter/ros-planner:latest",
      network = "planner-net",
      restart = "unless-stopped",
      volumes = { "/home/gedgar/Sqlite_Data:/data" },
      env = { SQLITE_DB = "/data/surface_ops.db" },
      services = {
        { name = "nats_client", protocol = "nats", data = { role = "client" } },
        { name = "mqtt_client", protocol = "mqtt", data = { role = "client" } },
      },
    },
    warehouse_ops_planner = {
      name = "warehouse_ops_planner", docker_name = "warehouse_ops-planner",
      depends_on = { "nats_server", "mqtt_broker" }, type = "action_server",
      image = "nanodatacenter/ros-planner:latest",
      network = "planner-net",
      restart = "unless-stopped",
      volumes = { "/home/gedgar/Sqlite_Data:/data" },
      env = { SQLITE_DB = "/data/warehouse_ops.db" },
      services = {
        { name = "nats_client", protocol = "nats", data = { role = "client" } },
        { name = "mqtt_client", protocol = "mqtt", data = { role = "client" } },
      },
    },
    fleet_manager = {
      name = "fleet_manager", docker_name = "fleet-manager",
      depends_on = { "nats_server" }, type = "action_server",
      image = "nanodatacenter/fleet:latest",
      network = "planner-net",
      restart = "unless-stopped",
      volumes = { "/home/gedgar/Sqlite_Data:/data" },
      env = { SQLITE_DB = "/data/fleet.db" },
      services = {
        { name = "nats_client", protocol = "nats", data = { role = "client" } },
      },
    },
    telemetry_collector = {
      name = "telemetry_collector", docker_name = "telemetry-collector",
      depends_on = { "nats_server" }, type = "action_server",
      image = "nanodatacenter/telemetry:latest",
      network = "planner-net",
      restart = "unless-stopped",
      volumes = { "/home/gedgar/Sqlite_Data:/data" },
      env = { SQLITE_DB = "/data/telemetry.db" },
      services = {
        { name = "nats_client", protocol = "nats", data = { role = "client" } },
      },
    },
  }
end

---------------------------------------------------------------------------
-- Test: topo_sort forward (startup order)
---------------------------------------------------------------------------

print("=== Test: topo_sort forward ===")
do
  local containers = make_containers()
  local order = graph.topo_sort(containers, true)

  assert_eq(#order, 9, "forward sort has 9 containers")

  local pos = {}
  for i, name in ipairs(order) do pos[name] = i end

  assert_eq(pos.nats_server <= 3, true, "nats_server in first 3")
  assert_eq(pos.mqtt_broker <= 3, true, "mqtt_broker in first 3")
  assert_eq(pos.postgres <= 3, true, "postgres in first 3")

  assert_eq(pos.web_gateway > pos.nats_server, true, "web_gateway after nats_server")
  assert_eq(pos.kv_bridge > pos.nats_server, true, "kv_bridge after nats_server")
  assert_eq(pos.kv_bridge > pos.mqtt_broker, true, "kv_bridge after mqtt_broker")
  assert_eq(pos.surface_ops_planner > pos.nats_server, true, "surface_ops after nats")
  assert_eq(pos.surface_ops_planner > pos.mqtt_broker, true, "surface_ops after mqtt")
end

---------------------------------------------------------------------------
-- Test: topo_sort reverse (shutdown order)
---------------------------------------------------------------------------

print("=== Test: topo_sort reverse ===")
do
  local containers = make_containers()
  local order = graph.topo_sort(containers, false)

  assert_eq(#order, 9, "reverse sort has 9 containers")

  local pos = {}
  for i, name in ipairs(order) do pos[name] = i end

  assert_eq(pos.surface_ops_planner < pos.nats_server, true, "surface_ops before nats in shutdown")
  assert_eq(pos.kv_bridge < pos.mqtt_broker, true, "kv_bridge before mqtt in shutdown")
  assert_eq(pos.web_gateway < pos.nats_server, true, "web_gateway before nats in shutdown")

  assert_eq(pos.nats_server >= 7, true, "nats_server in last 3 (shutdown)")
  assert_eq(pos.mqtt_broker >= 7, true, "mqtt_broker in last 3 (shutdown)")
  assert_eq(pos.postgres >= 7, true, "postgres in last 3 (shutdown)")
end

---------------------------------------------------------------------------
-- Test: topo_levels forward
---------------------------------------------------------------------------

print("=== Test: topo_levels forward ===")
do
  local containers = make_containers()
  local levels = graph.topo_levels(containers, true)

  assert_eq(#levels[1], 3, "level 1: 3 infra roots")
  local level1 = {}
  for _, n in ipairs(levels[1]) do level1[n] = true end
  assert_eq(level1.nats_server, true, "nats in level 1")
  assert_eq(level1.mqtt_broker, true, "mqtt in level 1")
  assert_eq(level1.postgres, true, "postgres in level 1")
end

---------------------------------------------------------------------------
-- Test: topo_levels reverse (shutdown)
---------------------------------------------------------------------------

print("=== Test: topo_levels reverse ===")
do
  local containers = make_containers()
  local levels = graph.topo_levels(containers, false)

  local last_level = {}
  for _, n in ipairs(levels[#levels]) do last_level[n] = true end
  assert_eq(last_level.nats_server, true, "nats stops last")
  assert_eq(last_level.mqtt_broker, true, "mqtt stops last")
  assert_eq(last_level.postgres, true, "postgres stops last")
end

---------------------------------------------------------------------------
-- Test: exclude containers
---------------------------------------------------------------------------

print("=== Test: exclude containers ===")
do
  local containers = make_containers()
  containers.postgres = nil

  local order = graph.topo_sort(containers, true)
  assert_eq(#order, 8, "8 containers after excluding postgres")
  for _, name in ipairs(order) do
    assert_eq(name ~= "postgres", true, name .. " is not postgres")
  end
end

---------------------------------------------------------------------------
-- Test: single node
---------------------------------------------------------------------------

print("=== Test: single node ===")
do
  local containers = {
    standalone = { name = "standalone", docker_name = "x", depends_on = {}, type = "service", services = {} },
  }
  local order = graph.topo_sort(containers, true)
  assert_eq(#order, 1, "single node sort")
  assert_eq(order[1], "standalone", "single node is standalone")
end

---------------------------------------------------------------------------
-- Test: linear chain
---------------------------------------------------------------------------

print("=== Test: linear chain ===")
do
  local containers = {
    a = { name = "a", docker_name = "a", depends_on = {}, type = "service", services = {} },
    b = { name = "b", docker_name = "b", depends_on = { "a" }, type = "service", services = {} },
    c = { name = "c", docker_name = "c", depends_on = { "b" }, type = "service", services = {} },
  }

  local order = graph.topo_sort(containers, true)
  assert_list_eq(order, { "a", "b", "c" }, "linear chain forward")

  local rev = graph.topo_sort(containers, false)
  assert_list_eq(rev, { "c", "b", "a" }, "linear chain reverse")

  local levels = graph.topo_levels(containers, true)
  assert_eq(#levels, 3, "3 levels in linear chain")
end

---------------------------------------------------------------------------
-- Test: resolve_service_env — NATS dependency
---------------------------------------------------------------------------

print("=== Test: resolve_service_env ===")
do
  local containers = make_containers()

  -- kv_bridge depends on nats_server + mqtt_broker (env_names: nats → NATS_URL)
  local env = graph.resolve_service_env(containers.kv_bridge, containers)
  assert_eq(env.NATS_URL, "nats://nats-js-ram:4222", "kv_bridge gets NATS_URL (custom env_name)")
  assert_eq(env.NATS_SERVER, nil, "kv_bridge does NOT get NATS_SERVER (overridden)")
  assert_eq(env.MQTT_HOST, "mosquitto-ram-ws_main", "kv_bridge gets MQTT_HOST from mqtt_broker")
  assert_eq(env.MQTT_PORT, "1883", "kv_bridge gets MQTT_PORT from mqtt_broker")

  -- web_gateway depends on nats_server + postgres, has pg_client but NO nats_client
  local wg_env = graph.resolve_service_env(containers.web_gateway, containers)
  assert_eq(wg_env.PG_HOST, "pg-vector", "web_gateway gets PG_HOST from postgres")
  assert_eq(wg_env.PG_PORT, "5432", "web_gateway gets PG_PORT from postgres")
  assert_eq(wg_env.PG_DBNAME, "knowledge_base", "web_gateway gets PG_DBNAME from postgres")
  assert_eq(wg_env.NATS_SERVER, nil, "web_gateway does NOT get NATS (no nats_client service)")
  assert_eq(wg_env.HTTP_HOST, nil, "web_gateway does NOT get HTTP (monitoring not injected)")

  -- surface_ops_planner depends on nats + mqtt, has nats_client + mqtt_client
  local sp_env = graph.resolve_service_env(containers.surface_ops_planner, containers)
  assert_eq(sp_env.NATS_SERVER, "nats://nats-js-ram:4222", "planner gets NATS_SERVER")
  assert_eq(sp_env.MQTT_HOST, "mosquitto-ram-ws_main", "planner gets MQTT_HOST")
  assert_eq(sp_env.HTTP_HOST, nil, "planner does NOT get HTTP (monitoring filtered)")
  assert_eq(sp_env.PG_HOST, nil, "planner does NOT get PG (no postgres dep)")

  -- nats_server has no deps — empty env
  local nats_env = graph.resolve_service_env(containers.nats_server, containers)
  local count = 0; for _ in pairs(nats_env) do count = count + 1 end
  assert_eq(count, 0, "nats_server has no service env (no deps)")
end

---------------------------------------------------------------------------
-- Test: merge_env — container env wins over service env
---------------------------------------------------------------------------

print("=== Test: merge_env ===")
do
  local svc_env = { MQTT_HOST = "auto-resolved", NATS_SERVER = "nats://auto:4222" }
  local ctr_env = { SQLITE_DB = "/data/test.db", LOG_LEVEL = "info" }

  local merged = graph.merge_env(ctr_env, svc_env)
  assert_eq(merged.MQTT_HOST, "auto-resolved", "service env present")
  assert_eq(merged.NATS_SERVER, "nats://auto:4222", "service env present")
  assert_eq(merged.SQLITE_DB, "/data/test.db", "container env present")
  assert_eq(merged.LOG_LEVEL, "info", "container env present")

  -- Container env overrides service env on conflict
  local svc2 = { MQTT_HOST = "auto" }
  local ctr2 = { MQTT_HOST = "override" }
  local m2 = graph.merge_env(ctr2, svc2)
  assert_eq(m2.MQTT_HOST, "override", "container env wins on conflict")
end

---------------------------------------------------------------------------
-- Test: resolve_env_refs — $VAR expansion
---------------------------------------------------------------------------

print("=== Test: resolve_env_refs ===")
do
  -- Set a test env var
  -- Can't set env from Lua, so test the non-$ path
  local env = { SQLITE_DB = "/data/test.db", PLAIN = "hello" }
  local resolved = graph.resolve_env_refs(env)
  assert_eq(resolved.SQLITE_DB, "/data/test.db", "plain value preserved")
  assert_eq(resolved.PLAIN, "hello", "plain value preserved")

  -- $VAR with no env set resolves to ""
  local env2 = { SECRET = "$NONEXISTENT_VAR_12345" }
  local r2 = graph.resolve_env_refs(env2)
  assert_eq(r2.SECRET, "", "$VAR with no env resolves to empty")
end

---------------------------------------------------------------------------
-- Test: build_run_cmd — full command generation
---------------------------------------------------------------------------

print("=== Test: build_run_cmd ===")
do
  local containers = make_containers()

  -- NATS server: ports + tmpfs, no network
  local nats_env = graph.resolve_env_refs(graph.merge_env(
    containers.nats_server.env,
    graph.resolve_service_env(containers.nats_server, containers)))
  local cmd = graph.build_run_cmd(containers.nats_server, nats_env)

  assert_contains(cmd, "docker run -d", "starts with docker run -d")
  assert_contains(cmd, "--name nats-js-ram", "has container name")
  assert_contains(cmd, "-p 4222:4222", "has NATS port")
  assert_contains(cmd, "-p 9222:9222", "has WS port")
  assert_contains(cmd, "--tmpfs /data:rw,size=50m", "has tmpfs")
  assert_contains(cmd, "nanodatacenter/nats-js-ram:latest", "has image")
  assert_not_contains(cmd, "--network", "no network for infra")
  assert_not_contains(cmd, "--restart", "no restart for infra")

  -- kv_bridge: network + restart + service env
  local kv_svc_env = graph.resolve_service_env(containers.kv_bridge, containers)
  local kv_merged  = graph.merge_env(containers.kv_bridge.env, kv_svc_env)
  local kv_final   = graph.resolve_env_refs(kv_merged)
  local kv_cmd = graph.build_run_cmd(containers.kv_bridge, kv_final)

  assert_contains(kv_cmd, "--network planner-net", "kv_bridge on planner-net")
  assert_contains(kv_cmd, "--restart unless-stopped", "kv_bridge restart policy")
  assert_contains(kv_cmd, "-e MQTT_HOST=mosquitto-ram-ws_main", "resolved MQTT_HOST")
  assert_contains(kv_cmd, "-e MQTT_PORT=1883", "resolved MQTT_PORT")
  assert_contains(kv_cmd, "-e NATS_URL=nats://nats-js-ram:4222", "resolved NATS_URL (custom env_name)")
  assert_contains(kv_cmd, "-e MQTT_TOPIC=kv_bridge/write", "container-own env")
  assert_contains(kv_cmd, "-e LOG_LEVEL=info", "container-own env")

  -- surface_ops_planner: volumes + network + service env
  local sp_svc_env = graph.resolve_service_env(containers.surface_ops_planner, containers)
  local sp_merged  = graph.merge_env(containers.surface_ops_planner.env, sp_svc_env)
  local sp_final   = graph.resolve_env_refs(sp_merged)
  local sp_cmd = graph.build_run_cmd(containers.surface_ops_planner, sp_final)

  assert_contains(sp_cmd, "--name surface_ops-planner", "planner name")
  assert_contains(sp_cmd, "--network planner-net", "planner on planner-net")
  assert_contains(sp_cmd, "-v /home/gedgar/Sqlite_Data:/data", "planner volume")
  assert_contains(sp_cmd, "-e SQLITE_DB=/data/surface_ops.db", "planner sqlite env")
  assert_contains(sp_cmd, "-e MQTT_HOST=mosquitto-ram-ws_main", "planner mqtt resolved")
  assert_contains(sp_cmd, "-e NATS_SERVER=nats://nats-js-ram:4222", "planner nats resolved")

  -- web_gateway: links + postgres env
  local wg_svc_env = graph.resolve_service_env(containers.web_gateway, containers)
  local wg_merged  = graph.merge_env(containers.web_gateway.env, wg_svc_env)
  local wg_final   = graph.resolve_env_refs(wg_merged)
  local wg_cmd = graph.build_run_cmd(containers.web_gateway, wg_final)

  assert_contains(wg_cmd, "--link pg-vector:pg-vector", "web_gateway link")
  assert_contains(wg_cmd, "-e PG_HOST=pg-vector", "web_gateway PG_HOST resolved from postgres")
  assert_contains(wg_cmd, "-e PG_DBNAME=knowledge_base", "web_gateway PG_DBNAME resolved")
  assert_contains(wg_cmd, "-v /srv/shell:/srv/shell:ro", "web_gateway shell volume")
  assert_not_contains(wg_cmd, "NATS_SERVER", "web_gateway no NATS (no nats_client)")
  assert_not_contains(wg_cmd, "HTTP_HOST", "web_gateway no HTTP monitoring injected")
end

---------------------------------------------------------------------------
-- Test: is_stale — detect config drift
---------------------------------------------------------------------------

print("=== Test: is_stale ===")
do
  local containers = make_containers()

  -- Mock docker_inspect_config to return known values
  local orig_inspect = graph.docker_inspect_config
  local mock_config = nil
  graph.docker_inspect_config = function() return mock_config end

  -- Container doesn't exist → not stale
  mock_config = nil
  assert_eq(graph.is_stale(containers.kv_bridge, containers), false, "nil inspect = not stale")

  -- Image matches, env matches → not stale
  mock_config = {
    image = "nanodatacenter/kv-bridge:latest",
    env = {
      MQTT_TOPIC = "kv_bridge/write", LOG_LEVEL = "info",
      MQTT_HOST = "mosquitto-ram-ws_main", MQTT_PORT = "1883",
      NATS_URL = "nats://nats-js-ram:4222",
    },
  }
  assert_eq(graph.is_stale(containers.kv_bridge, containers), false, "matching config = not stale")

  -- Image changed → stale
  mock_config.image = "nanodatacenter/kv-bridge:v2"
  assert_eq(graph.is_stale(containers.kv_bridge, containers), true, "different image = stale")

  -- Image matches, env differs → stale
  mock_config.image = "nanodatacenter/kv-bridge:latest"
  mock_config.env.NATS_URL = "nats://old-server:4222"
  assert_eq(graph.is_stale(containers.kv_bridge, containers), true, "different env = stale")

  -- Missing env key → stale
  mock_config.env.NATS_URL = nil
  assert_eq(graph.is_stale(containers.kv_bridge, containers), true, "missing env key = stale")

  -- Restore
  graph.docker_inspect_config = orig_inspect
end

---------------------------------------------------------------------------
-- Summary
---------------------------------------------------------------------------

print("")
print(string.format("=== %d assertions, %d failures ===",
  pass_count + fail_count, fail_count))

if fail_count > 0 then
  os.exit(1)
end
