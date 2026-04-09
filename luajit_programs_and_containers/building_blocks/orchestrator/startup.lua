--[[
  startup.lua — One-shot container startup orchestrator

  Reads the dependency graph from Postgres KB, topological sorts,
  and starts containers level by level (roots first).

  For each container:
    - If running: skip
    - If exists but stopped: docker start
    - If doesn't exist: docker run (full create from KB run spec)

  Service discovery env vars (MQTT_HOST, NATS_SERVER, etc.) are resolved
  from dependency containers' service nodes in the KB — not hardcoded.

  Usage:
    luajit startup.lua                     -- start all containers
    luajit startup.lua --exclude postgres  -- start all except postgres
    luajit startup.lua --network planner-net --connect nats_server,mqtt_broker
]]

local graph = require("graph")

---------------------------------------------------------------------------
-- Parse args
---------------------------------------------------------------------------

local exclude = {}
local network = nil
local connect_to_net = {}

local i = 1
while i <= #arg do
  if arg[i] == "--exclude" then
    i = i + 1
    for name in (arg[i] or ""):gmatch("[^,]+") do
      exclude[name] = true
    end
  elseif arg[i] == "--network" then
    i = i + 1
    network = arg[i]
  elseif arg[i] == "--connect" then
    i = i + 1
    for name in (arg[i] or ""):gmatch("[^,]+") do
      connect_to_net[name] = true
    end
  end
  i = i + 1
end

---------------------------------------------------------------------------
-- Load graph from Postgres
---------------------------------------------------------------------------

local pg_opts = {
  host     = os.getenv("PG_HOST") or "127.0.0.1",
  port     = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname   = os.getenv("PG_DBNAME") or "knowledge_base",
  user     = os.getenv("PG_USER") or "gedgar",
  password = os.getenv("POSTGRES_PASSWORD") or "",
}

print("=== Container Startup ===")
print("  Postgres: " .. pg_opts.host .. ":" .. pg_opts.port)

local containers = graph.load_from_postgres(pg_opts)

-- Remove excluded containers
for name in pairs(exclude) do
  if containers[name] then
    print("  Excluding: " .. name)
    containers[name] = nil
  end
end

---------------------------------------------------------------------------
-- Start in topological order (level by level)
---------------------------------------------------------------------------

local levels = graph.topo_levels(containers, true)

print("  Containers: " .. #graph.topo_sort(containers, true))
print("  Levels: " .. #levels)
print("")

for lvl_idx, level in ipairs(levels) do
  print(string.format("--- Level %d ---", lvl_idx))
  for _, name in ipairs(level) do
    local ctr = containers[name]

    local status, msg = graph.docker_ensure(ctr, containers)
    print(string.format("  %-25s %s", ctr.docker_name, status))
    if status == "failed" then
      print(string.format("    %s", msg))
    end

    -- Connect to network if requested
    if network and connect_to_net[name] then
      os.execute(string.format(
        "docker network connect %s %s 2>/dev/null", network, ctr.docker_name))
    end
  end

  -- Wait for this level to be healthy before starting next
  for _, name in ipairs(level) do
    local ctr = containers[name]
    if not graph.docker_wait_healthy(ctr.docker_name, 10) then
      print(string.format("  WARNING: %s not ready after 10s", ctr.docker_name))
    end
  end
end

print("")
print("=== Startup Complete ===")
