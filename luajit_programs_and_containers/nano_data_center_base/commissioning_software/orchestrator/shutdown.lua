--[[
  shutdown.lua — One-shot container shutdown orchestrator

  Reads the dependency graph from Postgres KB, reverse topological sorts,
  and stops containers level by level (leaves first).

  Usage:
    luajit shutdown.lua                     -- stop all containers
    luajit shutdown.lua --exclude postgres  -- stop all except postgres
]]

local graph = require("graph")

---------------------------------------------------------------------------
-- Parse args
---------------------------------------------------------------------------

local exclude = {}

local i = 1
while i <= #arg do
  if arg[i] == "--exclude" then
    i = i + 1
    for name in (arg[i] or ""):gmatch("[^,]+") do
      exclude[name] = true
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

print("=== Container Shutdown ===")
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
-- Stop in reverse topological order (leaves first)
---------------------------------------------------------------------------

local levels = graph.topo_levels(containers, false)

print("  Containers: " .. #graph.topo_sort(containers, true))
print("  Levels: " .. #levels)
print("")

for lvl_idx, level in ipairs(levels) do
  print(string.format("--- Level %d (shutdown) ---", lvl_idx))
  for _, name in ipairs(level) do
    local ctr = containers[name]
    local docker_name = ctr.docker_name

    if not graph.docker_is_running(docker_name) then
      print(string.format("  %-25s already stopped", docker_name))
    else
      local ok, out = graph.docker_stop(docker_name)
      if ok then
        print(string.format("  %-25s stopped", docker_name))
      else
        print(string.format("  %-25s FAILED: %s", docker_name, out))
      end
    end
  end
end

print("")
print("=== Shutdown Complete ===")
