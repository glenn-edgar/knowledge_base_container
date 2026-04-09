--[[
  kb_build.lua — One-shot KB rebuild orchestrator

  Sequence:
    1. Verify Postgres is running
    2. Stop all containers except Postgres (via shutdown graph)
    3. Run master_build.lua (Postgres KB + SQLite extract)
    4. Start all containers (via startup graph)

  Usage:
    luajit kb_build.lua

  Environment:
    PG_HOST, PG_PORT, PG_DBNAME, PG_USER, POSTGRES_PASSWORD
    SQLITE_DATA     — SQLite extract directory (default: /home/gedgar/Sqlite_Data)
    MASTER_BUILD    — path to master_build.lua (auto-detected from script location)
]]

local graph = require("graph")

---------------------------------------------------------------------------
-- Config
---------------------------------------------------------------------------

local pg_opts = {
  host     = os.getenv("PG_HOST") or "127.0.0.1",
  port     = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname   = os.getenv("PG_DBNAME") or "knowledge_base",
  user     = os.getenv("PG_USER") or "gedgar",
  password = os.getenv("POSTGRES_PASSWORD") or "",
}

-- Locate master_build.lua relative to this script
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)") or "./"
local kb_dsl_dir = script_dir .. "../kb_dsl/scripts/"
local master_build = os.getenv("MASTER_BUILD") or (kb_dsl_dir .. "master_build.lua")

local sqlite_data = os.getenv("SQLITE_DATA") or "/home/gedgar/Sqlite_Data"

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function run(cmd)
  print("  $ " .. cmd)
  local ok = os.execute(cmd)
  return ok
end

local function die(msg)
  io.stderr:write("ERROR: " .. msg .. "\n")
  os.exit(1)
end

---------------------------------------------------------------------------
-- Step 1: Verify Postgres
---------------------------------------------------------------------------

print("=== KB Rebuild ===")
print("")

print("--- Step 1: Verify Postgres ---")
if not graph.docker_is_running(
    os.getenv("PG_DOCKER_NAME") or "pg-vector") then
  die("Postgres container is not running")
end
print("  Postgres OK")
print("")

---------------------------------------------------------------------------
-- Step 2: Stop all containers except Postgres
---------------------------------------------------------------------------

print("--- Step 2: Stop containers (except Postgres) ---")

local containers = graph.load_from_postgres(pg_opts)

-- Exclude postgres from shutdown
local pg_name = nil
for name, ctr in pairs(containers) do
  if ctr.type == "infrastructure" and name == "postgres" then
    pg_name = name
    break
  end
end

local shutdown_containers = {}
for name, ctr in pairs(containers) do
  if name ~= pg_name then
    shutdown_containers[name] = ctr
  end
end

local shutdown_levels = graph.topo_levels(shutdown_containers, false)
for _, level in ipairs(shutdown_levels) do
  for _, name in ipairs(level) do
    local ctr = shutdown_containers[name]
    if graph.docker_is_running(ctr.docker_name) then
      local ok, out = graph.docker_stop(ctr.docker_name)
      if ok then
        print(string.format("  %-25s stopped", ctr.docker_name))
      else
        print(string.format("  %-25s FAILED: %s", ctr.docker_name, out))
      end
    else
      print(string.format("  %-25s already stopped", ctr.docker_name))
    end
  end
end
print("")

---------------------------------------------------------------------------
-- Step 3: Rebuild KB (Postgres + SQLite extract)
---------------------------------------------------------------------------

print("--- Step 3: Rebuild KB ---")
local build_cmd = string.format(
  "cd %s && luajit master_build.lua 2>&1", kb_dsl_dir)
if not run(build_cmd) then
  die("master_build.lua failed")
end
print("")

-- Verify SQLite extract
local test_db = sqlite_data .. "/surface_ops.db"
local f = io.open(test_db, "r")
if f then
  f:close()
  print("  SQLite extract verified: " .. test_db)
else
  die("SQLite extract failed — " .. test_db .. " not found")
end
print("")

---------------------------------------------------------------------------
-- Step 4: Start all containers (except Postgres, already running)
---------------------------------------------------------------------------

print("--- Step 4: Start containers ---")

-- Reload graph (KB was just rebuilt)
containers = graph.load_from_postgres(pg_opts)

local startup_containers = {}
for name, ctr in pairs(containers) do
  if name ~= pg_name then
    startup_containers[name] = ctr
  end
end

local startup_levels = graph.topo_levels(startup_containers, true)
for lvl_idx, level in ipairs(startup_levels) do
  print(string.format("  Level %d:", lvl_idx))
  for _, name in ipairs(level) do
    local ctr = startup_containers[name]
    if graph.docker_is_running(ctr.docker_name) then
      print(string.format("    %-25s already running", ctr.docker_name))
    else
      local ok, out = graph.docker_start(ctr.docker_name)
      if ok then
        print(string.format("    %-25s started", ctr.docker_name))
      else
        print(string.format("    %-25s FAILED: %s", ctr.docker_name, out))
      end
    end
  end

  -- Wait for level to be ready
  for _, name in ipairs(level) do
    local ctr = startup_containers[name]
    if not graph.docker_wait_healthy(ctr.docker_name, 10) then
      print(string.format("    WARNING: %s not ready after 10s", ctr.docker_name))
    end
  end
end

print("")
print("=== KB Rebuild Complete ===")
