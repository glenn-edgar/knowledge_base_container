--[[
  master_build.lua — Master DSL script for embedded data center KB

  Builds the Postgres KB (system controller) with two KBs:
    1. Physical tree  — site → CPU → container → service (the deployment)
    2. Software tree  — domains, robots (the logical layer)

  Containers live under CPUs (that's where they run).
  Services live under containers (that's what they provide).
  Each CPU has a master leaf. Master holds Postgres.
  Domains reference their container via ltree path into the physical tree.

  All definitions live in site_config.lua.
  Add a new mission planner or robot controller there — no builder changes needed.

  Usage:
    luajit scripts/master_build.lua
]]

-- Adjust package path to find KB construction modules (relative to kb_dsl/)
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)") or "./"
local bb_dir = script_dir .. "../../"
local kb_base = bb_dir .. "knowledge_base/postgres/construct_kb/?.lua"
local sqlite_base = bb_dir .. "knowledge_base/sqlite3/construct_kb/?.lua"
package.path = script_dir .. "?.lua;" .. kb_base .. ";" .. sqlite_base .. ";" .. package.path
package.cpath = bb_dir .. "knowledge_base/postgres/?.so;" .. bb_dir .. "knowledge_base/sqlite3/construct_kb/?.so;" .. package.cpath

local site_config    = require("site_config")
local physical_tree  = require("physical_tree")
local software_tree  = require("software_tree")
local planner_tree   = require("planner_tree")
local sqlite_extract = require("sqlite_extract")

---------------------------------------------------------------------------
-- Config
---------------------------------------------------------------------------
local pg_config = {
  host     = os.getenv("PG_HOST") or "localhost",
  port     = tonumber(os.getenv("PG_PORT")) or 5432,
  dbname   = os.getenv("PG_DBNAME") or "knowledge_base",
  user     = os.getenv("PG_USER") or "gedgar",
  password = os.getenv("POSTGRES_PASSWORD") or "",
}
--print(pg_config.host,pg_config.port,pg_config.dbname,pg_config.user,pg_config.password)

local sqlite_output_dir = os.getenv("SQLITE_DATA") or "/home/gedgar/Sqlite_Data"

-- Count containers across all CPUs
local total_containers = 0
for _, cpu in ipairs(site_config.cpus) do
  total_containers = total_containers + #cpu.containers
end

---------------------------------------------------------------------------
-- Build
---------------------------------------------------------------------------
print("=== KB Master Build ===")
print("Site: " .. site_config.site_name)
print("CPUs: " .. #site_config.cpus)
print("Containers: " .. total_containers)
print("Domains: " .. #site_config.domains)

-- Step 1: Connect to Postgres and build KB
local Construct_KB = require("construct_kb")
local kb = Construct_KB.new(
  pg_config.host, pg_config.port,
  pg_config.dbname, pg_config.user, pg_config.password
)
-- Step 2: Build trees (all driven by site_config)
print("\n--- Physical Tree (CPUs → Containers → Services) ---")
physical_tree.build(kb, site_config)

print("\n--- Software Tree (Domains → Robots) ---")
software_tree.build(kb, site_config)

print("\n--- Planner Tree (Boards, VNs, Robot Classes) ---")
planner_tree.build(kb, site_config)

-- Step 3: Verify
kb:check_installation()
print("\n[OK] Postgres KB built and verified.")

-- Step 4: Extract per-domain SQLite DBs
print("\n--- SQLite Extracts ---")
sqlite_extract.build(kb, sqlite_output_dir, site_config)

print("\n=== KB Master Build Complete ===")
