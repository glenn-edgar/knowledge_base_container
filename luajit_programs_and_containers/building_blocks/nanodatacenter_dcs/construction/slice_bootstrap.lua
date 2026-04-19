#!/usr/bin/env luajit
-- =============================================================================
-- slice_bootstrap.lua -- Generate per-CPU bootstrap.db file.
--
-- ONE host process per CPU runs both system_control + node_control halves
-- in the same VM (collapsed for shared connectors / process_globals dict).
--
-- Output per CPU:
--   deployment/<cpu_id>/bootstrap.db   (read-only)
--
-- Bootstrap.db schema = knowledge_base + knowledge_base_info tables only
-- (Construct_KB; no satellite tables — no runtime state at boot).
--
-- bootstrap.db content (per CPU, union of both halves' needs):
--   identity + pg connect : system.site.<S>.cpu.<this>.bootstrap.config
--   tree integrity        : site, cpu header rows
--   master CPU only       : system.container_definition.<infra_def>.* +
--                           system.site.<S>.cpu.<this>.container.<infra_inst>.*
--                           (postgres, nats, mosquitto, kv_bridge)
--   every CPU             : system.container_definition.<app_def>.* +
--                           system.site.<S>.cpu.<this>.container.<app_inst>.*
--                           (application instances; empty in v1)
--
-- After write, file is chmod 0444 (read-only).
-- =============================================================================

local DBI         = require("DBI")
local Construct_KB = require("construct_kb")
local h           = require("sqlite3_helpers")

---------------------------------------------------------------------------
-- locate sibling files
---------------------------------------------------------------------------

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1,1) == "@" then src = src:sub(2) end
  local rel = src:match("(.*/)") or "./"
  -- absolutize so paths survive cwd changes
  local p = io.popen("cd '" .. rel .. "' && pwd")
  local abs = p:read("*l")
  p:close()
  return abs .. "/"
end

local DIR        = script_dir()                        -- .../nanodatacenter_dcs/construction/
local DCS_ROOT   = DIR .. "../"                         -- .../nanodatacenter_dcs/
local OUT_ROOT   = DCS_ROOT .. "deployment/"            -- per-CPU artifact dir
local LTREE_PATH = DIR .. "../../knowledge_base/sqlite3/construct_kb/ltree"

local function load_lua(rel)
  local chunk, err = loadfile(DIR .. rel)
  if not chunk then error("loadfile " .. rel .. ": " .. tostring(err)) end
  return chunk()
end

local DEFINITIONS = load_lua("catalogs/definitions.lua")
local TOPOLOGY    = load_lua("catalogs/topology.lua")
local SITE        = TOPOLOGY.site
local PG          = TOPOLOGY.pg_connect

local PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not PASSWORD then
  error("POSTGRES_PASSWORD not set (source ~/.config/nanodatacenter/secrets.env)")
end

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function shell(cmd)
  local rc = os.execute(cmd)
  if rc ~= 0 and rc ~= true then error("command failed: " .. cmd) end
end

local function rmrf(path)   shell("rm -rf '" .. path .. "'") end
local function mkdirp(path) shell("mkdir -p '" .. path .. "'") end

local function pg_escape(v)
  if v == nil           then return "NULL" end
  if type(v) == "boolean" then return v and "TRUE" or "FALSE" end
  return "'" .. tostring(v):gsub("'", "''") .. "'"
end

---------------------------------------------------------------------------
-- compute include filter for a role on a CPU
---------------------------------------------------------------------------

-- Returns { subtree = {...}, exact = {...} } -- ltree fragments to include.
-- Combined-process slice: includes both infra (if master) and app instances
-- on this CPU. Single bootstrap.db serves the collapsed dcs host process.
local function compute_filters(cpu_id, is_master, all_cpus)
  local subtree, exact = {}, {}
  local function S(p) subtree[#subtree + 1] = p end
  local function E(p) exact[#exact + 1] = p end

  -- tree integrity
  E(string.format("system.site.%s",         SITE))
  E(string.format("system.site.%s.cpu.%s",  SITE, cpu_id))

  -- bootstrap.config (identity + pg connect) + its header
  E(string.format("system.site.%s.cpu.%s.bootstrap",        SITE, cpu_id))
  E(string.format("system.site.%s.cpu.%s.bootstrap.config", SITE, cpu_id))

  -- include kinds:
  --   infrastructure -- only on master (system_control half manages these)
  --   application    -- always (node_control half manages these on every CPU)
  local include_kinds = { application = true }
  if is_master then include_kinds.infrastructure = true end

  local cpu = all_cpus[cpu_id]
  for _, inst in ipairs(cpu.instances or {}) do
    local def = DEFINITIONS[inst.def]
    if def and include_kinds[def.kind] then
      S(string.format("system.site.%s.cpu.%s.container.%s",
                      SITE, cpu_id, inst.name))
      S(string.format("system.container_definition.%s", inst.def))
    end
  end

  return { subtree = subtree, exact = exact }
end

local function build_where(filters)
  local ors = {}
  for _, p in ipairs(filters.subtree) do
    ors[#ors + 1] = string.format("path <@ %s::ltree", pg_escape(p))
  end
  for _, p in ipairs(filters.exact) do
    ors[#ors + 1] = string.format("path = %s::ltree", pg_escape(p))
  end
  if #ors == 0 then return "FALSE" end
  return "(" .. table.concat(ors, " OR ") .. ")"
end

---------------------------------------------------------------------------
-- pg fetch
---------------------------------------------------------------------------

local function fetch_rows(pg, sql)
  local sth = pg:prepare(sql)
  if not sth then error("prepare: " .. sql) end
  local ok, err = sth:execute()
  if not ok then error("execute: " .. tostring(err) .. "\nsql: " .. sql) end
  local rows = {}
  while true do
    local row = sth:fetch(true)
    if not row then break end
    local copy = {}
    for k, v in pairs(row) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  sth:close()
  return rows
end

---------------------------------------------------------------------------
-- one CPU's bootstrap.db (combined: system_control + node_control halves)
---------------------------------------------------------------------------

local function slice_cpu(pg, db_path, cpu_id, is_master, all_cpus)
  os.remove(db_path)
  -- Pass nil for ltree path: slicer does plain INSERTs, doesn't need ltree
  -- query operators. Runtime chain-tree code loads ltree.so via its own path.
  local kb = Construct_KB.new(db_path, "knowledge_base", nil, false)
  local sdb = kb:get_db_objects()

  local filters = compute_filters(cpu_id, is_master, all_cpus)

  -- knowledge_base table rows
  local where = build_where(filters)
  local rows = fetch_rows(pg, string.format(
    [[SELECT knowledge_base, label, name, properties, data,
             has_link, has_link_mount, path
        FROM knowledge_base WHERE %s ORDER BY path]], where))
  for _, r in ipairs(rows) do
    h.sql_exec(sdb, string.format(
      [[INSERT INTO knowledge_base
        (knowledge_base, label, name, properties, data,
         has_link, has_link_mount, path)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)]],
      pg_escape(r.knowledge_base), pg_escape(r.label),
      pg_escape(r.name), pg_escape(r.properties), pg_escape(r.data),
      pg_escape(r.has_link), pg_escape(r.has_link_mount),
      pg_escape(r.path)))
  end

  -- knowledge_base_info: take all KB names that appear in our rows
  local kbs = {}
  for _, r in ipairs(rows) do kbs[r.knowledge_base] = true end
  if next(kbs) then
    local quoted = {}
    for n in pairs(kbs) do quoted[#quoted + 1] = pg_escape(n) end
    local info_rows = fetch_rows(pg, string.format(
      "SELECT knowledge_base, description FROM knowledge_base_info " ..
      "WHERE knowledge_base IN (%s)",
      table.concat(quoted, ", ")))
    for _, r in ipairs(info_rows) do
      h.sql_exec(sdb, string.format(
        "INSERT INTO knowledge_base_info (knowledge_base, description) " ..
        "VALUES (%s, %s)",
        pg_escape(r.knowledge_base), pg_escape(r.description)))
    end
  end

  kb:disconnect()

  -- chmod 0444
  shell(string.format("chmod 0444 '%s'", db_path))

  return #rows
end

---------------------------------------------------------------------------
-- main
---------------------------------------------------------------------------

local pg, err = DBI.Connect("PostgreSQL", PG.dbname, PG.user, PASSWORD,
                            PG.host, tostring(PG.port))
if not pg then error("pg connect: " .. tostring(err)) end
pg:autocommit(true)

-- Wipe only the per-CPU subdirs, not the deployment/ root itself --
-- README.md + .gitignore at the root are tracked repo files and
-- must survive re-slicing.
print("wiping per-CPU dirs under " .. OUT_ROOT)
for cpu_id, _ in pairs(TOPOLOGY.cpus) do
  rmrf(OUT_ROOT .. cpu_id)
end
mkdirp(OUT_ROOT)

for cpu_id, _ in pairs(TOPOLOGY.cpus) do
  local is_master = (cpu_id == TOPOLOGY.master)
  local out_dir   = OUT_ROOT .. cpu_id .. "/"
  mkdirp(out_dir)
  local db_path = out_dir .. "bootstrap.db"
  local n = slice_cpu(pg, db_path, cpu_id, is_master, TOPOLOGY.cpus)
  print(string.format("  %s/bootstrap.db (%d rows%s)",
                      cpu_id, n, is_master and ", master" or ""))
end

pg:close()
print("done.")
