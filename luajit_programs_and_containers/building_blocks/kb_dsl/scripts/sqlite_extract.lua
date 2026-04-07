--[[
  sqlite_extract.lua — Extract per-domain SQLite databases from Postgres KB

  For each domain in site_config, creates a SQLite DB containing the same
  ltree namespace as the master Postgres KB. This means code that queries
  by path works identically against either backend.

  Each SQLite DB gets:
    - system KB: the domain's container subtree (container + services)
    - system KB: the CPU node that hosts it (with master role)
    - system KB: infrastructure containers on that CPU (nats, mqtt, postgres endpoints)
    - subsystems KB: the domain's subtree (domain + robots)
]]

local M = {}

--- Query Postgres for rows matching a KB and path prefix.
-- Returns array of {label, name, properties, data, path}
local function query_subtree(pg_kb, kb_name, path_prefix)
  local sql = string.format(
    "SELECT label, name, properties::text, data::text, path::text " ..
    "FROM %s WHERE knowledge_base = '%s' AND path::text LIKE '%s%%' " ..
    "ORDER BY path::text",
    pg_kb.table_name, kb_name, path_prefix)

  local rows = pg_kb:_query(sql)
  return rows
end

--- Query Postgres for a single node by exact path.
local function query_node(pg_kb, kb_name, exact_path)
  local sql = string.format(
    "SELECT label, name, properties::text, data::text, path::text " ..
    "FROM %s WHERE knowledge_base = '%s' AND path::text = '%s'",
    pg_kb.table_name, kb_name, exact_path)

  local rows = pg_kb:_query(sql)
  return rows[1]
end

--- Query Postgres for nodes matching a KB, path prefix, and label filter.
local function query_by_label(pg_kb, kb_name, path_prefix, label)
  local sql = string.format(
    "SELECT label, name, properties::text, data::text, path::text " ..
    "FROM %s WHERE knowledge_base = '%s' AND path::text LIKE '%s%%' AND label = '%s' " ..
    "ORDER BY path::text",
    pg_kb.table_name, kb_name, path_prefix, label)

  return pg_kb:_query(sql)
end

--- Insert a row from Postgres into a SQLite KB using raw add_node.
-- properties and data come as JSON strings from Postgres.
local function insert_row(sqlite_kb, kb_name, row)
  local json = require("dkjson")
  local props = json.decode(row.properties) or {}
  local data  = json.decode(row.data) or {}

  sqlite_kb._kb:add_node(kb_name, row.label, row.name, props, data, row.path)
end

--- Modules that conflict between Postgres and SQLite (same name, different impl)
local CONFLICTING_MODULES = {
  "construct_kb", "knowledge_base_manager",
  "construct_data_tables", "construct_status_table",
  "construct_job_table", "construct_stream_table",
  "construct_rpc_client_table", "construct_rpc_server_table",
  "construct_bit_mask_store",
}

--- Switch package.loaded from Postgres to SQLite modules.
--- Called once — after this, all require() calls for conflicting modules
--- resolve to the SQLite versions. Cannot be reversed (FFI cdefs are global).
local _switched = false
local _pg_modules = {}

local function switch_to_sqlite_modules()
  if _switched then return end
  _switched = true

  local sqlite_dir = "../../knowledge_base/sqlite3/construct_kb/"
  package.path = sqlite_dir .. "?.lua;" .. package.path

  -- Save and remove Postgres versions
  for _, name in ipairs(CONFLICTING_MODULES) do
    if package.loaded[name] then
      _pg_modules[name] = package.loaded[name]
      package.loaded[name] = nil
    end
  end
end

local function load_sqlite_construct_kb()
  switch_to_sqlite_modules()
  return require("construct_kb")
end

local function load_sqlite_cdt()
  switch_to_sqlite_modules()
  return require("construct_data_tables")
end

function M.build(pg_kb, output_dir, config)
  local SQLite_Construct_KB = load_sqlite_construct_kb()

  -- ltree.so is in the sqlite construct_kb directory
  local ltree_path = "../../knowledge_base/sqlite3/construct_kb/ltree"

  for _, domain in ipairs(config.domains) do
    local db_file = output_dir .. "/" .. domain.name .. ".db"
    print("  Extracting: " .. db_file)

    -- Remove old DB file so we rebuild from scratch
    os.remove(db_file)

    -- Paths we need
    local cpu_path = "system.site." .. config.site_name .. ".cpu." .. domain.cpu
    local container_path = cpu_path .. ".container." .. domain.container
    local domain_path = "subsystems.domain." .. domain.name

    ---------------------------------------------------------------
    -- Step 1: If domain has planner data, build via CDT first.
    -- CDT creates all tables (KB, status, stream, bitmask, etc.)
    -- with upload_flag=false so schema is fully initialized.
    ---------------------------------------------------------------
    if domain.planner_data and domain.site then
      local SQLite_CDT = load_sqlite_cdt()
      local cdt = SQLite_CDT.new(db_file, "knowledge_base", ltree_path)

      cdt:add_kb(domain.site, "Planner data for " .. domain.name)
      cdt:select_kb(domain.site)

      local builder = require(domain.planner_data)
      builder(cdt, domain.site, domain)

      cdt:check_installation()
      cdt:disconnect()
      print("    + planner KB built via CDT")
    end

    ---------------------------------------------------------------
    -- Step 2: Open DB (existing or new) and insert extracted rows.
    -- For planner domains, the DB already exists with all tables.
    -- For non-planner domains, this creates a fresh DB.
    ---------------------------------------------------------------
    local sdb = SQLite_Construct_KB.new(db_file, "knowledge_base", ltree_path,
      domain.planner_data ~= nil)  -- upload_flag=true if DB already exists

    -- system KB: site root + CPU + master role + container subtree
    sdb:add_kb("system", "Physical topology (extract)")
    sdb:select_kb("system")

    local site_row = query_node(pg_kb, "system",
      "system.site." .. config.site_name)
    if site_row then
      insert_row(sdb, "system", site_row)
    end

    local cpu_row = query_node(pg_kb, "system", cpu_path)
    if cpu_row then
      insert_row(sdb, "system", cpu_row)
    end

    local role_row = query_node(pg_kb, "system", cpu_path .. ".role.master")
    if role_row then
      insert_row(sdb, "system", role_row)
    end

    local infra_containers = query_by_label(pg_kb, "system", cpu_path, "container")
    for _, crow in ipairs(infra_containers) do
      local props = require("dkjson").decode(crow.properties) or {}
      if props.type == "infrastructure" then
        insert_row(sdb, "system", crow)
        local svc_rows = query_subtree(pg_kb, "system", crow.path .. ".service.")
        for _, srow in ipairs(svc_rows) do
          insert_row(sdb, "system", srow)
        end
      end
    end

    local ctr_rows = query_subtree(pg_kb, "system", container_path)
    for _, crow in ipairs(ctr_rows) do
      insert_row(sdb, "system", crow)
    end

    -- subsystems KB: domain subtree (domain + robots)
    sdb:add_kb("subsystems", "Subsystem definitions (extract)")
    sdb:select_kb("subsystems")

    local domain_rows = query_subtree(pg_kb, "subsystems", domain_path)
    for _, drow in ipairs(domain_rows) do
      insert_row(sdb, "subsystems", drow)
    end

    sdb:disconnect()
    print("    -> " .. domain.name .. ".db OK")
  end

end

return M
