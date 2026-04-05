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

--- Load the SQLite construct_kb module without conflicting with the
--- already-loaded Postgres construct_kb (same module name).
local function load_sqlite_construct_kb()
  local sqlite_dir = "../../knowledge_base/sqlite3/construct_kb/"

  -- Temporarily swap package.path and clear cached Postgres modules
  local saved_path = package.path
  package.path = sqlite_dir .. "?.lua"

  -- Remove cached Postgres versions so require() finds the SQLite ones
  local cached = {}
  for name, _ in pairs(package.loaded) do
    if name == "construct_kb" or name == "knowledge_base_manager" then
      cached[name] = package.loaded[name]
      package.loaded[name] = nil
    end
  end

  local SQLite_CKB = require("construct_kb")

  -- Restore cached Postgres modules and path
  for name, mod in pairs(cached) do
    package.loaded[name] = mod
  end
  package.path = saved_path

  return SQLite_CKB
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

    local sdb = SQLite_Construct_KB.new(db_file, "knowledge_base", ltree_path)

    -- Paths we need
    local cpu_path = "system.site." .. config.site_name .. ".cpu." .. domain.cpu
    local container_path = cpu_path .. ".container." .. domain.container
    local domain_path = "subsystems.domain." .. domain.name

    ---------------------------------------------------------------
    -- system KB: site root + CPU + master role + container subtree
    ---------------------------------------------------------------
    sdb:add_kb("system", "Physical topology (extract)")
    sdb:select_kb("system")

    -- Site root
    local site_row = query_node(pg_kb, "system",
      "system.site." .. config.site_name)
    if site_row then
      insert_row(sdb, "system", site_row)
    end

    -- CPU node
    local cpu_row = query_node(pg_kb, "system", cpu_path)
    if cpu_row then
      insert_row(sdb, "system", cpu_row)
    end

    -- Master role
    local role_row = query_node(pg_kb, "system", cpu_path .. ".role.master")
    if role_row then
      insert_row(sdb, "system", role_row)
    end

    -- Infrastructure containers (nats, mqtt, postgres, web_gateway)
    -- so the domain knows how to reach services
    local infra_containers = query_by_label(pg_kb, "system", cpu_path, "container")
    for _, crow in ipairs(infra_containers) do
      local props = require("dkjson").decode(crow.properties) or {}
      if props.type == "infrastructure" then
        insert_row(sdb, "system", crow)
        -- Also grab services under this infrastructure container
        local svc_rows = query_subtree(pg_kb, "system", crow.path .. ".service.")
        for _, srow in ipairs(svc_rows) do
          insert_row(sdb, "system", srow)
        end
      end
    end

    -- This domain's container + its services
    local ctr_rows = query_subtree(pg_kb, "system", container_path)
    for _, crow in ipairs(ctr_rows) do
      insert_row(sdb, "system", crow)
    end

    ---------------------------------------------------------------
    -- subsystems KB: domain subtree (domain + robots)
    ---------------------------------------------------------------
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
