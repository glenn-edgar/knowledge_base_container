--[[
  knowledge_base_manager.lua
  
  LuaJIT PostgreSQL knowledge base manager using luadbi (libpq wrapper).
  
  Dependencies:
    - luadbi + luadbi-postgresql  (C wrapper around libpq)
    - dkjson                      (pure Lua JSON)
  
  Install:
    sudo apt install libpq-dev
    luarocks --lua-version 5.1 install luadbi
    luarocks --lua-version 5.1 install luadbi-postgresql
    luarocks --lua-version 5.1 install dkjson
  
  Usage:
    local KBManager = require("knowledge_base_manager")
    local mgr = KBManager.new("knowledge_base", conn_params)
]]

local DBI = require("DBI")
local json = require("dkjson")

local KnowledgeBaseManager = {}
KnowledgeBaseManager.__index = KnowledgeBaseManager

--- Escape a SQL identifier (table/column name).
local function quote_ident(name)
  return '"' .. name:gsub('"', '""') .. '"'
end

--- Escape a SQL literal string value.
local function quote_literal(val)
  if val == nil then return "NULL" end
  return "'" .. tostring(val):gsub("'", "''") .. "'"
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function KnowledgeBaseManager.new(table_name, connection_params, upload_flag)
  assert(type(table_name) == "string", "table_name must be a string")
  assert(type(connection_params) == "table", "connection_params must be a table")

  local self = setmetatable({}, KnowledgeBaseManager)
  self.table_name = table_name
  self.connection_params = connection_params
  self.upload_flag = upload_flag or false
  self.conn = nil

  self:_connect()
  if not self.upload_flag then
    self:_create_tables()
  end

  return self
end

---------------------------------------------------------------------------
-- Connection
---------------------------------------------------------------------------

function KnowledgeBaseManager:_connect()
  local params = self.connection_params
  local dbname = params.database or params.dbname

  local conn, err = DBI.Connect("PostgreSQL",
    dbname,
    params.user,
    params.password,
    params.host or "127.0.0.1",
    tostring(params.port or 5432))

  if not conn then
    error("Error connecting to database: " .. tostring(err))
  end

  self.conn = conn
  self.conn:autocommit(true)

  -- Enable ltree extension
  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
end

function KnowledgeBaseManager:disconnect()
  if self.conn then
    self.conn:close()
    self.conn = nil
  end
end

---------------------------------------------------------------------------
-- SQL execution helpers
---------------------------------------------------------------------------

--- Execute a SQL statement (no result set expected).
function KnowledgeBaseManager:_exec(sql_str)
  local stmt, err = self.conn:prepare(sql_str)
  if not stmt then
    error("SQL prepare error: " .. tostring(err) .. "\nQuery: " .. sql_str)
  end
  local ok, exec_err = stmt:execute()
  if not ok then
    error("SQL execute error: " .. tostring(exec_err) .. "\nQuery: " .. sql_str)
  end
  stmt:close()
end

--- Execute a query and return all rows as an array of named-column tables.
function KnowledgeBaseManager:_query(sql_str)
  local stmt, err = self.conn:prepare(sql_str)
  if not stmt then
    error("SQL prepare error: " .. tostring(err) .. "\nQuery: " .. sql_str)
  end
  local ok, exec_err = stmt:execute()
  if not ok then
    error("SQL execute error: " .. tostring(exec_err) .. "\nQuery: " .. sql_str)
  end

  local rows = {}
  local row = stmt:fetch(true)  -- true = named columns
  while row do
    rows[#rows + 1] = row
    row = stmt:fetch(true)
  end
  stmt:close()
  return rows
end

---------------------------------------------------------------------------
-- Table management
---------------------------------------------------------------------------

function KnowledgeBaseManager:_delete_table(tbl_name, schema)
  schema = schema or "public"
  self:_exec(string.format("DROP TABLE IF EXISTS %s.%s CASCADE;",
    quote_ident(schema), quote_ident(tbl_name)))
end

--- Terminate any other connections to this database that are idle in
--- transaction. They hold locks from a previously crashed/aborted build
--- and would block our DROP TABLE indefinitely otherwise.
function KnowledgeBaseManager:_terminate_stale_locks()
  -- Don't error if this fails — best-effort cleanup. Wrapped in pcall
  -- so a missing privilege doesn't kill the whole build.
  pcall(function()
    local rows = self:_query([[
      SELECT pg_terminate_backend(pid) AS killed, pid
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid()
        AND state IN ('idle in transaction', 'idle in transaction (aborted)');
    ]])
    if rows and #rows > 0 then
      io.stderr:write(string.format(
        "knowledge_base_manager: terminated %d stale idle-in-transaction connection(s)\n",
        #rows))
    end
  end)
end

function KnowledgeBaseManager:_create_tables()
  local tn = self.table_name

  -- Clean up any stale connections that might be holding locks on the
  -- tables we're about to drop. Without this, a previously crashed build
  -- can leave an idle-in-transaction connection that blocks the DROP
  -- forever. Then set a lock_timeout so even if something we couldn't
  -- terminate is holding the lock, we fail fast with a clear error
  -- instead of hanging.
  self:_terminate_stale_locks()
  pcall(function() self:_exec("SET lock_timeout = '10s';") end)

  self:_delete_table(tn)
  self:_delete_table(tn .. "_info")
  self:_delete_table(tn .. "_link")
  self:_delete_table(tn .. "_link_mount")

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      knowledge_base VARCHAR NOT NULL,
      label VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      properties JSON,
      data JSON,
      has_link BOOLEAN DEFAULT FALSE,
      has_link_mount BOOLEAN DEFAULT FALSE,
      path LTREE UNIQUE
    )
  ]], quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      knowledge_base VARCHAR NOT NULL UNIQUE,
      description VARCHAR
    )
  ]], quote_ident(tn .. "_info")))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      link_name VARCHAR NOT NULL,
      parent_node_kb VARCHAR NOT NULL,
      parent_path LTREE NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(link_name, parent_node_kb, parent_path)
    )
  ]], quote_ident(tn .. "_link")))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      link_name VARCHAR NOT NULL UNIQUE,
      knowledge_base VARCHAR NOT NULL,
      mount_path LTREE NOT NULL,
      description VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(knowledge_base, mount_path)
    )
  ]], quote_ident(tn .. "_link_mount")))

  -- Indexes
  local indexes = {
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (knowledge_base)",
      quote_ident("idx_"..tn.."_kb"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (path)",
      quote_ident("idx_"..tn.."_path"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (label)",
      quote_ident("idx_"..tn.."_label"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (name)",
      quote_ident("idx_"..tn.."_name"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (has_link)",
      quote_ident("idx_"..tn.."_has_link"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (has_link_mount)",
      quote_ident("idx_"..tn.."_has_link_mount"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (knowledge_base, path)",
      quote_ident("idx_"..tn.."_kb_path"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (knowledge_base)",
      quote_ident("idx_"..tn.."_info_kb"), quote_ident(tn.."_info")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (link_name)",
      quote_ident("idx_"..tn.."_link_name"), quote_ident(tn.."_link")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (parent_node_kb)",
      quote_ident("idx_"..tn.."_link_parent_kb"), quote_ident(tn.."_link")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (parent_path)",
      quote_ident("idx_"..tn.."_link_parent_path"), quote_ident(tn.."_link")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (created_at)",
      quote_ident("idx_"..tn.."_link_created"), quote_ident(tn.."_link")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (link_name, parent_node_kb)",
      quote_ident("idx_"..tn.."_link_composite"), quote_ident(tn.."_link")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (link_name)",
      quote_ident("idx_"..tn.."_mount_link_name"), quote_ident(tn.."_link_mount")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (knowledge_base)",
      quote_ident("idx_"..tn.."_mount_kb"), quote_ident(tn.."_link_mount")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (mount_path)",
      quote_ident("idx_"..tn.."_mount_path"), quote_ident(tn.."_link_mount")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (created_at)",
      quote_ident("idx_"..tn.."_mount_created"), quote_ident(tn.."_link_mount")),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (knowledge_base, mount_path)",
      quote_ident("idx_"..tn.."_mount_composite"), quote_ident(tn.."_link_mount")),
  }

  for _, idx_sql in ipairs(indexes) do
    self:_exec(idx_sql)
  end

  -- Restore default lock_timeout for the rest of this session
  pcall(function() self:_exec("SET lock_timeout = 0;") end)
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

function KnowledgeBaseManager:add_kb(kb_name, description)
  assert(type(kb_name) == "string", "kb_name must be a string")
  assert(type(description) == "string", "description must be a string")

  local info_table = quote_ident(self.table_name .. "_info")
  self:_exec(string.format([[
    INSERT INTO %s (knowledge_base, description)
    VALUES (%s, %s)
    ON CONFLICT (knowledge_base) DO NOTHING
  ]], info_table, quote_literal(kb_name), quote_literal(description)))
end

function KnowledgeBaseManager:add_node(kb_name, label, name, properties, data, path)
  assert(type(kb_name) == "string", "kb_name must be a string")
  assert(type(label) == "string", "label must be a string")
  assert(type(name) == "string", "name must be a string")
  assert(type(path) == "string", "path must be a string")
  assert(type(properties) == "table", "properties must be a table")
  assert(type(data) == "table", "data must be a table")

  local info_table = quote_ident(self.table_name .. "_info")
  local check = self:_query(string.format(
    "SELECT 1 FROM %s WHERE knowledge_base = %s",
    info_table, quote_literal(kb_name)))

  if #check == 0 then
    error("Knowledge base '" .. kb_name .. "' not found in info table")
  end

  local props_json = json.encode(properties)
  local data_json  = json.encode(data)

  local main_table = quote_ident(self.table_name)
  self:_exec(string.format([[
    INSERT INTO %s (knowledge_base, label, name, properties, data, has_link, path)
    VALUES (%s, %s, %s, %s, %s, FALSE, %s)
  ]], main_table,
      quote_literal(kb_name),
      quote_literal(label),
      quote_literal(name),
      quote_literal(props_json),
      quote_literal(data_json),
      quote_literal(path)))
end

function KnowledgeBaseManager:add_link(parent_kb, parent_path, link_name)
  assert(type(parent_kb) == "string", "parent_kb must be a string")
  assert(type(parent_path) == "string", "parent_path must be a string")
  assert(type(link_name) == "string", "link_name must be a string")

  local info_table = quote_ident(self.table_name .. "_info")
  local found = self:_query(string.format(
    "SELECT knowledge_base FROM %s WHERE knowledge_base = %s",
    info_table, quote_literal(parent_kb)))

  if #found == 0 then
    error("Parent knowledge base '" .. parent_kb .. "' not found")
  end

  local main_table = quote_ident(self.table_name)
  local paths = self:_query(string.format(
    "SELECT path FROM %s WHERE path = %s",
    main_table, quote_literal(parent_path)))

  if #paths == 0 then
    error("Parent node with path '" .. parent_path .. "' not found")
  end

  local mount_table = quote_ident(self.table_name .. "_link_mount")
  local mount_check = self:_query(string.format(
    "SELECT link_name FROM %s WHERE link_name = %s",
    mount_table, quote_literal(link_name)))

  if #mount_check == 0 then
    error("Link name '" .. link_name .. "' not found in link_mount table")
  end

  local link_table = quote_ident(self.table_name .. "_link")
  self:_exec(string.format([[
    INSERT INTO %s (parent_node_kb, parent_path, link_name)
    VALUES (%s, %s, %s)
  ]], link_table,
      quote_literal(parent_kb),
      quote_literal(parent_path),
      quote_literal(link_name)))

  self:_exec(string.format(
    "UPDATE %s SET has_link = TRUE WHERE path = %s",
    main_table, quote_literal(parent_path)))
end

function KnowledgeBaseManager:add_link_mount(knowledge_base, path, link_mount_name, description)
  description = description or ""
  assert(type(knowledge_base) == "string", "knowledge_base must be a string")
  assert(type(path) == "string", "path must be a string")
  assert(type(link_mount_name) == "string", "link_mount_name must be a string")
  assert(type(description) == "string", "description must be a string")

  local info_table  = quote_ident(self.table_name .. "_info")
  local main_table  = quote_ident(self.table_name)
  local mount_table = quote_ident(self.table_name .. "_link_mount")

  local kb_check = self:_query(string.format(
    "SELECT knowledge_base FROM %s WHERE knowledge_base = %s",
    info_table, quote_literal(knowledge_base)))

  if #kb_check == 0 then
    error("Knowledge base '" .. knowledge_base .. "' does not exist in info table")
  end

  local path_check = self:_query(string.format(
    "SELECT id FROM %s WHERE knowledge_base = %s AND path = %s",
    main_table, quote_literal(knowledge_base), quote_literal(path)))

  if #path_check == 0 then
    error("Path '" .. path .. "' does not exist for knowledge base '" .. knowledge_base .. "'")
  end

  local dup_check = self:_query(string.format(
    "SELECT link_name FROM %s WHERE link_name = %s",
    mount_table, quote_literal(link_mount_name)))

  if #dup_check > 0 then
    error("Link name '" .. link_mount_name .. "' already exists in link_mount table")
  end

  self:_exec(string.format([[
    INSERT INTO %s (link_name, knowledge_base, mount_path, description)
    VALUES (%s, %s, %s, %s)
  ]], mount_table,
      quote_literal(link_mount_name),
      quote_literal(knowledge_base),
      quote_literal(path),
      quote_literal(description)))

  self:_exec(string.format(
    "UPDATE %s SET has_link_mount = TRUE WHERE knowledge_base = %s AND path = %s",
    main_table, quote_literal(knowledge_base), quote_literal(path)))

  return knowledge_base, path
end

return KnowledgeBaseManager