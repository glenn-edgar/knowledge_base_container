--[[
  construct_doc_store.lua

  Construction-time DDL for the content-addressable document store.

  Three tables per KB database:
    - <db>_doc_class   (registry of declared classes, one row per kb:doc_class)
    - <db>_fs_blob     (content-addressable blob store, sha256 PK)
    - <db>_fs_node     (path -> blob pointer, one row per file or dir)

  DSL surface (exposed via construct_data_tables facade):
    kb:add_doc_class{
      namespace    = "apps.irrigation.schedules",
      writer       = "runtime" | "commissioning_only",
      source_dir   = "/path/on/laptop/for/load_dir_extract_dir",
      content_type = "application/json" (optional, default "application/octet-stream"),
      description  = "...",
    }

  Classes are registered in two places:
    1. <db>_doc_class row (for driver lookup at runtime).
    2. KB_DOC_CLASS node via construct_kb:add_info_node (for schema
       browsing + commissioning to find the class).

  Design-memory: project_dcs_doc_driver.md.
]]

local json = require("dkjson")
local C    = require("construct_driver_common")

local Construct_Doc_Store = {}
Construct_Doc_Store.__index = Construct_Doc_Store

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create the doc store for a KB database.
-- @param conn          DBI connection
-- @param construct_kb  Construct_KB instance (for add_info_node)
-- @param database      base KB table name (e.g. "system")
-- @param upload_flag   boolean, skip schema creation if true
function Construct_Doc_Store.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Doc_Store)
  self.conn         = conn
  self.construct_kb = construct_kb
  self.database     = database
  self.upload_flag  = upload_flag or false

  -- Table names (scoped per-KB following repo convention).
  self.t_class = database .. "_doc_class"
  self.t_blob  = database .. "_fs_blob"
  self.t_node  = database .. "_fs_node"

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Schema setup
---------------------------------------------------------------------------

function Construct_Doc_Store:_setup_schema()
  C.exec(self.conn, "CREATE EXTENSION IF NOT EXISTS ltree;")

  -- DROP in reverse-dependency order (node references blob and class).
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;", C.quote_ident(self.t_node)))
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;", C.quote_ident(self.t_blob)))
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;", C.quote_ident(self.t_class)))

  -- Class registry.
  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      namespace    LTREE PRIMARY KEY,
      writer       TEXT NOT NULL CHECK (writer IN ('runtime','commissioning_only')),
      source_dir   TEXT,
      content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
      description  TEXT NOT NULL DEFAULT '',
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  ]], C.quote_ident(self.t_class)))

  C.exec(self.conn, string.format(
    "CREATE INDEX %s ON %s USING GIST(namespace)",
    C.quote_ident("idx_" .. self.t_class .. "_ns_gist"),
    C.quote_ident(self.t_class)))

  -- Content-addressable blob store.
  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      sha256     BYTEA PRIMARY KEY,
      size       BIGINT NOT NULL,
      content    BYTEA NOT NULL,
      mime       TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  ]], C.quote_ident(self.t_blob)))

  -- Path -> blob map. One row per filesystem node.
  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      path            LTREE PRIMARY KEY,
      class_namespace LTREE NOT NULL REFERENCES %s(namespace) ON DELETE CASCADE,
      kind            TEXT NOT NULL CHECK (kind IN ('file','dir')),
      sha256          BYTEA REFERENCES %s(sha256),
      filename        TEXT NOT NULL,
      ext             TEXT,
      entity_key      TEXT,
      meta            JSONB NOT NULL DEFAULT '{}'::jsonb,
      mtime           TIMESTAMPTZ,
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT %s CHECK (
        (kind = 'dir'  AND sha256 IS NULL) OR
        (kind = 'file' AND sha256 IS NOT NULL)
      )
    )
  ]],
    C.quote_ident(self.t_node),
    C.quote_ident(self.t_class),
    C.quote_ident(self.t_blob),
    C.quote_ident(self.t_node .. "_kind_sha_ck")))

  -- Indexes on fs_node.
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s USING GIST(path)",
    C.quote_ident("idx_" .. self.t_node .. "_path_gist"), C.quote_ident(self.t_node)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s USING GIST(class_namespace)",
    C.quote_ident("idx_" .. self.t_node .. "_class_gist"), C.quote_ident(self.t_node)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s(entity_key) WHERE entity_key IS NOT NULL",
    C.quote_ident("idx_" .. self.t_node .. "_entity"), C.quote_ident(self.t_node)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s(sha256) WHERE sha256 IS NOT NULL",
    C.quote_ident("idx_" .. self.t_node .. "_sha"), C.quote_ident(self.t_node)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s(ext) WHERE ext IS NOT NULL",
    C.quote_ident("idx_" .. self.t_node .. "_ext"), C.quote_ident(self.t_node)))
end

---------------------------------------------------------------------------
-- Public API: declare a doc class (DSL-side)
---------------------------------------------------------------------------

--- Register a doc class. Emits one row in <db>_doc_class and records a
--- KB_DOC_CLASS info node so the class is browsable from the KB tree.
--- @param opts table with fields:
---   namespace    string  ltree path
---   writer       string  'runtime' | 'commissioning_only'
---   source_dir   string  (optional, required for load_dir / extract_dir)
---   content_type string  (optional, default 'application/octet-stream')
---   description  string  (optional)
function Construct_Doc_Store:add_doc_class(opts)
  assert(type(opts) == "table", "opts must be a table")
  local ns          = assert(opts.namespace,   "namespace required")
  local writer      = opts.writer or "commissioning_only"
  local source_dir  = opts.source_dir
  local content_type = opts.content_type or "application/octet-stream"
  local description  = opts.description or ""

  assert(writer == "runtime" or writer == "commissioning_only",
    "writer must be 'runtime' or 'commissioning_only'")

  local ok, err = C.is_valid_ltree(ns)
  assert(ok, err)

  -- Row in the registry table.
  C.exec(self.conn, string.format([[
    INSERT INTO %s (namespace, writer, source_dir, content_type, description)
    VALUES (%s::ltree, %s, %s, %s, %s)
    ON CONFLICT (namespace) DO UPDATE SET
      writer       = EXCLUDED.writer,
      source_dir   = EXCLUDED.source_dir,
      content_type = EXCLUDED.content_type,
      description  = EXCLUDED.description
  ]],
    C.quote_ident(self.t_class),
    C.quote_literal(ns),
    C.quote_literal(writer),
    source_dir and C.quote_literal(source_dir) or "NULL",
    C.quote_literal(content_type),
    C.quote_literal(description)))

  -- KB info node so browsers see the class. Node name is the last label
  -- of the namespace; parent path is everything before it. If the caller
  -- hasn't positioned the construct_kb stack there, the add_info_node
  -- call will land at the current stack position -- this matches how
  -- add_stream_field / add_status_field already work (positioning is
  -- the caller's responsibility).
  self.construct_kb:add_info_node(
    "KB_DOC_CLASS",
    ns,
    { writer = writer, content_type = content_type, source_dir = source_dir },
    {},
    description
  )

  return { added = ns, writer = writer }
end

---------------------------------------------------------------------------
-- Installation check
---------------------------------------------------------------------------

--- Validate that every fs_node row references a known class. Removes
--- orphan fs_node rows (class got retired without sweeping children).
--- Also orphan-sweeps fs_blob rows no longer referenced by any fs_node.
function Construct_Doc_Store:check_installation()
  -- Orphan fs_node rows (class removed but nodes linger due to
  -- SET NULL or a bulk DELETE that missed the FK cascade).
  C.exec(self.conn, string.format([[
    DELETE FROM %s n
    WHERE NOT EXISTS (SELECT 1 FROM %s c WHERE c.namespace = n.class_namespace)
  ]], C.quote_ident(self.t_node), C.quote_ident(self.t_class)))

  -- Orphan blob sweep. Cheap if rare, fine to do at install time.
  C.exec(self.conn, string.format([[
    DELETE FROM %s b
    WHERE NOT EXISTS (SELECT 1 FROM %s n WHERE n.sha256 = b.sha256)
  ]], C.quote_ident(self.t_blob), C.quote_ident(self.t_node)))
end

return Construct_Doc_Store
