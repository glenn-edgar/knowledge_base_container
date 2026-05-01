--[[
  construct_stream_store.lua

  Construction-time DDL for the class/instance capped-FIFO stream store.
  Distinct from the existing construct_stream_table (which is the
  static-path KB_STREAM_FIELD pattern); both can coexist.

  Three kinds of tables per KB database:
    - <db>_stream_class   (registry of declared classes; one row per
                           kb:stream_class)
    - <db>_stream_inst    (registry of instances; one row per open stream)
    - <db>_stream_msg__<ns>  (per-class messages; one table per class,
                              optionally UNLOGGED, fillfactor-tuned)

  Per-class push function emitted alongside each messages table:
    <db>_stream_push__<ns>(p_path ltree, p_payload bytea) RETURNS bigint

  DSL surface (via construct_data_tables facade):
    kb:add_stream_class{
      namespace       = "telemetry.robot_heartbeat",
      cap             = 256,
      unlogged        = true,
      retention_idle  = "7 days",       -- PG interval literal, optional
      description     = "...",
    }

  Design-memory: project_dcs_stream_driver.md.
]]

local json = require("dkjson")
local C    = require("construct_driver_common")

local Construct_Stream_Store = {}
Construct_Stream_Store.__index = Construct_Stream_Store

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function Construct_Stream_Store.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Stream_Store)
  self.conn         = conn
  self.construct_kb = construct_kb
  self.database     = database
  self.upload_flag  = upload_flag or false

  self.t_class = database .. "_stream_class"
  self.t_inst  = database .. "_stream_inst"

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Schema setup (global tables; per-class tables emitted in add_stream_class)
---------------------------------------------------------------------------

function Construct_Stream_Store:_setup_schema()
  C.exec(self.conn, "CREATE EXTENSION IF NOT EXISTS ltree;")

  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;", C.quote_ident(self.t_inst)))
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;", C.quote_ident(self.t_class)))

  -- Class registry.
  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      namespace       LTREE PRIMARY KEY,
      cap_default     INT NOT NULL CHECK (cap_default > 0),
      unlogged        BOOLEAN NOT NULL DEFAULT FALSE,
      retention_idle  INTERVAL,
      messages_table  TEXT NOT NULL,
      push_function   TEXT NOT NULL,
      description     TEXT NOT NULL DEFAULT '',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  ]], C.quote_ident(self.t_class)))

  C.exec(self.conn, string.format(
    "CREATE INDEX %s ON %s USING GIST(namespace)",
    C.quote_ident("idx_" .. self.t_class .. "_ns_gist"),
    C.quote_ident(self.t_class)))

  -- Instance registry.
  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      path            LTREE PRIMARY KEY,
      class_namespace LTREE NOT NULL REFERENCES %s(namespace) ON DELETE CASCADE,
      cap             INT NOT NULL CHECK (cap > 0),
      tail_seq        BIGINT NOT NULL DEFAULT 0,
      generation      INT NOT NULL DEFAULT 1,
      entity_key      TEXT,
      last_write_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      meta            JSONB NOT NULL DEFAULT '{}'::jsonb
    )
  ]], C.quote_ident(self.t_inst), C.quote_ident(self.t_class)))

  C.exec(self.conn, string.format("CREATE INDEX %s ON %s USING GIST(path)",
    C.quote_ident("idx_" .. self.t_inst .. "_path_gist"), C.quote_ident(self.t_inst)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s USING GIST(class_namespace)",
    C.quote_ident("idx_" .. self.t_inst .. "_class_gist"), C.quote_ident(self.t_inst)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s(entity_key) WHERE entity_key IS NOT NULL",
    C.quote_ident("idx_" .. self.t_inst .. "_entity"), C.quote_ident(self.t_inst)))
  C.exec(self.conn, string.format("CREATE INDEX %s ON %s(last_write_at)",
    C.quote_ident("idx_" .. self.t_inst .. "_lwa"), C.quote_ident(self.t_inst)))
end

---------------------------------------------------------------------------
-- add_stream_class (DSL entry)
---------------------------------------------------------------------------

--- Register a stream class. Creates the per-class messages table and
--- push function, then records the class row and a KB_STREAM_CLASS info
--- node for browsability.
function Construct_Stream_Store:add_stream_class(opts)
  assert(type(opts) == "table", "opts must be a table")
  local ns             = assert(opts.namespace, "namespace required")
  assert(opts.cap, "cap required")
  local cap            = tonumber(opts.cap)
  local unlogged       = opts.unlogged and true or false
  local retention_idle = opts.retention_idle  -- PG interval string or nil
  local description    = opts.description or ""

  assert(cap and cap > 0, "cap must be a positive integer")
  local ok, err = C.is_valid_ltree(ns)
  assert(ok, err)

  local t_msg = C.derived_table_name(self.database, "stream_msg", ns)
  local f_push = C.derived_function_name(self.database, "stream_push", ns)

  -- 1. Per-class messages table.
  local unlogged_sql = unlogged and "UNLOGGED" or ""
  C.exec(self.conn, string.format([[
    CREATE %s TABLE IF NOT EXISTS %s (
      stream_path  LTREE NOT NULL,
      seq          BIGINT NOT NULL,
      generation   INT NOT NULL,
      ts           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      payload      BYTEA NOT NULL,
      PRIMARY KEY (stream_path, seq)
    ) WITH (fillfactor = 90)
  ]], unlogged_sql, C.quote_ident(t_msg)))

  -- Per-table autovacuum tuning. Idempotent.
  C.exec(self.conn, string.format([[
    ALTER TABLE %s SET (
      autovacuum_vacuum_scale_factor  = 0.05,
      autovacuum_vacuum_threshold     = 1000,
      autovacuum_analyze_scale_factor = 0.05
    )
  ]], C.quote_ident(t_msg)))

  -- 2. Atomic push function. Row-lock on stream_inst serializes concurrent
  -- pushes to the same path; the range-delete does the cap eviction.
  local push_sql = string.format([[
    CREATE OR REPLACE FUNCTION %s(p_path LTREE, p_payload BYTEA)
    RETURNS BIGINT LANGUAGE plpgsql AS $FN$
    DECLARE v_seq BIGINT; v_cap INT; v_gen INT;
    BEGIN
      UPDATE %s
         SET tail_seq      = tail_seq + 1,
             last_write_at = clock_timestamp()
       WHERE path = p_path
         AND class_namespace = %s::LTREE
      RETURNING tail_seq, cap, generation INTO v_seq, v_cap, v_gen;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'no such stream instance: %%', p_path;
      END IF;

      INSERT INTO %s (stream_path, seq, generation, payload)
      VALUES (p_path, v_seq, v_gen, p_payload);

      DELETE FROM %s
       WHERE stream_path = p_path
         AND seq <= v_seq - v_cap;

      RETURN v_seq;
    END;
    $FN$
  ]],
    C.quote_ident(f_push),
    C.quote_ident(self.t_inst),
    C.quote_literal(ns),
    C.quote_ident(t_msg),
    C.quote_ident(t_msg))
  C.exec(self.conn, push_sql)

  -- 3. Class registry row.
  C.exec(self.conn, string.format([[
    INSERT INTO %s (namespace, cap_default, unlogged, retention_idle,
                    messages_table, push_function, description)
    VALUES (%s::LTREE, %d, %s, %s, %s, %s, %s)
    ON CONFLICT (namespace) DO UPDATE SET
      cap_default    = EXCLUDED.cap_default,
      unlogged       = EXCLUDED.unlogged,
      retention_idle = EXCLUDED.retention_idle,
      messages_table = EXCLUDED.messages_table,
      push_function  = EXCLUDED.push_function,
      description    = EXCLUDED.description
  ]],
    C.quote_ident(self.t_class),
    C.quote_literal(ns),
    cap,
    tostring(unlogged),
    retention_idle and (C.quote_literal(retention_idle) .. "::INTERVAL") or "NULL",
    C.quote_literal(t_msg),
    C.quote_literal(f_push),
    C.quote_literal(description)))

  -- 4. KB info node for browsability.
  self.construct_kb:add_info_node(
    "KB_STREAM_CLASS",
    ns,
    { cap_default = cap, unlogged = unlogged,
      messages_table = t_msg, push_function = f_push },
    {},
    description
  )

  return { added = ns, messages_table = t_msg, push_function = f_push }
end

---------------------------------------------------------------------------
-- Installation check
---------------------------------------------------------------------------

--- Orphan-sweep instances whose class was removed, and (optionally) drop
--- per-class tables whose class row is gone. Conservative: only sweeps
--- instances; does NOT drop tables here (safer to require explicit
--- class_drop).
function Construct_Stream_Store:check_installation()
  C.exec(self.conn, string.format([[
    DELETE FROM %s i
    WHERE NOT EXISTS (SELECT 1 FROM %s c WHERE c.namespace = i.class_namespace)
  ]], C.quote_ident(self.t_inst), C.quote_ident(self.t_class)))
end

return Construct_Stream_Store
