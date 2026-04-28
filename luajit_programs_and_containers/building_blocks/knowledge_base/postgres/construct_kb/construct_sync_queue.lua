--[[
  construct_sync_queue.lua

  Construction-time DDL for the Phase 6 sync-message queue.

  One UNLOGGED table per declared queue: <db>_sync_msg__<queue_name>.
  Plus a class registry table <db>_sync_queue_class for introspection +
  orphan sweep.

  DSL surface (via construct_data_tables facade):
    kb:add_sync_queue{
      queue_name  = "master_q",
      description = "Master inbox for slave -> master verbs",
    }

  Distinct from:
    - construct_stream_store: capped FIFO with class/instance, for
      durable ring buffers.
    - construct_rpc_*_table: slot-based async work-job RPC.

  Sync queues are message logs: append, drain, repeat. UNLOGGED because
  losing in-flight verbs on a pg crash is fine (cluster goes through
  full reset+rejoin anyway, per PHASE6_DESIGN §11).

  Design-memory: project_kb_sync_queue.md
]]

local C = require("construct_driver_common")

local Construct_Sync_Queue = {}
Construct_Sync_Queue.__index = Construct_Sync_Queue

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function Construct_Sync_Queue.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Sync_Queue)
  self.conn         = conn
  self.construct_kb = construct_kb
  self.database     = database
  self.upload_flag  = upload_flag or false

  self.t_class = database .. "_sync_queue_class"

  if not self.upload_flag then
    self:_setup_schema()
  end
  return self
end

---------------------------------------------------------------------------
-- Schema setup (registry only; per-queue tables created in add_sync_queue)
---------------------------------------------------------------------------

function Construct_Sync_Queue:_setup_schema()
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;",
    C.quote_ident(self.t_class)))

  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      queue_name      TEXT PRIMARY KEY,
      messages_table  TEXT NOT NULL,
      description     TEXT NOT NULL DEFAULT '',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  ]], C.quote_ident(self.t_class)))
end

---------------------------------------------------------------------------
-- add_sync_queue (DSL entry)
---------------------------------------------------------------------------

local function valid_qname(s)
  return type(s) == "string" and s:match("^[a-z][a-z0-9_]*$") ~= nil
end

--- Declare one sync queue. Creates the per-queue UNLOGGED table.
--- Idempotent on re-declare (CREATE TABLE IF NOT EXISTS + ON CONFLICT).
--- @param opts { queue_name = string, description = string (optional) }
function Construct_Sync_Queue:add_sync_queue(opts)
  assert(type(opts) == "table", "opts must be a table")
  local qname = assert(opts.queue_name, "queue_name required")
  assert(valid_qname(qname),
    "queue_name must match [a-z][a-z0-9_]*: " .. tostring(qname))
  local description = opts.description or ""

  local t_msg = self.database .. "_sync_msg__" .. qname

  -- 1. Per-queue UNLOGGED message table.
  C.exec(self.conn, string.format([[
    CREATE UNLOGGED TABLE IF NOT EXISTS %s (
      seq          BIGSERIAL PRIMARY KEY,
      verb         TEXT NOT NULL,
      payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
      inserted_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
  ]], C.quote_ident(t_msg)))

  -- Autovacuum tuning -- frequent inserts + deletes generate dead tuples.
  C.exec(self.conn, string.format([[
    ALTER TABLE %s SET (
      autovacuum_vacuum_scale_factor  = 0.05,
      autovacuum_vacuum_threshold     = 200,
      autovacuum_analyze_scale_factor = 0.1
    )
  ]], C.quote_ident(t_msg)))

  -- 2. Class registry row.
  C.exec(self.conn, string.format([[
    INSERT INTO %s (queue_name, messages_table, description)
    VALUES (%s, %s, %s)
    ON CONFLICT (queue_name) DO UPDATE SET
      messages_table = EXCLUDED.messages_table,
      description    = EXCLUDED.description
  ]],
    C.quote_ident(self.t_class),
    C.quote_literal(qname),
    C.quote_literal(t_msg),
    C.quote_literal(description)))

  -- 3. KB info node for browsability.
  self.construct_kb:add_info_node(
    "KB_SYNC_QUEUE",
    qname,
    { messages_table = t_msg },
    {},
    description
  )

  return { added = qname, messages_table = t_msg }
end

---------------------------------------------------------------------------
-- Installation check
---------------------------------------------------------------------------

--- Conservative orphan-check: does NOT drop per-queue tables; reports
--- only registry rows for which the messages_table is missing.
function Construct_Sync_Queue:check_installation()
  local rows = C.query_all(self.conn, string.format([[
    SELECT c.queue_name, c.messages_table,
           (SELECT to_regclass(c.messages_table)) AS exists_check
      FROM %s c
  ]], C.quote_ident(self.t_class)))
  for _, r in ipairs(rows) do
    if r.exists_check == nil then
      io.stderr:write(string.format(
        "[sync_queue] WARNING: registry row %s references missing table %s\n",
        r.queue_name, r.messages_table))
    end
  end
end

return Construct_Sync_Queue
