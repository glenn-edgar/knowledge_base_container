--[[
  construct_exception_store.lua

  SCADA-style SYS_EXCEPTION emitter. Promotes the legacy flat-info-node
  SYS_EXCEPTION pattern to a nested header with ISA-18.2 alarm-lifecycle
  children (state enum, priority, timestamps, operator ids, signature
  dedup jsonb, etc.).

  One call emits 1 header + 15 KB_STATUS_FIELD children + 1 KB_JSONB_FIELD
  (signatures). Reuses existing Construct_Data_Tables satellites
  (status_table, jsonb_table) under the hood — no new PG tables are
  created by this module.

  Usage (from a construction/subsystems/*.lua file):

    kb:add_exception("container_start_failed", {
      type = "docker", instance = "node_control",
      description = "docker run returned non-zero",
      priority = 2,
      response_procedure = "/ops/runbooks/container-start-failed.md",
    })

  Design-memory: project_dcs_task4_design.md.
]]

local Construct_Exception_Store = {}
Construct_Exception_Store.__index = Construct_Exception_Store

local STATE_VALUES = {
  NORMAL = true, UNACK_ACTIVE = true, ACK_ACTIVE = true,
  RTN_UNACK = true, SHELVED = true,
}

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create the exception-store helper.
-- @param conn        DBI connection (unused here; kept for facade symmetry)
-- @param cdt         Construct_Data_Tables facade (for add_status_field,
--                    add_jsonb_field, with_header)
-- @param database    base KB table name
-- @param upload_flag boolean, unused
function Construct_Exception_Store.new(conn, cdt, database, upload_flag)
  local self = setmetatable({}, Construct_Exception_Store)
  self.conn     = conn
  self.cdt      = cdt
  self.database = database
  -- No schema setup needed: everything we emit reuses existing
  -- KB_STATUS_FIELD / KB_JSONB_FIELD tables.
  return self
end

---------------------------------------------------------------------------
-- Helper emitting the standard child set
---------------------------------------------------------------------------

function Construct_Exception_Store:_emit_state_children(cdt)
  -- Alarm state: SCADA-style enum
  cdt:add_status_field("state", {},
    "alarm state (NORMAL | UNACK_ACTIVE | ACK_ACTIVE | RTN_UNACK | SHELVED)",
    { value = "NORMAL" })

  -- Lifecycle timestamps (epoch seconds; 0 = never)
  cdt:add_status_field("last_raised_ts",  {}, "last raise epoch s",      { value = 0 })
  cdt:add_status_field("last_rtn_ts",     {}, "returned-to-normal epoch s", { value = 0 })
  cdt:add_status_field("last_ack_ts",     {}, "operator ack epoch s",    { value = 0 })
  cdt:add_status_field("last_ack_by",     {}, "operator id on last ack", { value = "" })
  cdt:add_status_field("last_shelve_ts",  {}, "last shelve epoch s",     { value = 0 })
  cdt:add_status_field("last_shelve_by",  {}, "operator id on shelve",   { value = "" })
  cdt:add_status_field("shelve_until",    {}, "shelve expiry epoch s; 0 = not shelved", { value = 0 })

  -- Most-recent-raise summary (also stored in signatures[0] but kept here
  -- for cheap UI glance reads).
  cdt:add_status_field("last_error",         {}, "most recent error text", { value = "" })
  cdt:add_status_field("last_trigger_value", {}, "measured value",         { value = "" })
  cdt:add_status_field("last_limit_value",   {}, "limit violated",         { value = "" })
  cdt:add_status_field("last_source_path",   {}, "ltree of triggering log", { value = "" })
  cdt:add_status_field("last_comment",       {}, "operator comment, most recent", { value = "" })

  -- Aggregate counters
  cdt:add_status_field("hit_count",      {}, "lifetime raise() calls", { value = 0 })
  cdt:add_status_field("flap_rate_5min", {}, "rolling 5-min raise rate (flood detect)", { value = 0 })

  -- Signature-deduplicated summary (SCADA alarm summary, not journal).
  cdt:add_jsonb_field("signatures", "exception_signatures",
    "deduplicated signature list; each entry is a UNIQUE error content: " ..
    "{signature, error_b64, trigger_value, limit_value, source_path, " ..
    "first_occurrence_ts, last_occurrence_ts, occurrence_count}",
    {})
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Emit a SYS_EXCEPTION nested header with full SCADA lifecycle state.
--- @param name string    exception identifier (becomes header name)
--- @param opts table with fields:
---   type                string  subsystem category ('docker', 'resource', ...)
---   instance            string  subsystem name ('node_control', ...)
---   description         string  human-readable
---   priority            int     1 (Emergency) | 2 (High) | 3 (Medium, default) | 4 (Low)
---   response_procedure  string  runbook pointer (optional)
function Construct_Exception_Store:add_exception(name, opts)
  assert(type(name) == "string" and #name > 0, "exception name required")
  opts = opts or {}

  local priority = opts.priority or 3
  assert(type(priority) == "number" and priority >= 1 and priority <= 4,
    "priority must be integer 1..4")

  local props = {
    type               = opts.type or "unknown",
    instance           = opts.instance or "",
    priority           = priority,
    response_procedure = opts.response_procedure or "",
  }

  local description = opts.description or ""
  local cdt = self.cdt

  cdt:with_header("SYS_EXCEPTION", name, props, {}, description,
    function() self:_emit_state_children(cdt) end)

  return {
    added = name,
    priority = priority,
  }
end

---------------------------------------------------------------------------
-- check_installation - no-op (underlying satellites handle their state)
---------------------------------------------------------------------------

function Construct_Exception_Store:check_installation()
  -- Intentionally empty. The KB_STATUS_FIELD + KB_JSONB_FIELD children
  -- emitted by add_exception are synced by the status_table and
  -- jsonb_table satellites' own check_installation passes.
end

return Construct_Exception_Store
