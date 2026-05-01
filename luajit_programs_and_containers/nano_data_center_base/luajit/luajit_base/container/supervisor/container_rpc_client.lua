-- =============================================================================
-- container_rpc_client.lua -- Phase 6.4 container-side RPC client.
--
-- Drop-in for any luajit-base supervisor; talks to the master's container_rpc
-- module over per-container pg-backed sync queues.
--
-- Identity (from supervisor's ctx):
--   ctx.env.CONTAINER_NAME  -- e.g. "test_app_01"
--   ctx.env.APP_CPU_ID      -- e.g. "cpu_01"
--   ctx.env.APP_SITE        -- e.g. "moonbase.alpha.dcs"
--
-- Queue topology (must match construction/subsystems/container_queues.lua):
--   inbox  : container_<NAME>_q              (master writes; we read)
--   outbox : container_inbox_<APP_CPU_ID>_q  (we write; master reads)
--
-- Cadence: HEARTBEAT every 60s ±10% jitter; missed-ACK threshold 3 ->
-- os.exit(0) for watchdog respawn (one-reset-path discipline). Same -1
-- tolerance as sync layer to absorb cadence-boundary timing.
--
-- Verb set (mirrors design 7.2):
--   container -> master : CONTAINER_READY, HEARTBEAT
--   master -> container : HEARTBEAT_ACK, PAUSE, RESUME, DRAIN, RESET_HINT
--
-- Public API:
--   local cli = require("container_rpc_client").new(ctx)
--   cli:send_ready()                 -- one-shot at supervisor monitor entry
--   cli:tick()                       -- periodic; drives drain + HB cadence
--   cli:request_drain()              -- supervisor calls this on DRAIN verb;
--                                       returns true if teardown was initiated
--   cli:state()                      -- "DISCONNECTED" / "ACTIVE" / "PAUSED" /
--                                       "DRAINING"
-- =============================================================================

local sync_q = require("kb_sync_queue")

local M = {}
M.__index = M

local DATABASE              = "knowledge_base"
local HEARTBEAT_PERIOD_S    = 60.0
local HEARTBEAT_JITTER_PCT  = 0.10
local MISSED_ACK_THRESHOLD  = 3
local MAX_VERBS_PER_TICK    = 5

local S_DISCONNECTED = "DISCONNECTED"
local S_JOINING      = "JOINING"
local S_ACTIVE       = "ACTIVE"
local S_PAUSED       = "PAUSED"
local S_DRAINING     = "DRAINING"

local function now_s() return os.time() end

local function jittered(period, pct)
  return period * (1.0 + (math.random() * 2.0 - 1.0) * pct)
end

function M.new(ctx)
  assert(ctx and ctx.env, "container_rpc_client.new: ctx.env required")
  local name   = ctx.env.CONTAINER_NAME or ""
  local cpu_id = ctx.env.APP_CPU_ID     or ""
  assert(name   ~= "", "CONTAINER_NAME env var required")
  assert(cpu_id ~= "", "APP_CPU_ID env var required")

  local self = setmetatable({}, M)
  self.ctx       = ctx
  self.name      = name
  self.cpu_id    = cpu_id
  self.inbox_q   = "container_" .. name .. "_q"
  self.outbox_q  = "container_inbox_" .. cpu_id .. "_q"
  self.epoch     = math.floor(os.time())
  self.state     = S_DISCONNECTED

  self.next_hb_at      = 0
  self.next_hb_seq     = 1
  self.last_ack_seq    = 0
  self.last_ack_at     = 0
  self.missed_acks     = 0
  self.drain_requested = false   -- DRAIN verb received -- supervisor reads

  return self
end

function M:_log(msg)
  if self.ctx.log then
    self.ctx.log("crpc_cli", msg)
  else
    io.stderr:write("[crpc_cli] " .. msg .. "\n")
  end
end

function M:_pg()
  -- Supervisor stores its DBI handle as ctx.connectors.pg (set by
  -- VERIFY_PG handler). Caller must have run setup before calling tick().
  return self.ctx.connectors and self.ctx.connectors.pg
end

---------------------------------------------------------------------------
-- One-shot CONTAINER_READY at monitor entry.
---------------------------------------------------------------------------

function M:send_ready()
  local conn = self:_pg()
  if not conn then return false, "no pg connection" end
  self.epoch = math.floor(os.time())
  self.state = S_JOINING
  self.last_ack_at = now_s()       -- start the missed-ACK clock
  -- Fire the first HEARTBEAT on the very next tick rather than waiting
  -- HEARTBEAT_PERIOD_S. Master only promotes a peer to ACTIVE on receipt
  -- of HEARTBEAT (not HEARTBEAT_ACK), so without this nudge it sits at
  -- JOINING for ~60s -- same race we fixed in sync_rpc:_slave_on_join_ack.
  self.next_hb_at  = now_s()
  self.next_hb_seq = 1
  self.missed_acks = 0
  local _, err = sync_q.push(conn, DATABASE, self.outbox_q,
    "CONTAINER_READY", {
      name = self.name, slot = 1, epoch = self.epoch,
    })
  if err then
    self:_log("CONTAINER_READY push failed: " .. tostring(err))
    return false, err
  end
  self:_log(string.format(
    "CONTAINER_READY sent (epoch=%d) -> master inbox %s",
    self.epoch, self.outbox_q))
  return true
end

---------------------------------------------------------------------------
-- Inbound verb handlers
---------------------------------------------------------------------------

function M:_on_heartbeat_ack(payload)
  local seq = tonumber(payload.seq) or 0
  if seq < self.last_ack_seq then return end
  self.last_ack_seq = seq
  self.last_ack_at  = now_s()
  self.missed_acks  = 0
  if self.state == S_JOINING then
    self.state = S_ACTIVE
    self:_log(string.format("ACK seq=%d -> ACTIVE", seq))
  end
end

function M:_on_pause(payload)
  if self.state ~= S_ACTIVE then return end
  self.state = S_PAUSED
  self:_log(string.format("PAUSE received (reason=%s) -> PAUSED",
    tostring(payload.reason or "")))
  -- Apps wanting to honor pause should poll ctx.crpc:state(); the supervisor
  -- itself doesn't quiesce its child processes here -- that's an
  -- application-policy decision.
end

function M:_on_resume(_payload)
  if self.state == S_PAUSED then
    self.state = S_ACTIVE
    self:_log("RESUME received -> ACTIVE")
  end
end

function M:_on_drain(payload)
  self.state = S_DRAINING
  self.drain_requested = true
  self:_log(string.format("DRAIN received (reason=%s); supervisor will tear down",
    tostring(payload.reason or "")))
end

function M:_on_reset_hint(payload)
  self:_log(string.format(
    "RESET_HINT received (reason=%s) -- exiting for watchdog respawn",
    tostring(payload.reason or "")))
  os.exit(0)
end

local DISPATCH = {
  HEARTBEAT_ACK = M._on_heartbeat_ack,
  PAUSE         = M._on_pause,
  RESUME        = M._on_resume,
  DRAIN         = M._on_drain,
  RESET_HINT    = M._on_reset_hint,
}

---------------------------------------------------------------------------
-- Periodic tick: drain inbox, send HB if cadence elapsed, check missed-ACK.
---------------------------------------------------------------------------

function M:tick()
  local conn = self:_pg()
  if not conn then return end
  if self.state == S_DISCONNECTED then return end

  -- 1. Drain inbox.
  local rows, derr = sync_q.drain(conn, DATABASE, self.inbox_q,
                                  MAX_VERBS_PER_TICK)
  if derr then
    self:_log("drain " .. self.inbox_q .. " err: " .. tostring(derr))
    rows = {}
  end
  for _, row in ipairs(rows) do
    local h = DISPATCH[row.verb]
    if h then
      h(self, row.payload or {})
    else
      self:_log("unknown verb " .. tostring(row.verb))
    end
  end

  -- 2. HEARTBEAT cadence.
  local now = now_s()
  if now >= self.next_hb_at then
    local seq = self.next_hb_seq
    local _, perr = sync_q.push(conn, DATABASE, self.outbox_q,
      "HEARTBEAT", {
        name = self.name, epoch = self.epoch, seq = seq,
      })
    if perr then
      self:_log("HEARTBEAT push failed: " .. tostring(perr))
    end
    self.next_hb_seq = seq + 1
    self.next_hb_at  = now + jittered(HEARTBEAT_PERIOD_S, HEARTBEAT_JITTER_PCT)
  end

  -- 3. Missed-ACK detection. Same -1 tolerance as sync layer.
  if self.state == S_ACTIVE or self.state == S_PAUSED then
    local since_ack = now - self.last_ack_at
    local expected = math.max(0,
      math.floor(since_ack / HEARTBEAT_PERIOD_S) - 1)
    if expected > self.missed_acks then
      self.missed_acks = expected
      self:_log(string.format(
        "missed HEARTBEAT_ACK count = %d/%d (since_ack=%ds)",
        self.missed_acks, MISSED_ACK_THRESHOLD, since_ack))
    end
    if self.missed_acks >= MISSED_ACK_THRESHOLD then
      self:_log(string.format(
        "FAIL-STOP: %d missed ACKs -- exiting for watchdog respawn",
        self.missed_acks))
      os.exit(0)
    end
  end
end

---------------------------------------------------------------------------
-- Public state accessors used by the supervisor / apps.
---------------------------------------------------------------------------

function M:state_name() return self.state end
function M:drain_pending() return self.drain_requested end

return M
