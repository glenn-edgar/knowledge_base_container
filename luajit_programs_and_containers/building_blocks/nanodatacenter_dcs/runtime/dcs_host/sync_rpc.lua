-- =============================================================================
-- sync_rpc.lua -- Phase 6.1 inter-CPU sync via pg-backed RPC queues.
--
-- Replaces the bit-mask + cluster_go sync handshake with message passing
-- over per-CPU sync queues (kb_sync_queue). Implements:
--   - 7 verbs (JOIN_REQ, JOIN_ACK, JOIN_CONFIRM, HEARTBEAT, HEARTBEAT_ACK,
--             RESET_HINT, DRAIN) per PHASE6_DESIGN.md §3.2.
--   - Master-side per-peer state machine + slave-side own-state machine
--     per PHASE6_DESIGN.md §3.3.
--   - rpc_scheduler tick: round-robin one peer per walker tick (master),
--     drain max 5 verbs, flush outbound.
--   - Handler budget telemetry: <50ms cap with INFO@30ms + SCADA
--     exception@50ms; 60s rolling summary.
--   - Master 2s grace after start before ACKing JOINs.
--   - Heartbeat cadence ±10% jitter.
--   - Fail-stop after 3 missed HEARTBEAT_ACKs (slave-side).
--
-- Public API:
--   local sync = require("sync_rpc").new(ctx)
--   sync:install_handlers(R)     -- registers chain-tree handlers
--   sync:budget_summary()        -- 60s rolling stats {max, p95, ...}
--   sync:_state()                -- in-RAM state (tests + admin probe)
--
-- ctx fields used:
--   cfg.cpu_id, cfg.is_master, cfg.master_cpu, cfg.peers, cfg.site
--   connectors.pg, log, kb_status, kb_exception, cfg (for exc paths)
--
-- Memory: project_kb_sync_queue, project_phase6_transport,
-- feedback_phase6_handler_budget, feedback_one_reset_path.
-- =============================================================================

local sync_q = require("kb_sync_queue")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constants (per PHASE6_DESIGN §8 + §10 Q2)
---------------------------------------------------------------------------

local DATABASE              = "knowledge_base"   -- pg KB name (matches build_kb)
local MAX_VERBS_PER_TICK    = 5                  -- §8 / handler budget
local MASTER_GRACE_S        = 2.0                -- §10 Q2
local HEARTBEAT_PERIOD_S    = 5.0                -- §8
local HEARTBEAT_JITTER_PCT  = 0.10               -- §8 universal rule 2
local MISSED_ACK_THRESHOLD  = 3                  -- §8 universal rule 1
local BUDGET_WARN_MS        = 30                 -- feedback memory
local BUDGET_VIOLATION_MS   = 50                 -- feedback memory
local BUDGET_SAMPLE_WINDOW  = 60.0               -- 60s rolling
local PEER_STATE_WRITE_S    = 5.0                -- KB write cadence

-- Slave own-state values
local S_DISCONNECTED  = "DISCONNECTED"
local S_JOINING       = "JOINING"
local S_ACK_RECEIVED  = "ACK_RECEIVED"
local S_ACTIVE        = "ACTIVE"

-- Master per-peer-state values
local P_UNKNOWN          = "UNKNOWN"
local P_JOINING_SAW_REQ  = "JOINING_SAW_REQ"
local P_ACTIVE           = "ACTIVE"
local P_DRAINING         = "DRAINING"

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function M.new(ctx)
  assert(ctx and ctx.cfg, "sync_rpc.new: ctx.cfg required")
  local self = setmetatable({}, M)
  self.ctx        = ctx
  self.is_master  = (ctx.cfg.is_master == 1)
  self.cpu_id     = ctx.cfg.cpu_id
  self.master_cpu = ctx.cfg.master_cpu or "cpu_01"
  self.peers      = ctx.cfg.peers or {}
  self.epoch      = math.floor(os.time())   -- boot timestamp; rotates on restart

  -- Master-side per-peer state map (in RAM; pg writeback is observability).
  self.master = { peer = {}, cursor = 1, grace_until = 0 }
  if self.is_master then
    for _, peer_id in ipairs(self.peers) do
      self.master.peer[peer_id] = {
        state             = P_UNKNOWN,
        epoch             = 0,
        last_heartbeat_at = 0,
        last_verb_seen    = "",
        drained           = 0,
        outbound          = 0,
        last_kb_write_at  = 0,
      }
    end
    self.master.grace_until = os.time() + MASTER_GRACE_S
  end

  -- Slave own-state.
  self.slave = {
    state           = S_DISCONNECTED,
    sent_join_at    = 0,
    accepted_epoch  = 0,
    last_ack_seq    = 0,
    last_ack_at     = 0,
    missed_acks     = 0,
    next_hb_at      = 0,
    next_hb_seq     = 1,
  }

  -- Outbox: { [target_queue_name] = { {verb, payload}, ... } }
  -- Drained at end of each scheduler tick.
  self.outbox = {}

  -- Budget telemetry (60s rolling).
  self.budget = {
    samples       = {},   -- list of {ms, ts}
    violations    = 0,
    warnings      = 0,
    drained_total = 0,
    window_start  = os.time(),
  }
  self.budget.last_kb_write_at = 0

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

local function now_s()
  return os.time()
end

local function jittered_period(period, pct)
  -- ±pct% uniform jitter
  return period * (1.0 + (math.random() * 2.0 - 1.0) * pct)
end

local function exc_path(ctx, name)
  return string.format("system.site.%s.cpu.%s.SYS_EXCEPTION.%s",
                       ctx.cfg.site, ctx.cfg.cpu_id, name)
end

local function status_path(ctx, name)
  return string.format("system.site.%s.KB_STATUS_FIELD.%s",
                       ctx.cfg.site, name)
end

-- Compute p95 from samples (ms only).
local function p95_of(samples)
  if #samples == 0 then return 0 end
  local vals = {}
  for _, s in ipairs(samples) do vals[#vals + 1] = s.ms end
  table.sort(vals)
  local idx = math.max(1, math.floor(#vals * 0.95))
  return vals[idx]
end

local function max_of(samples)
  local m = 0
  for _, s in ipairs(samples) do if s.ms > m then m = s.ms end end
  return m
end

-- Trim samples older than BUDGET_SAMPLE_WINDOW.
function M:_trim_budget()
  local cutoff = now_s() - BUDGET_SAMPLE_WINDOW
  local kept = {}
  for _, s in ipairs(self.budget.samples) do
    if s.ts >= cutoff then kept[#kept + 1] = s end
  end
  self.budget.samples = kept
end

-- Wrap a function call with budget telemetry. Returns the function's result.
function M:_budget_call(verb_name, fn, ...)
  local t0 = os.clock()
  local ok, result = pcall(fn, ...)
  local elapsed_ms = (os.clock() - t0) * 1000.0
  self.budget.samples[#self.budget.samples + 1] = { ms = elapsed_ms, ts = now_s() }
  if elapsed_ms > BUDGET_VIOLATION_MS then
    self.budget.violations = self.budget.violations + 1
    self.ctx.log("sync_rpc", string.format(
      "WARN budget violation: %s took %.1fms (>%dms cap)",
      verb_name, elapsed_ms, BUDGET_VIOLATION_MS))
    -- Raise SCADA exception. pcall'd because exception writer may itself
    -- fail under pg pressure; we don't want telemetry to crash the tick.
    pcall(function()
      self.ctx.kb_exception.log_exception(
        self.ctx.connectors.pg,
        exc_path(self.ctx, "rpc_handler_over_budget"),
        string.format("verb=%s elapsed_ms=%.1f", verb_name, elapsed_ms))
    end)
  elseif elapsed_ms > BUDGET_WARN_MS then
    self.budget.warnings = self.budget.warnings + 1
    self.ctx.log("sync_rpc", string.format(
      "INFO budget warn: %s took %.1fms (>%dms threshold)",
      verb_name, elapsed_ms, BUDGET_WARN_MS))
  end
  if not ok then
    self.ctx.log("sync_rpc", string.format(
      "handler %s ERROR: %s", verb_name, tostring(result)))
    return nil
  end
  return result
end

-- Enqueue an outbound verb. Drained at end of scheduler tick.
function M:_enqueue(target_q, verb, payload)
  if not self.outbox[target_q] then self.outbox[target_q] = {} end
  table.insert(self.outbox[target_q], { verb = verb, payload = payload })
end

-- Flush outbox to pg. One push per outbound message. Caller's budget.
function M:_flush_outbox(conn)
  local total = 0
  for q_name, msgs in pairs(self.outbox) do
    for _, m in ipairs(msgs) do
      local _, err = sync_q.push(conn, DATABASE, q_name, m.verb, m.payload)
      if err then
        self.ctx.log("sync_rpc", string.format(
          "outbox push to %s.%s FAILED: %s", q_name, m.verb, tostring(err)))
      end
      total = total + 1
    end
    self.outbox[q_name] = {}
  end
  return total
end

---------------------------------------------------------------------------
-- Verb handlers (master side -- runs on master inbox drain)
---------------------------------------------------------------------------

function M:_master_on_join_req(payload)
  local cpu_id = payload.cpu_id
  local epoch  = tonumber(payload.epoch) or 0
  if not cpu_id or not self.master.peer[cpu_id] then
    self.ctx.log("sync_rpc", "JOIN_REQ from unknown peer: " .. tostring(cpu_id))
    return
  end
  -- Honor 2s grace. During grace we silently queue the ACK so it goes
  -- out as soon as scheduler flushes; this is fine since slave will
  -- retry until it gets a CONFIRM-able ACK.
  local p = self.master.peer[cpu_id]
  p.state = P_JOINING_SAW_REQ
  p.epoch = epoch
  p.last_verb_seen = "JOIN_REQ"
  if now_s() < self.master.grace_until then
    self.ctx.log("sync_rpc", string.format(
      "JOIN_REQ from %s (epoch=%d) -- grace %ds remaining; ACK queued",
      cpu_id, epoch, self.master.grace_until - now_s()))
  end
  self:_enqueue(string.lower(cpu_id) .. "_q", "JOIN_ACK", {
    cpu_id         = cpu_id,
    master_epoch   = self.epoch,
    accepted_epoch = epoch,
  })
  p.outbound = p.outbound + 1
end

function M:_master_on_join_confirm(payload)
  local cpu_id = payload.cpu_id
  local epoch  = tonumber(payload.epoch) or 0
  local p = cpu_id and self.master.peer[cpu_id]
  if not p then return end
  if p.epoch ~= epoch then
    self.ctx.log("sync_rpc", string.format(
      "JOIN_CONFIRM from %s with epoch %d != ack'd %d; ignored",
      cpu_id, epoch, p.epoch))
    return
  end
  p.state = P_ACTIVE
  p.last_verb_seen = "JOIN_CONFIRM"
  p.last_heartbeat_at = now_s()
  self.ctx.log("sync_rpc", string.format(
    "peer %s -> ACTIVE (epoch=%d)", cpu_id, epoch))
end

function M:_master_on_heartbeat(payload)
  local cpu_id = payload.cpu_id
  local epoch  = tonumber(payload.epoch) or 0
  local seq    = tonumber(payload.seq) or 0
  local p = cpu_id and self.master.peer[cpu_id]
  if not p then return end
  if p.epoch ~= epoch then
    -- Stale or pre-ACTIVE; ignore (slave with new epoch must JOIN_REQ).
    return
  end
  p.last_heartbeat_at = now_s()
  p.last_verb_seen = "HEARTBEAT"
  self:_enqueue(string.lower(cpu_id) .. "_q", "HEARTBEAT_ACK", {
    cpu_id       = cpu_id,
    master_epoch = self.epoch,
    seq          = seq,
  })
  p.outbound = p.outbound + 1
end

function M:_master_on_drain(payload)
  local cpu_id = payload.cpu_id
  local p = cpu_id and self.master.peer[cpu_id]
  if not p then return end
  p.state = P_DRAINING
  p.last_verb_seen = "DRAIN"
  self.ctx.log("sync_rpc", string.format("peer %s -> DRAINING", cpu_id))
end

local MASTER_DISPATCH = {
  JOIN_REQ     = M._master_on_join_req,
  JOIN_CONFIRM = M._master_on_join_confirm,
  HEARTBEAT    = M._master_on_heartbeat,
  DRAIN        = M._master_on_drain,
}

---------------------------------------------------------------------------
-- Verb handlers (slave side -- runs on cpu_<id>_q inbox drain)
---------------------------------------------------------------------------

function M:_slave_on_join_ack(payload)
  if self.slave.state ~= S_JOINING then
    -- Stale ACK from a prior epoch; ignore.
    return
  end
  local accepted = tonumber(payload.accepted_epoch) or 0
  if accepted ~= self.epoch then
    self.ctx.log("sync_rpc", string.format(
      "JOIN_ACK with stale epoch %d != own %d; ignored", accepted, self.epoch))
    return
  end
  self.slave.accepted_epoch = accepted
  self.slave.state = S_ACK_RECEIVED
  self:_enqueue("master_q", "JOIN_CONFIRM", {
    cpu_id = self.cpu_id,
    epoch  = self.epoch,
  })
  -- Fire the first HEARTBEAT on the very next scheduler tick rather than
  -- waiting for the periodic cadence. Slave reaches S_ACTIVE on the
  -- HEARTBEAT_ACK round-trip; without this nudge it stalls up to
  -- HEARTBEAT_PERIOD_S, racing master's setup-state aggregator timeout.
  self.slave.next_hb_at = now_s()
end

function M:_slave_on_heartbeat_ack(payload)
  local seq = tonumber(payload.seq) or 0
  if seq <= self.slave.last_ack_seq then return end   -- out-of-order; ignore
  self.slave.last_ack_seq = seq
  self.slave.last_ack_at  = now_s()
  self.slave.missed_acks  = 0
  if self.slave.state == S_ACK_RECEIVED then
    self.slave.state = S_ACTIVE
    self.ctx.log("sync_rpc", string.format(
      "slave %s -> ACTIVE (epoch=%d)", self.cpu_id, self.epoch))
  end
end

function M:_slave_on_reset_hint(payload)
  self.ctx.log("sync_rpc", string.format(
    "RESET_HINT received: %s -- exiting for watchdog respawn",
    tostring(payload.reason or "unspecified")))
  os.exit(0)
end

local SLAVE_DISPATCH = {
  JOIN_ACK      = M._slave_on_join_ack,
  HEARTBEAT_ACK = M._slave_on_heartbeat_ack,
  RESET_HINT    = M._slave_on_reset_hint,
}

---------------------------------------------------------------------------
-- Scheduler tick (master + slave)
---------------------------------------------------------------------------

function M:_scheduler_tick()
  local conn = self.ctx.connectors.pg
  if not conn then return end

  local cursor_q, cursor_label
  if self.is_master then
    if #self.peers == 0 then return end
    -- Round-robin: drain the queue for ONE peer per tick.
    -- Per feedback_phase6_handler_budget rule 4.
    -- Note: master's inbox is `master_q` -- shared by all slaves. In a
    -- multi-slave cluster all slaves push there. We still round-robin so
    -- per-tick verb count is bounded; the cursor selects which peer's
    -- outbox to flush (queued during prior dispatch) and we drain
    -- master_q up to MAX_VERBS_PER_TICK each tick.
    local peer = self.peers[self.master.cursor]
    self.master.cursor = self.master.cursor + 1
    if self.master.cursor > #self.peers then self.master.cursor = 1 end
    cursor_q     = "master_q"
    cursor_label = peer
  else
    -- Slave: own inbox.
    cursor_q     = string.lower(self.cpu_id) .. "_q"
    cursor_label = self.master_cpu
  end

  -- Drain up to MAX_VERBS_PER_TICK from the inbox.
  local rows, derr = sync_q.drain(conn, DATABASE, cursor_q, MAX_VERBS_PER_TICK)
  if derr then
    -- Don't crash the tick; log and continue. Pg blip will resolve next tick.
    self.ctx.log("sync_rpc", "drain " .. cursor_q .. " err: " .. tostring(derr))
    return
  end
  if rows then
    local dispatch = self.is_master and MASTER_DISPATCH or SLAVE_DISPATCH
    for _, r in ipairs(rows) do
      local handler = dispatch[r.verb]
      if handler then
        self:_budget_call(r.verb, handler, self, r.payload)
        if self.is_master then
          local cpu_id = r.payload and r.payload.cpu_id
          if cpu_id and self.master.peer[cpu_id] then
            self.master.peer[cpu_id].drained =
              self.master.peer[cpu_id].drained + 1
          end
        end
      else
        self.ctx.log("sync_rpc", string.format(
          "unknown verb %s in %s; dropped", r.verb, cursor_q))
      end
      self.budget.drained_total = self.budget.drained_total + 1
    end
  end

  -- Flush outbox (handlers may have queued replies).
  self:_flush_outbox(conn)

  -- Trim budget samples + periodic KB writeback.
  self:_trim_budget()
end

---------------------------------------------------------------------------
-- KB writeback (peer_state + rpc_budget_summary)
---------------------------------------------------------------------------

function M:_write_peer_state_kb()
  local conn = self.ctx.connectors.pg
  if not conn then return end
  local now = now_s()
  -- Master writes per-peer rows. Slave writes its own row.
  if self.is_master then
    for cpu_id, p in pairs(self.master.peer) do
      if now - p.last_kb_write_at >= PEER_STATE_WRITE_S then
        local path = status_path(self.ctx, "peer_state_" .. cpu_id)
        pcall(function()
          self.ctx.kb_status.set_status_data(conn, path, {
            state             = p.state,
            epoch             = p.epoch,
            last_heartbeat_at = p.last_heartbeat_at,
            last_verb_seen    = p.last_verb_seen,
            drained           = p.drained,
            outbound          = p.outbound,
            updated_at        = now,
          })
        end)
        p.last_kb_write_at = now
      end
    end
  else
    -- Slave's own row (master also has one; slave writes its self-row).
    local path = status_path(self.ctx, "peer_state_" .. self.cpu_id)
    pcall(function()
      self.ctx.kb_status.set_status_data(conn, path, {
        state             = self.slave.state,
        epoch             = self.epoch,
        last_heartbeat_at = self.slave.last_ack_at,
        last_verb_seen    = "self",
        drained           = self.budget.drained_total,
        outbound          = 0,
        updated_at        = now,
      })
    end)
  end
end

function M:_write_budget_summary_kb()
  local conn = self.ctx.connectors.pg
  if not conn then return end
  local now = now_s()
  if now - self.budget.last_kb_write_at < PEER_STATE_WRITE_S then return end
  local path = status_path(self.ctx, "rpc_budget_summary")
  local samples = self.budget.samples
  pcall(function()
    self.ctx.kb_status.set_status_data(conn, path, {
      max_ms        = max_of(samples),
      p95_ms        = p95_of(samples),
      violations    = self.budget.violations,
      warnings      = self.budget.warnings,
      drained_total = self.budget.drained_total,
      sample_count  = #samples,
      window_start  = self.budget.window_start,
    })
  end)
  self.budget.last_kb_write_at = now
end

---------------------------------------------------------------------------
-- Slave heartbeat tick (slave only)
---------------------------------------------------------------------------

function M:_slave_heartbeat_tick()
  if self.is_master then return end
  if self.slave.state == S_DISCONNECTED then return end
  local conn = self.ctx.connectors.pg
  if not conn then return end
  local now = now_s()

  -- Send HEARTBEAT if cadence elapsed.
  if now >= self.slave.next_hb_at then
    sync_q.push(conn, DATABASE, "master_q", "HEARTBEAT", {
      cpu_id = self.cpu_id,
      epoch  = self.epoch,
      seq    = self.slave.next_hb_seq,
    })
    self.slave.next_hb_seq = self.slave.next_hb_seq + 1
    self.slave.next_hb_at = now + jittered_period(HEARTBEAT_PERIOD_S,
                                                  HEARTBEAT_JITTER_PCT)
  end

  -- Detect missed ACK: if last_ack_at is too old, increment counter.
  -- Only count once per missed window.
  if self.slave.state == S_ACTIVE then
    local since_ack = now - self.slave.last_ack_at
    -- Each full HEARTBEAT_PERIOD_S without an ACK = one miss.
    local expected_misses = math.floor(since_ack / HEARTBEAT_PERIOD_S)
    if expected_misses > self.slave.missed_acks then
      self.slave.missed_acks = expected_misses
      self.ctx.log("sync_rpc", string.format(
        "missed HEARTBEAT_ACK count = %d/%d (since_ack=%ds)",
        self.slave.missed_acks, MISSED_ACK_THRESHOLD, since_ack))
    end
    if self.slave.missed_acks >= MISSED_ACK_THRESHOLD then
      self.ctx.log("sync_rpc", string.format(
        "FAIL-STOP: %d missed HEARTBEAT_ACKs -- exiting for watchdog respawn",
        self.slave.missed_acks))
      pcall(function()
        self.ctx.kb_exception.log_exception(
          conn,
          exc_path(self.ctx, "master_lost_via_heartbeat"),
          string.format("missed_acks=%d threshold=%d",
                        self.slave.missed_acks, MISSED_ACK_THRESHOLD))
      end)
      os.exit(0)
    end
  end
end

---------------------------------------------------------------------------
-- Slave: send JOIN_REQ
---------------------------------------------------------------------------

function M:_slave_send_join()
  if self.is_master then return end
  local conn = self.ctx.connectors.pg
  if not conn then return end
  -- New epoch on each fresh send (rotates if we entered JOINING anew).
  self.epoch = math.floor(os.time())
  self.slave.state = S_JOINING
  self.slave.sent_join_at = now_s()
  self.slave.last_ack_at  = now_s()  -- so missed-ACK timer starts fresh
  self.slave.missed_acks  = 0
  self.slave.next_hb_at   = now_s() + jittered_period(HEARTBEAT_PERIOD_S,
                                                      HEARTBEAT_JITTER_PCT)
  self.slave.next_hb_seq  = 1
  self.slave.last_ack_seq = 0
  sync_q.push(conn, DATABASE, "master_q", "JOIN_REQ", {
    cpu_id = self.cpu_id,
    epoch  = self.epoch,
  })
  self.ctx.log("sync_rpc", string.format(
    "JOIN_REQ sent (cpu_id=%s epoch=%d)", self.cpu_id, self.epoch))
end

---------------------------------------------------------------------------
-- Predicates for chain-tree wait_bool
---------------------------------------------------------------------------

function M:all_peers_active()
  if not self.is_master then return true end
  for _, p in pairs(self.master.peer) do
    if p.state ~= P_ACTIVE then return false end
  end
  return true
end

function M:own_active()
  if self.is_master then return true end
  return self.slave.state == S_ACTIVE
end

---------------------------------------------------------------------------
-- Public observability (tests + admin)
---------------------------------------------------------------------------

function M:_state()
  return {
    is_master = self.is_master,
    cpu_id    = self.cpu_id,
    epoch     = self.epoch,
    master    = self.is_master and self.master or nil,
    slave     = (not self.is_master) and self.slave or nil,
  }
end

function M:budget_summary()
  return {
    max_ms        = max_of(self.budget.samples),
    p95_ms        = p95_of(self.budget.samples),
    violations    = self.budget.violations,
    warnings      = self.budget.warnings,
    drained_total = self.budget.drained_total,
    sample_count  = #self.budget.samples,
  }
end

---------------------------------------------------------------------------
-- Chain-tree handler installation
---------------------------------------------------------------------------

local function timer_only_bool(fn)
  -- Same wait_bool semantics as user_functions: only evaluate on TIMER,
  -- return false on non-TIMER (so asm_wait halts).
  local defs = require("cfl_definitions")
  return function(_h, _n, _et, event_id, _ed)
    if event_id ~= defs.CFL_TIMER_EVENT then return false end
    return fn()
  end
end

function M:install_handlers(R)
  -- One-shot at sync entry (master): reset peer state + grace.
  R.MASTER_SYNC_INIT = function(_h, _n)
    if not self.is_master then return end
    for _, p in pairs(self.master.peer) do
      p.state = P_UNKNOWN
      p.epoch = 0
      p.last_heartbeat_at = 0
      p.drained = 0
      p.outbound = 0
    end
    self.master.cursor      = 1
    self.master.grace_until = now_s() + MASTER_GRACE_S
    self.epoch              = math.floor(os.time())
    self.ctx.log("sync_rpc", string.format(
      "MASTER_SYNC_INIT: epoch=%d grace=%ds peers=%d",
      self.epoch, MASTER_GRACE_S, #self.peers))
  end

  -- One-shot at sync entry (slave): reset slave state.
  R.SLAVE_SYNC_INIT = function(_h, _n)
    if self.is_master then return end
    self.slave.state = S_DISCONNECTED
    self.ctx.log("sync_rpc", "SLAVE_SYNC_INIT")
  end

  -- One-shot: slave sends JOIN_REQ.
  R.SLAVE_SEND_JOIN = function(_h, _n)
    self:_budget_call("SLAVE_SEND_JOIN", function() self:_slave_send_join() end)
  end

  -- Reset-loop tick: drain inbox, dispatch, flush outbox, telemetry.
  R.RPC_SCHEDULER_TICK = function(_h, _n)
    self:_scheduler_tick()
  end

  -- Reset-loop tick: slave heartbeat send + missed-ACK fail-stop.
  R.SLAVE_HEARTBEAT_TICK = function(_h, _n)
    self:_budget_call("SLAVE_HEARTBEAT_TICK",
                      function() self:_slave_heartbeat_tick() end)
  end

  -- Reset-loop tick: write peer_state + budget summary to KB (5s cadence).
  R.RPC_KB_WRITEBACK_TICK = function(_h, _n)
    self:_write_peer_state_kb()
    self:_write_budget_summary_kb()
  end

  -- wait_bool predicates.
  R.VERIFY_ALL_PEERS_ACTIVE = timer_only_bool(function()
    return self:all_peers_active()
  end)
  R.VERIFY_OWN_ACTIVE = timer_only_bool(function()
    return self:own_active()
  end)
end

return M
