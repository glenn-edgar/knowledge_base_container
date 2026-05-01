-- =============================================================================
-- container_rpc.lua -- Phase 6.4 container-layer RPC, master-side.
--
-- Mirrors sync_rpc.lua but for the container layer. Master listens on its
-- per-CPU container inbox queue (container_inbox_<cpu_id>_q) for messages
-- from local app containers; sends commands to per-container queues
-- (container_<name>_q).
--
-- Verb set (mirroring §3.2 from PHASE6_DESIGN, container layer):
--   container -> master : CONTAINER_READY, HEARTBEAT
--   master -> container : HEARTBEAT_ACK, PAUSE, RESUME, DRAIN, RESET_HINT
--
-- Per-container state machine (master's view):
--   C_UNKNOWN -> C_JOINING (on first CONTAINER_READY)
--             -> C_ACTIVE  (after HEARTBEAT round-trip seen)
--             -> C_PAUSED  (PAUSE acknowledged)
--             -> C_ACTIVE  (RESUME)
--             -> C_DRAINING (DRAIN issued, awaiting ack)
--             -> C_LOST    (3 missed HEARTBEATs)
--
-- Two-tier escalation (§7.3):
--   missed-HB threshold       -> RESET_HINT (in-place controller reset)
--   3 RESET_HINT failures     -> docker rm + node_control reconcile
--                                (escalation logged via SYS_EXCEPTION;
--                                 actual rm done by node_control's existing
--                                 reconcile path when state becomes LOST)
--
-- Cadence (per design §8):
--   container HEARTBEAT      : 60s +/- 10% jitter (container-side)
--   missed-ACK threshold     : 3 (so detection latency is ~3 min)
--   master scheduler tick    : 5 Hz drain (matches sync_rpc)
--
-- Budget: <50ms hard cap, <30ms warn (per feedback_phase6_handler_budget).
--
-- Memory: project_phase6_transport (Option A, pg queues),
-- feedback_phase6_handler_budget, feedback_one_reset_path.
-- =============================================================================

local sync_q    = require("kb_sync_queue")
local ndc_paths = require("ndc_paths")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constants
---------------------------------------------------------------------------

local DATABASE              = "knowledge_base"
local MAX_VERBS_PER_TICK    = 5                  -- mirrors sync layer
local HEARTBEAT_PERIOD_S    = 60.0               -- §8 container layer
local MISSED_HB_THRESHOLD   = 3                  -- §8 universal rule 1
local MAX_IN_PLACE_RESETS   = 3                  -- §7.3 then docker rm
local CONTAINER_STATE_WRITE_S = 5.0              -- KB writeback cadence
local BUDGET_VIOLATION_MS   = 50.0
local BUDGET_WARN_MS        = 30.0
local BUDGET_SAMPLE_WINDOW  = 60.0
local BUDGET_KB_WRITE_S     = 5.0

-- Per-container state values
local C_UNKNOWN  = "UNKNOWN"
local C_JOINING  = "JOINING"
local C_ACTIVE   = "ACTIVE"
local C_PAUSED   = "PAUSED"
local C_DRAINING = "DRAINING"
local C_LOST     = "LOST"

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function M.new(ctx)
  assert(ctx and ctx.cfg, "container_rpc.new: ctx.cfg required")
  local self = setmetatable({}, M)
  self.ctx        = ctx
  self.is_master  = (ctx.cfg.is_master == 1)
  self.cpu_id     = ctx.cfg.cpu_id
  self.epoch      = math.floor(os.time())
  self.inbox_q    = "container_inbox_" .. self.cpu_id .. "_q"

  -- Per-container state map: name -> { state, epoch, last_hb_at,
  --   last_verb_seen, missed_hbs, in_place_resets, drained, outbound,
  --   last_kb_write_at, ... }
  -- Populated lazily on first CONTAINER_READY OR via add_container().
  self.container = {}

  -- Outbox: { [target_q] = { {verb, payload}, ... } }
  self.outbox = {}

  -- Budget telemetry (60s rolling), namespaced separately from sync.
  self.budget = {
    samples         = {},
    violations      = 0,
    warnings        = 0,
    drained_total   = 0,
    window_start    = os.time(),
    last_kb_write_at = 0,
  }

  -- Round-robin cursor (not strictly needed since we drain a single
  -- inbox queue, but kept symmetric with sync_rpc for telemetry).
  self.cursor = 1

  return self
end

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function now_s() return os.time() end

local function exc_path(ctx, name)
  return ndc_paths.cpu_exception_path(ctx.cfg.site, ctx.cfg.cpu_id, name)
end

local function status_path(ctx, name)
  return ndc_paths.site_status_field_path(ctx.cfg.site, name)
end

local function p95_of(samples)
  if #samples == 0 then return 0 end
  local vals = {}
  for _, s in ipairs(samples) do vals[#vals + 1] = s.ms end
  table.sort(vals)
  return vals[math.max(1, math.floor(#vals * 0.95))]
end

local function max_of(samples)
  local m = 0
  for _, s in ipairs(samples) do if s.ms > m then m = s.ms end end
  return m
end

function M:_trim_budget()
  local cutoff = now_s() - BUDGET_SAMPLE_WINDOW
  local kept = {}
  for _, s in ipairs(self.budget.samples) do
    if s.ts >= cutoff then kept[#kept + 1] = s end
  end
  self.budget.samples = kept
end

function M:_budget_call(verb_name, fn, ...)
  local t0 = os.clock()
  local ok, result = pcall(fn, ...)
  local elapsed_ms = (os.clock() - t0) * 1000.0
  self.budget.samples[#self.budget.samples + 1] = { ms = elapsed_ms, ts = now_s() }
  if elapsed_ms > BUDGET_VIOLATION_MS then
    self.budget.violations = self.budget.violations + 1
    self.ctx.log("container_rpc", string.format(
      "WARN budget violation: %s took %.1fms (>%dms cap)",
      verb_name, elapsed_ms, BUDGET_VIOLATION_MS))
    pcall(function()
      self.ctx.kb_exception.log_exception(
        self.ctx.connectors.pg,
        exc_path(self.ctx, "container_rpc_handler_over_budget"),
        string.format("verb=%s elapsed_ms=%.1f", verb_name, elapsed_ms))
    end)
  elseif elapsed_ms > BUDGET_WARN_MS then
    self.budget.warnings = self.budget.warnings + 1
  end
  if not ok then
    self.ctx.log("container_rpc", string.format(
      "handler %s ERROR: %s", verb_name, tostring(result)))
    return nil
  end
  return result
end

function M:_enqueue(target_q, verb, payload)
  if not self.outbox[target_q] then self.outbox[target_q] = {} end
  table.insert(self.outbox[target_q], { verb = verb, payload = payload })
end

function M:_flush_outbox(conn)
  local total = 0
  for q_name, msgs in pairs(self.outbox) do
    for _, m in ipairs(msgs) do
      local _, err = sync_q.push(conn, DATABASE, q_name, m.verb, m.payload)
      if err then
        self.ctx.log("container_rpc", string.format(
          "outbox push to %s.%s FAILED: %s",
          q_name, m.verb, tostring(err)))
      end
      total = total + 1
    end
    self.outbox[q_name] = {}
  end
  return total
end

-- Lazily allocate per-container state on first contact. Apps that haven't
-- been declared at construction time still get an entry; node_control's
-- existing reconcile path is responsible for shutting down rogue
-- containers, not us.
function M:_get_or_init(name)
  local c = self.container[name]
  if c then return c end
  c = {
    state             = C_UNKNOWN,
    epoch             = 0,
    last_hb_at        = 0,
    last_hb_seq       = 0,
    last_verb_seen    = "",
    missed_hbs        = 0,
    in_place_resets   = 0,
    drained           = 0,
    outbound          = 0,
    last_kb_write_at  = 0,
  }
  self.container[name] = c
  return c
end

---------------------------------------------------------------------------
-- Verb handlers (master side)
---------------------------------------------------------------------------

function M:_on_container_ready(payload)
  local name = payload.name
  if type(name) ~= "string" or name == "" then
    self.ctx.log("container_rpc", "CONTAINER_READY with no name; ignored")
    return
  end
  local c = self:_get_or_init(name)
  c.epoch          = tonumber(payload.epoch) or 0
  c.state          = C_JOINING
  c.last_hb_at     = now_s()
  c.last_verb_seen = "CONTAINER_READY"
  c.missed_hbs     = 0
  c.in_place_resets = 0   -- fresh process, fresh budget
  self.ctx.log("container_rpc", string.format(
    "CONTAINER_READY from %s (epoch=%d) -> JOINING", name, c.epoch))
  -- Send a HEARTBEAT_ACK with seq=0 immediately so the container has
  -- a fresh ACK stamp; this also serves as the JOINING -> ACTIVE
  -- promotion trigger on next HB drain.
  self:_enqueue("container_" .. name .. "_q", "HEARTBEAT_ACK", {
    name         = name,
    master_epoch = self.epoch,
    seq          = 0,
  })
  c.outbound = c.outbound + 1
end

function M:_on_heartbeat(payload)
  local name = payload.name
  if type(name) ~= "string" or name == "" then return end
  local c = self.container[name]
  if not c then
    -- HEARTBEAT before CONTAINER_READY -- ignore; container will time out
    -- and re-send READY on its own retry path.
    self.ctx.log("container_rpc", string.format(
      "HEARTBEAT from unknown container %s; ignored (no prior READY)", name))
    return
  end
  local epoch = tonumber(payload.epoch) or 0
  if c.epoch ~= 0 and c.epoch ~= epoch then
    -- Stale epoch; container restarted but didn't send a fresh READY.
    -- Force back to JOINING and demand a fresh READY round.
    self.ctx.log("container_rpc", string.format(
      "HEARTBEAT epoch mismatch for %s (got=%d expected=%d) -- demoting to UNKNOWN",
      name, epoch, c.epoch))
    c.state = C_UNKNOWN
    c.epoch = 0
    return
  end
  c.last_hb_at     = now_s()
  c.last_hb_seq    = tonumber(payload.seq) or c.last_hb_seq
  c.last_verb_seen = "HEARTBEAT"
  c.missed_hbs     = 0
  if c.state == C_JOINING then
    c.state = C_ACTIVE
    self.ctx.log("container_rpc", string.format(
      "container %s -> ACTIVE (epoch=%d)", name, c.epoch))
  end
  -- ACK back. Container uses this as proof-of-life from master.
  self:_enqueue("container_" .. name .. "_q", "HEARTBEAT_ACK", {
    name         = name,
    master_epoch = self.epoch,
    seq          = c.last_hb_seq,
  })
  c.outbound = c.outbound + 1
end

local INBOUND_DISPATCH = {
  CONTAINER_READY = M._on_container_ready,
  HEARTBEAT       = M._on_heartbeat,
}

---------------------------------------------------------------------------
-- Outbound commands (called from chain-tree handlers, e.g. PAUSE_CONTAINER)
---------------------------------------------------------------------------

function M:send_pause(name, reason)
  local c = self:_get_or_init(name)
  c.state = C_PAUSED
  self:_enqueue("container_" .. name .. "_q", "PAUSE", {
    name = name, reason = reason or "infra_restart",
  })
  c.outbound = c.outbound + 1
  self.ctx.log("container_rpc", string.format("PAUSE -> %s (%s)", name, reason or ""))
end

function M:send_resume(name)
  local c = self.container[name]
  if c then c.state = C_ACTIVE end
  self:_enqueue("container_" .. name .. "_q", "RESUME", { name = name })
  self.ctx.log("container_rpc", "RESUME -> " .. name)
end

function M:send_drain(name)
  local c = self:_get_or_init(name)
  c.state = C_DRAINING
  self:_enqueue("container_" .. name .. "_q", "DRAIN", { name = name })
  c.outbound = c.outbound + 1
  self.ctx.log("container_rpc", "DRAIN -> " .. name)
end

function M:send_reset_hint(name, reason)
  self:_enqueue("container_" .. name .. "_q", "RESET_HINT", {
    name = name, reason = reason or "missed_hb",
  })
  local c = self.container[name]
  if c then c.outbound = c.outbound + 1 end
  self.ctx.log("container_rpc", string.format(
    "RESET_HINT -> %s (%s)", name, reason or ""))
end

---------------------------------------------------------------------------
-- Scheduler tick (master only): drain inbox, dispatch, flush outbox,
-- check missed-HB thresholds for two-tier escalation.
---------------------------------------------------------------------------

function M:_scheduler_tick()
  if not self.is_master then return end
  local conn = self.ctx.connectors.pg
  if not conn then return end

  -- Drain inbox.
  local rows, err = sync_q.drain(conn, DATABASE, self.inbox_q,
                                 MAX_VERBS_PER_TICK)
  if err then
    self.ctx.log("container_rpc", "drain " .. self.inbox_q .. " err: " .. tostring(err))
    rows = {}
  end
  for _, row in ipairs(rows) do
    local handler = INBOUND_DISPATCH[row.verb]
    if handler then
      self:_budget_call(row.verb, function()
        handler(self, row.payload or {})
      end)
      self.budget.drained_total = self.budget.drained_total + 1
      local n = row.payload and row.payload.name
      if n and self.container[n] then
        self.container[n].drained = self.container[n].drained + 1
      end
    else
      self.ctx.log("container_rpc", string.format(
        "unknown verb %s on %s; ignored", tostring(row.verb), self.inbox_q))
    end
  end

  -- Missed-HB scan + two-tier escalation. Done after drain so we use the
  -- freshest last_hb_at.
  self:_check_missed_hbs()

  -- Flush outbox.
  self:_flush_outbox(conn)

  -- Trim budget samples.
  self:_trim_budget()
end

function M:_check_missed_hbs()
  local now = now_s()
  for name, c in pairs(self.container) do
    if c.state == C_ACTIVE or c.state == C_PAUSED or c.state == C_JOINING then
      local since_hb = now - c.last_hb_at
      -- Same -1 tolerance as sync layer to avoid false positives at the
      -- HB cadence boundary.
      local expected_misses = math.max(0,
        math.floor(since_hb / HEARTBEAT_PERIOD_S) - 1)
      if expected_misses > c.missed_hbs then
        c.missed_hbs = expected_misses
        self.ctx.log("container_rpc", string.format(
          "missed HEARTBEAT count = %d/%d for %s (since_hb=%ds)",
          c.missed_hbs, MISSED_HB_THRESHOLD, name, since_hb))
      end
      if c.missed_hbs >= MISSED_HB_THRESHOLD then
        -- Tier 1: in-place reset
        c.in_place_resets = c.in_place_resets + 1
        if c.in_place_resets > MAX_IN_PLACE_RESETS then
          -- Tier 2: declare LOST. node_control's reconcile path
          -- detects the missing container by polling broker state and
          -- respawns it; we just log the SYS_EXCEPTION so ops sees it.
          c.state = C_LOST
          self.ctx.log("container_rpc", string.format(
            "container %s LOST after %d in-place resets -- escalating to docker rm + reconcile",
            name, c.in_place_resets))
          pcall(function()
            self.ctx.kb_exception.log_exception(
              self.ctx.connectors.pg,
              exc_path(self.ctx, "container_lost"),
              string.format("name=%s in_place_resets=%d", name, c.in_place_resets))
          end)
          -- Reset counter so next CONTAINER_READY starts fresh.
          c.in_place_resets = 0
        else
          -- In-place: send RESET_HINT, reset missed counter, keep state.
          self:send_reset_hint(name, string.format(
            "missed_hb_threshold (%d resets used)", c.in_place_resets))
          c.missed_hbs = 0
          c.last_hb_at = now      -- start a fresh window post-hint
          c.state      = C_JOINING -- expect fresh CONTAINER_READY
        end
      end
    end
  end
end

---------------------------------------------------------------------------
-- KB writeback (per-container observability rows + budget summary)
---------------------------------------------------------------------------

function M:_write_kb_state()
  if not self.is_master then return end
  local conn = self.ctx.connectors.pg
  if not conn then return end
  local now = now_s()
  for name, c in pairs(self.container) do
    if now - c.last_kb_write_at >= CONTAINER_STATE_WRITE_S then
      local path = status_path(self.ctx, "container_state_" .. name)
      pcall(function()
        self.ctx.kb_status.set_status_data(conn, path, {
          state             = c.state,
          epoch             = c.epoch,
          last_heartbeat_at = c.last_hb_at,
          last_verb_seen    = c.last_verb_seen,
          missed_acks       = c.missed_hbs,
          in_place_resets   = c.in_place_resets,
          drained           = c.drained,
          outbound          = c.outbound,
          updated_at        = now,
        })
      end)
      c.last_kb_write_at = now
    end
  end
end

function M:_write_budget_summary()
  if not self.is_master then return end
  local conn = self.ctx.connectors.pg
  if not conn then return end
  local now = now_s()
  if now - self.budget.last_kb_write_at < BUDGET_KB_WRITE_S then return end
  -- Reuse the rpc_budget_summary row? No -- container layer is a
  -- distinct telemetry stream. Use a separate row.
  local path = status_path(self.ctx, "container_rpc_budget_summary")
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
-- Chain-tree handler registration
---------------------------------------------------------------------------

function M:install_handlers(R)
  R.CONTAINER_RPC_SCHEDULER_TICK = function(_h, _n)
    self:_scheduler_tick()
  end
  R.CONTAINER_RPC_KB_WRITEBACK_TICK = function(_h, _n)
    self:_write_kb_state()
    self:_write_budget_summary()
  end
end

-- Public accessors for tests + admin probes.
function M:_state()
  return {
    cpu_id    = self.cpu_id,
    epoch     = self.epoch,
    inbox_q   = self.inbox_q,
    container = self.container,
    budget    = self.budget,
  }
end

return M
