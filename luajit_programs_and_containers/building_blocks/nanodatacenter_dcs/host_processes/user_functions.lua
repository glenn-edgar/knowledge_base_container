-- =============================================================================
-- user_functions.lua -- DCS chain-tree user-function registry.
--
-- Exposes M.build(ctx) -> { NAME = fn, ... } registry consumed by
-- dcs.lua's chain-tree loader. ctx provides shared state (connectors,
-- process_globals, chain_tree handle, logger) so functions stay closures
-- over real process state rather than globals.
--
-- Build 2a: all bodies are logging stubs that return success. Build 2b
-- replaces them incrementally with real docker/pg primitives.
-- =============================================================================

local eq_mod = require("cfl_event_queue")
local defs   = require("cfl_definitions")

local M = {}

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function oneshot_stub(log, name, half)
  return function(_handle, _node_idx)
    log(half or "stub", "would " .. name)
  end
end

local function verify_stub(log, name, half)
  return function(_handle, _node_idx, _event_type, _event_id, _event_data)
    log(half or "stub", "verify " .. name .. " -> true (stub)")
    return true
  end
end

---------------------------------------------------------------------------
-- build(ctx) returns the registered-name -> function map
---------------------------------------------------------------------------

function M.build(ctx)
  local log      = ctx.log
  local cfl_rt   = ctx.cfl_rt
  local docker   = ctx.docker
  local pg_conn  = ctx.pg_connector
  local bm       = ctx.bit_mask_helpers
  local kb_stat  = ctx.kb_status
  local kb_exc   = ctx.kb_exception
  local ct       = ctx.chain_tree
  local pg       = ctx.process_globals

  local R = {}

  ----------------------------------------------------------------------
  -- Per-agent SYS_EXCEPTION path resolver. Construct script writes:
  --   system.site.<S>.cpu.<id>.SYS_EXCEPTION.<exc_name>
  -- We inline the prefix from cfg so callers just pass the short name.
  ----------------------------------------------------------------------
  local function exc_path(name)
    return string.format("system.site.%s.cpu.%s.SYS_EXCEPTION.%s",
                         ctx.cfg.site, ctx.cfg.cpu_id, name)
  end

  -- Boolean helper: always returns true. Used as the loop-condition aux
  -- for define_while_column so the while-loop iterates forever.
  -- The chain-tree default aux (CFL_NULL) returns false, causing the
  -- loop to exit after one iteration and the enclosing state to
  -- terminate. Any state with an intended infinite monitor loop must
  -- pass DCS_ALWAYS_TRUE as its while-aux.
  R.DCS_ALWAYS_TRUE = function(_h, _n, _et, _eid, _ed) return true end

  ----------------------------------------------------------------------
  -- system_control half
  ----------------------------------------------------------------------

  -- launch oneshots
  R.READ_ENVIRONS             = oneshot_stub(log, "READ_ENVIRONS",             "system_control")
  R.READ_BOOTSTRAP_CONFIG     = oneshot_stub(log, "READ_BOOTSTRAP_CONFIG",     "system_control")
  R.KILL_NON_INFRA_CONTAINERS = function(_h, _n)
    -- v2c: infra is pre-placed by laptop scripts (not labelled by DCS);
    -- apps don't exist yet. So this is currently a no-op except for
    -- closing any stale connection handles before re-verify reopens them.
    if ctx.connectors.pg then pcall(function() ctx.connectors.pg:close() end) end
    ctx.connectors.pg   = nil
    ctx.connectors.nats = nil
    ctx.connectors.mqtt = nil
    log("system_control", "KILL_NON_INFRA_CONTAINERS -> reset connectors (no-op for infra; apps not yet present)")
  end
  R.SET_SYSTEM_STATE          = oneshot_stub(log, "SET_SYSTEM_STATE",          "system_control")

  -- Helper: start a pre-existing container by name. DCS does NOT create
  -- infra containers; the laptop install scripts created them. We just
  -- toggle their state via docker start / docker stop. Idempotent: if
  -- already running, docker start is a no-op.
  local function start_container(inst_name, _def_name)
    if docker.is_running(inst_name) then
      log("system_control",
          string.format("START %s: already running", inst_name))
      return
    end
    local ok, result = docker.start_existing(inst_name)
    if ok then
      log("system_control",
          string.format("START %s -> %s", inst_name, tostring(result):sub(1, 12)))
    else
      log("system_control",
          string.format("START %s FAILED: %s", inst_name, tostring(result)))
    end
  end

  local function stop_container(inst_name)
    local ok, err = docker.stop_only(inst_name)
    if ok then
      log("system_control", string.format("STOP %s", inst_name))
    else
      log("system_control",
          string.format("STOP %s FAILED: %s", inst_name, tostring(err)))
    end
  end

  -- infra start (proven container names per third_party_containers/ scripts).
  -- DCS calls docker start; never docker run/create.
  R.START_PG_CONTAINER        = function(_h, _n) start_container("pg-vector",             "postgres")  end
  R.START_NATS_CONTAINER      = function(_h, _n) start_container("nats-js-ram",           "nats")      end
  R.START_MQTT_CONTAINER      = function(_h, _n) start_container("mosquitto-ram-ws_main", "mosquitto") end
  R.START_KV_BRIDGE_CONTAINER = function(_h, _n) start_container("kv-bridge",             "kv_bridge") end

  -- infra stop (docker stop only; container persists for next start).
  R.STOP_PG_CONTAINER         = function(_h, _n) stop_container("pg-vector")             end
  R.STOP_NATS_CONTAINER       = function(_h, _n) stop_container("nats-js-ram")           end
  R.STOP_MQTT_CONTAINER       = function(_h, _n) stop_container("mosquitto-ram-ws_main") end
  R.STOP_KV_BRIDGE_CONTAINER  = function(_h, _n) stop_container("kv-bridge")             end

  -- shared kv + heartbeat
  R.CREATE_SHARED_KV_KEYS   = oneshot_stub(log, "CREATE_SHARED_KV_KEYS", "system_control")

  -- PUBLISH_SYSTEM_HEARTBEAT: write 64-bit ns timestamp to this CPU's
  -- bit_mask_table heartbeat row. Master reads all CPUs' heartbeats to
  -- detect dead CPUs (stale ts).
  R.PUBLISH_SYSTEM_HEARTBEAT = function(_h, _n)
    if not ctx.connectors.pg then
      log("system_control", "PUBLISH_SYSTEM_HEARTBEAT skipped (no pg conn)")
      return
    end
    local ts = bm.now_ns()
    local ok, err = bm.write_heartbeat_ts(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id, ts)
    if ok then
      log("system_control",
          string.format("heartbeat_ts -> %d", ts))
    else
      log("system_control", "heartbeat write FAILED: " .. tostring(err))
    end
  end

  -- SET/CLEAR own ready bit. Each CPU owns one bit_index in the
  -- site-level ready_bits mask. Set on entering monitor, clear on
  -- teardown. Atomic OR / AND-NOT in pg avoids RMW races across CPUs.
  R.SET_OWN_READY_BIT = function(_h, _n)
    if not ctx.connectors.pg then
      log("system_control", "SET_OWN_READY_BIT skipped (no pg conn)")
      return
    end
    local ok, err = bm.set_ready_bit(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.bit_index)
    log("system_control",
        ok and string.format("ready_bit %d SET", ctx.cfg.bit_index)
           or  "ready_bit set FAILED: " .. tostring(err))
  end
  R.CLEAR_OWN_READY_BIT = function(_h, _n)
    if not ctx.connectors.pg then return end
    local ok, err = bm.clear_ready_bit(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.bit_index)
    log("system_control",
        ok and string.format("ready_bit %d CLEARED", ctx.cfg.bit_index)
           or  "ready_bit clear FAILED: " .. tostring(err))
  end

  -- VERIFY_ALL_CPUS_READY: master-side aggregator. Reads ready_bits;
  -- compares to expected (1 << N) - 1. Returns true when all CPUs have
  -- their bit set. Until then, master's setup state holds.
  R.VERIFY_ALL_CPUS_READY = function(_h, _n)
    if not ctx.connectors.pg then return false end
    local actual, err = bm.read_ready_bits(ctx.connectors.pg, ctx.cfg.site)
    if not actual then
      log("system_control", "VERIFY_ALL_CPUS_READY -> false (read err: " .. tostring(err) .. ")")
      return false
    end
    local expected = bm.expected_full_mask(ctx.cfg.expected_cpu_count)
    local ok = (actual == expected)
    log("system_control",
        string.format("VERIFY_ALL_CPUS_READY -> %s (actual=0x%x expected=0x%x)",
                      tostring(ok), actual, expected))
    return ok
  end

  -- system_ready status field (pg). Apps poll this; UI consumes.
  -- Also caches in process_globals for in-VM consumers.
  local SYSTEM_READY_PATH = nil   -- lazy-computed below
  local function _system_ready_path()
    if not SYSTEM_READY_PATH then
      SYSTEM_READY_PATH =
        "system.site." .. ctx.cfg.site .. ".KB_STATUS_FIELD.system_ready"
    end
    return SYSTEM_READY_PATH
  end

  R.WRITE_SYSTEM_READY_TRUE  = function(_h, _n)
    pg.system_ready_current = true
    if ctx.connectors.pg then
      local ok, err = kb_stat.set_status_data(
        ctx.connectors.pg, _system_ready_path(), { value = 1 })
      if not ok then
        log("system_control", "system_ready pg write FAILED: " .. tostring(err))
      end
    end
    log("system_control", "system_ready = TRUE")
  end
  R.WRITE_SYSTEM_READY_FALSE = function(_h, _n)
    pg.system_ready_current = false
    if ctx.connectors.pg then
      local ok, err = kb_stat.set_status_data(
        ctx.connectors.pg, _system_ready_path(), { value = 0 })
      if not ok then
        log("system_control", "system_ready pg write FAILED: " .. tostring(err))
      end
    end
    log("system_control", "system_ready = FALSE")
  end

  R.COMMAND_NODE_CONTROL_TEARDOWN = function(_h, _n)
    pg.node_control_teardown_request = true
    log("system_control", "teardown_request = true")
  end

  -- KB activation / deactivation (wraps cfl_rt primitives)
  R.ENABLE_NODE_CONTROL_KB = function(_h, _n)
    log("system_control", string.format(
      "ENABLE_NODE_CONTROL_KB -> cfl_rt.add_test(%d)",
      ct.kb_indexes.node_control or -1))
    cfl_rt.add_test(ct.handle, ct.kb_indexes.node_control)
  end
  R.DISABLE_NODE_CONTROL_KB = function(_h, _n)
    log("system_control", "DISABLE_NODE_CONTROL_KB -> cfl_rt.delete_test")
    cfl_rt.delete_test(ct.handle, ct.kb_indexes.node_control)
  end

  -- verifies
  -- VERIFY_PG: single-attempt DBI connect. Chain-tree's asm_verify_timeout
  -- drives retries across ticks; the 30s verify window is long enough for
  -- pg to come up from cold start. On success populate ctx.connectors.pg
  -- so later code can reuse the connection without reconnecting.
  R.VERIFY_PG = function(_h, _n)
    if ctx.connectors.pg then
      -- already connected; consider healthy
      return true
    end
    local password = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    local conn, err = pg_conn.try_connect(ctx.cfg, password)
    if conn then
      ctx.connectors.pg = conn
      log("system_control",
          string.format("VERIFY_PG -> connected (%s@%s:%s/%s)",
                        ctx.cfg.pg_user, ctx.cfg.pg_host,
                        ctx.cfg.pg_port, ctx.cfg.pg_db))
      -- One-time mute of existing faults on master boot. Each restart of
      -- the master process re-runs this so previously-acked faults stay
      -- acked but un-acks come back via fresh log_exception calls.
      if ctx.cfg.is_master == 1 and not pg.exception_mute_done then
        local n, merr = kb_exc.mute_existing_on_boot(conn)
        if n then
          log("system_control",
              string.format("mute_existing_on_boot: auto-acked %d faults", n))
          pg.exception_mute_done = true
        else
          log("system_control",
              "mute_existing_on_boot FAILED: " .. tostring(merr))
        end
      end
      return true
    else
      log("system_control", "VERIFY_PG -> false: " .. tostring(err))
      return false
    end
  end
  -- Broker verifies: docker.is_running as a v1 health proxy. Sufficient
  -- until Build 3+ wires real Lua clients (NATS/MQTT subscribers, HTTP
  -- probe for kv_bridge) that exercise each broker's API. Container
  -- up => broker accepting connections (--restart=always keeps them alive).
  local function is_running_verify(name, half)
    return function(_h, _n)
      local ok = docker.is_running(name)
      log(half, string.format("VERIFY %s running -> %s", name, tostring(ok)))
      return ok
    end
  end
  R.VERIFY_NATS              = is_running_verify("nats-js-ram",           "system_control")
  R.VERIFY_MQTT              = is_running_verify("mosquitto-ram-ws_main", "system_control")
  R.VERIFY_KV_BRIDGE         = is_running_verify("kv-bridge",             "system_control")
  R.VERIFY_KV_BRIDGE_HEALTHY = is_running_verify("kv-bridge",             "system_control")

  -- VERIFY_NODE_CTRL_HEARTBEAT_FRESH: heartbeat_ts must be within
  -- heartbeat_fresh_s of now. node_control writes heartbeat_ts every
  -- monitor iteration; if stale, sys_sm trips ERR_MONITOR_TRIP.
  local heartbeat_fresh_s = (ctx.settings and ctx.settings.heartbeat_fresh_s) or 5
  R.VERIFY_NODE_CTRL_HEARTBEAT_FRESH = function(_h, _n)
    local ts = pg.node_control_heartbeat_ts or 0
    if ts == 0 then
      -- Grace: node_control hasn't written its first heartbeat yet
      -- (still in its own sync/setup or first monitor tick). VERIFY_NODE_
      -- CTRL_OPERATIONAL already gated on its setup completion; once
      -- monitor runs once, ts > 0 and the staleness check below applies.
      log("system_control", "VERIFY_NODE_CTRL_HEARTBEAT_FRESH -> true (grace; ts=0)")
      return true
    end
    local age = os.time() - ts
    local ok  = age <= heartbeat_fresh_s
    log("system_control", string.format(
        "VERIFY_NODE_CTRL_HEARTBEAT_FRESH -> %s (age=%ds, max=%ds)",
        tostring(ok), age, heartbeat_fresh_s))
    return ok
  end

  R.VERIFY_NODE_CTRL_OPERATIONAL = function(_h, _n)
    local ok = pg.node_control_operational == true
    log("system_control", "VERIFY_NODE_CTRL_OPERATIONAL -> " .. tostring(ok))
    return ok
  end
  R.VERIFY_NODE_CTRL_STOPPED = function(_h, _n)
    local ok = pg.node_control_stopped == true
    log("system_control", "VERIFY_NODE_CTRL_STOPPED -> " .. tostring(ok))
    return ok
  end

  -- Terminate the chain-tree VM cleanly. asm_verify registers error
  -- handlers as one-shots (can't return codes), so we pump a
  -- CFL_TERMINATE_SYSTEM_EVENT onto the event queue. cfl_runtime.run()
  -- pops it and returns; M.run_loop sees no active tests and exits
  -- with a non-zero code so the watchdog restarts us.
  local function request_terminate(handle, half, reason)
    log(half, "TERMINATE: " .. reason .. " -- stopping containers + closing connectors")
    -- cleanup: stop every labelled container (infra + any apps)
    pcall(function() docker.stop_labelled() end)
    -- drop connector handles (pointing at containers we just killed)
    if ctx.connectors.pg then
      pcall(function() ctx.connectors.pg:close() end)
      ctx.connectors.pg = nil
    end
    ctx.connectors.nats = nil
    ctx.connectors.mqtt = nil
    -- record why we're terminating so run_loop can log a final reason
    ctx.terminate_reason = reason
    -- pump the event (high priority=0, node_id=0, type=NULL, id=TERMINATE_SYSTEM)
    eq_mod.send_null(handle.event_queue, 0, 0, defs.CFL_TERMINATE_SYSTEM_EVENT)
  end

  -- error handlers. Each:
  --   1. log_exception(path, msg) -> persists fault in pg as a SYS_EXCEPTION
  --      status row (UPSERT). Master's mute_existing_on_boot will auto-ack
  --      these on next boot.
  --   2. request_terminate(...) -> stop containers, close connectors, pump
  --      CFL_TERMINATE_SYSTEM_EVENT. Watchdog restarts us.
  --
  -- Mapping ERR_* (chain-tree generic) to SYS_EXCEPTION (data-model semantic):
  --   ERR_INFRA_FAIL              -> nc:container_start_failed
  --   ERR_NODE_CTRL_START_FAIL    -> sys:aggregator_timeout
  --   ERR_CONTAINERS_START_FAIL   -> nc:container_start_failed
  --   ERR_MONITOR_TRIP            -> sys:slave_unreachable
  --   ERR_CONTAINER_DIED          -> nc:container_died
  --   ERR_TEARDOWN_REQUESTED      -> not an exception (normal flow)

  local function log_then_terminate(handle, half, reason, exc_name)
    if ctx.connectors.pg and exc_name then
      local ok, err = kb_exc.log_exception(
        ctx.connectors.pg, exc_path(exc_name), reason)
      if not ok then
        log(half, "log_exception FAILED: " .. tostring(err))
      end
    end
    request_terminate(handle, half, reason)
  end

  R.ERR_INFRA_FAIL = function(handle, _n)
    log_then_terminate(handle, "system_control",
                       "ERR_INFRA_FAIL", "container_start_failed")
  end
  R.ERR_NODE_CTRL_START_FAIL = function(handle, _n)
    log_then_terminate(handle, "system_control",
                       "ERR_NODE_CTRL_START_FAIL", "aggregator_timeout")
  end
  R.ERR_CONTAINERS_START_FAIL = function(handle, _n)
    log_then_terminate(handle, "node_control",
                       "ERR_CONTAINERS_START_FAIL", "container_start_failed")
  end

  R.ERR_MONITOR_TRIP = function(handle, _n)
    log_then_terminate(handle, "system_control",
                       "ERR_MONITOR_TRIP", "slave_unreachable")
  end
  R.ERR_CONTAINER_DIED = function(handle, _n)
    log_then_terminate(handle, "node_control",
                       "ERR_CONTAINER_DIED", "container_died")
  end
  R.ERR_TEARDOWN_REQUESTED = function(handle, _n)
    -- normal teardown signal; not a fault to log
    request_terminate(handle, "node_control", "ERR_TEARDOWN_REQUESTED")
  end

  -- ERR_TEARDOWN_FORCE fires when a graceful teardown verify timed out;
  -- the chain-tree continues past it to force-kill oneshots. Log, don't terminate.
  R.ERR_TEARDOWN_FORCE = oneshot_stub(log,
    "ERR_TEARDOWN_FORCE (graceful teardown timed out; proceeding to force kill)",
    "system_control")

  ----------------------------------------------------------------------
  -- node_control half
  ----------------------------------------------------------------------

  R.NODE_READ_OWN_CONFIG           = oneshot_stub(log, "NODE_READ_OWN_CONFIG",           "node_control")
  R.START_ASSIGNED_CONTAINERS      = oneshot_stub(log, "START_ASSIGNED_CONTAINERS (v1: none)", "node_control")
  R.STOP_ASSIGNED_CONTAINERS       = oneshot_stub(log, "STOP_ASSIGNED_CONTAINERS (v1: none)",  "node_control")
  R.LOG_SYSTEM_READY_TRANSITIONS   = oneshot_stub(log, "LOG_SYSTEM_READY_TRANSITIONS",         "node_control")

  R.WRITE_PROCESS_GLOBALS_NODE_OPERATIONAL_TRUE = function(_h, _n)
    pg.node_control_operational = true
    log("node_control", "node_control_operational = TRUE")
  end
  R.WRITE_PROCESS_GLOBALS_NODE_HEARTBEAT = function(_h, _n)
    pg.node_control_heartbeat_ts = os.time()
    log("node_control", "heartbeat_ts = " .. tostring(pg.node_control_heartbeat_ts))
  end
  R.WRITE_PROCESS_GLOBALS_NODE_STOPPED_TRUE = function(_h, _n)
    pg.node_control_stopped = true
    log("node_control", "node_control_stopped = TRUE")
  end

  R.NODE_VERIFY_BROKERS_REACHABLE          = verify_stub(log, "NODE_VERIFY_BROKERS_REACHABLE",          "node_control")
  R.VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY = verify_stub(log, "VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY", "node_control")
  R.VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED = verify_stub(log, "VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED", "node_control")

  R.VERIFY_NO_TEARDOWN_REQUEST = function(_h, _n)
    local ok = not pg.node_control_teardown_request
    log("node_control", "VERIFY_NO_TEARDOWN_REQUEST -> " .. tostring(ok))
    return ok
  end

  R.ERR_CONTAINERS_START_FAIL = oneshot_stub(log, "ERR_CONTAINERS_START_FAIL (would terminate_system)",   "node_control")
  R.ERR_CONTAINER_DIED        = oneshot_stub(log, "ERR_CONTAINER_DIED (would change_state teardown)",     "node_control")
  R.ERR_TEARDOWN_REQUESTED    = oneshot_stub(log, "ERR_TEARDOWN_REQUESTED (would change_state teardown)", "node_control")

  return R
end

return M
