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
  -- Verbose stubs were noisy under per-tick verifies; silent now and
  -- always return true. Real implementations should log only on failure.
  return function(_handle, _node_idx, _event_type, _event_id, _event_data)
    return true
  end
end

-- Wrap a verify bool function so it only runs on CFL_TIMER_EVENT. INIT
-- and TERMINATE pass through as `true` (don't trip the error handler on
-- lifecycle events). Without this, CFL_VERIFY calls the bool on every
-- event and a transient false on INIT fires the error immediately.
local function timer_only(fn)
  return function(h, n, et, event_id, ed)
    if event_id ~= defs.CFL_TIMER_EVENT then return true end
    return fn(h, n, et, event_id, ed)
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
    if not ok then
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
  R.VERIFY_ALL_CPUS_READY = timer_only(function(_h, _n)
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
  end)

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
  R.ENABLE_NODE_MONITOR_KB = function(_h, _n)
    local idx = ct.kb_indexes.node_monitor
    if not idx then
      log("node_control", "ENABLE_NODE_MONITOR_KB skipped (no kb_index)")
      return
    end
    log("node_control", string.format(
      "ENABLE_NODE_MONITOR_KB -> cfl_rt.add_test(%d)", idx))
    cfl_rt.add_test(ct.handle, idx)
  end
  R.DISABLE_NODE_MONITOR_KB = function(_h, _n)
    local idx = ct.kb_indexes.node_monitor
    if not idx then return end
    log("node_control", "DISABLE_NODE_MONITOR_KB -> cfl_rt.delete_test")
    cfl_rt.delete_test(ct.handle, idx)
  end

  -- verifies
  -- VERIFY_PG: single-attempt DBI connect. Chain-tree's asm_verify_timeout
  -- drives retries across ticks; the 30s verify window is long enough for
  -- pg to come up from cold start. On success populate ctx.connectors.pg
  -- so later code can reuse the connection without reconnecting.
  R.VERIFY_PG = timer_only(function(_h, _n)
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
  end)
  -- Broker verifies: docker.is_running as a v1 health proxy. Sufficient
  -- until Build 3+ wires real Lua clients (NATS/MQTT subscribers, HTTP
  -- probe for kv_bridge) that exercise each broker's API. Container
  -- up => broker accepting connections (--restart=always keeps them alive).
  local function is_running_verify(name, half)
    return timer_only(function(_h, _n)
      local ok = docker.is_running(name)
      if not ok then
        log(half, string.format("VERIFY %s running -> false", name))
      end
      return ok
    end)
  end
  R.VERIFY_NATS              = is_running_verify("nats-js-ram",           "system_control")
  R.VERIFY_MQTT              = is_running_verify("mosquitto-ram-ws_main", "system_control")
  R.VERIFY_KV_BRIDGE         = is_running_verify("kv-bridge",             "system_control")
  R.VERIFY_KV_BRIDGE_HEALTHY = is_running_verify("kv-bridge",             "system_control")

  -- Monitor-state aggregate: all operator-owned infra containers must be
  -- running. Single verify instead of one per container so the monitor
  -- stays compact; log identifies which specific container(s) tripped so
  -- the subsequent ERR_MONITOR_TRIP exception carries the detail.
  local SYSTEM_CONTAINERS = {
    "pg-vector", "nats-js-ram", "mosquitto-ram-ws_main", "kv-bridge",
  }
  R.VERIFY_SYSTEM_CONTAINERS_HEALTHY = timer_only(function(_h, _n)
    local failed = {}
    for _, name in ipairs(SYSTEM_CONTAINERS) do
      if not docker.is_running(name) then failed[#failed + 1] = name end
    end
    if #failed == 0 then return true end
    log("system_control", string.format(
      "VERIFY_SYSTEM_CONTAINERS_HEALTHY -> false (down: %s)",
      table.concat(failed, ", ")))
    return false
  end)

  -- VERIFY_NODE_CTRL_HEARTBEAT_FRESH: heartbeat_ts must be within
  -- heartbeat_fresh_s of now. node_control writes heartbeat_ts every
  -- monitor iteration; if stale, sys_sm trips ERR_MONITOR_TRIP.
  local heartbeat_fresh_s = (ctx.settings and ctx.settings.heartbeat_fresh_s) or 5
  R.VERIFY_NODE_CTRL_HEARTBEAT_FRESH = timer_only(function(_h, _n)
    local ts = pg.node_control_heartbeat_ts or 0
    if ts == 0 then return true end   -- grace before first heartbeat
    local age = os.time() - ts
    local ok  = age <= heartbeat_fresh_s
    if not ok then
      log("system_control", string.format(
          "VERIFY_NODE_CTRL_HEARTBEAT_FRESH -> false (age=%ds, max=%ds)",
          age, heartbeat_fresh_s))
    end
    return ok
  end)

  R.VERIFY_NODE_CTRL_OPERATIONAL = timer_only(function(_h, _n)
    return pg.node_control_operational == true
  end)
  R.VERIFY_NODE_CTRL_STOPPED = timer_only(function(_h, _n)
    return pg.node_control_stopped == true
  end)

  -- Terminate the chain-tree VM cleanly so the watchdog restarts us.
  --
  -- Pumps CFL_TERMINATE_SYSTEM_EVENT. cfl_runtime.run() pops it, clears
  -- active_tests, and returns; dcs.run_loop's any_active() flips false
  -- and the process exits 1 so start.sh restarts.
  --
  -- We do NOT call docker.stop_labelled here. That used `docker rm -f`
  -- on every labelled container, removing operator-owned infra
  -- (pg-vector, nats-js-ram, mosquitto, kv-bridge). Wrong policy: DCS
  -- only starts and stops infra, never creates/removes. Per-container
  -- stops happen in the chain-tree's teardown_st sequence.
  local function request_terminate(handle, half, reason)
    log(half, "TERMINATE: " .. reason .. " -- closing connectors")
    if ctx.connectors.pg then
      pcall(function() ctx.connectors.pg:close() end)
      ctx.connectors.pg = nil
    end
    ctx.connectors.nats = nil
    ctx.connectors.mqtt = nil
    ctx.terminate_reason = reason
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

  -- Pump CFL_CHANGE_STATE_EVENT directly. Used by monitor-state error
  -- handlers that want to advance the state machine to teardown rather
  -- than terminate the process: the chain-tree's own teardown_st column
  -- runs the orderly stop sequence (notify node_control, wait, stop
  -- containers, terminate_system).
  local sm_mod = require("cfl_state_machine")
  local function log_then_change_state(handle, half, reason, exc_name,
                                       sm_name, target_state)
    if ctx.connectors.pg and exc_name then
      local ok, err = kb_exc.log_exception(
        ctx.connectors.pg, exc_path(exc_name), reason)
      if not ok then
        log(half, "log_exception FAILED: " .. tostring(err))
      end
    end
    local sm = handle.flash_handle.sm_by_name[sm_name]
    if not sm then
      log(half, string.format("change_state: unknown sm %q -- terminating instead",
                              tostring(sm_name)))
      request_terminate(handle, half, reason)
      return
    end
    local idx = sm.states[target_state]
    if not idx then
      log(half, string.format("change_state: sm %q has no state %q -- terminating",
                              sm_name, tostring(target_state)))
      request_terminate(handle, half, reason)
      return
    end
    log(half, string.format("%s -> change_state(%s, %s)",
                            reason, sm_name, target_state))
    sm_mod.change_state(handle, 0, sm.node_id, idx, nil)
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

  -- Monitor-state errors advance the state machine to request_shutdown
  -- (cooperative pause: signal node_control + wait for ack), which then
  -- transitions to teardown either on ack or on timeout.
  R.ERR_MONITOR_TRIP = function(handle, _n)
    log_then_change_state(handle, "system_control",
                          "ERR_MONITOR_TRIP", "slave_unreachable",
                          "sys_sm", "request_shutdown")
  end

  -- Fired by request_shutdown's asm_wait timeout. Forces transition to
  -- teardown — at this point we've waited long enough for cooperative
  -- ack and we're going to stop infra regardless.
  R.ERR_FORCE_TEARDOWN = function(handle, _n)
    log_then_change_state(handle, "system_control",
                          "ERR_FORCE_TEARDOWN", "shutdown_timeout",
                          "sys_sm", "teardown")
  end

  -- Tell node_control (this CPU + remote CPUs in future) to begin its
  -- own teardown_st. node_control's monitor verify VERIFY_NO_TEARDOWN_
  -- REQUEST trips on this flag and fires ERR_TEARDOWN_REQUESTED, which
  -- change_state's node_sm to teardown.
  R.POST_SHUTDOWN_REQUEST = function(_h, _n)
    pg.node_control_teardown_request = true
    log("system_control", "POST_SHUTDOWN_REQUEST -> node_control_teardown_request=true")
  end

  -- Wait predicate: returns true when every node_control instance has
  -- finished its teardown_st. Single-CPU: just the local one. Multi-CPU
  -- later: aggregate across CPUs (likely via a pg bit_mask row similar
  -- to ready_bits).
  R.VERIFY_ALL_NODES_SHUTDOWN = timer_only(function(_h, _n)
    return pg.node_control_stopped == true
  end)
  R.ERR_CONTAINER_DIED = function(handle, _n)
    log_then_change_state(handle, "node_control",
                          "ERR_CONTAINER_DIED", "container_died",
                          "node_sm", "teardown")
  end
  R.ERR_TEARDOWN_REQUESTED = function(handle, _n)
    -- normal teardown signal; not a fault to log
    log_then_change_state(handle, "node_control",
                          "ERR_TEARDOWN_REQUESTED", nil,
                          "node_sm", "teardown")
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
  end
  R.WRITE_PROCESS_GLOBALS_NODE_STOPPED_TRUE = function(_h, _n)
    pg.node_control_stopped = true
    log("node_control", "node_control_stopped = TRUE")
  end

  R.NODE_VERIFY_BROKERS_REACHABLE          = verify_stub(log, "NODE_VERIFY_BROKERS_REACHABLE",          "node_control")
  R.VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY = verify_stub(log, "VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY", "node_control")
  R.VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED = verify_stub(log, "VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED", "node_control")

  R.VERIFY_NO_TEARDOWN_REQUEST = timer_only(function(_h, _n)
    return not pg.node_control_teardown_request
  end)

  -- (ERR_CONTAINER_DIED, ERR_TEARDOWN_REQUESTED, ERR_CONTAINERS_START_FAIL,
  -- ERR_TEARDOWN_FORCE all defined above with real bodies — do not stub
  -- them here or the late assignment overwrites the real handler.)

  ----------------------------------------------------------------------
  -- node_monitor (resource sampler) -- best-effort, never trips
  -- supervision. State held in ctx.monitor_state; pg writes are
  -- one-row-per-sample JSONB into the monitor.samples stream.
  ----------------------------------------------------------------------
  local sampler   = require("host_sampler")
  local kb_stream = require("kb_stream")
  ctx.monitor_state = ctx.monitor_state or {
    boot_epoch       = os.time(),
    pid              = nil,            -- filled by MONITOR_INIT_STATE
    cgroup_paths     = {},             -- container_name -> cgroup dir
    prev_cpu_stat    = nil,            -- for /proc/stat delta
    prev_proc_stat   = nil,            -- for /proc/<pid>/stat delta
    prev_cgroup      = {},             -- per-container previous reading
    prev_diskstats   = nil,            -- for /proc/diskstats delta
    prev_net_dev     = nil,            -- for /proc/net/dev delta
    welford          = {},             -- metric_key -> {n, mean, m2}
    sample_count     = 0,
    samples_dropped  = 0,
  }
  local mon = ctx.monitor_state

  -- Disk free probes — extend list as you mount more volumes.
  local DISK_PROBES = { "/", "/var/lib/docker" }

  -- Stream slots for this CPU live at:
  --   system.site.<S>.cpu.<id>.monitor.samples.KB_STREAM_FIELD.samples
  -- The construct_kb library inserts the satellite label (KB_STREAM_FIELD)
  -- as a path component, so the runtime path mirrors the build-time one.
  local function _monitor_stream_path()
    return string.format(
      "system.site.%s.cpu.%s.monitor.samples.KB_STREAM_FIELD.samples",
      ctx.cfg.site, ctx.cfg.cpu_id)
  end

  local function _emit_sample(kind, payload)
    mon.sample_count = mon.sample_count + 1
    payload.kind        = kind
    payload.boot_epoch  = mon.boot_epoch
    payload.pid         = mon.pid
    payload.sample_seq  = mon.sample_count
    payload.ts          = os.time()

    if not ctx.connectors.pg then
      -- pg not yet up; degrade to log so we still see samples during
      -- startup and after a teardown that closed the connector.
      mon.samples_dropped = mon.samples_dropped + 1
      return
    end
    local ok, err = kb_stream.push(
      ctx.connectors.pg, _monitor_stream_path(), payload)
    if not ok then
      mon.samples_dropped = mon.samples_dropped + 1
      log("node_monitor", string.format(
        "sample push FAILED (kind=%s): %s", kind, tostring(err)))
    end
  end

  R.MONITOR_DISCOVER_CGROUPS = function(_h, _n)
    -- Resolve cgroup path for every assigned + infra container we'd want
    -- to sample. node_control_globals.assignments is the long-term home;
    -- for v1 just probe the four operator-owned infra names.
    local names = {
      "pg-vector", "nats-js-ram", "mosquitto-ram-ws_main", "kv-bridge",
    }
    for _, asg in ipairs(ctx.node_control_globals.assignments or {}) do
      names[#names + 1] = asg.name or asg
    end
    mon.cgroup_paths = {}
    local found, missing = {}, {}
    for _, name in ipairs(names) do
      local path = sampler.cgroup_path(name)
      if path then
        mon.cgroup_paths[name] = path
        found[#found + 1] = name
      else
        missing[#missing + 1] = name
      end
    end

    if #found == 0 then
      -- Common on Docker Desktop + WSL2: container pids live in a
      -- separate kernel's namespace, so /proc/<pid>/cgroup doesn't
      -- exist on the host. On real Linux (bare metal, Pi, native
      -- docker) this resolves. Log once, don't spam per tick.
      mon.container_sampling_disabled = true
      log("node_monitor",
          "MONITOR_DISCOVER_CGROUPS: container sampling disabled " ..
          "(no cgroups resolvable from this host; typical for " ..
          "Docker Desktop + WSL2). Deploy on native Linux to enable.")
    else
      log("node_monitor", string.format(
        "MONITOR_DISCOVER_CGROUPS: %d/%d resolved (found: %s; missing: %s)",
        #found, #names,
        table.concat(found, ","),
        table.concat(missing, ",")))
    end
  end

  R.MONITOR_INIT_STATE = function(_h, _n)
    -- Get our own pid from /proc/self/status (no luaposix dependency).
    local s = sampler.read("/proc/self/status")
    if s then mon.pid = tonumber(s:match("Pid:%s+(%d+)")) end
    log("node_monitor", string.format(
      "MONITOR_INIT_STATE: pid=%s boot_epoch=%d",
      tostring(mon.pid), mon.boot_epoch))
  end

  R.SAMPLE_HOST = function(_h, _n)
    local cpu_now = sampler.cpu_stat()
    local cpu_pct = mon.prev_cpu_stat
                    and sampler.cpu_pct(mon.prev_cpu_stat, cpu_now)
                    or { cpu_pct = 0, iowait_pct = 0 }
    mon.prev_cpu_stat = cpu_now

    local mem  = sampler.meminfo()
    local load = sampler.loadavg()
    local temp_mC = sampler.cpu_temperature_mC()

    -- Disk usage + IO deltas
    local disk_free = {}
    for _, mp in ipairs(DISK_PROBES) do
      local d = sampler.df(mp)
      if d then
        disk_free[mp] = {
          avail_bytes = d.avail_bytes,
          used_pct    = d.used_pct,
          source      = d.source,
        }
      end
    end

    local diskstats_now = sampler.diskstats()
    local disk_io_delta = {}
    if diskstats_now and mon.prev_diskstats then
      for dev, cur in pairs(diskstats_now) do
        local prev = mon.prev_diskstats[dev]
        if prev then
          disk_io_delta[dev] = {
            reads          = cur.reads         - prev.reads,
            read_sectors   = cur.read_sectors  - prev.read_sectors,
            writes         = cur.writes        - prev.writes,
            write_sectors  = cur.write_sectors - prev.write_sectors,
            ios_in_flight  = cur.ios_in_flight,
          }
        end
      end
    end
    mon.prev_diskstats = diskstats_now

    -- Network IO deltas
    local net_now = sampler.net_dev()
    local net_delta = {}
    if net_now and mon.prev_net_dev then
      for iface, cur in pairs(net_now) do
        local prev = mon.prev_net_dev[iface]
        if prev then
          net_delta[iface] = {
            rx_bytes   = cur.rx_bytes   - prev.rx_bytes,
            rx_packets = cur.rx_packets - prev.rx_packets,
            rx_errors  = cur.rx_errors  - prev.rx_errors,
            tx_bytes   = cur.tx_bytes   - prev.tx_bytes,
            tx_packets = cur.tx_packets - prev.tx_packets,
            tx_errors  = cur.tx_errors  - prev.tx_errors,
          }
        end
      end
    end
    mon.prev_net_dev = net_now

    -- Welford the slow-drift metrics; leak detection uses these means.
    if mem then
      mon.welford.mem_available_kb = mon.welford.mem_available_kb
                                   or {n=0, mean=0, m2=0}
      sampler.welford_update(mon.welford.mem_available_kb,
                             mem.mem_available_kb)
    end

    _emit_sample("host", {
      cpu_pct          = cpu_pct.cpu_pct,
      iowait_pct       = cpu_pct.iowait_pct,
      load1            = load and load.load1   or 0,
      load5            = load and load.load5   or 0,
      mem_available_kb = mem  and mem.mem_available_kb or 0,
      mem_dirty_kb     = mem  and mem.dirty_kb         or 0,
      mem_slab_kb      = mem  and mem.slab_kb          or 0,
      swap_used_kb     = mem  and (mem.swap_total_kb - mem.swap_free_kb) or 0,
      cpu_temp_mC      = temp_mC,
      disk_free        = disk_free,
      disk_io_delta    = disk_io_delta,
      net_delta        = net_delta,
    })
  end

  R.SAMPLE_SYSTEM_CONTROL_PROCESS = function(_h, _n)
    if not mon.pid then return end
    local status = sampler.proc_status(mon.pid)
    local stat   = sampler.proc_stat(mon.pid)
    local io_r   = sampler.proc_io(mon.pid)

    local cpu_jiffy_delta = 0
    if mon.prev_proc_stat and stat then
      cpu_jiffy_delta = stat.cpu_jiffies - mon.prev_proc_stat.cpu_jiffies
    end
    mon.prev_proc_stat = stat

    _emit_sample("process", {
      pid              = mon.pid,
      rss_kb           = status and status.rss_kb    or 0,
      vmsize_kb        = status and status.vmsize_kb or 0,
      threads          = status and status.threads   or 0,
      cpu_jiffy_delta  = cpu_jiffy_delta,
      io_read_bytes    = io_r   and io_r.read_bytes  or 0,
      io_write_bytes   = io_r   and io_r.write_bytes or 0,
    })
  end

  R.SAMPLE_CONTAINERS = function(_h, _n)
    -- No-op when container sampling is disabled in this environment
    -- (see MONITOR_DISCOVER_CGROUPS). Cheap early-out; the reset loop
    -- still ticks and will retry discovery on the next process restart.
    if mon.container_sampling_disabled then return end

    -- One sample per discovered container; each is 3 small file reads.
    -- All inline this tick. If container count grows large enough that
    -- this overruns the tick budget, split into per-container one-shots
    -- stepped across ticks.
    for name, path in pairs(mon.cgroup_paths) do
      local now = sampler.cgroup_sample(path)
      if now then
        local prev = mon.prev_cgroup[name]
        local cpu_usec_delta = prev and (now.cpu_usec - prev.cpu_usec) or 0
        local rb_delta       = prev and (now.io_rbytes - prev.io_rbytes) or 0
        local wb_delta       = prev and (now.io_wbytes - prev.io_wbytes) or 0
        mon.prev_cgroup[name] = now
        _emit_sample("container", {
          name              = name,
          mem_current_bytes = now.mem_current_bytes,
          cpu_usec_delta    = cpu_usec_delta,
          io_rbytes_delta   = rb_delta,
          io_wbytes_delta   = wb_delta,
        })
      end
    end
  end

  R.COMPUTE_TRENDS = function(_h, _n)
    -- Read recent host samples from pg, fit a linear regression slope
    -- per leak-relevant metric. Slope unit is value-per-second.
    --
    -- For leak detection we care about: mem_available_kb (down-trend
    -- = leak), mem_dirty_kb (sustained up = sync issue), swap_used_kb
    -- (any non-zero up-trend is bad). Add metrics here as they become
    -- interesting.
    if not ctx.connectors.pg then return end

    local rows, err = kb_stream.read_recent(
      ctx.connectors.pg, _monitor_stream_path(), 360)  -- ~6h at 60s
    if not rows then
      log("node_monitor", "COMPUTE_TRENDS read_recent failed: " .. tostring(err))
      return
    end

    -- Bucket samples by metric. Only include host samples; segment by
    -- pid so a process restart doesn't fake a slope discontinuity.
    local metrics = {
      mem_available_kb = {},
      mem_dirty_kb     = {},
      swap_used_kb     = {},
      cpu_pct          = {},
    }
    local current_pid = mon.pid
    local t_ref       -- earliest ts in window, used as x=0
    for _, r in ipairs(rows) do
      local d = r.data
      if d and d.kind == "host" and d.pid == current_pid and d.ts then
        if not t_ref or d.ts < t_ref then t_ref = d.ts end
      end
    end
    if not t_ref then return end
    for _, r in ipairs(rows) do
      local d = r.data
      if d and d.kind == "host" and d.pid == current_pid and d.ts then
        local x = d.ts - t_ref
        for k, _ in pairs(metrics) do
          if d[k] ~= nil then
            metrics[k][#metrics[k] + 1] = { x, d[k] }
          end
        end
      end
    end

    local trends = {}
    for k, pts in pairs(metrics) do
      trends[k] = {
        n           = #pts,
        slope_per_s = sampler.linreg_slope(pts),
      }
    end
    _emit_sample("trend_snapshot", { trends = trends, window_s = 360 * 60 })
  end

  return R
end

return M
