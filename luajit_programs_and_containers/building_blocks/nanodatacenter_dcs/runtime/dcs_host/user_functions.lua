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
local kbcr   = require("kb_container_registry")
local kb_asg = require("kb_assignments")
local kb_log = require("kb_log")
local ptime  = require("posix_time")  -- sub-second timing for supervision metrics
local broker_client = require("broker_client")  -- read docker_host_broker state from KB
local spec_adapter  = require("spec_adapter")   -- catalog spec -> wire-protocol RunSpec

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

-- Wrapper for bool functions used with asm_wait (CFL_WAIT). asm_wait
-- advances when the bool returns true and halts when it returns false.
-- For INIT / CHANGE_STATE_EVENT we want to HALT (keep waiting), not
-- advance; so return false. Only the TIMER tick runs the real check.
-- Necessary because a naive timer_only wrapper returns true on those
-- non-TIMER events, causing asm_wait to race past before the first
-- real evaluation.
local function wait_bool(fn)
  return function(h, n, et, event_id, ed)
    if event_id ~= defs.CFL_TIMER_EVENT then return false end
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

  -- Configure broker_client with the site from bootstrap, plus the HTTP
  -- mutation endpoint. Verifies (read path) need site; START/STOP/RUN/RM
  -- handlers (mutation path) need http_base_url. BROKER_HTTP_URL falls
  -- back to the wire-protocol default (127.0.0.1:9100) so deployments
  -- without an explicit env still work.
  if ctx.cfg and ctx.cfg.site then
    broker_client.configure{
      site           = ctx.cfg.site,
      http_base_url  = os.getenv("BROKER_HTTP_URL") or "http://127.0.0.1:9100",
      http_timeout_s = 10,
    }
  end

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
  -- infra containers; the laptop install scripts created them. We toggle
  -- state via the docker_host_broker (POST /v1/cmd/start). Idempotent:
  -- broker returns 409 already_running which broker_client.start maps
  -- onto ok=true with info.idempotent=true.
  local function start_container(inst_name, _def_name)
    local ok, info = broker_client.start(inst_name)
    if ok then
      if type(info) == "table" and info.idempotent then
        log("system_control",
            string.format("START %s: already running", inst_name))
      else
        log("system_control", "START " .. inst_name)
      end
    else
      log("system_control",
          string.format("START %s FAILED: %s", inst_name, tostring(info)))
    end
  end

  local function stop_container(inst_name)
    -- Infra containers persist (laptop placed them); stop only, no rm.
    local ok, info = broker_client.stop(inst_name, 10)
    if ok then
      log("system_control", "STOP " .. inst_name)
    else
      log("system_control",
          string.format("STOP %s FAILED: %s", inst_name, tostring(info)))
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
  ----------------------------------------------------------------------
  -- sync_control handlers
  --
  -- sync_control runs BEFORE the operational phase and is patient-forever:
  -- no infra-reachability failure ever raises an exception; it just keeps
  -- polling. The single exception defined here fires only on the master,
  -- if a slave hasn't posted its cluster_sync_bit within the quorum-wait
  -- window -- and even then the master logs slave_never_joined and
  -- PROCEEDS (no teardown) so a dead slave can't pin the whole site.
  --
  -- Boundary: once sync_control's handoff fires ENABLE_SYSTEM_CONTROL_KB /
  -- ENABLE_NODE_CONTROL_KB + DISABLE_SYNC_CONTROL_*, the operational KBs
  -- take over with their strict fail-fast semantics. Any op-phase
  -- failure -> watchdog restart -> sync_control re-entered, which
  -- quietly re-verifies infra and re-hands-off.
  ----------------------------------------------------------------------

  -- Master quorum timeout tracking. One-shot start time seeded by the
  -- first VERIFY_SYNC_QUORUM_OR_TIMEOUT tick; reset by CLEAR_CLUSTER_GO
  -- on teardown so a subsequent sync cycle restarts the clock.
  local SYNC_QUORUM_TIMEOUT_S = 300.0
  local sync_quorum_started_at = nil

  -- Reusable helper: all 4 infra endpoints reachable right now.
  -- Done in two phases so we don't block the chain-tree tick on a libpq
  -- connect (which can stall 30s+ when pg is down):
  --   1. docker.is_running on each of the 4 container names. Fast
  --      (queries the local docker socket) and non-blocking.
  --   2. Only if all 4 containers are "Up", attempt a real pg connect
  --      to confirm pg accepts queries (rules out "started but not yet
  --      listening" or wrong-password).
  -- nats/mqtt/kv_bridge v1 rely on (1) -- container-up is a strong
  -- enough proxy for reachability inside a single-host lab. Add TCP
  -- probes here when a real multi-host deployment demands stricter.
  local function all_infra_reachable()
    for _, name in ipairs({ "pg-vector", "nats-js-ram",
                            "mosquitto-ram-ws_main", "kv-bridge" }) do
      if not docker.is_running(name) then return false end
    end
    -- Containers all up; confirm pg accepts queries.
    local pw = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    if not pw or pw == "" then return false end
    local conn, err = pg_conn.try_connect(ctx.cfg, pw)
    if not conn then return false end
    conn:close()
    return true
  end

  -- Used in the slave's wait_infra state's asm_wait: halts (returns false)
  -- on non-TIMER events and performs the real probe on each TIMER tick.
  R.VERIFY_ALL_INFRA_REACHABLE = wait_bool(function(_h, _n)
    return all_infra_reachable()
  end)

  R.SET_OWN_SYNC_BIT = function(_h, _n)
    if not ctx.connectors.pg then
      -- Re-open: sync phase may have closed the connector between cycles.
      local pw = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
      local conn, err = pg_conn.try_connect(ctx.cfg, pw or "")
      if not conn then
        log("sync_control", "SET_OWN_SYNC_BIT: pg connect failed: " .. tostring(err))
        return
      end
      ctx.connectors.pg = conn
    end
    local ok, err = bm.set_cluster_sync_bit(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.bit_index)
    log("sync_control", ok
      and string.format("cluster_sync bit %d SET", ctx.cfg.bit_index)
       or string.format("cluster_sync bit set FAILED: %s", tostring(err)))
  end

  R.CLEAR_OWN_SYNC_BIT = function(_h, _n)
    if not ctx.connectors.pg then return end
    local ok, err = bm.clear_cluster_sync_bit(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.bit_index)
    if not ok then
      log("sync_control", "cluster_sync bit clear FAILED: " .. tostring(err))
    end
  end

  -- Master-only: combines quorum check with a soft timeout. Returns true
  -- when all bits are set OR the timeout has elapsed (logging
  -- slave_never_joined in the latter case). Used with asm_wait so we
  -- halt on non-TIMER events (wait_bool wrapper) rather than racing
  -- past on state-entry events.
  R.VERIFY_SYNC_QUORUM_OR_TIMEOUT = wait_bool(function(_h, _n)
    if not ctx.connectors.pg then return false end
    if sync_quorum_started_at == nil then
      sync_quorum_started_at = os.time()
    end
    local actual, err = bm.read_cluster_sync_bits(
      ctx.connectors.pg, ctx.cfg.site)
    if not actual then
      log("sync_control", "VERIFY_SYNC_QUORUM read err: " .. tostring(err))
      return false
    end
    local expected = bm.expected_full_mask(ctx.cfg.expected_cpu_count)
    if actual == expected then
      log("sync_control", string.format(
        "SYNC QUORUM reached (bits=0x%x)", actual))
      return true
    end
    local elapsed = os.time() - sync_quorum_started_at
    if elapsed >= SYNC_QUORUM_TIMEOUT_S then
      log("sync_control", string.format(
        "SYNC QUORUM TIMEOUT after %ds; actual=0x%x expected=0x%x; logging slave_never_joined and proceeding",
        elapsed, actual, expected))
      kb_exc.log_exception(ctx.connectors.pg,
        exc_path("slave_never_joined"),
        string.format("quorum timeout; actual=0x%x expected=0x%x",
                      actual, expected))
      return true
    end
    return false
  end)

  R.WRITE_CLUSTER_GO_TRUE = function(_h, _n)
    if not ctx.connectors.pg then return end
    local path = "system.site." .. ctx.cfg.site .. ".KB_STATUS_FIELD.cluster_go"
    local ok, err = kb_stat.set_status_data(ctx.connectors.pg, path, { value = 1 })
    log("sync_control", ok
      and "cluster_go = 1 WRITTEN"
       or ("cluster_go write FAILED: " .. tostring(err)))
  end

  R.CLEAR_CLUSTER_GO = function(_h, _n)
    if not ctx.connectors.pg then return end
    local path = "system.site." .. ctx.cfg.site .. ".KB_STATUS_FIELD.cluster_go"
    kb_stat.set_status_data(ctx.connectors.pg, path, { value = 0 })
    -- Reset the per-process timeout tracker so a subsequent cycle
    -- restarts the clock from zero.
    sync_quorum_started_at = nil
  end

  -- Master-only: zero the cluster_sync mask at the start of a sync cycle
  -- so a restart after a crash doesn't see stale bits from last time.
  R.CLEAR_ALL_CLUSTER_SYNC_BITS = function(_h, _n)
    if not ctx.connectors.pg then
      local pw = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
      local conn, err = pg_conn.try_connect(ctx.cfg, pw or "")
      if not conn then
        log("sync_control", "CLEAR_ALL_CLUSTER_SYNC_BITS: pg connect failed: " .. tostring(err))
        return
      end
      ctx.connectors.pg = conn
    end
    local ok, err = bm.clear_all_cluster_sync_bits(ctx.connectors.pg, ctx.cfg.site)
    log("sync_control", ok and "cluster_sync_bits reset to 0"
                            or ("cluster_sync_bits reset FAILED: " .. tostring(err)))
  end

  -- Used in slave's wait_go state's asm_wait.
  R.VERIFY_CLUSTER_GO = wait_bool(function(_h, _n)
    if not ctx.connectors.pg then return false end
    local path = "system.site." .. ctx.cfg.site .. ".KB_STATUS_FIELD.cluster_go"
    local data = kb_stat.get_status_data(ctx.connectors.pg, path)
    if type(data) ~= "table" then return false end
    return tonumber(data.value) == 1
  end)

  -- KB activation handoff. ENABLE_NODE_CONTROL_KB already exists above.
  R.ENABLE_SYSTEM_CONTROL_KB = function(_h, _n)
    local idx = ct.kb_indexes.system_control
    log("sync_control", string.format(
      "ENABLE_SYSTEM_CONTROL_KB -> cfl_rt.add_test(%d)", idx or -1))
    cfl_rt.add_test(ct.handle, idx)
  end

  R.DISABLE_SYNC_CONTROL_MASTER_KB = function(_h, _n)
    local idx = ct.kb_indexes.sync_control_master
    if not idx then return end
    log("sync_control", string.format(
      "DISABLE_SYNC_CONTROL_MASTER_KB -> cfl_rt.delete_test(%d)", idx))
    cfl_rt.delete_test(ct.handle, idx)
  end

  R.DISABLE_SYNC_CONTROL_SLAVE_KB = function(_h, _n)
    local idx = ct.kb_indexes.sync_control_slave
    if not idx then return end
    log("sync_control", string.format(
      "DISABLE_SYNC_CONTROL_SLAVE_KB -> cfl_rt.delete_test(%d)", idx))
    cfl_rt.delete_test(ct.handle, idx)
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
  -- Broker verifies: read container state from docker_host_broker via the
  -- KB row it mirrors into knowledge_base_status. Reads are O(1) pg row
  -- lookups (~ms on local pg) cached for ~0.5s — does NOT shell out.
  -- See building_blocks/docker_host_broker/WIRE_PROTOCOL.md.
  -- Falls back to docker.is_running when the broker hasn't been configured
  -- (e.g., site not set, dcs.lua bootstrapping pre-broker), so this module
  -- still works during Phase 1c migration without a hard dependency.
  --
  -- Configured at the bottom of this build(ctx) function once cfg.site is
  -- known.
  local function is_running_verify(name, half)
    return timer_only(function(_h, _n)
      if ctx.connectors and ctx.connectors.pg then
        -- Force a refresh on miss to surface the underlying error.
        local rok, rerr = broker_client.refresh(ctx.connectors.pg)
        if not rok then
          log(half, string.format("VERIFY %s broker_client.refresh err: %s",
              name, tostring(rerr)))
          return false
        end
        local ok, why = broker_client.is_running(ctx.connectors.pg, name)
        if not ok then
          log(half, string.format("VERIFY %s running -> false (broker: %s)",
              name, tostring(why)))
        end
        return ok
      end
      -- Fallback path while ctx.connectors.pg is being established.
      local ok = docker.is_running(name)
      if not ok then
        log(half, string.format("VERIFY %s running -> false (docker fallback)", name))
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
  -- Hysteresis: VERIFY_SYSTEM_CONTAINERS_HEALTHY needs N consecutive
  -- failures before it returns false, so brief broker hiccups (vpnkit
  -- pause, docker daemon stall, broker restart < ~3 verify cycles)
  -- don't cascade into ERR_MONITOR_TRIP -> teardown_st -> respawn loop.
  -- Sustained broker death still trips fail-closed within N * cycle.
  -- Reset to zero on the first clean check; the closure dies with the
  -- process so no per-trip reset is needed.
  local sys_health_fail_streak = 0
  local SYS_HEALTH_TRIP_THRESHOLD = 3
  R.VERIFY_SYSTEM_CONTAINERS_HEALTHY = timer_only(function(_h, _n)
    local conn = ctx.connectors and ctx.connectors.pg
    local failed = {}
    local refresh_err
    if conn then
      -- Single broker_client refresh + N constant-time cache reads instead
      -- of N docker shell-outs per chain-tree tick. Removes the per-tick
      -- walker-blocking that originally tore down infra.
      local rok, rerr = broker_client.refresh(conn)
      if not rok then
        refresh_err = rerr
        for _, name in ipairs(SYSTEM_CONTAINERS) do
          failed[#failed + 1] = name
        end
      else
        for _, name in ipairs(SYSTEM_CONTAINERS) do
          local running = broker_client.is_running(conn, name)
          if not running then failed[#failed + 1] = name end
        end
      end
    else
      -- Fallback during early bootstrap before pg connector is wired.
      for _, name in ipairs(SYSTEM_CONTAINERS) do
        if not docker.is_running(name) then failed[#failed + 1] = name end
      end
    end

    if #failed == 0 then
      if sys_health_fail_streak > 0 then
        log("system_control", string.format(
          "VERIFY_SYSTEM_CONTAINERS_HEALTHY recovered after %d failed checks",
          sys_health_fail_streak))
      end
      sys_health_fail_streak = 0
      return true
    end

    sys_health_fail_streak = sys_health_fail_streak + 1
    local detail = refresh_err
                   and ("broker refresh: " .. tostring(refresh_err))
                   or ("down: " .. table.concat(failed, ", "))
    if sys_health_fail_streak < SYS_HEALTH_TRIP_THRESHOLD then
      log("system_control", string.format(
        "VERIFY_SYSTEM_CONTAINERS_HEALTHY transient %d/%d (%s) -- riding through",
        sys_health_fail_streak, SYS_HEALTH_TRIP_THRESHOLD, detail))
      return true
    end
    log("system_control", string.format(
      "VERIFY_SYSTEM_CONTAINERS_HEALTHY -> false sustained=%d (%s)",
      sys_health_fail_streak, detail))
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

  -- Runtime pre-flight: list host TCP listeners via `ss -tln`. Caller
  -- calls once per batch and reuses the result across assignments to avoid
  -- spawning a shell per container.
  local function listening_ports()
    local p = io.popen("ss -tln 2>/dev/null")
    if not p then return {} end
    local out = p:read("*a") or ""
    p:close()
    local ports = {}
    for line in out:gmatch("[^\r\n]+") do
      local port = line:match(":(%d+)%s")
      if port then ports[tonumber(port)] = true end
    end
    return ports
  end

  local function external_ports_of(asg)
    local out = {}
    for _, p in ipairs(asg.service and asg.service.ports or {}) do
      local ext = p.external or p.host
      if ext then out[#out + 1] = ext end
    end
    return out
  end

  -- CHECK_PORT_CONFLICT: any external port already bound on the host is
  -- a hard skip for that assignment (docker run would fail the bind with
  -- the same result, but this path lets us log the specific port).
  local function check_port_conflict(asg, listening)
    for _, port in ipairs(external_ports_of(asg)) do
      if listening[port] then return false, port end
    end
    return true
  end

  -- Log a per-container start failure: SYS_EXCEPTION + stderr. Doesn't
  -- propagate; node_control keeps iterating over the remaining assignments.
  local function log_container_failure(asg_name, reason)
    local msg = string.format("%s: %s", asg_name, reason)
    log("node_control", "container_start_failed -- " .. msg)
    if ctx.connectors.pg then
      local ok, err = kb_exc.log_exception(
        ctx.connectors.pg, exc_path("container_start_failed"), msg)
      if not ok then
        log("node_control", "log_exception FAILED: " .. tostring(err))
      end
    end
  end

  -- REGISTER: write both CONTAINER_REGISTRY rows (schema + status). Idempotent.
  local function register_assignment(asg)
    if not ctx.connectors.pg then
      log("node_control", "REGISTER " .. asg.name .. " skipped (no pg conn)")
      return false, "no pg conn"
    end
    local props = { definition = asg.definition, category = "application" }
    local data  = {
      host        = (asg.service and asg.service.host) or asg.name,
      ports       = (asg.service and asg.service.ports) or {},
    }
    local spec = (ctx.system_control_globals.build_specs or {})[asg.definition]
    if spec then
      data.image       = spec.image
      data.description = spec.description
                         or (asg.definition .. " :: " .. asg.name)
    end
    local ok, err = kbcr.register(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id,
      asg.name, props, data)
    if ok then
      log("node_control", "REGISTER " .. asg.name)
    else
      log("node_control", "REGISTER " .. asg.name .. " FAILED: " .. tostring(err))
    end
    return ok, err
  end

  -- DEREGISTER: delete both rows. No-op if absent.
  local function deregister_assignment(asg_name)
    if not ctx.connectors.pg then return false, "no pg conn" end
    local ok, err = kbcr.deregister(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id, asg_name)
    if ok then
      log("node_control", "DEREGISTER " .. asg_name)
    else
      log("node_control", "DEREGISTER " .. asg_name .. " FAILED: " .. tostring(err))
    end
    return ok, err
  end

  -- RECONCILE: at boot, drop any CONTAINER_REGISTRY rows on this CPU that
  -- aren't in our current assignment list. Keeps the gateway from routing
  -- to containers that no longer exist after a topology change.
  local function reconcile_registry(assignments)
    if not ctx.connectors.pg then return end
    local expected = {}
    for _, a in ipairs(assignments or {}) do expected[a.name] = true end
    local deleted, err = kbcr.reconcile(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id, expected)
    if not deleted then
      log("node_control", "RECONCILE FAILED: " .. tostring(err))
      return
    end
    if deleted > 0 then
      log("node_control", string.format(
        "RECONCILE removed %d stale CONTAINER_REGISTRY row(s)", deleted))
    end
  end

  -- NODE_READ_OWN_CONFIG: load assignment list from pg, stash for later,
  -- reconcile registry. Runs once when node_control's setup state enters.
  R.NODE_READ_OWN_CONFIG = function(_h, _n)
    if not ctx.connectors.pg then
      log("node_control", "NODE_READ_OWN_CONFIG skipped (no pg conn)")
      ctx.node_control_globals.assignments = {}
      return
    end
    local assignments, err = kb_asg.list_node_managed(
      ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id)
    if not assignments then
      log("node_control",
          "NODE_READ_OWN_CONFIG list_node_managed FAILED: " .. tostring(err))
      ctx.node_control_globals.assignments = {}
      return
    end
    ctx.node_control_globals.assignments = assignments
    local names = {}
    for _, a in ipairs(assignments) do names[#names + 1] = a.name end
    log("node_control", string.format(
      "NODE_READ_OWN_CONFIG: %d assignment(s)%s", #assignments,
      (#names > 0) and (" [" .. table.concat(names, ", ") .. "]") or ""))
    reconcile_registry(assignments)
  end

  -- launch_assignment: shared machinery for START + RECONCILE + WATCHDOG.
  -- Assumes caller has already confirmed the container is NOT running
  -- and handled port/listening accounting. Returns (true, id_short) on
  -- success, (false, reason) on failure. Registers on success.
  local function launch_assignment(asg, listening)
    local build_specs = ctx.system_control_globals.build_specs or {}
    local spec = build_specs[asg.definition]
    if not spec then
      return false, "build.spec not loaded for definition " ..
                    tostring(asg.definition)
    end
    local ok, bad_port = check_port_conflict(asg, listening)
    if not ok then
      return false, "external port " .. tostring(bad_port) ..
                    " already in use on host"
    end
    -- Merge: keep def.spec but override ports with per-instance
    -- resolved records (slot/internal/external from service.main).
    local merged = {}
    for k, v in pairs(spec) do merged[k] = v end
    merged.ports = (asg.service and asg.service.ports) or {}
    -- PG_HOST normalization: the DCS process reads pg at the host's
    -- localhost, but a bridge-network container's localhost is itself.
    -- Swap to host.docker.internal so the app's luajit-base supervisor
    -- can VERIFY_PG successfully. (The --add-host flag in run_from_spec
    -- creates the alias on both Docker Desktop and Linux-native.)
    local host_pg = ctx.cfg.pg_host
    if host_pg == "localhost" or host_pg == "127.0.0.1" then
      host_pg = "host.docker.internal"
    end
    local extra_env = {
      CONTAINER_NAME = asg.name,
      APP_SITE       = ctx.cfg.site,
      APP_CPU_ID     = ctx.cfg.cpu_id,
      PG_HOST        = host_pg,
      PG_PORT        = tostring(ctx.cfg.pg_port),
      PG_DB          = ctx.cfg.pg_db,
      PG_USER        = ctx.cfg.pg_user,
      PG_PASSWORD    = os.getenv("PG_PASSWORD")
                       or os.getenv("POSTGRES_PASSWORD") or "",
    }
    -- Catalog spec -> wire-protocol RunSpec. Resolves env_required from
    -- os.getenv, expands ~ in volume paths, normalizes ports.
    local run_spec, sa_err = spec_adapter.build_run_spec(asg.name, merged, extra_env)
    if not run_spec then
      return false, tostring(sa_err)
    end
    -- rm-then-run: docker.run_from_spec did `docker rm -f <name>` first
    -- to clear any stale stopped instance from a prior boot. We emulate
    -- that with broker_client.rm(force=true), which is idempotent on 404
    -- (already gone) and removes any stopped/dead remnant otherwise.
    broker_client.rm(asg.name, true)
    local rok, rres = broker_client.run(run_spec)
    if not rok then
      return false, tostring(rres)
    end
    register_assignment(asg)
    -- Optimistic: our ports are ours now; update the listening set so a
    -- second assignment with the same port would be flagged.
    for _, port in ipairs(external_ports_of(asg)) do
      listening[port] = true
    end
    -- broker_client.run returns info table on success; .id is the new
    -- container ID. Caller logs the first 12 chars to mirror prior format.
    local id_short = tostring((type(rres) == "table" and rres.id) or ""):sub(1, 12)
    return true, id_short
  end

  -- START_ASSIGNED_CONTAINERS: for each assignment, pre-flight port
  -- conflict, docker run from merged spec, register on success. Per-
  -- assignment failures are logged + SYS_EXCEPTION'd but do NOT halt the
  -- batch; other assignments still get a chance to start.
  R.START_ASSIGNED_CONTAINERS = function(_h, _n)
    local assignments = ctx.node_control_globals.assignments or {}
    if #assignments == 0 then
      log("node_control", "START_ASSIGNED_CONTAINERS: 0 assignments")
      return
    end
    local listening = listening_ports()
    ctx.node_control_globals.last_restart_ts =
      ctx.node_control_globals.last_restart_ts or {}
    local lrts = ctx.node_control_globals.last_restart_ts
    local now  = os.time()

    -- Refresh broker cache once before the per-assignment is_running
    -- check. Skips a docker-ps shell-out per pass (used to be the
    -- chain-tree-walker-starvation source on WSL2).
    if ctx.connectors.pg then
      broker_client.refresh(ctx.connectors.pg)
    end

    for _, asg in ipairs(assignments) do
      if ctx.connectors.pg and broker_client.is_running(ctx.connectors.pg, asg.name) then
        log("node_control", "START " .. asg.name .. ": already running, re-register")
        register_assignment(asg)
        -- Record so watchdog's boot grace window applies to already-running
        -- containers too. Without this, after a dcs.lua restart the
        -- watchdog will probe containers it didn't just spawn with zero
        -- grace and fire container_hung on any slow-to-respond service.
        lrts[asg.name] = now
      else
        local ok, res = launch_assignment(asg, listening)
        if ok then
          log("node_control", string.format("START %s -> %s", asg.name, res))
          -- CRITICAL: stamp last_restart_ts so watchdog's boot_grace
          -- window applies to first-time spawns. Was only set by the
          -- reconcile + watchdog respawn paths; first boot got zero
          -- grace and watchdog fired container_hung within seconds.
          lrts[asg.name] = now
        else
          log_container_failure(asg.name, res)
        end
      end
    end
  end

  ----------------------------------------------------------------------
  -- Runtime supervision: reconcile (cold-respawn missing containers) +
  -- watchdog (HTTP probe running containers; docker-restart unresponsive
  -- ones). Both run as parallel columns under node_control monitor_st
  -- with a leading asm_wait_time (settle-then-check). Per-container
  -- state lives in node_control_globals so it survives column resets.
  ----------------------------------------------------------------------

  local RECONCILE_COOLDOWN_S = 30     -- don't respawn same container within
  local WATCHDOG_PROBE_TIMEOUT_S = 2  -- curl --connect-timeout + --max-time
  local WATCHDOG_FAIL_THRESHOLD  = 3  -- consecutive strikes before kick
  local WATCHDOG_BOOT_GRACE_S    = 30 -- post-restart probe hold-off

  -- Skip a container if its maintenance lease is active (either per-
  -- container or CPU-wide). Reads APPLY_MAINTENANCE_TRANSITIONS's
  -- cached map; falls back to "not in maintenance" if the first
  -- maintenance tick hasn't populated it yet.
  local function is_in_maintenance(asg_name)
    local m = ctx.node_control_globals.maintenance_state or {}
    return m[asg_name] == true
  end

  -- Reconcile helper -- calls launch_assignment with a fresh listening
  -- snapshot. Separate from the one used by START to avoid stale
  -- entries after containers have come and gone.
  local function respawn_and_log(asg, cause)
    local listening = listening_ports()
    local ok, res = launch_assignment(asg, listening)
    if ok then
      log("node_control", string.format(
        "%s %s -> broker run %s", cause, asg.name, res))
      if ctx.connectors.pg then
        local xpath = exc_path(
          (cause == "WATCHDOG") and "container_hung" or "container_respawned")
        kb_exc.raise(ctx.connectors.pg, xpath, {
          error       = string.format("%s respawn %s", cause, asg.name),
          source_path = asg.name,
        })
      end
    else
      log_container_failure(asg.name,
        string.lower(cause) .. " respawn failed: " .. tostring(res))
    end
    return ok
  end

  -- RECONCILE_ASSIGNED_CONTAINERS: fires once per monitor-column tick
  -- (after the column's asm_wait_time). For each assignment not under
  -- maintenance, check docker.is_running; respawn if absent. Quiet when
  -- everything is healthy -- only logs on actual state change.
  R.RECONCILE_ASSIGNED_CONTAINERS = function(_h, _n)
    local assignments = ctx.node_control_globals.assignments or {}
    if #assignments == 0 then return end
    ctx.node_control_globals.last_restart_ts =
      ctx.node_control_globals.last_restart_ts or {}
    local lrts = ctx.node_control_globals.last_restart_ts
    local now = os.time()
    local t0  = ptime.now_sec()

    -- Read running set from the broker cache (pg-mirrored); skip the
    -- `docker ps` shell-out that previously starved the chain-tree
    -- walker on WSL2. Falls back to docker.list_running only if the
    -- broker cache is unavailable.
    local running
    if ctx.connectors.pg then
      local rok = broker_client.refresh(ctx.connectors.pg)
      if rok then
        running = {}
        for _, asg in ipairs(assignments) do
          if broker_client.is_running(ctx.connectors.pg, asg.name) then
            running[asg.name] = true
          end
        end
      end
    end
    if not running then
      running = docker.list_running()
    end

    for _, asg in ipairs(assignments) do
      if not is_in_maintenance(asg.name) then
        if running[asg.name] then
          -- Healthy: nothing to do, stay quiet.
        else
          local last = lrts[asg.name] or 0
          if (now - last) < RECONCILE_COOLDOWN_S then
            -- Give the previous respawn a chance to settle before
            -- trying again; avoids tight restart loops when a container
            -- keeps crashing on boot.
          else
            log("node_control", string.format(
              "RECONCILE %s missing -- respawning", asg.name))
            if respawn_and_log(asg, "RECONCILE") then
              lrts[asg.name] = now
            end
          end
        end
      end
    end

    -- Publish pass duration. Includes docker.is_running syscalls across
    -- every assignment, so useful for catching docker-socket slowness.
    if ctx.connectors.pg then
      local dur_ms = (ptime.now_sec() - t0) * 1000.0
      pcall(kb_log.push_sample, ctx.connectors.pg,
        string.format("system.site.%s.cpu.%s.KB_LOG.reconcile_check_ms",
                      ctx.cfg.site, ctx.cfg.cpu_id),
        dur_ms)
    end
  end

  -- HTTP probe a container's primary external port. Returns true on
  -- any 2xx/3xx/4xx (the server is talking); false on connect failure
  -- or timeout. Uses curl because it's the one tool every linux host
  -- we care about already has, and its exit code is deterministic.
  -- Parallel HTTP probes. Builds a single shell pipeline that launches
  -- one curl per (name, port) in background and `wait`s. Total wall-time
  -- ≈ max(individual curl), not sum. Used to be serial: N × 50ms
  -- (healthy) to N × 3s (hung) per watchdog pass. Parallel cuts that to
  -- one curl's cost regardless of N. Output format: "<name> <code>\n"
  -- per probe. code=0 means curl couldn't connect (exit non-zero);
  -- anything else is the HTTP status.
  local function probe_many(targets)
    if not targets or #targets == 0 then return {} end
    local parts = {}
    for _, t in ipairs(targets) do
      parts[#parts + 1] = string.format(
        "(printf '%%s ' %q; " ..
        "curl -s -o /dev/null -w '%%{http_code}' " ..
        "--connect-timeout %d --max-time %d " ..
        "http://127.0.0.1:%d/ 2>/dev/null; echo) &",
        t.name,
        WATCHDOG_PROBE_TIMEOUT_S, WATCHDOG_PROBE_TIMEOUT_S + 1, t.port)
    end
    parts[#parts + 1] = "wait"
    local p = io.popen(table.concat(parts, "\n"))
    if not p then return {} end
    local out = p:read("*a") or ""
    p:close()
    local results = {}
    for line in out:gmatch("[^\r\n]+") do
      local name, code = line:match("^(%S+)%s+(%d+)$")
      if name and code then
        results[name] = (tonumber(code) or 0) > 0
      end
    end
    return results
  end

  -- Kept for legacy / single-probe callers.
  local function probe_container(port)
    local cmd = string.format(
      "curl -s -o /dev/null -w '%%{http_code}' " ..
      "--connect-timeout %d --max-time %d " ..
      "http://127.0.0.1:%d/ 2>/dev/null",
      WATCHDOG_PROBE_TIMEOUT_S, WATCHDOG_PROBE_TIMEOUT_S + 1, port)
    local p = io.popen(cmd)
    if not p then return false end
    local code_s = p:read("*a") or ""
    p:close()
    local code = tonumber((code_s:gsub("%s+", "")))
    return code and code > 0
  end

  -- WATCHDOG_CHECK_ASSIGNED_CONTAINERS (Phase 1c migration):
  --
  -- Was: parallel curl HTTP probes against 127.0.0.1:<port> per
  -- assignment, 3-strike kick → docker stop + respawn.
  --
  -- Problem (observed 2026-04-25): on Docker Desktop / WSL2, vpnkit
  -- port-forwarder hiccups produce coordinated probe failures across
  -- all containers simultaneously. Three consecutive vpnkit hiccups
  -- across the 3-strike window then fire docker stop+run for every
  -- "hung" assignment, which (a) destroys healthy containers, and
  -- (b) blocks the chain-tree walker for 15+s on the docker mutations
  -- → starves the heartbeat publisher → ERR_MONITOR_TRIP detonates infra.
  -- Documented end-to-end in observability/continue.md.
  --
  -- Now: rely on broker container state. The broker watches docker via
  -- the SDK and reports state (running/exited/restarting) plus health
  -- (when a HEALTHCHECK is defined). If state ≠ running, watchdog
  -- restarts. If state == running and health == "unhealthy", watchdog
  -- restarts. Otherwise, leave the container alone.
  --
  -- Loss of capability vs. HTTP probes: containers without a Docker
  -- HEALTHCHECK and that are "running but stuck" (the RPC/HTTP server
  -- inside the container is wedged) won't be detected. Phase 4 of the
  -- broker plan adds container-internal-IP probes inside the broker
  -- itself (bypassing vpnkit) to restore that capability.
  R.WATCHDOG_CHECK_ASSIGNED_CONTAINERS = function(_h, _n)
    local assignments = ctx.node_control_globals.assignments or {}
    if #assignments == 0 then return end
    ctx.node_control_globals.watchdog_state =
      ctx.node_control_globals.watchdog_state or {}
    ctx.node_control_globals.last_restart_ts =
      ctx.node_control_globals.last_restart_ts or {}
    local wstate = ctx.node_control_globals.watchdog_state
    local lrts   = ctx.node_control_globals.last_restart_ts
    local now    = os.time()
    local t0     = ptime.now_sec()

    local conn = ctx.connectors.pg
    if not conn then return end  -- no broker access; nothing safe to do

    local rok, rerr = broker_client.refresh(conn)
    if not rok then
      log("node_control", "WATCHDOG broker refresh: " .. tostring(rerr))
      return
    end

    -- Reconcile already handles "completely missing" containers; this
    -- watchdog pass focuses on "broker reports unhealthy/exited/dead
    -- while the assignment expects it running."
    for _, asg in ipairs(assignments) do
      local last_restart = lrts[asg.name] or 0
      if (now - last_restart) < WATCHDOG_BOOT_GRACE_S then
        -- in boot grace; skip
      elseif is_in_maintenance(asg.name) then
        -- under maintenance; skip
      else
        local ci = broker_client.get_container(conn, asg.name)
        local st = wstate[asg.name] or { fail_count = 0 }
        wstate[asg.name] = st

        local fault_reason = nil
        if not ci then
          -- Reconcile owns this case; don't double-act.
        elseif ci.state == "running" and ci.health == "unhealthy" then
          fault_reason = "broker reports health=unhealthy"
        elseif ci.state == "exited" or ci.state == "dead" then
          fault_reason = "broker reports state=" .. tostring(ci.state)
        end

        if fault_reason then
          st.fail_count = st.fail_count + 1
          log("node_control", string.format(
            "WATCHDOG %s %s fail=%d/%d",
            asg.name, fault_reason, st.fail_count, WATCHDOG_FAIL_THRESHOLD))
          if st.fail_count >= WATCHDOG_FAIL_THRESHOLD then
            log("node_control", string.format(
              "WATCHDOG %s hung -- restarting (%s)", asg.name, fault_reason))
            -- Best-effort SIGTERM grace before the launch_assignment path
            -- force-removes and re-creates. broker_client.stop is idempotent
            -- on already-stopped, so it's safe even when watchdog tripped on
            -- state=exited rather than a still-running hang.
            broker_client.stop(asg.name, 10)
            if respawn_and_log(asg, "WATCHDOG") then
              lrts[asg.name] = now
            end
            st.fail_count = 0
          end
        else
          if st.fail_count > 0 then
            log("node_control", string.format(
              "WATCHDOG %s recovered (was fail=%d)", asg.name, st.fail_count))
          end
          pcall(kb_exc.clear, conn, exc_path("container_hung"))
          st.fail_count = 0
        end
      end
    end

    local dur_ms = (ptime.now_sec() - t0) * 1000.0
    pcall(kb_log.push_sample, conn,
      string.format("system.site.%s.cpu.%s.KB_LOG.watchdog_probe_ms",
                    ctx.cfg.site, ctx.cfg.cpu_id),
      dur_ms)
  end

  -- STOP_ASSIGNED_CONTAINERS: for each assignment, broker stop + rm +
  -- deregister. Mirrors the prior docker.stop semantics (SIGTERM + grace
  -- + rm -f). broker_client.stop returns ok-idempotent on already-stopped
  -- and broker_client.rm with force=true is idempotent on 404, so this
  -- pass is safe to re-run on an already-empty assignment set.
  R.STOP_ASSIGNED_CONTAINERS = function(_h, _n)
    local assignments = ctx.node_control_globals.assignments or {}
    for _, asg in ipairs(assignments) do
      broker_client.stop(asg.name, 10)
      broker_client.rm(asg.name, true)
      log("node_control", "STOP " .. asg.name)
      deregister_assignment(asg.name)
    end
  end

  -- APPLY_MAINTENANCE_TRANSITIONS (Phase 7b / X7): poll every
  -- assignment's maintenance_until status field and act only on
  -- transitions:
  --   false -> true : operator just put it in maintenance.
  --                   docker.stop + deregister.
  --   true  -> false: lease expired or operator hit Start Now.
  --                   docker run from spec + register.
  --
  -- Transition-based (not state-based) so the handler never fights
  -- with crashes/manual docker actions. The process-local prev map
  -- resets on DCS restart -- after restart every container looks
  -- "just entered" its current pg-declared state, which is safe
  -- (re-stop is idempotent; non-maintenance containers show no
  -- transition).
  R.APPLY_MAINTENANCE_TRANSITIONS = function(_h, _n)
    if not ctx.connectors.pg then return end
    local assignments = ctx.node_control_globals.assignments or {}
    if #assignments == 0 then return end
    ctx.node_control_globals.maintenance_state =
      ctx.node_control_globals.maintenance_state or {}
    local prev = ctx.node_control_globals.maintenance_state
    local now = os.time()
    local build_specs = ctx.system_control_globals.build_specs or {}

    -- Read maintenance_until per assignment. Handfuls of containers
    -- per CPU, so per-row is fine; batches can come later if the
    -- container count ever explodes.
    local function read_m_until(container_name)
      local path = string.format(
        "system.site.%s.cpu.%s.container.%s.KB_STATUS_FIELD.maintenance_until",
        ctx.cfg.site, ctx.cfg.cpu_id, container_name)
      local sth, perr = ctx.connectors.pg:prepare(string.format(
        "SELECT COALESCE((data->>'value')::bigint, 0) AS m_until " ..
        "FROM knowledge_base_status WHERE path = '%s'::ltree",
        path:gsub("'", "''")))
      if not sth then return 0 end
      local ok, eerr = sth:execute()
      if not ok then sth:close(); return 0 end
      local row = sth:fetch(true)
      sth:close()
      return row and tonumber(row.m_until) or 0
    end

    -- CPU-wide maintenance lease (X4). When active, every assignment
    -- is treated as if individually in maintenance even if its own
    -- flag is 0. One query per tick.
    local function read_cpu_m_until()
      local path = string.format(
        "system.site.%s.cpu.%s.KB_STATUS_FIELD.cpu_maintenance_until",
        ctx.cfg.site, ctx.cfg.cpu_id)
      local sth, perr = ctx.connectors.pg:prepare(string.format(
        "SELECT COALESCE((data->>'value')::bigint, 0) AS m_until " ..
        "FROM knowledge_base_status WHERE path = '%s'::ltree",
        path:gsub("'", "''")))
      if not sth then return 0 end
      local ok, eerr = sth:execute()
      if not ok then sth:close(); return 0 end
      local row = sth:fetch(true)
      sth:close()
      return row and tonumber(row.m_until) or 0
    end
    local cpu_m_until     = read_cpu_m_until()
    local cpu_in_maint    = cpu_m_until > now

    for _, asg in ipairs(assignments) do
      local m_until         = read_m_until(asg.name)
      -- CPU-wide lease wins -- a paused CPU puts every container in
      -- maintenance regardless of the container's own flag.
      local in_maintenance  = m_until > now or cpu_in_maint
      local was_in          = prev[asg.name] == true
      if in_maintenance and not was_in then
        log("node_control", string.format(
          "MAINTENANCE enter %s until epoch=%d", asg.name, m_until))
        broker_client.stop(asg.name, 10)
        broker_client.rm(asg.name, true)
        deregister_assignment(asg.name)
      elseif was_in and not in_maintenance then
        local spec = build_specs[asg.definition]
        if not spec then
          log_container_failure(asg.name,
            "maintenance end: build.spec missing for " ..
            tostring(asg.definition))
        else
          local merged = {}
          for k, v in pairs(spec) do merged[k] = v end
          merged.ports = (asg.service and asg.service.ports) or {}
          local host_pg = ctx.cfg.pg_host
          if host_pg == "localhost" or host_pg == "127.0.0.1" then
            host_pg = "host.docker.internal"
          end
          local extra_env = {
            CONTAINER_NAME = asg.name,
            APP_SITE       = ctx.cfg.site,
            APP_CPU_ID     = ctx.cfg.cpu_id,
            PG_HOST        = host_pg,
            PG_PORT        = tostring(ctx.cfg.pg_port),
            PG_DB          = ctx.cfg.pg_db,
            PG_USER        = ctx.cfg.pg_user,
            PG_PASSWORD    = os.getenv("PG_PASSWORD")
                             or os.getenv("POSTGRES_PASSWORD") or "",
          }
          local run_spec, sa_err = spec_adapter.build_run_spec(asg.name, merged, extra_env)
          if not run_spec then
            log_container_failure(asg.name,
              "maintenance restart spec error: " .. tostring(sa_err))
          else
            broker_client.rm(asg.name, true)
            local rok, rres = broker_client.run(run_spec)
            if rok then
              local id_short = tostring((type(rres) == "table" and rres.id) or ""):sub(1, 12)
              log("node_control", string.format(
                "MAINTENANCE exit %s -> broker run %s",
                asg.name, id_short))
              register_assignment(asg)
            else
              log_container_failure(asg.name,
                "maintenance restart failed: " .. tostring(rres))
            end
          end
        end
      end
      prev[asg.name] = in_maintenance
    end
  end

  R.LOG_SYSTEM_READY_TRANSITIONS   = oneshot_stub(log, "LOG_SYSTEM_READY_TRANSITIONS",         "node_control")

  R.WRITE_PROCESS_GLOBALS_NODE_OPERATIONAL_TRUE = function(_h, _n)
    pg.node_control_operational = true
    log("node_control", "node_control_operational = TRUE")
  end
  R.WRITE_PROCESS_GLOBALS_NODE_HEARTBEAT = function(_h, _n)
    pg.node_control_heartbeat_ts = os.time()
    -- Also publish to the per-CPU bit_mask_table heartbeat row so
    -- cross-process readers (admin UI, master-side aggregators) can
    -- see this CPU's liveness. Master's system_control already does
    -- this via PUBLISH_SYSTEM_HEARTBEAT, but slaves never run
    -- system_control, so without this publish slaves would show
    -- bit_mask_table.heartbeat = 0 forever.
    if ctx.connectors.pg then
      local ok, err = bm.write_heartbeat_ts(
        ctx.connectors.pg, ctx.cfg.site, ctx.cfg.cpu_id)
      if not ok then
        log("node_control",
            "heartbeat write FAILED: " .. tostring(err))
      end
    end
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

    -- Fan out the 6 declared host KB_LOGs (cpu_logs.lua subsystem). Each
    -- ring gets a single scalar per sample; the jsonb blob above stays
    -- the authoritative raw record. Rings are declared at 1/60 Hz to
    -- match the SAMPLE_HOST cadence. Net rates need an elapsed-time
    -- divisor -- take it from the wall-clock delta since last push.
    if ctx.connectors.pg and mem then
      local conn   = ctx.connectors.pg
      local now_ts = os.time()
      local elapsed_s = mon.prev_host_push_ts
                        and (now_ts - mon.prev_host_push_ts) or 0
      mon.prev_host_push_ts = now_ts

      local mem_used_mb = math.floor((mem.mem_total_kb - mem.mem_available_kb) / 1024)
      local mem_free_mb = math.floor(mem.mem_available_kb / 1024)

      -- Aggregate rx/tx bytes across all ifaces, convert delta to kbps.
      local rx_bytes_total, tx_bytes_total = 0, 0
      for _, d in pairs(net_delta) do
        rx_bytes_total = rx_bytes_total + (d.rx_bytes or 0)
        tx_bytes_total = tx_bytes_total + (d.tx_bytes or 0)
      end
      local rx_kbps = (elapsed_s > 0)
                      and math.floor((rx_bytes_total * 8) / (1000 * elapsed_s))
                      or 0
      local tx_kbps = (elapsed_s > 0)
                      and math.floor((tx_bytes_total * 8) / (1000 * elapsed_s))
                      or 0

      -- Root filesystem disk_used_pct (first DISK_PROBES entry).
      local root_used_pct = (disk_free["/"] and disk_free["/"].used_pct) or 0

      local cpu_log_root = string.format(
        "system.site.%s.cpu.%s.KB_LOG", ctx.cfg.site, ctx.cfg.cpu_id)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".host_cpu_pct",      cpu_pct.cpu_pct)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".host_mem_used_mb",  mem_used_mb)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".host_mem_free_mb",  mem_free_mb)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".net_rx_kbps",       rx_kbps)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".net_tx_kbps",       tx_kbps)
      pcall(kb_log.push_sample, conn, cpu_log_root .. ".disk_used_pct",     root_used_pct)
    end
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

  -- Fallback container sampling via `docker stats`. Works on every host
  -- (Docker Desktop WSL2, Mac, native Linux) because it goes through the
  -- docker daemon instead of /proc/<pid>/cgroup, which isn't visible from
  -- the host process in Docker Desktop/WSL2.
  --
  -- Cost: one shell-out per tick, regardless of container count (docker
  -- stats takes ~1 second for the CPU-delta regardless of N). Fans the
  -- 5 per-metric rings declared in container_logs.lua subsystem.
  --
  -- docker stats returns HumanReadable strings like "5.2MiB", "1.3%",
  -- "12.3MB / 456KB"; we parse them into scalars. Net + block IO are
  -- cumulative, not rates -- the subsystem declares these as rates,
  -- so we compute a delta / wall-clock-elapsed divisor here.
  local function parse_human_bytes(s)
    if not s then return 0 end
    local n, u = s:match("^([%d%.]+)%s*([kmgtKMGT]?[iI]?[bB])")
    n = tonumber(n) or 0
    u = (u or ""):lower()
    if     u:sub(1,1) == "k" then n = n * 1024
    elseif u:sub(1,1) == "m" then n = n * 1024 * 1024
    elseif u:sub(1,1) == "g" then n = n * 1024 * 1024 * 1024
    elseif u:sub(1,1) == "t" then n = n * 1024 * 1024 * 1024 * 1024
    end
    return n
  end

  local function sample_containers_via_docker_stats()
    -- Phase 1c: read pre-computed per-container stats from the broker
    -- (pg-mirrored). Replaces the prior `docker stats` shell-out which
    -- could block the chain-tree walker for 1-3 seconds and starve the
    -- node_control heartbeat publisher. Broker has already done the
    -- delta-rate computation, so we just emit samples.
    local conn = ctx.connectors.pg
    if not conn then return end
    local site   = ctx.cfg.site
    local cpu_id = ctx.cfg.cpu_id

    local stats, err = broker_client.get_stats(conn)
    if not stats then
      log("node_monitor", "SAMPLE_CONTAINERS broker stats unavailable: " .. tostring(err))
      return
    end
    local fresh, ferr = broker_client.stats_fresh()
    if not fresh then
      log("node_monitor", "SAMPLE_CONTAINERS broker stats stale: " .. tostring(ferr))
      return
    end

    for name, s in pairs(stats) do
      local cpu_pct         = tonumber(s.cpu_pct)        or 0
      local mem_mb          = math.floor(tonumber(s.mem_rss_mb) or 0)
      local disk_read_kbps  = math.floor(tonumber(s.disk_read_kbps)  or 0)
      local disk_write_kbps = math.floor(tonumber(s.disk_write_kbps) or 0)

      _emit_sample("container", {
        name                = name,
        cpu_pct             = cpu_pct,
        mem_rss_mb          = mem_mb,
        disk_read_kbps      = disk_read_kbps,
        disk_write_kbps     = disk_write_kbps,
        source              = "broker",
      })

      local root = string.format(
        "system.site.%s.cpu.%s.container.%s.KB_LOG",
        site, cpu_id, name)
      pcall(kb_log.push_sample, conn, root .. ".container_cpu_pct",        cpu_pct)
      pcall(kb_log.push_sample, conn, root .. ".container_mem_rss_mb",     mem_mb)
      pcall(kb_log.push_sample, conn, root .. ".container_disk_read_kbps", disk_read_kbps)
      pcall(kb_log.push_sample, conn, root .. ".container_disk_write_kbps",disk_write_kbps)
    end
  end

  R.SAMPLE_CONTAINERS = function(_h, _n)
    -- If cgroup-based sampling is disabled (Docker Desktop / WSL2 case),
    -- fall back to `docker stats` which works across all platforms.
    if mon.container_sampling_disabled then
      sample_containers_via_docker_stats()
      return
    end

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
