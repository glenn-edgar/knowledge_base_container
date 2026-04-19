#!/usr/bin/env luajit
-- =============================================================================
-- construct_dcs_kb.lua -- Build the master Postgres KB for the DCS.
--
-- Reads:
--   definitions.lua  -- catalog of container_definition entries
--   topology.lua     -- per-CPU placement of container instances
--   POSTGRES_PASSWORD (env)
--
-- Writes (into one Postgres database, three KB namespaces):
--   "system"      : enterprise topology + per-container runtime state schema
--                   container_definition.<def>.*
--                   site.<SITE>.cpu.<cpu>.container.<inst>.*
--   "subsystems"  : domain registry (logical name -> site + container)
--   "<SITE>"      : site-local data (empty in v1)
--
-- Sanity passes (fail loud, terminate before any DB writes):
--   1. every instance.def resolves in definitions
--   2. instance names are globally unique across CPUs
--   3. exactly one system_control instance exists, on the master CPU
--   4. master CPU is declared in cpus
--
-- Usage:
--   POSTGRES_PASSWORD=... ./build_kb.sh
-- =============================================================================

local CDT = require("construct_data_tables")

---------------------------------------------------------------------------
-- Locate sibling lua files (definitions.lua, topology.lua) regardless of cwd
---------------------------------------------------------------------------

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  return src:match("(.*/)") or "./"
end

local DIR = script_dir()

local function load_lua(filename)
  local path = DIR .. filename
  local chunk, err = loadfile(path)
  if not chunk then error("failed to load " .. path .. ": " .. tostring(err)) end
  return chunk()
end

---------------------------------------------------------------------------
-- Inputs
---------------------------------------------------------------------------

local DEFINITIONS = load_lua("definitions.lua")
local TOPOLOGY    = load_lua("topology.lua")

local SITE       = TOPOLOGY.site   or error("topology.site missing")
local MASTER_CPU = TOPOLOGY.master or error("topology.master missing")
local PG         = TOPOLOGY.pg_connect or error("topology.pg_connect missing")

local PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not PASSWORD then
  error("POSTGRES_PASSWORD environment variable not set " ..
        "(source ~/.config/nanodatacenter/secrets.env)")
end

---------------------------------------------------------------------------
-- Sanity passes
---------------------------------------------------------------------------

-- Helper: extract the external port number from a legacy `ports` entry.
-- Legacy shapes: { host = X, cont = Y } | { X, Y } | X.
local function legacy_external(p)
  if type(p) == "table" then return p.host or p[1] end
  if type(p) == "number" then return p end
  return nil
end

local function sanity_check()
  -- master CPU exists in topology
  if not TOPOLOGY.cpus[MASTER_CPU] then
    error(string.format("master CPU %q not found in topology.cpus", MASTER_CPU))
  end

  -- Def-level: (a) port_spec and legacy `ports` are mutually exclusive;
  -- (b) each port_spec slot must declare a numeric `internal`; (c) within
  -- a single port_spec, `internal` is unique across slots (else the image
  -- can't bind all of them).
  for def_name, def in pairs(DEFINITIONS) do
    if def.port_spec then
      if def.ports then
        error(string.format(
          "definition %q has both legacy `ports` and new `port_spec` -- pick one",
          def_name))
      end
      local seen_internal = {}
      for slot, spec in pairs(def.port_spec) do
        if type(slot) ~= "string" or slot == "" then
          error(string.format(
            "definition %q port_spec key must be a non-empty string slot name",
            def_name))
        end
        if type(spec) ~= "table" or type(spec.internal) ~= "number" then
          error(string.format(
            "definition %q port_spec.%s.internal must be a number",
            def_name, slot))
        end
        if seen_internal[spec.internal] then
          error(string.format(
            "definition %q port_spec: slots %q and %q both claim internal port %d",
            def_name, seen_internal[spec.internal], slot, spec.internal))
        end
        seen_internal[spec.internal] = slot
      end
    end
  end

  local instance_owner = {}   -- inst_name -> cpu_id (for uniqueness check)
  local bit_owner      = {}   -- bit_index -> cpu_id (for bit uniqueness)
  local max_bit        = -1

  for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
    -- bit_index required + range-checked + unique + tracked for contiguity
    if type(cpu.bit_index) ~= "number" then
      error(string.format("cpu %q missing bit_index (integer 0..63)", cpu_id))
    end
    if cpu.bit_index < 0 or cpu.bit_index > 63 then
      error(string.format("cpu %q bit_index=%d out of range 0..63",
                          cpu_id, cpu.bit_index))
    end
    if bit_owner[cpu.bit_index] then
      error(string.format("bit_index=%d claimed by both %q and %q",
                          cpu.bit_index, bit_owner[cpu.bit_index], cpu_id))
    end
    bit_owner[cpu.bit_index] = cpu_id
    if cpu.bit_index > max_bit then max_bit = cpu.bit_index end

    -- Per-CPU external-port ownership tracker. Catches collisions across
    -- both port_spec placements and legacy-ports definitions on the same CPU.
    local cpu_ext_port = {}  -- ext -> { inst = name, slot = slot_or_legacy_idx }

    for _, inst in ipairs(cpu.instances or {}) do
      -- 1. definition must resolve
      if not DEFINITIONS[inst.def] then
        error(string.format(
          "cpu %q instance %q references undeclared definition %q",
          cpu_id, inst.name, inst.def))
      end
      -- 2. instance name globally unique
      if instance_owner[inst.name] then
        error(string.format(
          "instance name %q appears on %q and %q (must be globally unique)",
          inst.name, instance_owner[inst.name], cpu_id))
      end
      instance_owner[inst.name] = cpu_id
      -- 3. container definitions only — host_process kinds don't belong
      -- under cpu.<id>.container (they're not docker targets).
      local def = DEFINITIONS[inst.def]
      if def.kind == "host_process" then
        error(string.format(
          "cpu %q instance %q has kind=host_process; these are not " ..
          "container placements -- remove from topology (DCS host process " ..
          "runs from build_output/<cpu>/start.sh, not docker)",
          cpu_id, inst.name))
      end

      -- 4. port_spec path: every slot must have an instance.ports entry,
      --    and instance.ports must not declare unknown slots. Register
      --    each external into cpu_ext_port; collisions are hard errors.
      if def.port_spec then
        local inst_ports = inst.ports
        if inst_ports ~= nil and type(inst_ports) ~= "table" then
          error(string.format(
            "cpu %q instance %q ports must be a table (got %s)",
            cpu_id, inst.name, type(inst_ports)))
        end
        inst_ports = inst_ports or {}
        for slot, _ in pairs(def.port_spec) do
          local ext = inst_ports[slot]
          if type(ext) ~= "number" then
            error(string.format(
              "cpu %q instance %q (def=%s) missing ports.%s " ..
              "(external port for slot %q)",
              cpu_id, inst.name, inst.def, slot, slot))
          end
          local prior = cpu_ext_port[ext]
          if prior then
            error(string.format(
              "cpu %q external port %d collision: %s.%s and %s.%s",
              cpu_id, ext, prior.inst, prior.slot, inst.name, slot))
          end
          cpu_ext_port[ext] = { inst = inst.name, slot = slot }
        end
        for slot, _ in pairs(inst_ports) do
          if not def.port_spec[slot] then
            error(string.format(
              "cpu %q instance %q ports.%s has no matching slot in def %q port_spec",
              cpu_id, inst.name, slot, inst.def))
          end
        end

      -- 5. legacy ports path: externals come from def.ports. Instance must
      --    NOT declare a ports field (it's only meaningful for port_spec).
      elseif def.ports then
        if inst.ports ~= nil then
          error(string.format(
            "cpu %q instance %q declares `ports` but def %q uses legacy " ..
            "port mapping (def.ports). Upgrade def to port_spec or drop " ..
            "instance.ports.",
            cpu_id, inst.name, inst.def))
        end
        for i, p in ipairs(def.ports) do
          local ext = legacy_external(p)
          if type(ext) ~= "number" then
            error(string.format(
              "cpu %q instance %q def %q legacy ports[%d] has no external number",
              cpu_id, inst.name, inst.def, i))
          end
          local prior = cpu_ext_port[ext]
          if prior then
            error(string.format(
              "cpu %q external port %d collision: %s.%s and %s[legacy:%d]",
              cpu_id, ext, prior.inst, prior.slot, inst.name, i))
          end
          cpu_ext_port[ext] = { inst = inst.name, slot = "legacy:" .. i }
        end
      end
    end
  end

  -- bit indices contiguous 0..max_bit (no gaps)
  for i = 0, max_bit do
    if not bit_owner[i] then
      error(string.format(
        "bit_index gap: %d unused, but bit_indices go up to %d (%q)",
        i, max_bit, bit_owner[max_bit]))
    end
  end

  print(string.format("sanity passes: ok (cpu count = %d)", max_bit + 1))
end

-- Resolve a placement's final port list. For port_spec defs, returns
-- an ordered list of records with both internal AND external, plus the
-- slot/protocol/purpose/description metadata needed by the registry and
-- the gateway. For legacy defs, returns def.ports unchanged (old format)
-- so infrastructure keeps working without topology edits.
local function resolve_instance_ports(def, inst)
  if def.port_spec then
    local out = {}
    for slot, spec in pairs(def.port_spec) do
      table.insert(out, {
        slot        = slot,
        internal    = spec.internal,
        external    = inst.ports[slot],
        protocol    = spec.protocol    or "tcp",
        purpose     = spec.purpose     or "service",
        description = spec.description or "",
      })
    end
    return out
  end
  return def.ports or {}
end

sanity_check()

---------------------------------------------------------------------------
-- Connect
---------------------------------------------------------------------------

local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD,
                   "knowledge_base", false)

---------------------------------------------------------------------------
-- KB "system" -- container_definition.* + site.<S>.cpu.<C>.container.<I>.*
---------------------------------------------------------------------------

kb:add_kb("system", "Enterprise hardware/container topology")
kb:select_kb("system")

-- ---- container_definition.<def_name>.build (one per definition) ----
for def_name, def in pairs(DEFINITIONS) do
  kb:add_header_node("container_definition", def_name,
                     { kind = def.kind, runtime = def.runtime or "docker" },
                     {},
                     "Container definition " .. def_name)
    kb:add_info_node("build", "spec",
                     { kind = def.kind },
                     {
                       runtime        = def.runtime or "docker",
                       image          = def.image,
                       build_ctx      = def.build_ctx,
                       entrypoint     = def.entrypoint     or {},
                       env_defaults   = def.env_defaults   or {},
                       env_required   = def.env_required   or {},
                       default_cfg    = def.default_cfg    or {},
                       ports          = def.ports          or {},
                       port_spec      = def.port_spec      or {},
                       volumes        = def.volumes        or {},
                       labels         = def.labels         or {},
                       restart_policy = def.restart_policy or "no",
                       cli_databases  = def.cli_databases  or {},
                     },
                     "Build + runtime spec")
  kb:leave_header_node("container_definition", def_name)
end

-- ---- site / cpu / container instances ----
kb:add_header_node("site", SITE, { site_type = "dcs" }, {},
                   "DCS site root")

  -- Site-level state (Build 2c data model):
  --   system_ready: pg-only flag derived by master from ready_bits;
  --                 apps poll this status field.
  --   unmonitor_lease_default_s: site-wide default lease length when
  --                              UI toggles a container offline.
  kb:add_status_field("system_ready", {},
                      "site-wide ready gate (1 = all CPUs synced+operational)",
                      { value = 0 })
  kb:add_status_field("unmonitor_lease_default_s", {},
                      "default unmonitor lease length in seconds (operator policy)",
                      { value = 900 })

  -- Gateway poll cadence. Baked-in default = 15s; operator can override by
  -- writing a new {"value":N} to knowledge_base_status at this path. The
  -- dcs_console gateway reads this on every poll tick and uses COALESCE so
  -- an empty runtime row falls back to the schema default below.
  kb:add_status_field("gateway_poll_interval_sec", {},
                      "dcs_console gateway poll cadence in seconds (operator-tunable)",
                      { value = 15 })

  -- Compute total CPU count once — used for ready_bits size +
  -- bootstrap.config.expected_cpu_count.
  local CPU_COUNT = 0
  for _, _ in pairs(TOPOLOGY.cpus) do CPU_COUNT = CPU_COUNT + 1 end

  -- Site-level ready_bits bit_mask: each CPU sets its bit_index when
  -- synced+operational; master compares whole mask to (1 << N) - 1.
  kb:clear_bit_mask_flags()
  for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
    kb:add_bit_mask_flag("CPU_" .. cpu_id .. "_READY",
                         cpu.bit_index,
                         "CPU " .. cpu_id .. " synced+operational")
  end
  kb:create_bit_mask_entry("system", "ready_bits",
                           CPU_COUNT, 0,
                           "site-wide CPU readiness mask")

  -- Site-level cluster_sync_bits: written during the sync_control KB
  -- (pre-operational phase). Each CPU sets its bit once it has verified
  -- all 4 infra services (pg/nats/mqtt/kv_bridge) are reachable. Master
  -- flips cluster_go once this mask hits (1 << N) - 1, with a hard
  -- timeout that fires a slave_never_joined SYS_EXCEPTION.
  kb:clear_bit_mask_flags()
  for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
    kb:add_bit_mask_flag("CPU_" .. cpu_id .. "_SYNCED",
                         cpu.bit_index,
                         "CPU " .. cpu_id .. " verified all 4 infra services reachable")
  end
  kb:create_bit_mask_entry("system", "cluster_sync_bits",
                           CPU_COUNT, 0,
                           "sync-phase mask: per-CPU infra-verified bits")

  -- Site-level cluster_go status field: master flips to 1 once every CPU
  -- has set its cluster_sync bit; slaves poll this before transitioning
  -- from sync_control to node_control. Cleared to 0 on teardown so a
  -- restart re-enters sync cleanly.
  kb:add_status_field("cluster_go", {},
                      "sync-phase gate: 1 = all CPUs synced, handoff to operational",
                      { value = 0 })

for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
  local cpu_props = cpu.properties or {}
  cpu_props.is_master = (cpu_id == MASTER_CPU) and 1 or 0
  cpu_props.bit_index = cpu.bit_index

  kb:add_header_node("cpu", cpu_id, cpu_props, {},
                     "CPU " .. cpu_id)

    -- Bootstrap config info-node: identity + pg connect info read by host
    -- processes (system_control, node_control) at startup. Sliced into
    -- per-role bootstrap.db files; everything else queried from pg at runtime.
    kb:add_info_node("bootstrap", "config",
                     { kind = "bootstrap" },
                     {
                       site               = SITE,
                       cpu_id             = cpu_id,
                       is_master          = (cpu_id == MASTER_CPU) and 1 or 0,
                       kb_root            = string.format("system.site.%s.cpu.%s",
                                                          SITE, cpu_id),
                       pg_host            = PG.host,
                       pg_port            = PG.port,
                       pg_db              = PG.dbname,
                       pg_user            = PG.user,
                       -- Build 2c additions:
                       bit_index          = cpu.bit_index,
                       expected_cpu_count = CPU_COUNT,
                       -- pg_password from ~/.config/nanodatacenter/secrets.env
                     },
                     "Bootstrap config for host processes")

    -- Per-CPU heartbeat: bit_mask_table row where the 64-bit bit_mask
    -- column holds a Unix nanosecond timestamp (repurposed). Each CPU's
    -- local agent writes its own; master reads to detect dead CPUs.
    -- size=1 because we use the full 64-bit value as a timestamp, not bits.
    kb:clear_bit_mask_flags()
    kb:add_bit_mask_flag("LSB", 0, "raw timestamp byte (unused as flag)")
    kb:create_bit_mask_entry("system", "heartbeat", 1, 0,
                             "CPU " .. cpu_id .. " heartbeat (64-bit ns timestamp)")

    -- Build 2c: agent exception schema rows. label = SYS_EXCEPTION (custom);
    -- properties = {type, instance, description}; data column null. The
    -- runtime status row is created lazily by kb_exception.log_exception
    -- on first raise.
    --
    -- system_control exceptions: master CPU only.
    -- local_system_monitor + node_control exceptions: every CPU.
    local AE = TOPOLOGY.agent_exceptions or {}
    local function _emit(catalog)
      for _, exc in ipairs(catalog or {}) do
        kb:add_info_node("SYS_EXCEPTION", exc.name,
                         { type = exc.type,
                           instance = exc.instance,
                           description = exc.description },
                         {},
                         exc.description)
      end
    end
    if cpu_id == MASTER_CPU then _emit(AE.system_control) end
    _emit(AE.local_system_monitor)
    _emit(AE.node_control)

    -- ---- node_monitor sampler namespace -----------------------------
    -- Owned by the node_monitor KB; the chain-tree pushes one stream
    -- row per sample (kind = host/process/container/trend_snapshot)
    -- as JSONB. trend_state holds the last-computed slopes; sampler
    -- updates it from COMPUTE_TRENDS.
    --
    -- 1440 samples = 24h at 60s cadence × N sample kinds. Sized for
    -- ~1 day of raw history; a downsampler can roll older data later.
    kb:add_header_node("monitor", "samples",
                       { kind = "monitor" }, {},
                       "Resource monitor namespace for CPU " .. cpu_id)
      kb:add_stream_field("samples", 1440,
                          "resource sample ring (host/process/container/trend)")
      kb:add_jsonb_field("trend_state", "trend",
                         "last-computed slopes per metric",
                         {})
      kb:add_status_field("sampler_pid", {},
                          "host process pid running the sampler",
                          { value = 0 })
      kb:add_status_field("last_sample_ts", {},
                          "wall-clock seconds of last successful sample",
                          { value = 0 })
      kb:add_status_field("samples_dropped", {},
                          "count of failed sample-write attempts",
                          { value = 0 })
    kb:leave_header_node("monitor", "samples")

  for _, inst in ipairs(cpu.instances or {}) do
    local def       = DEFINITIONS[inst.def]
    local managed   = (def.kind == "infrastructure") and "manual"
                       or (inst.def == "kv_bridge" and "system_control"
                           or (def.kind == "control" and "self" or "node_control"))

    kb:add_header_node("container", inst.name,
                       { definition = inst.def, kind = def.kind,
                         managed_by = managed },
                       { instance_params = inst.params or {} },
                       "Container instance " .. inst.name)

      -- service info_node holds the dial-info for this instance.
      -- `ports` is the resolved join of def.port_spec + inst.ports
      -- (records with slot/internal/external/protocol/purpose/description)
      -- for port_spec defs, or the legacy list for infrastructure.
      -- node_control reads this to issue `docker run -p ext:int` and
      -- (later) to write the container_registry row the gateway consumes.
      kb:add_info_node("service",
                       (def.kind == "infrastructure") and inst.name
                                                       or "main",
                       { type = def.kind },
                       {
                         host  = def.image and inst.name,  -- docker DNS = inst name
                         ports = resolve_instance_ports(def, inst),
                         cfg   = def.default_cfg or {},
                       },
                       "Service endpoint")

      -- ---- per-kind runtime state schema ----
      if def.kind == "infrastructure" then
        kb:add_status_field("health", {}, "broker health", { value = 0 })
        kb:add_stream_field("events", 64, "lifecycle events")

      elseif inst.def == "system_control" then
        -- system-wide flags (durable shadow of NATS KV)
        kb:add_status_field("system_ready", {},
                            "site-wide ready gate", { value = 0 })
        kb:add_stream_field("system_ready_transitions", 64, "ready flips")
        kb:add_status_field("master_heartbeat_ts",    {},
                            "last heartbeat (ms)", { value = 0 })
        kb:add_status_field("master_heartbeat_count", {},
                            "heartbeat counter", { value = 0 })

        -- (bit_mask intentionally not declared in v1; reintroduce when a
        -- chain-tree consumer needs s-expression gating across infra checks)

        -- ops RPC inbox + telemetry + exceptions
        kb:add_rpc_server_field("ops_rpc", 16,
                                "ops commands (teardown/reload/query)")
        kb:add_stream_field("resource_samples", 100, "resource sampler ring")
        kb:add_jsonb_field("welford_state", "welford",
                           "running mean/variance for resources",
                           { mean = 0, m2 = 0, n = 0 })
        kb:add_stream_field("exceptions", 200,
                            "system_control exception ring")

      elseif inst.def == "node_control" then
        kb:add_status_field("operational",      {},
                            "node up & apps healthy", { value = 0 })
        kb:add_status_field("heartbeat_ts",     {},
                            "last node heartbeat (ms)", { value = 0 })
        kb:add_status_field("teardown_request", {},
                            "teardown asked", { value = 0 })
        kb:add_status_field("stopped",          {},
                            "all apps stopped", { value = 0 })

        kb:add_jsonb_field("assignments", "node_assignments",
                           "list of app containers assigned to this CPU",
                           { app_containers = {} })

        kb:add_rpc_server_field("ctrl_rpc", 8,
                                "system_control -> node_control commands")
        kb:add_stream_field("resource_samples", 100,
                            "container resources sampler ring")
        kb:add_jsonb_field("welford_state", "welford",
                           "running mean/var for app resources",
                           { mean = 0, m2 = 0, n = 0 })
        kb:add_stream_field("exceptions", 200,
                            "node_control exception ring")

      elseif def.kind == "application" then
        kb:add_status_field("health",        {}, "app health",
                            { value = 0 })
        kb:add_status_field("started_ts",    {}, "last start (ms)",
                            { value = 0 })
        kb:add_status_field("restart_count", {}, "restart count",
                            { value = 0 })
        -- Operator-initiated maintenance lease (Phase 7b, X7).
        -- 0 = not in maintenance. > 0 = epoch seconds when lease
        -- expires; while now() < this, node_control holds the
        -- container stopped and deregistered. Flipped back to 0 by
        -- operator (start-now) or lazily when the lease expires.
        kb:add_status_field("maintenance_until", {},
                            "operator maintenance lease (epoch seconds; 0 = not in maintenance)",
                            { value = 0 })
        kb:add_stream_field("events", 32, "app lifecycle events")
      end

    kb:leave_header_node("container", inst.name)
  end

  kb:leave_header_node("cpu", cpu_id)
end

kb:leave_header_node("site", SITE)

---------------------------------------------------------------------------
-- KB "subsystems" -- domain registry
---------------------------------------------------------------------------

kb:add_kb("subsystems", "Logical domain -> site/container registry")
kb:select_kb("subsystems")
kb:add_info_node("domain", "dcs", {},
                 { site = SITE, container = "system_control" },
                 "DCS control plane domain")

---------------------------------------------------------------------------
-- KB "<SITE>" -- site-local data (empty in v1)
---------------------------------------------------------------------------

kb:add_kb(SITE, "DCS site-local data (reserved; empty in v1)")
kb:select_kb(SITE)

---------------------------------------------------------------------------
-- Verify and disconnect
---------------------------------------------------------------------------

local ok, err = pcall(function() kb:check_installation() end)
if not ok then
  print("check_installation failed: " .. tostring(err))
  kb:disconnect()
  os.exit(1)
end

print(string.format("=== DCS KB built ==="))
print(string.format("  site      : %s", SITE))
print(string.format("  master    : %s", MASTER_CPU))
print(string.format("  pg        : %s:%s/%s", PG.host, PG.port, PG.dbname))
local cpu_count, inst_count = 0, 0
for _, cpu in pairs(TOPOLOGY.cpus) do
  cpu_count = cpu_count + 1
  inst_count = inst_count + #(cpu.instances or {})
end
local def_count = 0
for _ in pairs(DEFINITIONS) do def_count = def_count + 1 end
print(string.format("  defs      : %d", def_count))
print(string.format("  cpus      : %d", cpu_count))
print(string.format("  instances : %d", inst_count))

kb:disconnect()
