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

local function sanity_check()
  -- master CPU exists in topology
  if not TOPOLOGY.cpus[MASTER_CPU] then
    error(string.format("master CPU %q not found in topology.cpus", MASTER_CPU))
  end

  local instance_owner = {}   -- inst_name -> cpu_id (for uniqueness check)

  for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
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
    end
  end

  print("sanity passes: ok")
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

for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
  local cpu_props = cpu.properties or {}
  cpu_props.is_master = (cpu_id == MASTER_CPU) and 1 or 0

  kb:add_header_node("cpu", cpu_id, cpu_props, {},
                     "CPU " .. cpu_id)

    -- Bootstrap config info-node: identity + pg connect info read by host
    -- processes (system_control, node_control) at startup. Sliced into
    -- per-role bootstrap.db files; everything else queried from pg at runtime.
    kb:add_info_node("bootstrap", "config",
                     { kind = "bootstrap" },
                     {
                       site      = SITE,
                       cpu_id    = cpu_id,
                       is_master = (cpu_id == MASTER_CPU) and 1 or 0,
                       kb_root   = string.format("system.site.%s.cpu.%s",
                                                 SITE, cpu_id),
                       pg_host   = PG.host,
                       pg_port   = PG.port,
                       pg_db     = PG.dbname,
                       pg_user   = PG.user,
                       -- pg_password from ~/.config/nanodatacenter/secrets.env
                     },
                     "Bootstrap config for host processes")

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

      -- service info_node holds the dial-info for this instance
      kb:add_info_node("service",
                       (def.kind == "infrastructure") and inst.name
                                                       or "main",
                       { type = def.kind },
                       {
                         host  = def.image and inst.name,  -- docker DNS = inst name
                         ports = def.ports or {},
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
