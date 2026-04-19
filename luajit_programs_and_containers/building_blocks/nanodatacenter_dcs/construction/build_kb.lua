#!/usr/bin/env luajit
-- =============================================================================
-- build_kb.lua -- Orchestrator. Reads catalogs + drives subsystem modules.
--
-- Responsibilities (thin; content lives in subsystems/*.lua):
--   1. Load catalogs/definitions.lua + catalogs/topology.lua.
--   2. Sanity-check inputs before touching pg.
--   3. Open the master KB connection via Construct_Data_Tables facade.
--   4. Iterate a SUBSYSTEMS list and call these hooks in order:
--        install_system_kb(ctx)                    -- at system KB root
--        install_site(ctx)                         -- inside system.site.<SITE>
--        install_cpu(ctx, cpu_id, cpu_cfg)         -- inside cpu.<cpu_id>
--        install_container(ctx, cpu_id, cpu_cfg, inst, def)
--                                                  -- inside container.<inst>
--        install_own_kb(ctx)                       -- after site walk
--   5. Verify + disconnect.
--
-- **To add a subsystem: drop a file under subsystems/, add ONE line to
-- SUBSYSTEMS below.** No other edit required here.
--
-- Usage:
--   POSTGRES_PASSWORD=... ./build_kb.sh
-- =============================================================================

local CDT = require("construct_data_tables")

---------------------------------------------------------------------------
-- Locate sibling lua files (catalogs/, subsystems/) regardless of cwd
---------------------------------------------------------------------------

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  return src:match("(.*/)") or "./"
end

local DIR = script_dir()

local function load_lua(rel)
  local path = DIR .. rel
  local chunk, err = loadfile(path)
  if not chunk then error("failed to load " .. path .. ": " .. tostring(err)) end
  return chunk()
end

---------------------------------------------------------------------------
-- Inputs
---------------------------------------------------------------------------

local DEFINITIONS = load_lua("catalogs/definitions.lua")
local TOPOLOGY    = load_lua("catalogs/topology.lua")

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

-- Extract the external port number from a legacy `ports` entry.
-- Legacy shapes: { host = X, cont = Y } | { X, Y } | X.
local function legacy_external(p)
  if type(p) == "table" then return p.host or p[1] end
  if type(p) == "number" then return p end
  return nil
end

local function sanity_check()
  if not TOPOLOGY.cpus[MASTER_CPU] then
    error(string.format("master CPU %q not found in topology.cpus", MASTER_CPU))
  end

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

  local instance_owner = {}
  local bit_owner      = {}
  local max_bit        = -1

  for cpu_id, cpu in pairs(TOPOLOGY.cpus) do
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

    local cpu_ext_port = {}
    for _, inst in ipairs(cpu.instances or {}) do
      if not DEFINITIONS[inst.def] then
        error(string.format(
          "cpu %q instance %q references undeclared definition %q",
          cpu_id, inst.name, inst.def))
      end
      if instance_owner[inst.name] then
        error(string.format(
          "instance name %q appears on %q and %q (must be globally unique)",
          inst.name, instance_owner[inst.name], cpu_id))
      end
      instance_owner[inst.name] = cpu_id
      local def = DEFINITIONS[inst.def]
      if def.kind == "host_process" then
        error(string.format(
          "cpu %q instance %q has kind=host_process; these are not " ..
          "container placements -- remove from topology",
          cpu_id, inst.name))
      end

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

      elseif def.ports then
        if inst.ports ~= nil then
          error(string.format(
            "cpu %q instance %q declares `ports` but def %q uses legacy " ..
            "port mapping (def.ports)",
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

  for i = 0, max_bit do
    if not bit_owner[i] then
      error(string.format(
        "bit_index gap: %d unused, but bit_indices go up to %d (%q)",
        i, max_bit, bit_owner[max_bit]))
    end
  end

  print(string.format("sanity passes: ok (cpu count = %d)", max_bit + 1))
end

-- Resolve an instance's port list. For port_spec defs, returns records
-- with slot/internal/external/protocol/purpose/description. For legacy
-- defs, returns def.ports verbatim. Used by container_runtime.
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

local CPU_COUNT = 0
for _ in pairs(TOPOLOGY.cpus) do CPU_COUNT = CPU_COUNT + 1 end

---------------------------------------------------------------------------
-- Connect to master pg
---------------------------------------------------------------------------

local kb = CDT.new(PG.host, PG.port, PG.dbname, PG.user, PASSWORD,
                   "knowledge_base", false)

---------------------------------------------------------------------------
-- Subsystem registry
---------------------------------------------------------------------------

-- To add a subsystem:
--   1. Drop a file under subsystems/<name>.lua exposing any of:
--        install_system_kb(ctx), install_site(ctx),
--        install_cpu(ctx, cpu_id, cpu_cfg),
--        install_container(ctx, cpu_id, cpu_cfg, inst, def),
--        install_own_kb(ctx)
--   2. Add ONE line below.
--
-- Order matters: earlier subsystems emit before later ones within each
-- hook phase. The sequence below mirrors the original monolith's
-- ordering so row insertion order is stable across the refactor.

local SUBSYSTEMS = {
  "container_definitions",
  "site_scalars",
  "readiness_sync",
  "cpu_bootstrap",
  "cpu_heartbeat",
  "cpu_maintenance",
  "cpu_exceptions",
  "node_monitor",
  "container_runtime",
  "domain_registry",
  "site_local",
}

local modules = {}
for _, name in ipairs(SUBSYSTEMS) do
  modules[name] = load_lua("subsystems/" .. name .. ".lua")
end

local ctx = {
  kb                     = kb,
  SITE                   = SITE,
  MASTER_CPU             = MASTER_CPU,
  TOPOLOGY               = TOPOLOGY,
  DEFINITIONS            = DEFINITIONS,
  CPU_COUNT              = CPU_COUNT,
  resolve_instance_ports = resolve_instance_ports,
}

local function fire(hook, ...)
  local args = { ... }
  for _, name in ipairs(SUBSYSTEMS) do
    local fn = modules[name][hook]
    if fn then fn(ctx, (table.unpack or unpack)(args)) end
  end
end

---------------------------------------------------------------------------
-- Build the "system" KB
---------------------------------------------------------------------------

kb:add_kb("system", "Enterprise hardware/container topology")
kb:select_kb("system")

-- System-KB-root hooks (emit outside of site/.).
fire("install_system_kb")

-- Enter site.<SITE> and walk the topology.
kb:with_header("site", SITE, { site_type = "dcs" }, {}, "DCS site root",
function()
  fire("install_site")

  for cpu_id, cpu_cfg in pairs(TOPOLOGY.cpus) do
    local cpu_props = cpu_cfg.properties or {}
    cpu_props.is_master = (cpu_id == MASTER_CPU) and 1 or 0
    cpu_props.bit_index = cpu_cfg.bit_index

    kb:with_header("cpu", cpu_id, cpu_props, {}, "CPU " .. cpu_id,
    function()
      fire("install_cpu", cpu_id, cpu_cfg)

      for _, inst in ipairs(cpu_cfg.instances or {}) do
        local def     = DEFINITIONS[inst.def]
        local managed = (def.kind == "infrastructure") and "manual"
                         or (inst.def == "kv_bridge" and "system_control"
                             or (def.kind == "control" and "self" or "node_control"))

        kb:with_header("container", inst.name,
          { definition = inst.def, kind = def.kind, managed_by = managed },
          { instance_params = inst.params or {} },
          "Container instance " .. inst.name,
        function()
          fire("install_container", cpu_id, cpu_cfg, inst, def)
        end)
      end
    end)
  end
end)

-- Subsystems that own their own KB (e.g. "subsystems", "<SITE>").
fire("install_own_kb")

---------------------------------------------------------------------------
-- Verify + disconnect
---------------------------------------------------------------------------

local ok, err = pcall(function() kb:check_installation() end)
if not ok then
  print("check_installation failed: " .. tostring(err))
  kb:disconnect()
  os.exit(1)
end

local def_count = 0
for _ in pairs(DEFINITIONS) do def_count = def_count + 1 end
local inst_count = 0
for _, cpu in pairs(TOPOLOGY.cpus) do
  inst_count = inst_count + #(cpu.instances or {})
end

print("=== DCS KB built ===")
print(string.format("  site      : %s", SITE))
print(string.format("  master    : %s", MASTER_CPU))
print(string.format("  pg        : %s:%s/%s", PG.host, PG.port, PG.dbname))
print(string.format("  defs      : %d", def_count))
print(string.format("  cpus      : %d", CPU_COUNT))
print(string.format("  instances : %d", inst_count))
print(string.format("  subsystems: %d", #SUBSYSTEMS))

kb:disconnect()
