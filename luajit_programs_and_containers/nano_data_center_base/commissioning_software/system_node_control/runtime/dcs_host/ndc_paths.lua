-- =============================================================================
-- ndc_paths.lua -- Single source of truth for the DCS KB path shape.
--
-- Every dcs_host module composes ltree paths through these helpers instead
-- of `string.format("system.<sys>.site.%s....")`. When the path shape
-- changes, only this module and topology / config wiring change -- callers
-- don't.
--
-- Stateful: a one-time `M.configure{system_name=...}` MUST be called at
-- bootstrap before any path emission. site is still passed per-call (a
-- system contains many sites; processes that span multiple sites would
-- otherwise need a per-site configure shuffle).
--
-- Shape: "system.<system_name>.site.<S>.*"
-- =============================================================================

local M = {}

local cfg = {
  system_name = nil,
}

---------------------------------------------------------------------------
-- one-time configure (call at bootstrap before any path emission)
---------------------------------------------------------------------------

function M.configure(opts)
  opts = opts or {}
  if not opts.system_name or opts.system_name == "" then
    error("ndc_paths.configure: system_name required")
  end
  if cfg.system_name and cfg.system_name ~= opts.system_name then
    error(string.format(
      "ndc_paths.configure: already configured with system_name=%q, " ..
      "refusing to reconfigure to %q",
      cfg.system_name, opts.system_name))
  end
  cfg.system_name = opts.system_name
end

function M.system_name()
  return cfg.system_name
end

local function require_configured()
  if not cfg.system_name then
    error("ndc_paths: not configured (call ndc_paths.configure{system_name=...} at bootstrap)")
  end
end

---------------------------------------------------------------------------
-- General-purpose composers
---------------------------------------------------------------------------

function M.site_root(site)
  require_configured()
  return string.format("system.%s.site.%s", cfg.system_name, site)
end

function M.site_path(site, suffix)
  if suffix == nil or suffix == "" then return M.site_root(site) end
  return M.site_root(site) .. "." .. suffix
end

function M.cpu_root(site, cpu_id)
  return M.site_path(site, "cpu." .. cpu_id)
end

function M.cpu_path(site, cpu_id, suffix)
  if suffix == nil or suffix == "" then return M.cpu_root(site, cpu_id) end
  return M.cpu_root(site, cpu_id) .. "." .. suffix
end

function M.container_root(site, cpu_id, container_name)
  return M.cpu_path(site, cpu_id, "container." .. container_name)
end

function M.container_path(site, cpu_id, container_name, suffix)
  local base = M.container_root(site, cpu_id, container_name)
  if suffix == nil or suffix == "" then return base end
  return base .. "." .. suffix
end

---------------------------------------------------------------------------
-- Common specialized paths (fixed class markers)
---------------------------------------------------------------------------

function M.heartbeat_path(site, cpu_id)
  return M.cpu_path(site, cpu_id, "KB_BIT_MASK.heartbeat")
end

function M.ready_bits_path(site)
  return M.site_path(site, "KB_BIT_MASK.ready_bits")
end

function M.cpu_exception_path(site, cpu_id, name)
  return M.cpu_path(site, cpu_id, "SYS_EXCEPTION." .. name)
end

function M.site_status_field_path(site, name)
  return M.site_path(site, "KB_STATUS_FIELD." .. name)
end

function M.cpu_status_field_path(site, cpu_id, name)
  return M.cpu_path(site, cpu_id, "KB_STATUS_FIELD." .. name)
end

function M.container_status_field_path(site, cpu_id, container_name, name)
  return M.container_path(site, cpu_id, container_name, "KB_STATUS_FIELD." .. name)
end

function M.cpu_log_path(site, cpu_id)
  return M.cpu_path(site, cpu_id, "KB_LOG")
end

function M.container_log_path(site, cpu_id, container_name)
  return M.container_path(site, cpu_id, container_name, "KB_LOG")
end

function M.container_registry_path(site, cpu_id, name)
  return M.cpu_path(site, cpu_id, "CONTAINER_REGISTRY." .. name)
end

function M.broker_snapshot_path(site)
  return M.site_path(site, "docker_broker.containers.KB_STATUS_FIELD.snapshot")
end

function M.broker_heartbeat_path(site)
  return M.site_path(site, "docker_broker.heartbeat.KB_STATUS_FIELD.last")
end

function M.broker_stats_path(site)
  return M.site_path(site, "docker_broker.containers.KB_STATUS_FIELD.stats")
end

---------------------------------------------------------------------------
-- App container paths (Phase B Layer A)
-- Anchor: system.<sys>.site.<S>.app_containers.<container_name>.*
-- Sub-paths under each anchor:
--   spec.manifest.*  -- manifest data, written by apps-builder at commission
--                       (with_header("spec","manifest",...) in kb_build.lua)
--   runtime.<name>.* -- runtime state, written by container at startup +
--                       each tick (heartbeat, host, ui_port, ...)
--
-- The two-segment push (spec/manifest, runtime/<state-name>) is forced by
-- the construct_kb DSL's add_header_node which always pushes link+name
-- pairs. Treat "spec" + "runtime" as namespace class markers and the
-- second segment as a name within that namespace.
---------------------------------------------------------------------------

function M.app_root(site, container_name)
  return M.site_path(site, "app_containers." .. container_name)
end

function M.app_path(site, container_name, suffix)
  local base = M.app_root(site, container_name)
  if suffix == nil or suffix == "" then return base end
  return base .. "." .. suffix
end

--- Path to a status-field row inside the manifest namespace.
-- Produces: app_containers.<c>.spec.manifest.KB_STATUS_FIELD.<name>
function M.app_manifest_status_path(site, container_name, name)
  return M.app_path(site, container_name,
    "spec.manifest.KB_STATUS_FIELD." .. name)
end

--- Path to a jsonb-field row inside the manifest namespace.
-- Produces: app_containers.<c>.spec.manifest.KB_JSONB_FIELD.<key>
function M.app_manifest_jsonb_path(site, container_name, key)
  return M.app_path(site, container_name,
    "spec.manifest.KB_JSONB_FIELD." .. key)
end

--- Path to a status-field row inside a runtime sub-namespace.
-- Produces: app_containers.<c>.runtime.<state_name>.KB_STATUS_FIELD.<name>
function M.app_runtime_status_path(site, container_name, state_name, name)
  return M.app_path(site, container_name,
    "runtime." .. state_name .. ".KB_STATUS_FIELD." .. name)
end

return M
