-- =============================================================================
-- ndc_paths.lua -- Single source of truth for the DCS KB path shape.
--
-- Every dcs_host module composes ltree paths through these helpers instead
-- of `string.format("system.site.%s....")`. When the path shape changes
-- (Phase B Layer M phase 2 adds a `system.<system_name>` segment in front),
-- only this module and topology / config wiring change -- callers don't.
--
-- Stateless and pure: every helper takes `site` (and `cpu_id` / etc) as
-- args. No module-private cfg today. When the system_name segment lands,
-- helpers will read it from a one-time `M.configure{system_name=...}` set
-- during dcs.lua bootstrap.
--
-- Shape today: "system.site.<S>.*"
-- Shape after Layer M phase 2: "system.<sys>.site.<S>.*"
-- =============================================================================

local M = {}

---------------------------------------------------------------------------
-- General-purpose composers
---------------------------------------------------------------------------

function M.site_root(site)
  return string.format("system.site.%s", site)
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

return M
