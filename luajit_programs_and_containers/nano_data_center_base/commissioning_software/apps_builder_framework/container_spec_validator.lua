-- =============================================================================
-- container_spec_validator.lua -- validate the per-app container_spec contract.
--
-- Each ported app ships TWO files alongside its kb_build.lua:
--   container_spec.lua   -- declarative shape: image, ports, kind, env, volumes
--   kb_build.lua         -- function(ctx) that emits scope-relative KB rows
--
-- This validator enforces the container_spec shape BEFORE the apps-builder
-- driver invokes kb_build, so a malformed spec fails fast with a clear
-- error instead of half-writing rows to PG. Mirrors the validation
-- subsystems/container_definitions.lua does on def-side, but at the
-- per-instance contract layer.
--
-- container_spec table shape (Q0 contract, locked 2026-05-01):
--   {
--     class       = "<app_class>",        -- string; matches a definitions.lua key
--     image       = "<docker image:tag>", -- string; what `docker run` pulls
--     kind        = "application",         -- "application" | "infrastructure"
--                                          -- (apps-builder ports are always "application")
--     port_spec   = { <slot> = { internal = N, protocol = "tcp"|"udp",
--                                purpose  = "ui"|"service",
--                                description = "..." }, ... },
--     env_required = { "VAR1", "VAR2", ... },  -- vars node_control must inject
--     volumes      = { "/host/path:/container/path[:ro]", ... },  -- docker -v args
--   }
--
-- Returns ok, err where err is human-readable on failure.
-- =============================================================================

local M = {}

local VALID_KIND     = { application = true, infrastructure = true }
local VALID_PROTOCOL = { tcp = true, udp = true }
local VALID_PURPOSE  = { ui = true, service = true }

local function err(fmt, ...)
  return false, string.format(fmt, ...)
end

local function check_string(t, key, ctx)
  local v = t[key]
  if type(v) ~= "string" or v == "" then
    return err("%s.%s must be a non-empty string (got %s)", ctx, key, type(v))
  end
  return true
end

--- Validate a container_spec table.
--- @param spec table  the spec returned by the app's container_spec.lua
--- @param app_name string  used in error messages
--- @return boolean, string?  ok, err_message
function M.validate(spec, app_name)
  app_name = app_name or "<unknown>"
  local ctx = string.format("container_spec[%q]", app_name)

  if type(spec) ~= "table" then
    return err("%s must return a table (got %s)", ctx, type(spec))
  end

  local ok, e
  ok, e = check_string(spec, "class", ctx)  if not ok then return ok, e end
  ok, e = check_string(spec, "image", ctx)  if not ok then return ok, e end
  ok, e = check_string(spec, "kind",  ctx)  if not ok then return ok, e end

  if not VALID_KIND[spec.kind] then
    return err("%s.kind must be one of {application, infrastructure} (got %q)",
               ctx, tostring(spec.kind))
  end

  -- port_spec: optional, but if present must be well-formed.
  if spec.port_spec ~= nil then
    if type(spec.port_spec) ~= "table" then
      return err("%s.port_spec must be a table (got %s)", ctx, type(spec.port_spec))
    end
    local seen_internal = {}
    for slot, ps in pairs(spec.port_spec) do
      if type(slot) ~= "string" or slot == "" then
        return err("%s.port_spec key must be a non-empty slot name", ctx)
      end
      if type(ps) ~= "table" then
        return err("%s.port_spec.%s must be a table", ctx, slot)
      end
      if type(ps.internal) ~= "number" then
        return err("%s.port_spec.%s.internal must be a number", ctx, slot)
      end
      if seen_internal[ps.internal] then
        return err("%s.port_spec: slots %q and %q both claim internal port %d",
                   ctx, seen_internal[ps.internal], slot, ps.internal)
      end
      seen_internal[ps.internal] = slot
      if ps.protocol ~= nil and not VALID_PROTOCOL[ps.protocol] then
        return err("%s.port_spec.%s.protocol must be tcp|udp (got %q)",
                   ctx, slot, tostring(ps.protocol))
      end
      if ps.purpose ~= nil and not VALID_PURPOSE[ps.purpose] then
        return err("%s.port_spec.%s.purpose must be ui|service (got %q)",
                   ctx, slot, tostring(ps.purpose))
      end
    end
  end

  -- env_required + volumes: optional arrays of strings.
  if spec.env_required ~= nil then
    if type(spec.env_required) ~= "table" then
      return err("%s.env_required must be an array of strings", ctx)
    end
    for i, v in ipairs(spec.env_required) do
      if type(v) ~= "string" or v == "" then
        return err("%s.env_required[%d] must be a non-empty string", ctx, i)
      end
    end
  end
  if spec.volumes ~= nil then
    if type(spec.volumes) ~= "table" then
      return err("%s.volumes must be an array of strings", ctx)
    end
    for i, v in ipairs(spec.volumes) do
      if type(v) ~= "string" or v == "" then
        return err("%s.volumes[%d] must be a non-empty string", ctx, i)
      end
    end
  end

  return true
end

return M
