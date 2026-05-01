-- setpoints_catalog.lua -- Hand-coded list of operator-tunable
-- site-level setpoints. Each entry declares its validation envelope
-- (min / max / kind) and the human-readable metadata for the
-- Setpoints view. Also consumed by the action handler so validation
-- is shared between read + write paths.
--
-- Adding a new setpoint:
--   1. Extend construct_dcs_kb.lua with a new kb:add_status_field(...)
--      at site level.
--   2. Add an entry below.
--   3. build_kb.sh + slice_bootstrap.sh + DCS restart to plant the
--      new row in pg.

local M = {}

M.list = {
  {
    name        = "gateway_poll_interval_sec",
    label       = "Gateway poll interval",
    unit        = "seconds",
    kind        = "int",
    min         = 1,
    max         = 300,
    description = "How often the dcs_console gateway re-reads " ..
                  "CONTAINER_REGISTRY to refresh its route table.",
  },
  {
    name        = "unmonitor_lease_default_s",
    label       = "Unmonitor lease default",
    unit        = "seconds",
    kind        = "int",
    min         = 60,
    max         = 86400,
    description = "Default duration when an operator takes a " ..
                  "container offline for maintenance (future X7 action).",
  },
}

-- Quick name -> spec lookup for validation in the action handler.
M.by_name = {}
for _, s in ipairs(M.list) do M.by_name[s.name] = s end

-- Validate a proposed value against the spec for `name`. Returns
-- (ok, normalised_value_or_err).
function M.validate(name, raw_value)
  local spec = M.by_name[name]
  if not spec then return false, "unknown setpoint: " .. tostring(name) end
  if spec.kind == "int" then
    local n = tonumber(raw_value)
    if not n then return false, "value must be an integer" end
    n = math.floor(n)
    if spec.min and n < spec.min then
      return false, string.format("value must be >= %d", spec.min)
    end
    if spec.max and n > spec.max then
      return false, string.format("value must be <= %d", spec.max)
    end
    return true, n
  end
  return false, "unsupported kind: " .. tostring(spec.kind)
end

return M
