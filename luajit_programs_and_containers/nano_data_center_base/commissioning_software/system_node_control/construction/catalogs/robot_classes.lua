-- =============================================================================
-- catalogs/robot_classes.lua
--
-- Site-wide robot CLASS catalog. One entry per robot class. Robots are
-- instances of these classes; a robot's KB row carries class = "<name>"
-- (and an optional capabilities_extra list for one-off variations).
--
-- Per-entry shape:
--   description   string  human-readable docs
--   capabilities  list    canonical action_ids this class can perform
--                         (must each exist in catalogs/actions.lua)
--
-- Empty fixture is valid -- if a site has no robots yet, kb_build
-- still runs and emits zero class rows.
-- =============================================================================

return {
  surface_hauler_v2 = {
    description  = "Long-range surface hauler with recharge + dock support",
    capabilities = { "recharge", "dock_in", "dock_out" },
  },
}
