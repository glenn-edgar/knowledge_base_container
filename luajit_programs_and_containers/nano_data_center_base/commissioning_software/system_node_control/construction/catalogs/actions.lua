-- =============================================================================
-- catalogs/actions.lua
--
-- Canonical site-wide action_id catalog. One entry per virtual action a
-- planner can request via cmd_activate_action_t. Build-time, this is the
-- source of truth: kb_build cross-checks every reference (active-node
-- robot_virtual_action dict, robot class capabilities, DSL-compiled path
-- tree leaves) against the keys here.
--
-- Add a new action_id by adding one entry. Removing an action_id will
-- fail kb_build if any reference site still names it -- that is by
-- design.
--
-- Per-entry shape:
--   description      string  human-readable docs
--   parameter_schema table   field name -> wire type ("string" / "int" /
--                            "float" / "bool"). The DSL compiler uses
--                            this to validate `activate{ params = {...} }`
--                            shape at compile time. Empty table = no
--                            params required.
-- =============================================================================

return {
  recharge = {
    description      = "Charge robot battery to target state-of-charge",
    parameter_schema = { target_soc = "float" },
  },
  dock_in = {
    description      = "Drive into dock cradle and engage mechanical lock",
    parameter_schema = {},
  },
  dock_out = {
    description      = "Disengage cradle and exit dock approach corridor",
    parameter_schema = {},
  },
}
