-- =============================================================================
-- subsystems/site_local.lua
--
-- Placeholder for site-local data, reserved under the '<SITE>' KB name.
-- Currently empty; the KB is created so downstream consumers can
-- attach per-site data without a schema migration.
-- =============================================================================

return {

  install_own_kb = function(ctx)
    local kb = ctx.kb
    kb:add_kb(ctx.SITE, "DCS site-local data (reserved; empty in v1)")
    kb:select_kb(ctx.SITE)
  end,

}
