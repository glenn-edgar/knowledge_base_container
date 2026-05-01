-- =============================================================================
-- subsystems/domain_registry.lua
--
-- The 'subsystems' KB: logical domain name -> site + container mapping.
-- Currently holds one entry (dcs -> system_control); grows as new control-plane
-- domains get added.
-- =============================================================================

return {

  install_own_kb = function(ctx)
    local kb = ctx.kb
    kb:add_kb("subsystems", "Logical domain -> site/container registry")
    kb:select_kb("subsystems")
    kb:add_info_node("domain", "dcs", {},
      { site = ctx.SITE, container = "system_control" },
      "DCS control plane domain")
  end,

}
