-- =============================================================================
-- subsystems/infrastructure_registry.lua
--
-- Pre-allocates the runtime-addressing schema for every infra def that
-- declares a service_contract in catalogs/definitions.lua. Path shape:
--
--   system.<sys>.site.<S>.infrastructure.<service_type>.KB_STATUS_FIELD.{
--     host, port, protocol, healthy, last_seen
--   }
--
-- Empty defaults at construction time. system_control's INFRA_PUBLISH
-- chain-tree state writes runtime values after each broker poll. App
-- containers consume via /opt/apps/lib/infra_discovery.lua.
--
-- This subsystem is the build-time half of the runtime-addressing
-- mechanism described in continue.md (Phase B Layer A-pre).
-- =============================================================================

return {

  install_site = function(ctx)
    local kb   = ctx.kb
    local DEFS = ctx.DEFINITIONS

    -- Collect service_contracts from infra defs. Validates uniqueness of
    -- service_type so two defs can't claim the same abstract name.
    local seen = {}
    local contracts = {}
    for def_name, def in pairs(DEFS) do
      local sc = def.service_contract
      if sc then
        assert(def.kind == "infrastructure",
          string.format("def %q has service_contract but kind != infrastructure",
            def_name))
        assert(type(sc.service_type) == "string" and sc.service_type ~= "",
          string.format("def %q service_contract.service_type required", def_name))
        assert(type(sc.port) == "number",
          string.format("def %q service_contract.port required (number)", def_name))
        if seen[sc.service_type] then
          error(string.format(
            "service_contract.service_type=%q claimed by both %q and %q",
            sc.service_type, seen[sc.service_type], def_name))
        end
        seen[sc.service_type] = def_name
        contracts[#contracts + 1] = {
          def_name     = def_name,
          service_type = sc.service_type,
          port         = sc.port,
          protocol     = sc.protocol or "tcp",
        }
      end
    end

    table.sort(contracts, function(a, b) return a.service_type < b.service_type end)

    kb:with_header("infrastructure", "registry",
      { kind = "registry" }, {},
      "Site-wide infra-service registry (system_control populates at runtime)",
      function()
        for _, c in ipairs(contracts) do
          kb:with_header("service", c.service_type,
            { def_name = c.def_name, contract_port = c.port,
              contract_protocol = c.protocol },
            {},
            string.format("Runtime addressing for %s (def=%s)",
              c.service_type, c.def_name),
            function()
              kb:add_status_field("host", {},
                "container hostname (resolves via docker-net DNS)",
                { value = "" })
              kb:add_status_field("port", {},
                "service port (defaults to contract port; system_control may override)",
                { value = c.port })
              kb:add_status_field("protocol", {},
                "wire protocol (tcp|udp|http|...)",
                { value = c.protocol })
              kb:add_status_field("healthy", {},
                "true when system_control's last poll observed the container running",
                { value = false })
              kb:add_status_field("last_seen", {},
                "epoch seconds when system_control last refreshed this entry",
                { value = 0 })
            end)
        end
      end)
  end,

}
