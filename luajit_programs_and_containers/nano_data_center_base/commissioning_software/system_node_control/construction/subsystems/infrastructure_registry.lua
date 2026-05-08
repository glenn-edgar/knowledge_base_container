-- =============================================================================
-- subsystems/infrastructure_registry.lua
--
-- Two responsibilities:
--
-- 1. Pre-allocates runtime-addressing schema for every infra def that
--    declares a service_contract in catalogs/definitions.lua. Path shape:
--
--      system.<sys>.site.<S>.infrastructure.<service_type>.KB_STATUS_FIELD.{
--        host, port, protocol, healthy, last_seen
--      }
--
--    Empty defaults at construction time. system_control's INFRA_PUBLISH
--    chain-tree state writes runtime values after each broker poll. App
--    containers consume via /opt/apps/lib/infra_discovery.lua.
--
-- 2. (Phase B.2 Planner Phase 1 C2) Emits the active-node definition
--    catalog for every infra def carrying a `robot_virtual_action` table.
--    Path shape:
--
--      system.<sys>.site.<S>.infrastructure.registry.active_node_def.<def>.
--        action.<action_id> -> { cmd_topic, status_topic, ... }
--
--    Per-DEF (def_name keyed), one entry per action_id. Per-INSTANCE
--    binding (dock_id, broker address) lands in Phase 6 when actual
--    dock instances are placed in topology. kb_build cross-validates
--    every action_id key against ctx.ACTIONS (catalogs/actions.lua) so
--    typos fail the build with a specific error.
-- =============================================================================

return {

  install_site = function(ctx)
    local kb      = ctx.kb
    local DEFS    = ctx.DEFINITIONS
    local ACTIONS = ctx.ACTIONS or {}

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

    -- Collect active-node defs. Any infra def carrying a
    -- `robot_virtual_action` table contributes a per-action vocabulary
    -- entry. Cross-validates action_id keys against ctx.ACTIONS
    -- (catalogs/actions.lua) so typos fail the build with a specific
    -- error before any reference site (DSL compiler, runtime planner)
    -- has a chance to mis-route.
    local active_node_defs = {}
    for def_name, def in pairs(DEFS) do
      local rva = def.robot_virtual_action
      if rva ~= nil then
        assert(def.kind == "infrastructure",
          string.format(
            "def %q has robot_virtual_action but kind != infrastructure",
            def_name))
        assert(type(rva) == "table",
          string.format(
            "def %q robot_virtual_action must be table (got %s)",
            def_name, type(rva)))
        local action_list = {}
        for action_id, entry in pairs(rva) do
          if type(action_id) ~= "string" or action_id == "" then
            error(string.format(
              "def %q robot_virtual_action key must be non-empty string",
              def_name))
          end
          if type(entry) ~= "table" then
            error(string.format(
              "def %q robot_virtual_action.%s must be table (got %s)",
              def_name, action_id, type(entry)))
          end
          if not ACTIONS[action_id] then
            error(string.format(
              "def %q robot_virtual_action.%s references unknown action_id " ..
              "(not in catalogs/actions.lua)",
              def_name, action_id))
          end
          action_list[#action_list + 1] = { id = action_id, entry = entry }
        end
        if #action_list > 0 then
          table.sort(action_list, function(a, b) return a.id < b.id end)
          active_node_defs[#active_node_defs + 1] = {
            def_name = def_name, actions = action_list,
          }
        end
      end
    end

    table.sort(active_node_defs,
      function(a, b) return a.def_name < b.def_name end)

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

        for _, ad in ipairs(active_node_defs) do
          kb:with_header("active_node_def", ad.def_name,
            { kind = "active_node" }, {},
            string.format("Active-node virtual-action vocabulary for def=%s",
              ad.def_name),
            function()
              for _, a in ipairs(ad.actions) do
                kb:add_info_node("action", a.id, {}, a.entry,
                  string.format("Action %q template for def=%s",
                    a.id, ad.def_name))
              end
            end)
        end
      end)
  end,

}
