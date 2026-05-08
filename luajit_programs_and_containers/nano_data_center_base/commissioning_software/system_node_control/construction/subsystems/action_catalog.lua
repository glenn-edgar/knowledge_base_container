-- =============================================================================
-- subsystems/action_catalog.lua
--
-- Emits the site-wide virtual-action catalog under
--   system.<sys>.site.<S>.actions.catalog.action.<action_id>
-- One row per entry in catalogs/actions.lua.
--
-- This subsystem is the canonical source of truth for action_ids. Three
-- downstream sites cross-check against it at kb_build time:
--   - infrastructure_registry: active-node robot_virtual_action dict keys
--   - robot_classes:           class capabilities + per-row capabilities_extra
--   - DSL compiler (Phase 4):  path-tree activate{} leaves
-- The DSL compiler also validates parameter_schema shape; this subsystem
-- only emits + sanity-checks the catalog itself.
--
-- Sanity-checks performed here:
--   - action_id is a non-empty string
--   - parameter_schema is a table (may be empty)
--   - parameter_schema field types are one of {"string","int","float","bool"}
--   - description is a non-empty string
-- A typo or malformed entry fails the build before any reference site
-- runs, giving cleaner error messages downstream.
-- =============================================================================

local VALID_TYPES = {
  string = true, int = true, float = true, bool = true,
}

local function sanity_check(actions)
  for action_id, spec in pairs(actions) do
    if type(action_id) ~= "string" or action_id == "" then
      error(string.format(
        "action_catalog: action_id key must be non-empty string (got %s)",
        type(action_id)))
    end
    if type(spec) ~= "table" then
      error(string.format(
        "action_catalog: %q value must be table (got %s)",
        action_id, type(spec)))
    end
    if type(spec.description) ~= "string" or spec.description == "" then
      error(string.format(
        "action_catalog: %q.description required (non-empty string)",
        action_id))
    end
    local schema = spec.parameter_schema
    if schema == nil then
      error(string.format(
        "action_catalog: %q.parameter_schema required (table; may be empty)",
        action_id))
    end
    if type(schema) ~= "table" then
      error(string.format(
        "action_catalog: %q.parameter_schema must be table (got %s)",
        action_id, type(schema)))
    end
    for field, wire_type in pairs(schema) do
      if type(field) ~= "string" or field == "" then
        error(string.format(
          "action_catalog: %q.parameter_schema field name must be non-empty string",
          action_id))
      end
      if not VALID_TYPES[wire_type] then
        error(string.format(
          "action_catalog: %q.parameter_schema.%s type %q not in {string,int,float,bool}",
          action_id, field, tostring(wire_type)))
      end
    end
  end
end

return {

  install_site = function(ctx)
    local actions = ctx.ACTIONS
    if type(actions) ~= "table" then
      error("action_catalog: ctx.ACTIONS missing (build_kb.lua should load catalogs/actions.lua)")
    end

    sanity_check(actions)

    -- Deterministic emission order so KB row insertion is stable.
    local ids = {}
    for k, _ in pairs(actions) do ids[#ids + 1] = k end
    table.sort(ids)

    ctx.kb:with_header("actions", "catalog",
      { kind = "catalog" }, {},
      "Site-wide virtual-action catalog (canonical action_id source of truth)",
      function()
        for _, action_id in ipairs(ids) do
          local spec = actions[action_id]
          ctx.kb:add_info_node("action", action_id, {},
            {
              description      = spec.description,
              parameter_schema = spec.parameter_schema,
            },
            "Action " .. action_id)
        end
      end)
  end,

}
