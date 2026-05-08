-- =============================================================================
-- subsystems/robot_classes.lua
--
-- Emits the site-wide robot-class catalog under
--   system.<sys>.site.<S>.robot_classes.class.<class_name>
-- One row per entry in catalogs/robot_classes.lua.
--
-- kb_build cross-validates every action_id in a class's capability
-- list against ctx.ACTIONS (catalogs/actions.lua) so a typo in
-- "recharge" fails the build with a specific error -- not at runtime
-- when a planner tries to dispatch a mission to the dock.
--
-- Per-robot `capabilities_extra` (architectural memo) is validated
-- when per-robot KB rows land in a later phase (robot subsystem +
-- mission_planner instance namespace work). C3 only ships class-level
-- validation.
--
-- Empty class catalog is valid: a site without robots emits zero
-- class rows. The header row still emits so downstream queries don't
-- 404 on the namespace.
-- =============================================================================

local function sanity_check(classes, actions)
  for class_name, spec in pairs(classes) do
    if type(class_name) ~= "string" or class_name == "" then
      error(string.format(
        "robot_classes: class name must be non-empty string (got %s)",
        type(class_name)))
    end
    if type(spec) ~= "table" then
      error(string.format(
        "robot_classes: %q value must be table (got %s)",
        class_name, type(spec)))
    end
    if type(spec.description) ~= "string" or spec.description == "" then
      error(string.format(
        "robot_classes: %q.description required (non-empty string)",
        class_name))
    end
    local caps = spec.capabilities
    if type(caps) ~= "table" then
      error(string.format(
        "robot_classes: %q.capabilities required (list of action_id strings)",
        class_name))
    end
    local seen = {}
    for i, action_id in ipairs(caps) do
      if type(action_id) ~= "string" or action_id == "" then
        error(string.format(
          "robot_classes: %q.capabilities[%d] must be non-empty string",
          class_name, i))
      end
      if seen[action_id] then
        error(string.format(
          "robot_classes: %q.capabilities lists %q more than once",
          class_name, action_id))
      end
      seen[action_id] = true
      if not actions[action_id] then
        error(string.format(
          "robot_classes: %q.capabilities[%d]=%q references unknown action_id " ..
          "(not in catalogs/actions.lua)",
          class_name, i, action_id))
      end
    end
  end
end

return {

  install_site = function(ctx)
    local classes = ctx.ROBOT_CLASSES
    if type(classes) ~= "table" then
      error("robot_classes: ctx.ROBOT_CLASSES missing " ..
            "(build_kb.lua should load catalogs/robot_classes.lua)")
    end
    local actions = ctx.ACTIONS or {}

    sanity_check(classes, actions)

    local names = {}
    for k, _ in pairs(classes) do names[#names + 1] = k end
    table.sort(names)

    ctx.kb:with_header("robot_classes", "catalog",
      { kind = "catalog" }, {},
      "Site-wide robot-class catalog (capability declarations validated against actions catalog)",
      function()
        for _, class_name in ipairs(names) do
          local spec = classes[class_name]
          ctx.kb:add_info_node("class", class_name, {},
            {
              description  = spec.description,
              capabilities = spec.capabilities,
            },
            "Robot class " .. class_name)
        end
      end)
  end,

}
