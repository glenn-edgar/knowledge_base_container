-- =============================================================================
-- subsystems/robots.lua
--
-- Phase 7 robot-sim layer: emits one KB row per robot under
--   system.<sys>.site.<S>.planner.<ns>.robots.<robot_id>
--
-- Reads ctx.ROBOTS (enumerated in build_kb.lua from topology instances
-- with def="robot_sim"). Each robot's params declare:
--   - robot_id           string, unique identifier
--   - planner_namespace  string, owning tenant (must match a planner
--                         instance's namespace from ctx.PLANNERS)
--   - capabilities       array of action_id strings (each must exist
--                         in ctx.ACTIONS)
--   - container_name     derived from inst.name; the runtime container
--                         that hosts this robot (used by future
--                         status / heartbeat lookups)
--
-- Validation (fails the build with a specific error on violation):
--   - robot_id non-empty + matches [%w_%-%.]+ (lib/submit.lua's chars)
--   - robot_id unique across all robots (no two declarations with
--     same id even across different tenants)
--   - planner_namespace exists in ctx.PLANNERS (no orphan robots)
--   - every capability is a known action_id in ctx.ACTIONS
--
-- Empty ROBOTS list is valid (a site with no robots emits zero rows).
-- The header rows still emit so future planner_ui list_robots queries
-- have a stable namespace to target.
-- =============================================================================

local function sanity_check(robots, planners, actions)
  local seen_ids = {}
  local known_namespaces = {}
  for _, p in ipairs(planners) do known_namespaces[p.namespace] = true end

  for i, r in ipairs(robots) do
    if type(r.robot_id) ~= "string" or r.robot_id == "" then
      error(string.format(
        "robots[%d]: robot_id required (non-empty string)", i))
    end
    if not r.robot_id:match("^[%w_%-%.]+$") then
      error(string.format(
        "robots[%d]: robot_id %q has invalid characters (allowed: alnum / _ / - / .)",
        i, r.robot_id))
    end
    if seen_ids[r.robot_id] then
      error(string.format(
        "robots[%d]: robot_id %q already declared at index %d (must be unique)",
        i, r.robot_id, seen_ids[r.robot_id]))
    end
    seen_ids[r.robot_id] = i

    if not known_namespaces[r.planner_namespace] then
      error(string.format(
        "robots[%d] (id=%q): planner_namespace %q does not match any " ..
        "mission_planner instance in topology (no orphan robots)",
        i, r.robot_id, tostring(r.planner_namespace)))
    end

    if type(r.capabilities) ~= "table" then
      error(string.format(
        "robots[%d] (id=%q): capabilities required (list of action_id strings)",
        i, r.robot_id))
    end
    local cap_seen = {}
    for j, action_id in ipairs(r.capabilities) do
      if type(action_id) ~= "string" or action_id == "" then
        error(string.format(
          "robots[%d] (id=%q): capabilities[%d] must be non-empty string",
          i, r.robot_id, j))
      end
      if cap_seen[action_id] then
        error(string.format(
          "robots[%d] (id=%q): capabilities lists %q more than once",
          i, r.robot_id, action_id))
      end
      cap_seen[action_id] = true
      if not actions[action_id] then
        error(string.format(
          "robots[%d] (id=%q): capabilities[%d]=%q references unknown " ..
          "action_id (not in catalogs/actions.lua)",
          i, r.robot_id, j, action_id))
      end
    end
  end
end

return {

  install_site = function(ctx)
    local robots   = ctx.ROBOTS   or {}
    local planners = ctx.PLANNERS or {}
    local actions  = ctx.ACTIONS  or {}

    -- Empty ROBOTS list -> nothing to validate or emit. Don't even
    -- open the planner.<ns>.robots header (mission_planner instances
    -- exist in the topology without any sim robots — that's fine).
    if #robots == 0 then return end

    sanity_check(robots, planners, actions)

    -- Group by planner_namespace so each tenant subtree gets its own
    -- robots catalog header. Two-tenant case: surface_ops.robots.{...}
    -- and tunnel_ops.robots.{...} land under separate planner subtrees.
    local by_ns = {}
    for _, r in ipairs(robots) do
      by_ns[r.planner_namespace] = by_ns[r.planner_namespace] or {}
      table.insert(by_ns[r.planner_namespace], r)
    end

    local namespaces = {}
    for ns, _ in pairs(by_ns) do namespaces[#namespaces + 1] = ns end
    table.sort(namespaces)

    for _, ns in ipairs(namespaces) do
      local rs = by_ns[ns]
      table.sort(rs, function(a, b) return a.robot_id < b.robot_id end)

      ctx.kb:with_header("planner", ns,
        { kind = "planner_namespace" }, {},
        "Per-tenant data for planner " .. ns,
        function()
          ctx.kb:with_header("robots", "catalog",
            { kind = "catalog" }, {},
            "Robot fleet for planner " .. ns,
            function()
              for _, r in ipairs(rs) do
                ctx.kb:add_info_node("robot", r.robot_id, {},
                  {
                    robot_id       = r.robot_id,
                    container_name = r.container_name,
                    capabilities   = r.capabilities,
                  },
                  string.format("Robot %s (sim container %s)",
                                r.robot_id, r.container_name))
              end
            end)
        end)
    end
  end,

}
