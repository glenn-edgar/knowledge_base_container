-- =============================================================================
-- subsystems/boards.lua
--
-- Registers a file_store doc-class per planner_namespace for navigation
-- boards. Phase 7 (2026-05-10): boards become per-tenant. Each
-- mission_planner instance has its own catalog under
--   system.<sys>.site.<S>.planner.<ns>.boards
-- so two planners (surface_ops + tunnel_ops) maintain independent
-- board sets even though they share the same physical site.
--
-- Upload of actual board files is a separate operator-driven step
-- (compile_board.lua + upload_board.lua). Both must be invoked with
-- the target planner namespace so the fs_node row lands in the right
-- subtree.
--
-- Policy: writer = "commissioning_only". Boards are revisable, but
-- only via the operator/CLI commission path. Runtime code
-- (mission_planner) reads boards through doc_get -> JSON parse,
-- never writes.
--
-- Storage shape (per file_store):
--   - knowledge_base_doc_class row at namespace =
--       system.<sys>.site.<S>.planner.<ns>.boards
--     (one row per planner instance enumerated in topology)
--   - knowledge_base_fs_node row per uploaded board at
--       system.<sys>.site.<S>.planner.<ns>.boards.<board_name>
--     -> sha256 pointer
--   - knowledge_base_fs_blob row per unique blob (content-addressable;
--     old versions auto-retained until explicit gc).
--
-- Mid-mission revision policy is (1) drain-then-flip for now; mission
-- state captures the hash at start so future (3) replan-in-place can
-- detect drift.
--
-- Cross-tenant board sharing: not supported here. If two planners
-- need the same logical resource (e.g. a shared landmark), it lives
-- under infrastructure.registry.* (Q4 of project_phase7_multitenant_design.md).
-- =============================================================================

return {

  install_site = function(ctx)
    local planners = ctx.PLANNERS or {}
    if #planners == 0 then
      -- No mission_planner instances declared in topology (test fixture
      -- or pre-Phase-7 build) -- silently skip. boards is purely a
      -- per-tenant concept under Phase 7; without tenants there's no
      -- doc_class to register.
      return
    end

    if type(ctx.kb.add_doc_class) ~= "function" then
      -- Bare Construct_KB (test context); skip silently. Tests that
      -- exercise file_store supply their own class registration.
      return
    end

    for _, planner in ipairs(planners) do
      local class_ns = string.format(
        "system.%s.site.%s.planner.%s.boards",
        ctx.SYSTEM_NAME, ctx.SITE, planner.namespace)

      ctx.kb:add_doc_class{
        namespace    = class_ns,
        writer       = "commissioning_only",
        content_type = "application/json",
        description  = string.format(
          "Navigation boards for planner '%s' (per-tenant; revised " ..
          "via compile_board.lua + upload_board.lua commission CLI)",
          planner.namespace),
      }
    end
  end,

}
