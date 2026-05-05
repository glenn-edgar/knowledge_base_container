-- =============================================================================
-- subsystems/boards.lua
--
-- Registers the file_store doc-class for navigation boards under
-- system.<sys>.site.<S>.boards. Upload of actual board files is a
-- separate operator-driven step (see commissioning_software/scripts/
-- upload_board.lua).
--
-- Policy: writer = "commissioning_only". Boards are revisable, but only
-- via the operator/CLI commission path. Runtime code (mission_planner)
-- reads boards through doc_get -> JSON parse, never writes.
--
-- Storage shape (per file_store):
--   - knowledge_base_doc_class row at namespace = system.<sys>.site.<S>.boards
--   - knowledge_base_fs_node row per uploaded board at
--       system.<sys>.site.<S>.boards.<board_name> -> sha256 pointer
--   - knowledge_base_fs_blob row per unique blob (content-addressable;
--     old versions auto-retained until explicit gc).
--
-- Mid-mission revision policy is (1) drain-then-flip for now; mission
-- state captures the hash at start so future (3) replan-in-place can
-- detect drift. Audit-log row per upload is added by upload_board.lua
-- when invoked.
-- =============================================================================

return {

  install_site = function(ctx)
    -- Class namespace = under site root (NOT per-planner). Boards are a
    -- site-level concern: the physical world is shared across every
    -- planner instance. Path discipline: this matches how
    -- infrastructure_registry installs site-wide things.
    local class_ns = string.format("system.%s.site.%s.boards",
        ctx.SYSTEM_NAME, ctx.SITE)

    if type(ctx.kb.add_doc_class) ~= "function" then
      -- Bare Construct_KB (test context); skip silently. Tests that
      -- exercise file_store supply their own class registration.
      return
    end

    ctx.kb:add_doc_class{
      namespace    = class_ns,
      writer       = "commissioning_only",
      content_type = "application/json",
      description  = "Navigation boards for mission planners (site-wide; "
                  .. "revised via upload_board.lua commission CLI)",
    }
  end,

}
