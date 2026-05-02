-- =============================================================================
-- ctx_builder.lua -- build the slim ctx that gets passed to each kb_build.
--
-- Per Phase B design (project_phase_b_design.md), apps' kb_build(ctx)
-- functions receive a SLIM ctx with no anchor string baked in -- the kb
-- handle has scope already pushed to the container's anchor by the
-- driver. Apps call kb-relative methods (with_header, add_status_field,
-- etc.) and the DSL writes under the anchor automatically.
--
-- The ctx fields are:
--   instance_id   string   the per-deployment container name (e.g. "irrigation_01")
--   app_class     string   the class (e.g. "irrigation_controller"); matches container_spec.class
--   cpu           string   the cpu_id this container is assigned to (placement.cpu)
--   role          string   "active" | "passive" | etc. (placement.role)
--   spec          table    the validated container_spec table (read-only handle)
--   kb            table    scoped writer; scope already at app_containers.<instance_id>
--   read_kb       table    unscoped reader for cross-app discovery during build
--
-- The driver guarantees `kb` has scope correctly set (and torn down) around
-- each kb_build invocation. Apps MUST NOT call kb:select_kb() or
-- kb:leave_header_node() or any DSL method that mutates the path stack
-- below the entry depth -- doing so corrupts the driver's scope tracking.
-- The driver verifies post-call that the path stack returned to its
-- entry depth and aborts the build with a clear error if it didn't.
-- =============================================================================

local M = {}

--- Build a ctx for one container's kb_build invocation.
--- @param placement table  one row from the placement table:
---                          { instance_id, app_class, cpu, role, spec }
--- @param kb table          the scoped writer (driver pushes scope before call)
--- @param read_kb table     unscoped reader for cross-container discovery
function M.build(placement, kb, read_kb)
  assert(type(placement) == "table", "placement must be a table")
  assert(type(placement.instance_id) == "string" and placement.instance_id ~= "",
         "placement.instance_id required")
  assert(type(placement.app_class) == "string" and placement.app_class ~= "",
         "placement.app_class required")
  assert(type(placement.cpu) == "string" and placement.cpu ~= "",
         "placement.cpu required")
  assert(type(placement.spec) == "table",
         "placement.spec required (validated container_spec table)")

  return {
    instance_id = placement.instance_id,
    app_class   = placement.app_class,
    cpu         = placement.cpu,
    role        = placement.role or "active",
    spec        = placement.spec,
    kb          = kb,
    read_kb     = read_kb,
  }
end

return M
