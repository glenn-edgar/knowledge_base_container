-- =============================================================================
-- subsystems/cpu_exceptions.lua
--
-- SYS_EXCEPTION nested-header rows (SCADA-style, ISA-18.2 lifecycle).
-- Emits one full exception per catalog entry via kb:add_exception, which
-- composes the header + 15 state children + signatures jsonb blob.
--
-- system_control exceptions: master CPU only.
-- local_system_monitor + node_control exceptions: every CPU.
--
-- Catalog source: TOPOLOGY.agent_exceptions (edited in catalogs/topology.lua).
-- Priority defaulting: exc.priority in the catalog, else 3 (Medium).
--
-- See project_dcs_task4_design.md for the full locked design.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb
    local AE = ctx.TOPOLOGY.agent_exceptions or {}

    local function emit(catalog)
      for _, exc in ipairs(catalog or {}) do
        kb:add_exception(exc.name, {
          type               = exc.type,
          instance           = exc.instance,
          description        = exc.description,
          priority           = exc.priority or 3,
          response_procedure = exc.response_procedure or "",
        })
      end
    end

    if cpu_id == ctx.MASTER_CPU then emit(AE.system_control) end
    emit(AE.local_system_monitor)
    emit(AE.node_control)
  end,

}
