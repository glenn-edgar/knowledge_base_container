-- =============================================================================
-- subsystems/cpu_exceptions.lua
--
-- SYS_EXCEPTION schema rows. label = SYS_EXCEPTION (custom), properties
-- = {type, instance, description}, data column null. The runtime status
-- row is created lazily by kb_exception.log_exception on first raise.
--
-- system_control exceptions: master CPU only.
-- local_system_monitor + node_control exceptions: every CPU.
--
-- Catalog source: TOPOLOGY.agent_exceptions (edited in catalogs/topology.lua).
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb
    local AE = ctx.TOPOLOGY.agent_exceptions or {}

    local function emit(catalog)
      for _, exc in ipairs(catalog or {}) do
        kb:add_info_node("SYS_EXCEPTION", exc.name,
          { type        = exc.type,
            instance    = exc.instance,
            description = exc.description },
          {},
          exc.description)
      end
    end

    if cpu_id == ctx.MASTER_CPU then emit(AE.system_control) end
    emit(AE.local_system_monitor)
    emit(AE.node_control)
  end,

}
