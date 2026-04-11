-- capabilities.lua -- Lunar rover virtual node capabilities.
-- Matches moonbase.alpha.surface_ops robot_class.lunar_rover.infra.shared

return {
    -- Virtual nodes (path + lifecycle)
    init_check  = true,
    path_spline = true,
    path_line   = true,
    operation   = true,
    idle        = true,

    -- Operation types this robot supports
    operation_types = {
        "base", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge",
    },
}
