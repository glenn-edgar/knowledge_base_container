-- capabilities.lua -- Lunar rover virtual node capabilities.
-- Matches moonbase.alpha.surface_ops robot_class.lunar_rover.infra.shared

return {
    init_check      = true,
    path_spline     = true,
    path_line       = true,
    path_wall       = true,
    path_rotate     = true,
    deliver_part    = true,
    paint_sample    = true,
    load_shipping   = true,
    pass_gate       = true,
    inspection_scan = true,
    recharge        = true,
    idle            = true,
}
