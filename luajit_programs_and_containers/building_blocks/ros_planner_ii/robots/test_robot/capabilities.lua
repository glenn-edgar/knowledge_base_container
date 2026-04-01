-- capabilities.lua -- test_robot supports all virtual node types

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
    idle            = true,
}
