-- path_spline.lua — Code-only plugin. Data comes from KB VN definitions.
local common_tree = require("kb.common_tree")

return {
    name         = "path_spline",
    index        = 2,
    packet_ctype = "cmd_path_spline_t",

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
