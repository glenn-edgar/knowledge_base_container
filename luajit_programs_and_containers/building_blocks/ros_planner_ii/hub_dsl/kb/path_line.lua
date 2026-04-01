local common_tree = require("kb.common_tree")

return {
    name           = "path_line",
    index          = 3,
    packet_ctype   = "cmd_path_line_t",
    packet_type_id = 3,

    json_schema = {
        { name = "from_x",    type = "float", default = 0 },
        { name = "from_y",    type = "float", default = 0 },
        { name = "to_x",      type = "float", default = 0 },
        { name = "to_y",      type = "float", default = 0 },
        { name = "speed",     type = "float", default = 100 },
        { name = "distance",  type = "float", default = 0 },
    },

    mapping = {},

    bitmask = {
        { name = "seg_complete",   bit = 0 },
        { name = "obstacle",       bit = 1 },
        { name = "motor_fault",    bit = 2 },
    },

    pose_fields = { "delta_x", "delta_y", "delta_heading" },

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
