local common_tree = require("kb.common_tree")

return {
    name           = "paint_sample",
    index          = 7,
    packet_ctype   = "cmd_paint_sample_t",
    packet_type_id = 7,

    json_schema = {
        { name = "arm_target",   type = "float",  required = true },
        { name = "arm_speed",    type = "float",  default = 60 },
        { name = "arm_return",   type = "float",  default = 0 },
        { name = "hold_time",    type = "float",  default = 500 },
    },

    mapping = {},

    bitmask = {
        { name = "arm_at_target",    bit = 0 },
        { name = "action_complete",  bit = 1 },
        { name = "arm_fault",        bit = 2 },
    },

    pose_fields = { "delta_arm_angle" },

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
