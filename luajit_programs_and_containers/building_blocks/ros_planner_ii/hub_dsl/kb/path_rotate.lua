local common_tree = require("kb.common_tree")

return {
    name           = "path_rotate",
    index          = 5,
    packet_ctype   = "cmd_rotate_t",
    packet_type_id = 3,

    json_schema = {
        { name = "from_heading", type = "float", required = true },
        { name = "to_heading",   type = "float", required = true },
    },

    mapping = {},

    bitmask = {
        { name = "rotate_complete", bit = 0 },
        { name = "motor_fault",     bit = 1 },
    },

    pose_fields = { "delta_heading" },

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
