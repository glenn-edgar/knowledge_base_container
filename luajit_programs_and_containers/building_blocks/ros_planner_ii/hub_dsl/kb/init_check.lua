local common_tree = require("kb.common_tree")

return {
    name           = "init_check",
    index          = 1,
    packet_ctype   = "cmd_init_check_t",
    packet_type_id = 1,

    json_schema = {},
    mapping = {},

    bitmask = {
        { name = "battery_ok",  bit = 0 },
        { name = "motors_ok",   bit = 1 },
        { name = "sensors_ok",  bit = 2 },
        { name = "comms_ok",    bit = 3 },
    },

    pose_fields = {},

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
