local common_tree = require("kb.common_tree")

return {
    name           = "deliver_part",
    index          = 6,
    packet_ctype   = "cmd_arm_t",
    packet_type_id = 4,

    json_schema = {
        { name = "arm_target",   type = "float",  required = true },
        { name = "arm_speed",    type = "float",  default = 80 },
        { name = "arm_return",   type = "float",  default = 0 },
        { name = "payload",      type = "uint8",  default = 1,
          enum = { none = 0, part = 1, container = 2 } },
    },

    mapping = { payload = "payload_type" },

    bitmask = {
        { name = "arm_at_target",    bit = 0 },
        { name = "payload_gripped",  bit = 1 },
        { name = "action_complete",  bit = 2 },
        { name = "arm_fault",        bit = 3 },
    },

    pose_fields = { "delta_arm_angle" },

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
