local common_tree = require("kb.common_tree")

return {
    name           = "pass_gate",
    index          = 9,
    packet_ctype   = "cmd_pass_gate_t",
    packet_type_id = 9,

    json_schema = {
        { name = "rpc_open_hash",  type = "uint32", default = 0 },
        { name = "rpc_close_hash", type = "uint32", default = 0 },
        { name = "drive_through",  type = "float",  default = 200 },
    },

    mapping = {},

    bitmask = {
        { name = "gate_opened",      bit = 0 },
        { name = "drive_complete",   bit = 1 },
        { name = "gate_closed",      bit = 2 },
        { name = "action_complete",  bit = 3 },
    },

    pose_fields = { "delta_x", "delta_y", "delta_heading" },

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
