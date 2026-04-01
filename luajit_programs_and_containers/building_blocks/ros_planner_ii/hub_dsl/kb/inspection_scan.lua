local common_tree = require("kb.common_tree")

return {
    name           = "inspection_scan",
    index          = 10,
    packet_ctype   = "cmd_sensor_read_t",
    packet_type_id = 6,

    json_schema = {
        { name = "sensor_port", type = "uint8", default = 0 },
        { name = "sensor_type", type = "uint8", default = 0,
          enum = { color = 0, distance = 1, force = 2 } },
    },

    mapping = {},

    bitmask = {
        { name = "reading_ready",   bit = 0 },
        { name = "sensor_fault",    bit = 1 },
    },

    pose_fields = {},

    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
