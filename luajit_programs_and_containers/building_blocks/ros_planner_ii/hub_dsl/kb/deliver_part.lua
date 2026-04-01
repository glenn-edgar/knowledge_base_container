local common_tree = require("kb.common_tree")
return {
    name = "deliver_part", index = 6, packet_ctype = "cmd_deliver_part_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
