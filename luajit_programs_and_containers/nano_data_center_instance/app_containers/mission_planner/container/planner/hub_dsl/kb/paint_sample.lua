local common_tree = require("kb.common_tree")
return {
    name = "paint_sample", index = 7, packet_ctype = "cmd_paint_sample_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
