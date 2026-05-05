local common_tree = require("kb.common_tree")
return {
    name = "path_rotate", index = 5, packet_ctype = "cmd_path_rotate_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
