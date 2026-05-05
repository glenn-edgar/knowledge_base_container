local common_tree = require("kb.common_tree")
return {
    name = "path_line", index = 3, packet_ctype = "cmd_path_line_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
