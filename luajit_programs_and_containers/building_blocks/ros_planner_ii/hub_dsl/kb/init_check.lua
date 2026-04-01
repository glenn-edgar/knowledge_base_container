local common_tree = require("kb.common_tree")
return {
    name = "init_check", index = 1, packet_ctype = "cmd_init_check_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
