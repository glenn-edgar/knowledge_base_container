local common_tree = require("kb.common_tree")
return {
    name = "idle", index = 11, packet_ctype = "cmd_idle_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build_fire_and_forget(ct, kb_name, one_shot_name)
    end,
}
