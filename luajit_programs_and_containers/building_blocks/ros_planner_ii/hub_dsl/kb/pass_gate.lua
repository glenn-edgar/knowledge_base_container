local common_tree = require("kb.common_tree")
return {
    name = "pass_gate", index = 9, packet_ctype = "cmd_pass_gate_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
