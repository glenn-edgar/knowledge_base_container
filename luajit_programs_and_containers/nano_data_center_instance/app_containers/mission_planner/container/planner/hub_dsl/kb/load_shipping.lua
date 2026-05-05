local common_tree = require("kb.common_tree")
return {
    name = "load_shipping", index = 8, packet_ctype = "cmd_load_shipping_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
