local common_tree = require("kb.common_tree")
return {
    name = "inspection_scan", index = 10, packet_ctype = "cmd_inspection_scan_t",
    define_tree = function(ct, kb_name, one_shot_name, plugin)
        common_tree.build(ct, kb_name, one_shot_name, plugin)
    end,
}
