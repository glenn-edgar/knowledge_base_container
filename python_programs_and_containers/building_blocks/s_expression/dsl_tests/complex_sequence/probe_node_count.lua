local _here = arg[0]:match("(.-)[^/]+$") or "./"
dofile(_here .. "se_path.lua")
local se_runtime = require("se_runtime")

local ok, md = pcall(require, "complex_sequence_module")
assert(ok, md)
local mod = se_runtime.new_module(md)

local function max_index(node, cur)
    if node.node_index and node.node_index > cur then cur = node.node_index end
    for _, child in ipairs(node.children or {}) do
        cur = max_index(child, cur)
    end
    return cur
end

for _, tname in ipairs(md.tree_order) do
    local tree = md.trees[tname]
    local max_ni = -1
    for _, root in ipairs(tree.nodes or {}) do
        max_ni = max_index(root, max_ni)
    end
    print(string.format("tree=%s  node_count=%s  max_node_index=%d  states_needed=%d",
        tname,
        tostring(tree.node_count),
        max_ni,
        max_ni + 1))
end

