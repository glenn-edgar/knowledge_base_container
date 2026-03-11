local _here = arg[0]:match("(.-)[^/]+$") or "./"
dofile(_here .. "se_path.lua")
local se_runtime = require("se_runtime")

local ok, md = pcall(require, "complex_sequence_module")
assert(ok, md)
local mod = se_runtime.new_module(md)

-- Walk all trees and report any child with node_index == nil
local function walk(node, path)
    if node.node_index == nil then
        print(string.format("NIL node_index at: %s  call_type=%s func_name=%s",
            path, tostring(node.call_type), tostring(node.func_name)))
    end
    for i, child in ipairs(node.children or {}) do
        walk(child, path .. "[" .. i .. "]")
    end
end

for _, tname in ipairs(md.tree_order) do
    local tree = md.trees[tname]
    for i, root in ipairs(tree.nodes or {}) do
        walk(root, tname .. ".root[" .. i .. "]")
    end
end
print("probe done")

