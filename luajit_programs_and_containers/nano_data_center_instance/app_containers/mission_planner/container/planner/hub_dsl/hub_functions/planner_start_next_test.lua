--[[
    planner_start_next_test.lua -- Chain to next KB from blackboard.

    One-shot: copies next_test_json → current_test_json,
    reads test_id, activates that KB.

    KB index map loaded from plugins at registration time.
]]

local engine = require("ct_engine")
local json_util = require("json_util")

local M = {}

-- Built dynamically from KB plugins
M.kb_by_index = {}

function M.set_plugins(plugins)
    M.kb_by_index = {}
    for _, p in ipairs(plugins) do
        M.kb_by_index[p.index] = p.name
    end
end

M.one_shot = {}
M.one_shot.PLANNER_START_NEXT_TEST = function(handle, node)
    local bb = handle.blackboard
    local next_json = bb.next_test_json or ""
    if next_json == "" then return end

    bb.current_test_json = next_json
    bb.next_test_json = ""

    local ok, action = pcall(json_util.decode, next_json)
    if not ok or not action then return end

    local test_id = action.test_id or 0
    if test_id == 0 then return end

    local kb_name = M.kb_by_index[test_id]
    if not kb_name then return end

    local kb = handle.kb_table[kb_name]
    if not kb then return end

    for _, nid in ipairs(kb.node_ids) do
        local n = handle.nodes[nid]
        if n then
            n.ct_control.enabled = false
            n.ct_control.initialized = false
        end
        handle.node_state[nid] = nil
    end

    engine.init_test(handle, kb_name)
    handle.active_tests[kb_name] = true
    handle.active_test_count = (handle.active_test_count or 0) + 1
end

M.registry = {
    one_shot = M.one_shot,
}

return M
