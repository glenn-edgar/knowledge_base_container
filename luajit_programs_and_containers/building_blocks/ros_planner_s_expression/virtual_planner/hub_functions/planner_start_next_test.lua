-- planner_start_next_test.lua -- Start next KB from blackboard
--
-- One-shot function: copies next_test_json → current_test_json,
-- reads test_id from the new current_test_json, activates that KB.
--
-- Used in the finalize column of each virtual node KB.
-- When the current KB finishes, this function chains to the next.
--
-- Blackboard fields:
--   current_test_json: string — JSON for active action {test_id, next_test, ...}
--   next_test_json:    string — JSON for next action {test_id, next_test, ...}
--
-- KB index mapping (matches test_list order in hub_dsl.lua):
--   1  = init_check
--   2  = path_spline
--   3  = path_line
--   4  = path_wall
--   5  = path_rotate
--   6  = deliver_part
--   7  = paint_sample
--   8  = load_shipping
--   9  = pass_gate
--   10 = inspection_scan
--   11 = idle
--   0  = done (no more tests)

local engine   = require("ct_engine")
local json_util = require("json_util")

local M = {}

-- KB index to name mapping (matches test_list order in hub_dsl.lua)
M.kb_by_index = {
    [1]  = "init_check",
    [2]  = "path_spline",
    [3]  = "path_line",
    [4]  = "path_wall",
    [5]  = "path_rotate",
    [6]  = "deliver_part",
    [7]  = "paint_sample",
    [8]  = "load_shipping",
    [9]  = "pass_gate",
    [10] = "inspection_scan",
    [11] = "idle",
}

-- One-shot function: promote next_test_json and activate next KB
M.one_shot = {}
M.one_shot.PLANNER_START_NEXT_TEST = function(handle, node)
    local bb = handle.blackboard

    -- Promote next_test_json → current_test_json
    local next_json = bb.next_test_json or ""
    if next_json == "" then
        -- No next action staged — system will terminate
        return
    end

    bb.current_test_json = next_json
    bb.next_test_json = ""

    -- Decode to get test_id
    local ok, action = pcall(json_util.decode, next_json)
    if not ok or not action then
        print("PLANNER_START_NEXT_TEST: failed to decode next_test_json")
        return
    end

    local test_id = action.test_id or 0
    if test_id == 0 then
        -- Done
        return
    end

    local kb_name = M.kb_by_index[test_id]
    if not kb_name then
        print(string.format("PLANNER_START_NEXT_TEST: unknown test_id %d", test_id))
        return
    end

    local kb = handle.kb_table[kb_name]
    if not kb then
        print(string.format("PLANNER_START_NEXT_TEST: KB '%s' not found", kb_name))
        return
    end

    -- Reset the KB's nodes before activation
    for _, nid in ipairs(kb.node_ids) do
        local n = handle.nodes[nid]
        if n then
            n.ct_control.enabled = false
            n.ct_control.initialized = false
        end
        handle.node_state[nid] = nil
    end

    -- Activate the next KB
    engine.init_test(handle, kb_name)
    handle.active_tests[kb_name] = true
    handle.active_test_count = (handle.active_test_count or 0) + 1
end

-- Registry for fn_registry.register_functions()
M.registry = {
    one_shot = M.one_shot,
}

return M
