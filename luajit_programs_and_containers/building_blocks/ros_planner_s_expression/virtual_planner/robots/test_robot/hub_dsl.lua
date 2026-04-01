--[[
    Hub Virtual Node Trees -- ChainTree DSL for the hub side.

    One KB (test) per virtual node type. The sequencer activates
    the appropriate KB when it reaches that virtual node in the plan.

    Virtual node KBs (index = test_id value):
      1  init_check        -- preflight fitness check
      2  path_spline       -- spline follow between board positions
      3  path_line         -- line follow (junctions handled separately)
      4  path_wall         -- wall ride (distance + wall separation)
      5  path_rotate       -- turn in place to heading
      6  deliver_part      -- arm operation at assembly station
      7  paint_sample      -- arm operation at painting station
      8  load_shipping     -- arm operation at shipping station
      9  pass_gate         -- RPC gate open/drive/close at gate_zone
      10 inspection_scan   -- sensor read at inspection station
      11 idle              -- robot completed all tasks

    Blackboard uses two generic JSON slots (not per-KB fields):
      current_test_json — active action: {test_id, next_test, ...params}
      next_test_json    — staged ahead:  {test_id, next_test, ...params}

    Chaining: PLANNER_START_NEXT_TEST in finalize copies next_test_json
    to current_test_json, reads test_id, activates that KB. Sequencer
    writes the new next_test_json one step ahead.
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Blackboard (shared across all hub KBs)
-- =========================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("hub_state")
        -- Bitmap inputs (from streaming channel, shared by all KBs)
        ct:bb_field("bitmap_robot_good_conditions", "uint64", 0)
        ct:bb_field("bitmap_robot_faults",          "uint64", 0)
        ct:bb_field("local_distance_x",             "float", 0)
        ct:bb_field("local_distance_y",             "float", 0)
        ct:bb_field("current_heading",              "float", 0)
        ct:bb_field("current_speed",                "float", 0)
        -- Command packet (pointer to FFI struct, written by GENERATE_*_AVRC_PACKET)
        ct:bb_field("command_packet",               "pointer", 0)
        ct:bb_field("command_pending",              "bool", false)
        -- Status output (read by sequencer after KB completes)
        ct:bb_field("action_status",                "int32", 0)
        ct:bb_field("recovery_count",               "int32", 0)
        -- Double-buffered action JSON (sequencer stages these)
        -- Each contains: {test_id, next_test, ...action params}
        ct:bb_field("current_test_json",            "string", "")
        ct:bb_field("next_test_json",               "string", "")
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- KB: init_check (index 1)
-- =========================================================================
-- Preflight fitness check. Always the first virtual node.
-- Checks robot bitmaps: battery, motors, sensors, comms.
-- Reads current_test_json: { test_id=1, next_test, ... }

local function init_check(ct, kb_name)
    ct:start_test(kb_name)

    local main_col = ct:define_column("init_check_main", nil, nil, nil, nil, {}, true)

        -- Decode JSON and generate AVRC command packet
        ct:asm_one_shot_handler("GENERATE_INIT_CHECK_AVRC_PACKET",
            {"current_test_json", "command_packet"})

        -- TODO: send packet to remote, wait for bitmap response,
        --       verify all systems, report ready or abort

    ct:end_column(main_col)

    ct:end_test()
end

-- =========================================================================
-- KB: path_spline (index 2)
-- =========================================================================
-- Spline follow between board positions.
-- Path geometry from edge waypoints handles curves and heading changes.
-- Reads current_test_json: { test_id=2, next_test, from, to, speed,
--   max_distance, segments: [{from, to, distance, speed, waypoints}] }

local function path_spline(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: path_line (index 3)
-- =========================================================================
-- Line follow between board positions.
-- Junctions handled separately (future work).
-- Reads current_test_json: { test_id=3, next_test, from, to, speed,
--   max_distance, distance }

local function path_line(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: path_wall (index 4)
-- =========================================================================
-- Wall ride. Distance and wall separation only.
-- Reads current_test_json: { test_id=4, next_test, distance, speed,
--   wall_separation }

local function path_wall(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: path_rotate (index 5)
-- =========================================================================
-- Turn in place to a heading.
-- Inserted by global planner before straight paths and missions.
-- Reads current_test_json: { test_id=5, next_test, from_heading,
--   to_heading }

local function path_rotate(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: deliver_part (index 6)
-- =========================================================================
-- Arm operation at assembly station.
-- Reads current_test_json: { test_id=6, next_test, arm_target,
--   arm_speed, payload, arm_return, approach_heading }

local function deliver_part(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: paint_sample (index 7)
-- =========================================================================
-- Arm operation at painting station.
-- Reads current_test_json: { test_id=7, next_test, arm_target,
--   arm_speed, hold_time, arm_return, approach_heading }

local function paint_sample(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: load_shipping (index 8)
-- =========================================================================
-- Arm operation at shipping station.
-- Reads current_test_json: { test_id=8, next_test, arm_target,
--   arm_speed, payload, arm_return, approach_heading }

local function load_shipping(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: pass_gate (index 9)
-- =========================================================================
-- RPC gate open/drive/close at gate_zone.
-- Reads current_test_json: { test_id=9, next_test, rpc_open,
--   rpc_close, drive_through, approach_heading }

local function pass_gate(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: inspection_scan (index 10)
-- =========================================================================
-- Sensor read at inspection station.
-- Reads current_test_json: { test_id=10, next_test, sensor_port,
--   sensor_type, approach_heading }

local function inspection_scan(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree
    ct:end_test()
end

-- =========================================================================
-- KB: idle (index 11)
-- =========================================================================
-- Terminal state. Robot has completed all tasks.
-- Reads current_test_json: { test_id=11, next_test=0 }

local function idle(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree -- park robot, report done
    ct:end_test()
end

-- =========================================================================
-- Test list and dispatch
-- =========================================================================

local test_list = {
    "init_check",       -- 1
    "path_spline",      -- 2
    "path_line",        -- 3
    "path_wall",        -- 4
    "path_rotate",      -- 5
    "deliver_part",     -- 6
    "paint_sample",     -- 7
    "load_shipping",    -- 8
    "pass_gate",        -- 9
    "inspection_scan",  -- 10
    "idle",             -- 11
}

local test_dict = {
    init_check       = init_check,
    path_spline      = path_spline,
    path_line        = path_line,
    path_wall        = path_wall,
    path_rotate      = path_rotate,
    deliver_part     = deliver_part,
    paint_sample     = paint_sample,
    load_shipping    = load_shipping,
    pass_gate        = pass_gate,
    inspection_scan  = inspection_scan,
    idle             = idle,
}

-- =========================================================================
-- Main: generate JSON + debug YAML
-- =========================================================================

if arg then
    if #arg ~= 1 then
        print("Usage: luajit hub_dsl.lua <json_file>")
        os.exit(1)
    end

    local json_file = arg[1]

    local ct = add_header(json_file)
    for _, test_name in ipairs(test_list) do
        test_dict[test_name](ct, test_name)
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    local kbs = ct:list_kbs()
    print("KBs: " .. table.concat(kbs, ", "))
    print("Total nodes: " .. ct.ctb:get_total_node_count())
end
