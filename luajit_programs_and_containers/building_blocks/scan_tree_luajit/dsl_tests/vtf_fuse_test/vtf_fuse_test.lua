-- Generated scan tree descriptor: vtf_fuse_test
-- DO NOT EDIT - produced by codegen_luajit.lua

local builtins = require('st_builtins')

local function make_desc()

    local desc = {}
    desc.name = "vtf_fuse_test"

    -- buf_descs: 1-indexed, buf_id = array_index - 1
    desc.buf_descs = {
        {path="vtf_fuse_test.power_inputs", key=1270316981, size=3, elem_size=1, is_layer=false, buf_index=0, level=255}, -- buf_id=0
        {path="vtf_fuse_test.safety_inputs", key=1957815593, size=3, elem_size=1, is_layer=false, buf_index=1, level=255}, -- buf_id=1
        {path="vtf_fuse_test.pump_current", key=2840648192, size=4, elem_size=4, is_layer=false, buf_index=2, level=255}, -- buf_id=2
        {path="vtf_fuse_test.pump_limits", key=2502281965, size=4, elem_size=4, is_layer=false, buf_index=3, level=255}, -- buf_id=3
        {path="vtf_fuse_test.chlorine_level", key=4022264352, size=1, elem_size=4, is_layer=false, buf_index=4, level=255}, -- buf_id=4
        {path="vtf_fuse_test.chlorine_max", key=679793888, size=1, elem_size=4, is_layer=false, buf_index=5, level=255}, -- buf_id=5
        {path="vtf_fuse_test.fuse_clear", key=47313200, size=5, elem_size=1, is_layer=false, buf_index=6, level=255}, -- buf_id=6
        {path="vtf_fuse_test.infrastructure.infra_output", key=2919713824, size=3, elem_size=1, is_layer=true, buf_index=0, level=0}, -- buf_id=7
        {path="vtf_fuse_test.infrastructure.safety_check.safety_scratch", key=3305844372, size=1, elem_size=1, is_layer=true, buf_index=1, level=0}, -- buf_id=8
        {path="vtf_fuse_test.equipment.equip_output", key=2769517364, size=4, elem_size=1, is_layer=true, buf_index=2, level=1}, -- buf_id=9
        {path="vtf_fuse_test.equipment.intake_pumps.intake_output", key=2912553048, size=4, elem_size=1, is_layer=true, buf_index=3, level=1}, -- buf_id=10
        {path="vtf_fuse_test.equipment.dosing.dosing_output", key=2016065524, size=2, elem_size=1, is_layer=true, buf_index=4, level=1}, -- buf_id=11
        {path="vtf_fuse_test.equipment.dist_pumps.dist_output", key=1984289124, size=4, elem_size=1, is_layer=true, buf_index=5, level=1}, -- buf_id=12
        {path="vtf_fuse_test.equipment.intake_agg.intake_scratch", key=1011986612, size=1, elem_size=1, is_layer=true, buf_index=6, level=1}, -- buf_id=13
        {path="vtf_fuse_test.equipment.dist_agg.dist_scratch", key=546691484, size=1, elem_size=1, is_layer=true, buf_index=7, level=1}, -- buf_id=14
        {path="vtf_fuse_test.process.process_output", key=1293846721, size=3, elem_size=1, is_layer=true, buf_index=8, level=2}, -- buf_id=15
        {path="vtf_fuse_test.process.intake_ready_check.intake_rdy_scratch", key=3894334832, size=2, elem_size=1, is_layer=true, buf_index=9, level=2}, -- buf_id=16
        {path="vtf_fuse_test.process.treat_ready_check.treat_rdy_scratch", key=2854690500, size=2, elem_size=1, is_layer=true, buf_index=10, level=2}, -- buf_id=17
        {path="vtf_fuse_test.process.dist_ready_check.dist_rdy_scratch", key=2517800952, size=2, elem_size=1, is_layer=true, buf_index=11, level=2}, -- buf_id=18
        {path="vtf_fuse_test.plant_status.plant_output", key=2114625600, size=2, elem_size=1, is_layer=true, buf_index=12, level=3}, -- buf_id=19
    }

    -- node_descs: 1-indexed
    desc.node_descs = {
        { -- [1] VFT_or (system) -> buf[8][0]
            func = builtins.vft_or,
            output_buf_id = 8, output_pos = 0,
            inputs = {{buf_id=1, start=0, count=3, role=0}},
            n_inputs = 1,
            raw_deps = {2},
        },
        { -- [2] VFT_or (system) -> buf[7][0]
            func = builtins.vft_or,
            output_buf_id = 7, output_pos = 0,
            inputs = {{buf_id=0, start=0, count=3, role=0}},
            n_inputs = 1,
            raw_deps = {1},
        },
        { -- [3] VFT_not (system) -> buf[7][1]
            func = builtins.vft_not,
            output_buf_id = 7, output_pos = 1,
            inputs = {{buf_id=8, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {2},
        },
        { -- [4] VFT_and (system) -> buf[7][2]
            func = builtins.vft_and,
            output_buf_id = 7, output_pos = 2,
            inputs = {{buf_id=7, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {3},
        },
        { -- [5] VFT_gt (system) -> buf[10][0]
            func = builtins.vft_gt,
            output_buf_id = 10, output_pos = 0,
            inputs = {{buf_id=2, start=0, count=1, role=5}, {buf_id=3, start=0, count=1, role=6}},
            n_inputs = 2,
            raw_deps = {12},
        },
        { -- [6] VFT_gt (system) -> buf[10][1]
            func = builtins.vft_gt,
            output_buf_id = 10, output_pos = 1,
            inputs = {{buf_id=2, start=1, count=1, role=5}, {buf_id=3, start=1, count=1, role=6}},
            n_inputs = 2,
            raw_deps = {12},
        },
        { -- [7] VFT_fuse (system) -> buf[10][2]
            func = builtins.vft_fuse,
            output_buf_id = 10, output_pos = 2,
            inputs = {{buf_id=10, start=0, count=1, role=7}, {buf_id=6, start=0, count=1, role=2}},
            n_inputs = 2,
            raw_deps = {76},
        },
        { -- [8] VFT_fuse (system) -> buf[10][3]
            func = builtins.vft_fuse,
            output_buf_id = 10, output_pos = 3,
            inputs = {{buf_id=10, start=1, count=1, role=7}, {buf_id=6, start=1, count=1, role=2}},
            n_inputs = 2,
            raw_deps = {76},
        },
        { -- [9] VFT_gt (system) -> buf[11][0]
            func = builtins.vft_gt,
            output_buf_id = 11, output_pos = 0,
            inputs = {{buf_id=4, start=0, count=1, role=5}, {buf_id=5, start=0, count=1, role=6}},
            n_inputs = 2,
            raw_deps = {48},
        },
        { -- [10] VFT_fuse (system) -> buf[11][1]
            func = builtins.vft_fuse,
            output_buf_id = 11, output_pos = 1,
            inputs = {{buf_id=11, start=0, count=1, role=7}, {buf_id=6, start=2, count=1, role=2}},
            n_inputs = 2,
            raw_deps = {112},
        },
        { -- [11] VFT_gt (system) -> buf[12][0]
            func = builtins.vft_gt,
            output_buf_id = 12, output_pos = 0,
            inputs = {{buf_id=2, start=2, count=1, role=5}, {buf_id=3, start=2, count=1, role=6}},
            n_inputs = 2,
            raw_deps = {12},
        },
        { -- [12] VFT_gt (system) -> buf[12][1]
            func = builtins.vft_gt,
            output_buf_id = 12, output_pos = 1,
            inputs = {{buf_id=2, start=3, count=1, role=5}, {buf_id=3, start=3, count=1, role=6}},
            n_inputs = 2,
            raw_deps = {12},
        },
        { -- [13] VFT_fuse (system) -> buf[12][2]
            func = builtins.vft_fuse,
            output_buf_id = 12, output_pos = 2,
            inputs = {{buf_id=12, start=0, count=1, role=7}, {buf_id=6, start=3, count=1, role=2}},
            n_inputs = 2,
            raw_deps = {76},
        },
        { -- [14] VFT_fuse (system) -> buf[12][3]
            func = builtins.vft_fuse,
            output_buf_id = 12, output_pos = 3,
            inputs = {{buf_id=12, start=1, count=1, role=7}, {buf_id=6, start=4, count=1, role=2}},
            n_inputs = 2,
            raw_deps = {76},
        },
        { -- [15] VFT_or (system) -> buf[13][0]
            func = builtins.vft_or,
            output_buf_id = 13, output_pos = 0,
            inputs = {{buf_id=10, start=2, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {76},
        },
        { -- [16] VFT_or (system) -> buf[14][0]
            func = builtins.vft_or,
            output_buf_id = 14, output_pos = 0,
            inputs = {{buf_id=12, start=2, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {76},
        },
        { -- [17] VFT_not (system) -> buf[9][0]
            func = builtins.vft_not,
            output_buf_id = 9, output_pos = 0,
            inputs = {{buf_id=13, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {76},
        },
        { -- [18] VFT_not (system) -> buf[9][1]
            func = builtins.vft_not,
            output_buf_id = 9, output_pos = 1,
            inputs = {{buf_id=11, start=1, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {112},
        },
        { -- [19] VFT_not (system) -> buf[9][2]
            func = builtins.vft_not,
            output_buf_id = 9, output_pos = 2,
            inputs = {{buf_id=14, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {76},
        },
        { -- [20] VFT_copy (system) -> buf[9][3]
            func = builtins.vft_copy,
            output_buf_id = 9, output_pos = 3,
            inputs = {{buf_id=7, start=2, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {3},
        },
        { -- [21] VFT_copy (system) -> buf[16][0]
            func = builtins.vft_copy,
            output_buf_id = 16, output_pos = 0,
            inputs = {{buf_id=9, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [22] VFT_copy (system) -> buf[16][1]
            func = builtins.vft_copy,
            output_buf_id = 16, output_pos = 1,
            inputs = {{buf_id=9, start=3, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [23] VFT_copy (system) -> buf[17][0]
            func = builtins.vft_copy,
            output_buf_id = 17, output_pos = 0,
            inputs = {{buf_id=9, start=1, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [24] VFT_copy (system) -> buf[17][1]
            func = builtins.vft_copy,
            output_buf_id = 17, output_pos = 1,
            inputs = {{buf_id=9, start=3, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [25] VFT_copy (system) -> buf[18][0]
            func = builtins.vft_copy,
            output_buf_id = 18, output_pos = 0,
            inputs = {{buf_id=9, start=2, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [26] VFT_copy (system) -> buf[18][1]
            func = builtins.vft_copy,
            output_buf_id = 18, output_pos = 1,
            inputs = {{buf_id=9, start=3, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [27] VFT_and (system) -> buf[15][0]
            func = builtins.vft_and,
            output_buf_id = 15, output_pos = 0,
            inputs = {{buf_id=9, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [28] VFT_and (system) -> buf[15][0]
            func = builtins.vft_and,
            output_buf_id = 15, output_pos = 0,
            inputs = {{buf_id=16, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [29] VFT_and (system) -> buf[15][1]
            func = builtins.vft_and,
            output_buf_id = 15, output_pos = 1,
            inputs = {{buf_id=17, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [30] VFT_and (system) -> buf[15][2]
            func = builtins.vft_and,
            output_buf_id = 15, output_pos = 2,
            inputs = {{buf_id=18, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [31] VFT_or (system) -> buf[19][0]
            func = builtins.vft_or,
            output_buf_id = 19, output_pos = 0,
            inputs = {{buf_id=15, start=0, count=3, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
        { -- [32] VFT_and (system) -> buf[19][1]
            func = builtins.vft_and,
            output_buf_id = 19, output_pos = 1,
            inputs = {{buf_id=15, start=0, count=3, role=0}},
            n_inputs = 1,
            raw_deps = {127},
        },
    }

    -- lookup: sorted by key, 1-indexed
    desc.lookup = {
        {key=47313200, buf_id=6},
        {key=546691484, buf_id=14},
        {key=679793888, buf_id=5},
        {key=1011986612, buf_id=13},
        {key=1270316981, buf_id=0},
        {key=1293846721, buf_id=15},
        {key=1957815593, buf_id=1},
        {key=1984289124, buf_id=12},
        {key=2016065524, buf_id=11},
        {key=2114625600, buf_id=19},
        {key=2502281965, buf_id=3},
        {key=2517800952, buf_id=18},
        {key=2769517364, buf_id=9},
        {key=2840648192, buf_id=2},
        {key=2854690500, buf_id=17},
        {key=2912553048, buf_id=10},
        {key=2919713824, buf_id=7},
        {key=3305844372, buf_id=8},
        {key=3894334832, buf_id=16},
        {key=4022264352, buf_id=4},
    }

    desc.n_bufs = 20
    desc.n_nodes = 32
    desc.n_raw = 7
    desc.n_layer = 13

    -- fuse_table: node_1based -> action name (resolved at test time)
    desc.fuse_table = {
        [7] = "on_intake_p0_fuse",  -- on_intake_p0_fuse
        [8] = "on_intake_p1_fuse",  -- on_intake_p1_fuse
        [10] = "on_chlorine_fuse",  -- on_chlorine_fuse
        [13] = "on_dist_p0_fuse",  -- on_dist_p0_fuse
        [14] = "on_dist_p1_fuse",  -- on_dist_p1_fuse
    }

    -- Buffer ID constants
    desc.IDS = {
        VTF_FUSE_TEST_POWER_INPUTS = 0,
        VTF_FUSE_TEST_SAFETY_INPUTS = 1,
        VTF_FUSE_TEST_PUMP_CURRENT = 2,
        VTF_FUSE_TEST_PUMP_LIMITS = 3,
        VTF_FUSE_TEST_CHLORINE_LEVEL = 4,
        VTF_FUSE_TEST_CHLORINE_MAX = 5,
        VTF_FUSE_TEST_FUSE_CLEAR = 6,
        VTF_FUSE_TEST_INFRASTRUCTURE_INFRA_OUTPUT = 7,
        VTF_FUSE_TEST_INFRASTRUCTURE_SAFETY_CHECK_SAFETY_SCRATCH = 8,
        VTF_FUSE_TEST_EQUIPMENT_EQUIP_OUTPUT = 9,
        VTF_FUSE_TEST_EQUIPMENT_INTAKE_PUMPS_INTAKE_OUTPUT = 10,
        VTF_FUSE_TEST_EQUIPMENT_DOSING_DOSING_OUTPUT = 11,
        VTF_FUSE_TEST_EQUIPMENT_DIST_PUMPS_DIST_OUTPUT = 12,
        VTF_FUSE_TEST_EQUIPMENT_INTAKE_AGG_INTAKE_SCRATCH = 13,
        VTF_FUSE_TEST_EQUIPMENT_DIST_AGG_DIST_SCRATCH = 14,
        VTF_FUSE_TEST_PROCESS_PROCESS_OUTPUT = 15,
        VTF_FUSE_TEST_PROCESS_INTAKE_READY_CHECK_INTAKE_RDY_SCRATCH = 16,
        VTF_FUSE_TEST_PROCESS_TREAT_READY_CHECK_TREAT_RDY_SCRATCH = 17,
        VTF_FUSE_TEST_PROCESS_DIST_READY_CHECK_DIST_RDY_SCRATCH = 18,
        VTF_FUSE_TEST_PLANT_STATUS_PLANT_OUTPUT = 19,
    }

    return desc
end

return make_desc
