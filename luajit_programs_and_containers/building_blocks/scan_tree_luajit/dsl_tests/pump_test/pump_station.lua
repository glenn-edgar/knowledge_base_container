-- Generated scan tree descriptor: pump_station
-- DO NOT EDIT - produced by codegen_luajit.lua

local builtins = require('st_builtins')

-- User VFTs must be provided via the user_vfts table parameter
local function make_desc(user_vfts)
    user_vfts = user_vfts or {}

    local desc = {}
    desc.name = "pump_station"

    -- buf_descs: 1-indexed, buf_id = array_index - 1
    desc.buf_descs = {
        {path="pump_station.pump_faults", key=3222008445, size=4, elem_size=1, is_layer=false, buf_index=0, level=255}, -- buf_id=0
        {path="pump_station.power_status", key=1041461993, size=2, elem_size=1, is_layer=false, buf_index=1, level=255}, -- buf_id=1
        {path="pump_station.alarm_clear", key=4068160602, size=4, elem_size=1, is_layer=false, buf_index=2, level=255}, -- buf_id=2
        {path="pump_station.motor_current", key=2436826296, size=4, elem_size=4, is_layer=false, buf_index=3, level=255}, -- buf_id=3
        {path="pump_station.motor_thresholds", key=1705306400, size=4, elem_size=4, is_layer=false, buf_index=4, level=255}, -- buf_id=4
        {path="pump_station.power.power_output", key=3147815416, size=1, elem_size=1, is_layer=true, buf_index=0, level=0}, -- buf_id=5
        {path="pump_station.actuation.actuation_output", key=11734820, size=2, elem_size=1, is_layer=true, buf_index=1, level=1}, -- buf_id=6
        {path="pump_station.actuation.group_a.group_a_output", key=3222746060, size=3, elem_size=1, is_layer=true, buf_index=2, level=1}, -- buf_id=7
        {path="pump_station.actuation.group_b.group_b_output", key=1989217160, size=3, elem_size=1, is_layer=true, buf_index=3, level=1}, -- buf_id=8
    }

    -- node_descs: 1-indexed
    desc.node_descs = {
        { -- [1] VFT_or (system) -> buf[5][0]
            func = builtins.vft_or,
            output_buf_id = 5, output_pos = 0,
            inputs = {{buf_id=1, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {2},
        },
        { -- [2] VFT_user_motor_health_check (user) -> buf[7][0]
            func = user_vfts.user_vft_motor_health_check,
            output_buf_id = 7, output_pos = 0,
            inputs = {{buf_id=3, start=0, count=1, role=0}, {buf_id=4, start=0, count=1, role=0}},
            n_inputs = 2,
            raw_deps = {24},
        },
        { -- [3] VFT_user_motor_health_check (user) -> buf[7][1]
            func = user_vfts.user_vft_motor_health_check,
            output_buf_id = 7, output_pos = 1,
            inputs = {{buf_id=3, start=1, count=1, role=0}, {buf_id=4, start=1, count=1, role=0}},
            n_inputs = 2,
            raw_deps = {24},
        },
        { -- [4] VFT_or (system) -> buf[7][2]
            func = builtins.vft_or,
            output_buf_id = 7, output_pos = 2,
            inputs = {{buf_id=7, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {24},
        },
        { -- [5] VFT_user_motor_health_check (user) -> buf[8][0]
            func = user_vfts.user_vft_motor_health_check,
            output_buf_id = 8, output_pos = 0,
            inputs = {{buf_id=3, start=2, count=1, role=0}, {buf_id=4, start=2, count=1, role=0}},
            n_inputs = 2,
            raw_deps = {24},
        },
        { -- [6] VFT_user_motor_health_check (user) -> buf[8][1]
            func = user_vfts.user_vft_motor_health_check,
            output_buf_id = 8, output_pos = 1,
            inputs = {{buf_id=3, start=3, count=1, role=0}, {buf_id=4, start=3, count=1, role=0}},
            n_inputs = 2,
            raw_deps = {24},
        },
        { -- [7] VFT_or (system) -> buf[8][2]
            func = builtins.vft_or,
            output_buf_id = 8, output_pos = 2,
            inputs = {{buf_id=8, start=0, count=2, role=0}},
            n_inputs = 1,
            raw_deps = {24},
        },
        { -- [8] VFT_copy (system) -> buf[6][0]
            func = builtins.vft_copy,
            output_buf_id = 6, output_pos = 0,
            inputs = {{buf_id=7, start=2, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {24},
        },
        { -- [9] VFT_copy (system) -> buf[6][1]
            func = builtins.vft_copy,
            output_buf_id = 6, output_pos = 1,
            inputs = {{buf_id=5, start=0, count=1, role=0}},
            n_inputs = 1,
            raw_deps = {2},
        },
    }

    -- lookup: sorted by key, 1-indexed
    desc.lookup = {
        {key=11734820, buf_id=6},
        {key=1041461993, buf_id=1},
        {key=1705306400, buf_id=4},
        {key=1989217160, buf_id=8},
        {key=2436826296, buf_id=3},
        {key=3147815416, buf_id=5},
        {key=3222008445, buf_id=0},
        {key=3222746060, buf_id=7},
        {key=4068160602, buf_id=2},
    }

    desc.n_bufs = 9
    desc.n_nodes = 9
    desc.n_raw = 5
    desc.n_layer = 4

    desc.fuse_table = {}

    -- Buffer ID constants
    desc.IDS = {
        PUMP_STATION_PUMP_FAULTS = 0,
        PUMP_STATION_POWER_STATUS = 1,
        PUMP_STATION_ALARM_CLEAR = 2,
        PUMP_STATION_MOTOR_CURRENT = 3,
        PUMP_STATION_MOTOR_THRESHOLDS = 4,
        PUMP_STATION_POWER_POWER_OUTPUT = 5,
        PUMP_STATION_ACTUATION_ACTUATION_OUTPUT = 6,
        PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT = 7,
        PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT = 8,
    }

    return desc
end

return make_desc
