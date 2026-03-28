#!/usr/bin/env luajit
--[[
    Pump Station Test - LuaJIT Scan Tree Runtime

    Port of scan_tree_c/dsl_tests/pump_test/main.c
    Exercises: user VFTs, OR, COPY, multi-level graph, change detection
--]]

-- Set up module paths
local script_dir = arg[0]:match("(.*/)" ) or "./"
local runtime_dir = script_dir .. "../../"
package.path = runtime_dir .. "?.lua;" .. script_dir .. "?.lua;" .. package.path

local st_runtime = require("st_runtime")
local st_display = require("st_display")
local Handle = st_runtime.Handle
local Display = st_display.Display

-- Load generated descriptor and user functions
local make_desc = require("pump_station")
local user_funcs = require("user_functions")

-- Build descriptor with user VFTs
local desc = make_desc(user_funcs)
local IDS = desc.IDS

-- =====================================================================
-- State name helper
-- =====================================================================
local function sn(s)
    if s == 1 then return "ACTIVE"
    elseif s == 0 then return "FAULT"
    else return "NOT_OP" end
end

local function print_states(label, states, size)
    io.write(string.format("  %s:", label))
    for i = 0, size - 1 do
        io.write(string.format(" [%d]=%s", i, sn(states[i])))
    end
    io.write("\n")
end

-- =====================================================================
-- Main
-- =====================================================================
local h = Handle.new(desc)

-- Cache raw buffer pointers
local power_status     = h:raw_current(IDS.PUMP_STATION_POWER_STATUS)
local motor_current    = h:raw_current(IDS.PUMP_STATION_MOTOR_CURRENT)
local motor_thresholds = h:raw_current(IDS.PUMP_STATION_MOTOR_THRESHOLDS)

-- Cache layer state accessors
local power_states, power_sz     = h:layer_states(IDS.PUMP_STATION_POWER_POWER_OUTPUT)
local act_states, act_sz         = h:layer_states(IDS.PUMP_STATION_ACTUATION_ACTUATION_OUTPUT)
local group_a_states, ga_sz      = h:layer_states(IDS.PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT)
local group_b_states, gb_sz      = h:layer_states(IDS.PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT)

-- Build display hierarchy
local disp = Display.new(h)

h:print_info()

-- --- Step 0: Initial state ---
print("\n=== Step 0: Initial ===")
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

-- --- Step 1: Power ON, set thresholds ---
print("\n=== Step 1: Power ON, thresholds=100A ===")
power_status[0] = 1
for i = 0, 3 do motor_thresholds[i] = 100.0 end
h:cycle()
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

-- --- Step 2: Pump 0 overcurrent ---
print("\n=== Step 2: Pump 0 overcurrent (150A) ===")
motor_current[0] = 150.0
h:cycle()
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

-- --- Step 3: Pump 0 current returns to normal ---
print("\n=== Step 3: Pump 0 current normal (50A) ===")
motor_current[0] = 50.0
h:cycle()
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

-- --- Step 4: Power OFF ---
print("\n=== Step 4: Power OFF ===")
power_status[0] = 0
h:cycle()
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

-- --- Step 5: Power back ON ---
print("\n=== Step 5: Power back ON ===")
power_status[0] = 1
h:cycle()
print_states("power",     power_states,   power_sz)
print_states("actuation", act_states,     act_sz)
print_states("group_a",   group_a_states, ga_sz)
print_states("group_b",   group_b_states, gb_sz)
disp:print_tree(h)

print("\nDone.")
