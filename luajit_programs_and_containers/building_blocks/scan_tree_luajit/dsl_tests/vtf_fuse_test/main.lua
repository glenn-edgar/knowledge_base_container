#!/usr/bin/env luajit
--[[
    VTF Fuse Test - LuaJIT Scan Tree Runtime

    Port of scan_tree_c/dsl_tests/vtf_fuse_test/main.c
    4-level water treatment plant with VFT_fuse.
    Exercises: normal startup, overcurrent trip, fuse persistence,
               operator clear cycle, safety interlock, power loss.
--]]

local script_dir = arg[0]:match("(.*/)") or "./"
local runtime_dir = script_dir .. "../../"
package.path = runtime_dir .. "?.lua;" .. script_dir .. "?.lua;" .. package.path

local st_runtime = require("st_runtime")
local st_display = require("st_display")
local Handle = st_runtime.Handle
local Display = st_display.Display

local make_desc = require("vtf_fuse_test")
local desc = make_desc()
local IDS = desc.IDS

-- Fuse action callbacks (resolve string names to functions)
local fuse_log = {}
local function make_fuse_action(name)
    return function(user_handle)
        fuse_log[#fuse_log + 1] = name
        io.write(string.format("  ** FUSE ACTION: %s **\n", name))
    end
end

-- Replace string fuse actions with callable functions
for nid, action_name in pairs(desc.fuse_table) do
    desc.fuse_table[nid] = make_fuse_action(action_name)
end

local h = Handle.new(desc)
local disp = Display.new(h)

-- Cache raw buffers
local power    = h:raw_current(IDS.VTF_FUSE_TEST_POWER_INPUTS)
local safety   = h:raw_current(IDS.VTF_FUSE_TEST_SAFETY_INPUTS)
local pump_cur = h:raw_current(IDS.VTF_FUSE_TEST_PUMP_CURRENT)
local pump_lim = h:raw_current(IDS.VTF_FUSE_TEST_PUMP_LIMITS)
local cl_level = h:raw_current(IDS.VTF_FUSE_TEST_CHLORINE_LEVEL)
local cl_max   = h:raw_current(IDS.VTF_FUSE_TEST_CHLORINE_MAX)
local fuse_clr = h:raw_current(IDS.VTF_FUSE_TEST_FUSE_CLEAR)

h:print_info()

-- Step 0: Initial
print("\n--- Step 0: Initial (all NOT_OP) ---")
disp:print_tree(h)

-- Step 1: Normal startup
print("\n--- Step 1: Normal startup ---")
power[0] = 1; power[1] = 1; power[2] = 1
safety[0] = 0; safety[1] = 0; safety[2] = 0
pump_cur[0] = 10; pump_cur[1] = 12; pump_cur[2] = 8; pump_cur[3] = 9
pump_lim[0] = 50; pump_lim[1] = 50; pump_lim[2] = 50; pump_lim[3] = 50
cl_level[0] = 2; cl_max[0] = 5
h:cycle()
disp:print_tree(h)

-- Step 2: Intake pump 0 overcurrent
print("\n--- Step 2: Intake pump 0 overcurrent (80A > 50A limit) ---")
pump_cur[0] = 80
h:cycle()
disp:print_tree(h)

-- Step 3: Current drops but fuse stays blown
print("\n--- Step 3: Current drops to 10A — fuse still blown ---")
pump_cur[0] = 10
h:cycle()
disp:print_tree(h)

-- Step 4: Operator clear cycle — assert clear high
print("\n--- Step 4: Operator asserts clear for intake pump 0 ---")
fuse_clr[0] = 1
h:cycle()
disp:print_tree(h)

-- Step 5: Operator releases clear — fuse re-arms
print("\n--- Step 5: Operator releases clear — fuse re-armed ---")
fuse_clr[0] = 0
h:cycle()
disp:print_tree(h)

-- Step 6: Chlorine alarm + dist pump 1 overcurrent
print("\n--- Step 6: Chlorine high (8 ppm) + dist pump 1 overcurrent (70A) ---")
cl_level[0] = 8
pump_cur[3] = 70
h:cycle()
disp:print_tree(h)

-- Step 7: Emergency stop pressed
print("\n--- Step 7: Emergency stop pressed ---")
safety[0] = 1
h:cycle()
disp:print_tree(h)

-- Step 8: E-stop released, clear all fuses
print("\n--- Step 8: E-stop released, assert all clears ---")
safety[0] = 0
cl_level[0] = 2
pump_cur[3] = 9
fuse_clr[2] = 1
fuse_clr[4] = 1
h:cycle()
disp:print_tree(h)

-- Step 9: Release all clears — fuses re-arm
print("\n--- Step 9: Release all clears — full recovery ---")
fuse_clr[2] = 0
fuse_clr[4] = 0
h:cycle()
disp:print_tree(h)

-- Step 10: Power loss
print("\n--- Step 10: Total power loss ---")
power[0] = 0; power[1] = 0; power[2] = 0
h:cycle()
disp:print_tree(h)

print("\nDone.")
