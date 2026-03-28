--[[
    Scan Tree Test: Pump Station with System + User VFTs

    This is the DSL source file — run via st_build.sh to produce:
      1. pump_test.json (intermediate)
      2. pump_station.lua (generated const descriptor)
--]]

local scan_tree = require("scan_tree_dsl")
local vft = require("vft_helpers")
local ScanTreeDSL = scan_tree.ScanTreeDSL

-- Register a user VFT
local VFT_motor_check = vft.user_vft("motor_health_check", {
    {name = "current",   type = "float", count = 1},
    {name = "threshold", type = "float", count = 1},
})

local dsl = ScanTreeDSL.new()

local tree = dsl:SCAN_TREE_start("pump_station")

-- Raw buffers
local pump_faults = dsl:define_buffer("pump_faults", "bool", 4, "Pump fault signals")
dsl:define_pin(pump_faults, "p0_fault", 0, "Pump 0 fault")
dsl:define_pin(pump_faults, "p1_fault", 1, "Pump 1 fault")
dsl:define_pin(pump_faults, "p2_fault", 2, "Pump 2 fault")
dsl:define_pin(pump_faults, "p3_fault", 3, "Pump 3 fault")
dsl:end_buffer(pump_faults)

local power_status = dsl:define_buffer("power_status", "bool", 2, "Power source status")
dsl:define_pin(power_status, "grid_power", 0, "Grid power")
dsl:define_pin(power_status, "backup_power", 1, "Backup power")
dsl:end_buffer(power_status)

local alarm_clear = dsl:define_buffer("alarm_clear", "bool", 4, "Operator alarm clear")
dsl:define_pin(alarm_clear, "clr_p0", 0, "Clear pump 0")
dsl:define_pin(alarm_clear, "clr_p1", 1, "Clear pump 1")
dsl:define_pin(alarm_clear, "clr_p2", 2, "Clear pump 2")
dsl:define_pin(alarm_clear, "clr_p3", 3, "Clear pump 3")
dsl:end_buffer(alarm_clear)

local motor_current = dsl:define_buffer("motor_current", "float", 4, "Motor current readings",
    {units = "amps"})
dsl:define_pin(motor_current, "p0_amps", 0, "Pump 0 amps")
dsl:define_pin(motor_current, "p1_amps", 1, "Pump 1 amps")
dsl:define_pin(motor_current, "p2_amps", 2, "Pump 2 amps")
dsl:define_pin(motor_current, "p3_amps", 3, "Pump 3 amps")
dsl:end_buffer(motor_current)

local motor_thresholds = dsl:define_buffer("motor_thresholds", "float", 4,
    "Motor overcurrent thresholds", {units = "amps"})
dsl:define_pin(motor_thresholds, "p0_max", 0, "Pump 0 max")
dsl:define_pin(motor_thresholds, "p1_max", 1, "Pump 1 max")
dsl:define_pin(motor_thresholds, "p2_max", 2, "Pump 2 max")
dsl:define_pin(motor_thresholds, "p3_max", 3, "Pump 3 max")
dsl:end_buffer(motor_thresholds)

-- Level 0: Power
local l0 = dsl:SCAN_TREE_level_start("power")
local power_out = dsl:define_buffer("power_output", "bool", 1, "Power summary")
dsl:define_pin(power_out, "power_ok", 0, "At least one source available")
dsl:end_buffer(power_out)
dsl:instantiate_vft(vft.VFT_or, "power_output:0-1", "power_status:0-2")
dsl:SCAN_TREE_level_end(l0)

-- Level 1: Actuation
local l1 = dsl:SCAN_TREE_level_start("actuation")
local act_out = dsl:define_buffer("actuation_output", "bool", 2, "Pump system status")
dsl:define_pin(act_out, "pumps_ok", 0, "Pumps healthy")
dsl:define_pin(act_out, "has_power", 1, "Power prerequisite")
dsl:end_buffer(act_out)

-- Sub-level: Group A (pumps 0-1)
local sl_a = dsl:SCAN_TREE_sub_level("group_a")
local ga_out = dsl:define_buffer("group_a_output", "bool", 3, "Group A status")
dsl:define_pin(ga_out, "p0_healthy", 0, "Pump 0 healthy")
dsl:define_pin(ga_out, "p1_healthy", 1, "Pump 1 healthy")
dsl:define_pin(ga_out, "ga_ok", 2, "Group A OK")
dsl:end_buffer(ga_out)
dsl:instantiate_vft(VFT_motor_check, "group_a_output:0-1", "motor_current:0-1", "motor_thresholds:0-1")
dsl:instantiate_vft(VFT_motor_check, "group_a_output:1-1", "motor_current:1-1", "motor_thresholds:1-1")
dsl:instantiate_vft(vft.VFT_or, "group_a_output:2-1", "group_a_output:0-2")
dsl:SCAN_TREE_sub_level_end(sl_a)

-- Sub-level: Group B (pumps 2-3)
local sl_b = dsl:SCAN_TREE_sub_level("group_b")
local gb_out = dsl:define_buffer("group_b_output", "bool", 3, "Group B status")
dsl:define_pin(gb_out, "p2_healthy", 0, "Pump 2 healthy")
dsl:define_pin(gb_out, "p3_healthy", 1, "Pump 3 healthy")
dsl:define_pin(gb_out, "gb_ok", 2, "Group B OK")
dsl:end_buffer(gb_out)
dsl:instantiate_vft(VFT_motor_check, "group_b_output:0-1", "motor_current:2-1", "motor_thresholds:2-1")
dsl:instantiate_vft(VFT_motor_check, "group_b_output:1-1", "motor_current:3-1", "motor_thresholds:3-1")
dsl:instantiate_vft(vft.VFT_or, "group_b_output:2-1", "group_b_output:0-2")
dsl:SCAN_TREE_sub_level_end(sl_b)

-- Level-scope VFTs
dsl:instantiate_vft(vft.VFT_copy, "actuation_output:0-1", "group_a_output:2-1")
dsl:instantiate_vft(vft.VFT_copy, "actuation_output:1-1", "power_output:0-1")

dsl:SCAN_TREE_level_end(l1)

dsl:SCAN_TREE_end(tree)

-- Write JSON
local output_path = arg[1] or "pump_test.json"
dsl:write_json(output_path)
dsl:print_summary()
