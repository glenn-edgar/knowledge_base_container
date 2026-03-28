--[[
    Water Treatment Plant - 4-Level Scan Tree with VFT_fuse

    Level 3 (top):  plant_status    — overall plant health
    Level 2:        process         — intake, treatment, distribution
    Level 1:        equipment       — pumps, chemical dosing, valves
    Level 0:        infrastructure  — power, safety interlocks

    Fuses on:
      - intake pump overcurrent (2 pumps)
      - chlorine dosing high-level alarm
      - distribution pump overcurrent (2 pumps)
--]]

local scan_tree = require("scan_tree_dsl")
local vft = require("vft_helpers")
local ScanTreeDSL = scan_tree.ScanTreeDSL

local dsl = ScanTreeDSL.new()
local tree = dsl:SCAN_TREE_start("vtf_fuse_test")

-- =====================================================================
-- Raw I/O Buffers
-- =====================================================================

local power_inputs = dsl:define_buffer("power_inputs", "bool", 3, "Power source status")
dsl:define_pin(power_inputs, "grid",      0, "Grid power")
dsl:define_pin(power_inputs, "generator", 1, "Backup generator")
dsl:define_pin(power_inputs, "ups",       2, "UPS status")
dsl:end_buffer(power_inputs)

local safety_inputs = dsl:define_buffer("safety_inputs", "bool", 3, "Safety interlock signals")
dsl:define_pin(safety_inputs, "e_stop",     0, "Emergency stop")
dsl:define_pin(safety_inputs, "high_water", 1, "High water level alarm")
dsl:define_pin(safety_inputs, "gas_detect", 2, "Gas detection alarm")
dsl:end_buffer(safety_inputs)

local pump_current = dsl:define_buffer("pump_current", "float", 4, "Pump motor current",
    {units = "amps"})
dsl:define_pin(pump_current, "intake_p0",  0, "Intake pump 0 amps")
dsl:define_pin(pump_current, "intake_p1",  1, "Intake pump 1 amps")
dsl:define_pin(pump_current, "dist_p0",    2, "Distribution pump 0 amps")
dsl:define_pin(pump_current, "dist_p1",    3, "Distribution pump 1 amps")
dsl:end_buffer(pump_current)

local pump_limits = dsl:define_buffer("pump_limits", "float", 4, "Pump current limits",
    {units = "amps"})
dsl:define_pin(pump_limits, "intake_p0_max",  0, "Intake pump 0 limit")
dsl:define_pin(pump_limits, "intake_p1_max",  1, "Intake pump 1 limit")
dsl:define_pin(pump_limits, "dist_p0_max",    2, "Distribution pump 0 limit")
dsl:define_pin(pump_limits, "dist_p1_max",    3, "Distribution pump 1 limit")
dsl:end_buffer(pump_limits)

local chlorine_level = dsl:define_buffer("chlorine_level", "float", 1, "Chlorine dosing level",
    {units = "ppm"})
dsl:define_pin(chlorine_level, "level", 0, "Current chlorine level")
dsl:end_buffer(chlorine_level)

local chlorine_max = dsl:define_buffer("chlorine_max", "float", 1, "Chlorine max threshold",
    {units = "ppm"})
dsl:define_pin(chlorine_max, "max_ppm", 0, "Max allowable ppm")
dsl:end_buffer(chlorine_max)

local fuse_clear = dsl:define_buffer("fuse_clear", "bool", 5, "Operator fuse clear signals")
dsl:define_pin(fuse_clear, "clr_intake_p0",  0, "Clear intake pump 0")
dsl:define_pin(fuse_clear, "clr_intake_p1",  1, "Clear intake pump 1")
dsl:define_pin(fuse_clear, "clr_chlorine",   2, "Clear chlorine alarm")
dsl:define_pin(fuse_clear, "clr_dist_p0",    3, "Clear dist pump 0")
dsl:define_pin(fuse_clear, "clr_dist_p1",    4, "Clear dist pump 1")
dsl:end_buffer(fuse_clear)

-- =====================================================================
-- Level 0: Infrastructure (power + safety)
-- =====================================================================
local l0 = dsl:SCAN_TREE_level_start("infrastructure")

local infra_out = dsl:define_buffer("infra_output", "bool", 3, "Infrastructure status")
dsl:define_pin(infra_out, "power_ok",  0, "Any power source available")
dsl:define_pin(infra_out, "safety_ok", 1, "No safety violations")
dsl:define_pin(infra_out, "infra_ok",  2, "Infrastructure overall")
dsl:end_buffer(infra_out)

-- power_ok = any power source up
dsl:instantiate_vft(vft.VFT_or, "infra_output:0-1", "power_inputs:0-3")
-- safety_ok = NOT e_stop AND NOT high_water AND NOT gas_detect
-- (safety inputs are active-high alarms, so all must be 0 for safe)
-- We use: safety_ok = NOR of all safety inputs
-- NOR = NOT(OR(all))
local sl_safety = dsl:SCAN_TREE_sub_level("safety_check")
local safety_scratch = dsl:define_buffer("safety_scratch", "bool", 1, "Safety scratch")
dsl:define_pin(safety_scratch, "any_alarm", 0, "Any safety alarm active")
dsl:end_buffer(safety_scratch)
dsl:instantiate_vft(vft.VFT_or, "safety_scratch:0-1", "safety_inputs:0-3")
dsl:SCAN_TREE_sub_level_end(sl_safety)

dsl:instantiate_vft(vft.VFT_not, "infra_output:1-1", "safety_scratch:0-1")
-- infra_ok = power AND safety
dsl:instantiate_vft(vft.VFT_and, "infra_output:2-1", "infra_output:0-2")

dsl:SCAN_TREE_level_end(l0)

-- =====================================================================
-- Level 1: Equipment groups
-- =====================================================================
local l1 = dsl:SCAN_TREE_level_start("equipment")

local equip_out = dsl:define_buffer("equip_output", "bool", 4, "Equipment status")
dsl:define_pin(equip_out, "intake_ok",    0, "Intake pumps OK")
dsl:define_pin(equip_out, "dosing_ok",    1, "Chemical dosing OK")
dsl:define_pin(equip_out, "dist_ok",      2, "Distribution pumps OK")
dsl:define_pin(equip_out, "has_infra",    3, "Infrastructure prerequisite")
dsl:end_buffer(equip_out)

-- Sub-level: Intake pumps with fuses
local sl_intake = dsl:SCAN_TREE_sub_level("intake_pumps")
local intake_out = dsl:define_buffer("intake_output", "bool", 4, "Intake pump status")
dsl:define_pin(intake_out, "p0_overcurrent", 0, "Pump 0 overcurrent detected")
dsl:define_pin(intake_out, "p1_overcurrent", 1, "Pump 1 overcurrent detected")
dsl:define_pin(intake_out, "p0_fused",       2, "Pump 0 fuse blown")
dsl:define_pin(intake_out, "p1_fused",       3, "Pump 1 fuse blown")
dsl:end_buffer(intake_out)

-- Overcurrent detection: current > limit
dsl:instantiate_vft(vft.VFT_gt, "intake_output:0-1", "pump_current:0-1", "pump_limits:0-1")
dsl:instantiate_vft(vft.VFT_gt, "intake_output:1-1", "pump_current:1-1", "pump_limits:1-1")
-- Fuse: trips on overcurrent, needs operator clear
dsl:instantiate_vft(vft.VFT_fuse, "intake_output:2-1", "intake_output:0-1",
    "fuse_clear:0-1", "on_intake_p0_fuse")
dsl:instantiate_vft(vft.VFT_fuse, "intake_output:3-1", "intake_output:1-1",
    "fuse_clear:1-1", "on_intake_p1_fuse")
dsl:SCAN_TREE_sub_level_end(sl_intake)

-- Sub-level: Chemical dosing with fuse
local sl_dosing = dsl:SCAN_TREE_sub_level("dosing")
local dosing_out = dsl:define_buffer("dosing_output", "bool", 2, "Dosing status")
dsl:define_pin(dosing_out, "cl_high",  0, "Chlorine over-limit")
dsl:define_pin(dosing_out, "cl_fused", 1, "Chlorine fuse blown")
dsl:end_buffer(dosing_out)

dsl:instantiate_vft(vft.VFT_gt, "dosing_output:0-1", "chlorine_level:0-1", "chlorine_max:0-1")
dsl:instantiate_vft(vft.VFT_fuse, "dosing_output:1-1", "dosing_output:0-1",
    "fuse_clear:2-1", "on_chlorine_fuse")
dsl:SCAN_TREE_sub_level_end(sl_dosing)

-- Sub-level: Distribution pumps with fuses
local sl_dist = dsl:SCAN_TREE_sub_level("dist_pumps")
local dist_out = dsl:define_buffer("dist_output", "bool", 4, "Distribution pump status")
dsl:define_pin(dist_out, "p0_overcurrent", 0, "Dist pump 0 overcurrent")
dsl:define_pin(dist_out, "p1_overcurrent", 1, "Dist pump 1 overcurrent")
dsl:define_pin(dist_out, "p0_fused",       2, "Dist pump 0 fuse blown")
dsl:define_pin(dist_out, "p1_fused",       3, "Dist pump 1 fuse blown")
dsl:end_buffer(dist_out)

dsl:instantiate_vft(vft.VFT_gt, "dist_output:0-1", "pump_current:2-1", "pump_limits:2-1")
dsl:instantiate_vft(vft.VFT_gt, "dist_output:1-1", "pump_current:3-1", "pump_limits:3-1")
dsl:instantiate_vft(vft.VFT_fuse, "dist_output:2-1", "dist_output:0-1",
    "fuse_clear:3-1", "on_dist_p0_fuse")
dsl:instantiate_vft(vft.VFT_fuse, "dist_output:3-1", "dist_output:1-1",
    "fuse_clear:4-1", "on_dist_p1_fuse")
dsl:SCAN_TREE_sub_level_end(sl_dist)

-- Level-scope: aggregate equipment status
-- intake_ok = no intake fuses blown (NOT(p0_fused OR p1_fused))
-- We combine: intake_ok = NOR of fuse bits
local sl_intake_agg = dsl:SCAN_TREE_sub_level("intake_agg")
local intake_scratch = dsl:define_buffer("intake_scratch", "bool", 1, "Intake scratch")
dsl:define_pin(intake_scratch, "any_fused", 0, "Any intake fuse blown")
dsl:end_buffer(intake_scratch)
dsl:instantiate_vft(vft.VFT_or, "intake_scratch:0-1", "intake_output:2-2")
dsl:SCAN_TREE_sub_level_end(sl_intake_agg)
dsl:instantiate_vft(vft.VFT_not, "equip_output:0-1", "intake_scratch:0-1")

-- dosing_ok = NOT chlorine fuse blown
dsl:instantiate_vft(vft.VFT_not, "equip_output:1-1", "dosing_output:1-1")

-- dist_ok = no dist fuses blown
local sl_dist_agg = dsl:SCAN_TREE_sub_level("dist_agg")
local dist_scratch = dsl:define_buffer("dist_scratch", "bool", 1, "Dist scratch")
dsl:define_pin(dist_scratch, "any_fused", 0, "Any dist fuse blown")
dsl:end_buffer(dist_scratch)
dsl:instantiate_vft(vft.VFT_or, "dist_scratch:0-1", "dist_output:2-2")
dsl:SCAN_TREE_sub_level_end(sl_dist_agg)
dsl:instantiate_vft(vft.VFT_not, "equip_output:2-1", "dist_scratch:0-1")

-- has_infra = infrastructure OK
dsl:instantiate_vft(vft.VFT_copy, "equip_output:3-1", "infra_output:2-1")

dsl:SCAN_TREE_level_end(l1)

-- =====================================================================
-- Level 2: Process stages
-- =====================================================================
local l2 = dsl:SCAN_TREE_level_start("process")

local proc_out = dsl:define_buffer("process_output", "bool", 3, "Process stage status")
dsl:define_pin(proc_out, "intake_ready",  0, "Intake process ready")
dsl:define_pin(proc_out, "treat_ready",   1, "Treatment process ready")
dsl:define_pin(proc_out, "dist_ready",    2, "Distribution process ready")
dsl:end_buffer(proc_out)

-- intake_ready = intake_ok AND has_infra
dsl:instantiate_vft(vft.VFT_and, "process_output:0-1", "equip_output:0-1")
-- BUT we need both intake_ok AND has_infra... wait, VFT_and takes a range.
-- Let's use: intake_ready = AND(equip_output[0], equip_output[3]) — need both pins.
-- Actually VFT_and reads a contiguous range. We need a 2-input AND.
-- Use sub-level scratch for combining non-contiguous pins.

local sl_intake_rdy = dsl:SCAN_TREE_sub_level("intake_ready_check")
local intake_rdy_scratch = dsl:define_buffer("intake_rdy_scratch", "bool", 2, "Intake ready scratch")
dsl:define_pin(intake_rdy_scratch, "pumps",  0, "Intake pumps ok")
dsl:define_pin(intake_rdy_scratch, "infra",  1, "Infrastructure ok")
dsl:end_buffer(intake_rdy_scratch)
dsl:instantiate_vft(vft.VFT_copy, "intake_rdy_scratch:0-1", "equip_output:0-1")
dsl:instantiate_vft(vft.VFT_copy, "intake_rdy_scratch:1-1", "equip_output:3-1")
dsl:SCAN_TREE_sub_level_end(sl_intake_rdy)
dsl:instantiate_vft(vft.VFT_and, "process_output:0-1", "intake_rdy_scratch:0-2")

-- treat_ready = dosing_ok AND has_infra
local sl_treat_rdy = dsl:SCAN_TREE_sub_level("treat_ready_check")
local treat_rdy_scratch = dsl:define_buffer("treat_rdy_scratch", "bool", 2, "Treat ready scratch")
dsl:define_pin(treat_rdy_scratch, "dosing", 0, "Dosing ok")
dsl:define_pin(treat_rdy_scratch, "infra",  1, "Infrastructure ok")
dsl:end_buffer(treat_rdy_scratch)
dsl:instantiate_vft(vft.VFT_copy, "treat_rdy_scratch:0-1", "equip_output:1-1")
dsl:instantiate_vft(vft.VFT_copy, "treat_rdy_scratch:1-1", "equip_output:3-1")
dsl:SCAN_TREE_sub_level_end(sl_treat_rdy)
dsl:instantiate_vft(vft.VFT_and, "process_output:1-1", "treat_rdy_scratch:0-2")

-- dist_ready = dist_ok AND has_infra
local sl_dist_rdy = dsl:SCAN_TREE_sub_level("dist_ready_check")
local dist_rdy_scratch = dsl:define_buffer("dist_rdy_scratch", "bool", 2, "Dist ready scratch")
dsl:define_pin(dist_rdy_scratch, "pumps", 0, "Dist pumps ok")
dsl:define_pin(dist_rdy_scratch, "infra", 1, "Infrastructure ok")
dsl:end_buffer(dist_rdy_scratch)
dsl:instantiate_vft(vft.VFT_copy, "dist_rdy_scratch:0-1", "equip_output:2-1")
dsl:instantiate_vft(vft.VFT_copy, "dist_rdy_scratch:1-1", "equip_output:3-1")
dsl:SCAN_TREE_sub_level_end(sl_dist_rdy)
dsl:instantiate_vft(vft.VFT_and, "process_output:2-1", "dist_rdy_scratch:0-2")

dsl:SCAN_TREE_level_end(l2)

-- =====================================================================
-- Level 3: Plant status (top level)
-- =====================================================================
local l3 = dsl:SCAN_TREE_level_start("plant_status")

local plant_out = dsl:define_buffer("plant_output", "bool", 2, "Plant status")
dsl:define_pin(plant_out, "operational", 0, "Plant can operate")
dsl:define_pin(plant_out, "full_capacity", 1, "All processes ready")
dsl:end_buffer(plant_out)

-- operational = at least one process ready (OR)
dsl:instantiate_vft(vft.VFT_or, "plant_output:0-1", "process_output:0-3")
-- full_capacity = all processes ready (AND)
dsl:instantiate_vft(vft.VFT_and, "plant_output:1-1", "process_output:0-3")

dsl:SCAN_TREE_level_end(l3)

dsl:SCAN_TREE_end(tree)

-- Write JSON
local output_path = arg[1] or "vtf_fuse_test.json"
print("output_path: " .. output_path)
--local out_path = dir .. "water_plant.json"
dsl:write_json(output_path)

dsl:print_summary()
-- Summary
