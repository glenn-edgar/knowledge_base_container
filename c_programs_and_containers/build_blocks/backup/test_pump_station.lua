--[[
    Scan Tree DSL Test - Pump System Example (LuaJIT)

    Simulates a simple pump station with:
      - 4 pumps with fault detection
      - Power monitoring
      - Redundancy (2-of-2 groups with OR)
      - Fault latching with operator clear

    Hierarchy:
      Level 0: Power
        - Grid power and backup power status
      Level 1: Actuation
        - Sub-level: Pump Group A (pumps 0-1)
        - Sub-level: Pump Group B (pumps 2-3)
        - Level output: overall pump status
--]]

local scan_tree = require("scan_tree_dsl")
local vft = require("vft_helpers")

local ScanTreeDSL = scan_tree.ScanTreeDSL

-- ==================================================================
-- Create DSL instance
-- ==================================================================
local dsl = ScanTreeDSL.new()

local tree = dsl:SCAN_TREE_start("pump_station")

-- ==================================================================
-- Raw Buffers
-- ==================================================================

-- Pump fault signals (from I/O)
local pump_faults = dsl:define_buffer(
    "pump_faults", "bool", 4,
    "Pump fault signals from field instruments",
    {source = "plc_rack_1", scan_group = "fast"})
dsl:define_pin(pump_faults, "pump_0_fault", 0, "Pump 0 fault signal")
dsl:define_pin(pump_faults, "pump_1_fault", 1, "Pump 1 fault signal")
dsl:define_pin(pump_faults, "pump_2_fault", 2, "Pump 2 fault signal")
dsl:define_pin(pump_faults, "pump_3_fault", 3, "Pump 3 fault signal")
dsl:end_buffer(pump_faults)

-- Pump running signals (from I/O)
local pump_running = dsl:define_buffer(
    "pump_running", "bool", 4,
    "Pump running feedback from field",
    {source = "plc_rack_1", scan_group = "fast"})
dsl:define_pin(pump_running, "pump_0_run", 0, "Pump 0 running")
dsl:define_pin(pump_running, "pump_1_run", 1, "Pump 1 running")
dsl:define_pin(pump_running, "pump_2_run", 2, "Pump 2 running")
dsl:define_pin(pump_running, "pump_3_run", 3, "Pump 3 running")
dsl:end_buffer(pump_running)

-- Power status signals
local power_status = dsl:define_buffer(
    "power_status", "bool", 2,
    "Power source status",
    {source = "plc_rack_2", scan_group = "fast"})
dsl:define_pin(power_status, "grid_power", 0, "Grid power available")
dsl:define_pin(power_status, "backup_power", 1, "Backup power available")
dsl:end_buffer(power_status)

-- Pressure readings
local pressure = dsl:define_buffer(
    "pressure", "float", 2,
    "Pressure transmitter readings",
    {source = "plc_rack_1", scan_group = "normal", units = "psi"})
dsl:define_pin(pressure, "discharge_pressure", 0, "Discharge header pressure")
dsl:define_pin(pressure, "suction_pressure", 1, "Suction header pressure")
dsl:end_buffer(pressure)

-- Pressure thresholds (configuration)
local pressure_thresholds = dsl:define_buffer(
    "pressure_thresholds", "float", 2,
    "Pressure alarm thresholds",
    {source = "config", scan_group = "static"})
dsl:define_pin(pressure_thresholds, "low_discharge", 0, "Low discharge pressure threshold")
dsl:define_pin(pressure_thresholds, "high_suction", 1, "High suction pressure threshold")
dsl:end_buffer(pressure_thresholds)

-- Operator alarm clear (from controller)
local alarm_clear = dsl:define_buffer(
    "alarm_clear", "bool", 4,
    "Operator alarm clear commands from controller",
    {source = "controller", scan_group = "on_demand"})
dsl:define_pin(alarm_clear, "clear_pump_0", 0, "Clear pump 0 alarm")
dsl:define_pin(alarm_clear, "clear_pump_1", 1, "Clear pump 1 alarm")
dsl:define_pin(alarm_clear, "clear_pump_2", 2, "Clear pump 2 alarm")
dsl:define_pin(alarm_clear, "clear_pump_3", 3, "Clear pump 3 alarm")
dsl:end_buffer(alarm_clear)

-- Voting thresholds (configuration)
local voting_config = dsl:define_buffer(
    "voting_config", "uint8_t", 1,
    "Voting threshold configuration",
    {source = "config", scan_group = "static"})
dsl:define_pin(voting_config, "pump_group_k", 0, "K value for pump group voting (k-of-n)")
dsl:end_buffer(voting_config)

-- ==================================================================
-- Level 0: Power
-- ==================================================================
print("--- Level 0: Power ---")

local l0 = dsl:SCAN_TREE_level_start("power")

-- Level output buffer: power status summary
local power_out = dsl:define_buffer(
    "power_output", "bool", 1,
    "Power availability summary")
dsl:define_pin(power_out, "power_available", 0, "At least one power source available")
dsl:end_buffer(power_out)

-- Power is available if grid OR backup is available
dsl:instantiate_vft(vft.VFT_or, "power_output:0-1", "power_status:0-2")

dsl:SCAN_TREE_level_end(l0)

-- ==================================================================
-- Level 1: Actuation (Pumps)
-- ==================================================================
print("--- Level 1: Actuation ---")

local l1 = dsl:SCAN_TREE_level_start("actuation")

-- Level output buffer: overall pump system status
local actuation_out = dsl:define_buffer(
    "actuation_output", "bool", 2,
    "Pump system status summary")
dsl:define_pin(actuation_out, "pumps_ok", 0, "At least one pump group healthy")
dsl:define_pin(actuation_out, "has_power", 1, "Power prerequisite met")
dsl:end_buffer(actuation_out)

-- --- Sub-level: Pump Group A (pumps 0-1) ---
local sl_a = dsl:SCAN_TREE_sub_level("group_a")

local group_a_out = dsl:define_buffer(
    "group_a_output", "bool", 3,
    "Pump Group A status")
dsl:define_pin(group_a_out, "pump_0_healthy", 0, "Pump 0 healthy (not faulted)")
dsl:define_pin(group_a_out, "pump_1_healthy", 1, "Pump 1 healthy (not faulted)")
dsl:define_pin(group_a_out, "group_a_ok", 2, "Group A has sufficient pumps")
dsl:end_buffer(group_a_out)

-- Pump 0: latch fault, clear from operator
dsl:instantiate_vft(vft.VFT_latch, "group_a_output:0-1", "pump_faults:0-1", "alarm_clear:0-1")
-- Pump 1: latch fault, clear from operator
dsl:instantiate_vft(vft.VFT_latch, "group_a_output:1-1", "pump_faults:1-1", "alarm_clear:1-1")
-- Group A OK if at least 1 of 2 pumps healthy
dsl:instantiate_vft(vft.VFT_or, "group_a_output:2-1", "group_a_output:0-2")

dsl:SCAN_TREE_sub_level_end(sl_a)

-- --- Sub-level: Pump Group B (pumps 2-3) ---
local sl_b = dsl:SCAN_TREE_sub_level("group_b")

local group_b_out = dsl:define_buffer(
    "group_b_output", "bool", 3,
    "Pump Group B status")
dsl:define_pin(group_b_out, "pump_2_healthy", 0, "Pump 2 healthy (not faulted)")
dsl:define_pin(group_b_out, "pump_3_healthy", 1, "Pump 3 healthy (not faulted)")
dsl:define_pin(group_b_out, "group_b_ok", 2, "Group B has sufficient pumps")
dsl:end_buffer(group_b_out)

-- Pump 2: latch fault, clear from operator
dsl:instantiate_vft(vft.VFT_latch, "group_b_output:0-1", "pump_faults:2-1", "alarm_clear:2-1")
-- Pump 3: latch fault, clear from operator
dsl:instantiate_vft(vft.VFT_latch, "group_b_output:1-1", "pump_faults:3-1", "alarm_clear:3-1")
-- Group B OK if at least 1 of 2 pumps healthy
dsl:instantiate_vft(vft.VFT_or, "group_b_output:2-1", "group_b_output:0-2")

dsl:SCAN_TREE_sub_level_end(sl_b)

-- --- Level virtual functions (use sub-level outputs) ---

-- Pumps OK if group A is OK
dsl:instantiate_vft(vft.VFT_or, "actuation_output:0-1", "group_a_output:2-1")

-- Power prerequisite: read from level 0 output
dsl:instantiate_vft(vft.VFT_and, "actuation_output:1-1", "power_output:0-1")

dsl:SCAN_TREE_level_end(l1)

-- ==================================================================
-- Finalize
-- ==================================================================
dsl:SCAN_TREE_end(tree)

-- ==================================================================
-- Output JSON
-- ==================================================================
local script_dir = arg[0]:match("(.*/)")  or "./"
local output_path = script_dir .. "pump_station_lua.json"
dsl:write_json(output_path)

-- ==================================================================
-- Print summary
-- ==================================================================
print("")
print("=== Scan Tree Construction Summary ===")
print(string.format("Raw buffers: %d", #dsl._json_data.raw_buffers))
print(string.format("Levels: %d", #dsl._json_data.levels))

for _, level in ipairs(dsl._json_data.levels) do
    print(string.format("  %s (level %d)", level.name, level.level_number))
    local ob_name = level.output_buffer and level.output_buffer.name or "None"
    print(string.format("    Output buffer: %s", ob_name))
    print(string.format("    Sub-levels: %d", #level.sub_levels))
    print(string.format("    Virtual functions: %d", #level.virtual_functions))
    for _, sub in ipairs(level.sub_levels) do
        local sub_ob = sub.output_buffer and sub.output_buffer.name or "None"
        print(string.format("      %s", sub.name))
        print(string.format("        Output buffer: %s", sub_ob))
        print(string.format("        Virtual functions: %d", #sub.virtual_functions))
    end
end

print(string.format("\nJSON written to: %s", output_path))
print("Done.")

