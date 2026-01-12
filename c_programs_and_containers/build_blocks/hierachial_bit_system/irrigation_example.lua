--[[
  irrigation_valves.lua - Irrigation Valve Status Schema
  
  Three buffer types:
    - ALARM_LATCHED : OR_LATCH - alarms latch until cleared
    - ALARM_MASK    : OR_MASK  - alarms with mask for selective propagation
    - AND_LATCHED   : AND      - all children must have bit set
]]

local S = require("schema_builder")

-- Define the schema
S.schema("irrigation_valves", "1.0.0")

-- Define the three buffer types
S.buffer("ALARM_LATCHED", "OR_LATCH")  -- Alarms that latch until cleared
S.buffer("ALARM_MASK",    "OR_MASK")   -- Alarms with mask for selective propagation
S.buffer("AND_LATCHED",   "AND")       -- All children must have bit set

-- Define leaf classes (actual valve banks)
S.class("Valve_Bank_Leaf", {ALARM_LATCHED = 8, ALARM_MASK = 8, AND_LATCHED = 0})
  S.bits("ALARM_LATCHED", "overcurrent", "stuck_open", "stuck_closed", "leak", 
                          "overtemp", "comm_fail", "low_pressure", "high_pressure")
  S.bits("ALARM_MASK", "overcurrent", "stuck_open", "stuck_closed", "leak",
                       "overtemp", "comm_fail", "low_pressure", "high_pressure")
S.end_class()

S.class("AND_Valve_Bank_Leaf", {ALARM_LATCHED = 0, ALARM_MASK = 0, AND_LATCHED = 8})
  S.bits("AND_LATCHED", "powered", "calibrated", "enabled", "ready",
                        "comm_ok", "pressure_ok", "flow_ok", "position_ok")
S.end_class()

-- Aggregate classes are auto-generated from children's max bank sizes

-- Define the node tree
-- VALVE_STATUS tree (OR_LATCH and OR_MASK alarms)
S.node("VALVE_STATUS", "Valve_Aggregate")

  S.node("STATION_1_VALVE_STATUS", "Valve_Aggregate")
    S.node("BANK_1_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_4_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_2_VALVE_STATUS", "Valve_Aggregate")
    S.node("BANK_1_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_3_VALVE_STATUS", "Valve_Aggregate")
    S.node("BANK_1_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_4_VALVE_STATUS", "Valve_Aggregate")
    S.node("BANK_1_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATUS", "Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

S.end_node()

-- VALVE_STATE tree (AND logic for ready states)
S.node("VALVE_STATE", "AND_Valve_Aggregate")

  S.node("STATION_1_VALVE_STATE", "AND_Valve_Aggregate")
    S.node("BANK_1_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_4_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_2_VALVE_STATE", "AND_Valve_Aggregate")
    S.node("BANK_1_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_3_VALVE_STATE", "AND_Valve_Aggregate")
    S.node("BANK_1_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

  S.node("STATION_4_VALVE_STATE", "AND_Valve_Aggregate")
    S.node("BANK_1_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_2_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
    S.node("BANK_3_VALVE_STATE", "AND_Valve_Bank_Leaf")
    S.end_node()
  S.end_node()

S.end_node()

-- Debug output
S.dump()

-- Return the built schema
return S.build()