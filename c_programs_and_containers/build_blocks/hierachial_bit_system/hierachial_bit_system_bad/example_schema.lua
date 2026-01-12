--[[
  ChainTree Hierarchical Bit Map Schema
  Stack-based DSL - clean, flat structure
  
  Leaf classes only - aggregate classes auto-generated
]]

local S = require("schema_builder")

-- Schema header
local schema = S.start_schema("schema", "ChainBitTreeDemo", "1.0.0")

S.options("max_ram", 8192, "max_rom", 16384, "max_depth", 5)

-- Bitspaces
local bs = S.start_bitspaces("bs")
  S.bitspace_priority("STATE", "ESTOP", "FAULT", "WARNING", "RUNNING", "IDLE")
  S.bitspace_or("ALARM")
  S.bitspace_or_latch("ALARM_LATCHED")
  S.bitspace_or_latch_safe("SAFETY_ALARM")
  S.bitspace_mask("ALARM_ACK", "OR")
  S.bitspace_mask_latch("ALARM_ACK_LATCHED", "OR")
  S.bitspace_and("READY")
  S.bitspace_and_latch("READY_LATCHED")
  S.bitspace_or("INHIBIT")
  S.bitspace_and("PERMIT")
  S.bitspace_or("CMD_REQ")
  S.bitspace_or("CMD_ACK")
  S.bitspace_priority_latch("WORST_STATE", "ESTOP", "FAULT", "WARNING", "RUNNING", "IDLE")
S.end_(bs)

-- Classes (leaf only - aggregates auto-generated from tree structure)
local classes = S.start_classes("classes")

  -- RobotArm - full featured device
  local cls = S.start_class("cls", "RobotArm",
    "STATE", 8,
    "ALARM", 32,
    "ALARM_LATCHED", 32,
    "SAFETY_ALARM", 16,
    "ALARM_ACK", 16,
    "ALARM_ACK_LATCHED", 16,
    "READY", 8,
    "READY_LATCHED", 8,
    "INHIBIT", 8,
    "PERMIT", 8,
    "CMD_REQ", 8,
    "CMD_ACK", 8,
    "WORST_STATE", 8
  )
    local bits = S.start_bits("bits", "CMD_REQ")
      S.bit("Start")
      S.bit("Stop")
      S.bit("Reset")
      S.bit("Home")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "CMD_ACK")
      S.bit("StartAck")
      S.bit("StopAck")
      S.bit("ResetAck")
      S.bit("HomeAck")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "ALARM")
      S.bit("OverTorque")
      S.bit("OverTemp")
      S.bit("CommLost")
      S.bit("EncoderFault")
      S.bit("LowAirPress")
      S.bit("NeedsGrease")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "STATE")
      S.bit("Idle")
      S.bit("Running")
      S.bit("Warning")
      S.bit("Fault")
      S.bit("EStop")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "SAFETY_ALARM")
      S.bit("LightCurtain")
      S.bit("EStopPressed")
      S.bit("DoorOpen")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "READY")
      S.bit("Homed")
      S.bit("Enabled")
      S.bit("NoFaults")
      S.bit("InPosition")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "ALARM_LATCHED")
      S.bit("OverTorque")
      S.bit("OverTemp")
      S.bit("CommLost")
      S.bit("EncoderFault")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "ALARM_ACK")
      S.bit("OverTorqueAck")
      S.bit("OverTempAck")
      S.bit("CommLostAck")
    S.end_(bits)
  S.end_(cls)
  
  -- Conveyor - simpler device
  local cls = S.start_class("cls", "Conveyor",
    "STATE", 8,
    "ALARM", 16,
    "ALARM_LATCHED", 16,
    "SAFETY_ALARM", 8,
    "ALARM_ACK", 8,
    "ALARM_ACK_LATCHED", 8,
    "READY", 8,
    "READY_LATCHED", 8,
    "INHIBIT", 8,
    "PERMIT", 8,
    "CMD_REQ", 8,
    "CMD_ACK", 8,
    "WORST_STATE", 8
  )
    local bits = S.start_bits("bits", "CMD_REQ")
      S.bit("Run")
      S.bit("Stop")
      S.bit("Jog")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "CMD_ACK")
      S.bit("RunAck")
      S.bit("StopAck")
      S.bit("JogAck")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "ALARM")
      S.bit("Jammed")
      S.bit("MotorFault")
      S.bit("SensorFail")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "STATE")
      S.bit("Idle")
      S.bit("Running")
      S.bit("Fault")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "READY")
      S.bit("MotorReady")
      S.bit("PathClear")
    S.end_(bits)
  S.end_(cls)
  
  -- Sensor - minimal device (no CMD_REQ/CMD_ACK)
  local cls = S.start_class("cls", "Sensor",
    "STATE", 8,
    "ALARM", 8,
    "ALARM_LATCHED", 8,
    "SAFETY_ALARM", 0,       -- doesn't participate
    "ALARM_ACK", 0,          -- doesn't participate
    "ALARM_ACK_LATCHED", 0,  -- doesn't participate
    "READY", 8,
    "READY_LATCHED", 8,
    "INHIBIT", 0,            -- doesn't participate
    "PERMIT", 0,             -- doesn't participate
    "CMD_REQ", 0,            -- doesn't participate
    "CMD_ACK", 0,            -- doesn't participate
    "WORST_STATE", 8
  )
    local bits = S.start_bits("bits", "ALARM")
      S.bit("OutOfRange")
      S.bit("CalExpired")
      S.bit("CommFail")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "STATE")
      S.bit("Idle")
      S.bit("Active")
      S.bit("Fault")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "READY")
      S.bit("Calibrated")
      S.bit("InRange")
    S.end_(bits)
  S.end_(cls)

S.end_(classes)

-- Nodes (tree structure)
-- NOTE: Plant, Line, Cell classes will be auto-generated as aggregates
local nodes = S.start_nodes("nodes")

  local plant = S.start_node("plant", "Plant", "Plant")
  
    local line1 = S.start_node("line1", "Line1", "Line")
    
      local cell1 = S.start_node("cell1", "Cell1", "Cell")
        local robot1 = S.start_node("robot1", "Robot1", "RobotArm")
          S.config("Config.Motion.MaxSpeed", 1500)
          S.config("Config.Motion.MaxAccel", 4.0)
          S.config("Config.Safety.Enabled", true)
          S.config("Config.Safety.LimitSwitch", "LS-001")
          S.config("Config.Comm.Enabled", true)
          S.config("Config.Name", "Welder_01")
        S.end_(robot1)
        
        local robot2 = S.start_node("robot2", "Robot2", "RobotArm")
          S.config("Name", "Welder_02")
        S.end_(robot2)
        
        local sensor1 = S.start_node("sensor1", "Sensor1", "Sensor")
        S.end_(sensor1)
        
        local conveyor1 = S.start_node("conveyor1", "Conveyor1", "Conveyor")
        S.end_(conveyor1)
      S.end_(cell1)
      
      local cell2 = S.start_node("cell2", "Cell2", "Cell")
        local robot1 = S.start_node("robot1", "Robot1", "RobotArm")
          S.config("Name", "Painter_01")
        S.end_(robot1)
        
        local sensor1 = S.start_node("sensor1", "Sensor1", "Sensor")
        S.end_(sensor1)
      S.end_(cell2)
      
      local cell3 = S.start_node("cell3", "Cell3", "Cell")
        local conveyor1 = S.start_node("conveyor1", "Conveyor1", "Conveyor")
        S.end_(conveyor1)
        
        local sensor1 = S.start_node("sensor1", "Sensor1", "Sensor")
        S.end_(sensor1)
      S.end_(cell3)
      
    S.end_(line1)
    
  S.end_(plant)

S.end_(nodes)

S.end_(schema)

return S.build()