--[[
    sensor_data.lua - Instance data definitions
    
    This file generates:
    - sensor_data.bin (packet binary)
    - sensor_data_data.h (embedded binary blob)
    
    Requires sensor_msgs.lua to be run first (for schema definitions)
    
    Note: Must have exactly one instance per record type defined in schema.
]]--

require("avro_dsl").export_globals()

-- Load schema definitions
dofile("sensor_msgs.lua")

--------------------------------------------------------------------------------
-- User-defined constants (Lua handles symbolic names)
--------------------------------------------------------------------------------

local SENSOR_TYPE = {
    IDLE        = 0,
    TEMPERATURE = 1,
    PRESSURE    = 2,
    HUMIDITY    = 3,
    LIGHT       = 4,
}

local SENSOR_STATE = {
    OFFLINE = 0,
    ONLINE  = 1,
    ERROR   = 2,
    INIT    = 3,
}

--------------------------------------------------------------------------------
-- Data File Definition (one instance per record type)
--------------------------------------------------------------------------------

DATA_FILE("sensor_data")

-- Instance for sensor_header record (index 0)
INSTANCE("sensor_header", "default_header")
    SET("device_id", 0x0001)
    SET("seq", 0)
    SET("timestamp", 0)
END_INSTANCE()

-- Instance for sensor_reading record (index 1)
INSTANCE("sensor_reading", "default_reading")
    SET("sensor_type", SENSOR_TYPE.TEMPERATURE)
    SET("sensor_state", SENSOR_STATE.ONLINE)
    SET("value", 25.5)
    SET("min_value", -40.0)
    SET("max_value", 85.0)
END_INSTANCE()

-- Instance for device_config record (index 2)
INSTANCE("device_config", "default_config")
    SET("device_id", 0x0042)
    SET("mac", {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF})
    SET("name", "Device-01")
    SET("poll_interval_ms", 1000)
    SET("enabled", 1)
END_INSTANCE()

GENERATE_DATA()