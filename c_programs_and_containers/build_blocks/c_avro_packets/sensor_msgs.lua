--[[
    sensor_msgs.lua - Schema definitions
    
    Generates: sensor_msgs.h
    
    Run directly:     luajit sensor_msgs.lua
    Or load from data: dofile("sensor_msgs.lua")
]]--

-- Only init DSL if not already done
if not FILE then
    require("avro_dsl").export_globals()
end

FILE("sensor_msgs")

INCLUDE_BRACKET("stdint.h")
INCLUDE_BRACKET("stdbool.h")

-- Fixed-size types
FIXED("mac_addr", 6)
FIXED("device_name", 16)

-- Pointer types (for callbacks, user data)
POINTER("callback_data")

-- Records
RECORD("sensor_header")
    FIELD("device_id", "uint16")
    FIELD("seq", "uint16")
    FIELD("timestamp", "uint32")
END_RECORD()

RECORD("sensor_reading")
    FIELD("header", "sensor_header")
    FIELD("sensor_type", "uint8")
    FIELD("sensor_state", "uint8")
    FIELD("value", "float")
    FIELD("min_value", "float")
    FIELD("max_value", "float")
END_RECORD()

RECORD("device_config")
    FIELD("device_id", "uint16")
    FIELD("mac", "mac_addr")
    FIELD("name", "device_name")
    FIELD("poll_interval_ms", "uint32")
    FIELD("enabled", "bool")
END_RECORD()

-- Generate header (safe to call multiple times)
GENERATE()