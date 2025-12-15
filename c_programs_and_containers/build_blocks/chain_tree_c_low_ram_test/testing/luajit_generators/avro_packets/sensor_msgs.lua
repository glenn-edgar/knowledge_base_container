-- sensor_msgs.lua
-- Sample schema definition for sensor messages

require("avro_dsl").export_globals()

--------------------------------------------------------------------------------
-- FILE DECLARATION
--------------------------------------------------------------------------------
FILE("sensor_msgs")

INCLUDE_BRACKET("stdint.h")
INCLUDE_BRACKET("stdbool.h")
INCLUDE_BRACKET("string.h")
INCLUDE_STRING("avro_common.h")

--------------------------------------------------------------------------------
-- ENUMS
--------------------------------------------------------------------------------
ENUM("sensor_type")
    VALUE("TEMP", 0)
    VALUE("PRESSURE", 1)
    VALUE("HUMIDITY", 2)
    VALUE("FLOW", 3)
END_ENUM()

ENUM("sensor_state")
    VALUE("IDLE", 0)
    VALUE("SAMPLING", 1)
    VALUE("ERROR", 2)
    VALUE("CALIBRATING", 3)
END_ENUM()

--------------------------------------------------------------------------------
-- FIXED ARRAYS
--------------------------------------------------------------------------------
FIXED("mac_addr", 6)
FIXED("uuid", 16)

--------------------------------------------------------------------------------
-- FIXED STRINGS
--------------------------------------------------------------------------------
STRING("label", 16)
STRING("unit", 8)

--------------------------------------------------------------------------------
-- HELPER STRUCTS (no dispatch index)
--------------------------------------------------------------------------------
STRUCT("packet_header")
    FIELD("device_id", "uint16")
    FIELD("seq", "uint16")
    FIELD("timestamp", "uint32")
END_STRUCT()

STRUCT("sensor_config")
    FIELD("sample_rate", "uint16")
    FIELD("threshold_lo", "float")
    FIELD("threshold_hi", "float")
    FIELD("enabled", "bool")
END_STRUCT()

--------------------------------------------------------------------------------
-- RECORDS (dispatchable packet types - index is position-based)
--------------------------------------------------------------------------------
RECORD("temp_reading")          -- index 0
    FIELD("header", "packet_header")
    FIELD("celsius", "float")
    FIELD("state", "sensor_state")
END_RECORD()

RECORD("pressure_reading")      -- index 1
    FIELD("header", "packet_header")
    FIELD("pascals", "uint32")
    FIELD("state", "sensor_state")
END_RECORD()

RECORD("humidity_reading")      -- index 2
    FIELD("header", "packet_header")
    FIELD("percent", "float")
    FIELD("state", "sensor_state")
END_RECORD()

RECORD("sensor_batch")          -- index 3
    FIELD("header", "packet_header")
    FIELD("count", "uint8")
    FIELD("temps", "float", 8)
    FIELD("pressures", "uint32", 8)
END_RECORD()

RECORD("device_info")           -- index 4
    FIELD("mac", "mac_addr")
    FIELD("name", "label")
    FIELD("firmware_ver", "uint32")
    FIELD("config", "sensor_config")
END_RECORD()

--------------------------------------------------------------------------------
-- GENERATE OUTPUT
--------------------------------------------------------------------------------
GENERATE()