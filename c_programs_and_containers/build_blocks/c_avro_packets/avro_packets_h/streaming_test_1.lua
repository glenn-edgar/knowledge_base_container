require("avro_dsl").export_globals()

--------------------------------------------------------------------------------
-- FILE DECLARATION
--------------------------------------------------------------------------------
FILE("streaming_test_1")

INCLUDE_BRACKET("stdint.h")
INCLUDE_BRACKET("stdbool.h")
INCLUDE_BRACKET("string.h")
INCLUDE_STRING("avro_common.h")
--[[
STRUCT("packet_header")
    FIELD("device_id", "uint16")
    FIELD("seq", "uint16")
    FIELD("timestamp", "uint64")
END_STRUCT()
]]--

RECORD("accelerometer_reading")          -- index 0
    
    FIELD("x", "float")
    FIELD("y", "float")
    FIELD("z", "float")
END_RECORD()


-------------------------------------------------------------------------------
-- GENERATE OUTPUT
--------------------------------------------------------------------------------
GENERATE()