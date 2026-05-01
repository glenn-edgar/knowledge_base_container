-- struct_record_test.lua
-- Exercises the post-fix DSL paths: recursive resolve_field_size, struct-aware
-- format_c_value (C source const packets), struct-aware pack_field_value
-- (binary const packets). Schema mirrors comm_manifest_v1 from the libcomm
-- design session 2026-04-26.
--
-- Run from this directory:  luajit struct_record_test.lua

local dsl = require("avro_dsl")
dsl.export_globals()

FILE("struct_record_test")
INCLUDE_BRACKET("stdint.h")

STRUCT("manifest_slave")
    FIELD("mcu",              "uint8")
    FIELD("addr",             "uint8")
    FIELD("physics_model_id", "uint32")
END_STRUCT()

STRUCT("manifest_tunables")
    FIELD("max_miss",         "uint8")
    FIELD("tick_period_ms",   "uint16")
    FIELD("join_timeout_ms",  "uint16")
END_STRUCT()

RECORD("comm_manifest_v1")
    FIELD("version",       "uint8")
    FIELD("bus_id",        "uint8")
    FIELD("slave_count",   "uint8")
    FIELD("slaves",        "manifest_slave", 32)
    FIELD("tunables",      "manifest_tunables")
END_RECORD()

CONST_PACKET("comm_manifest_v1", "rover_1_manifest", 0)
    SET("version",     1)
    SET("bus_id",      0)
    SET("slave_count", 2)
    SET("slaves", {
        { mcu = 1, addr = 1, physics_model_id = 0xDEADBEEF },
        { mcu = 1, addr = 2, physics_model_id = 0xCAFEBABE },
    })
    SET("tunables", {
        max_miss        = 3,
        tick_period_ms  = 20,
        join_timeout_ms = 500,
    })
END_CONST_PACKET()

GENERATE_ALL()
GENERATE_CONST_PACKETS("struct_record_test_const.h")

-- Sanity-check the wire size.
-- Expected packed wire size for record body:
--   version(1) + bus_id(1) + slave_count(1)
--   + 32 * (uint8 + uint8 + uint32 = 6) = 192
--   + tunables(uint8 + uint16 + uint16 = 5)
--   = 200 bytes
-- Plus the 16-byte common header in the binary blob = 216 bytes.
local function file_size(path)
    local f = assert(io.open(path, "rb"))
    local s = f:seek("end")
    f:close()
    return s
end

-- The .bin output is just the raw schema, not the const packet, so check the
-- _bin.h header for the rover_1_manifest_bin[N] declaration.
local bin_h = assert(io.open("struct_record_test_bin.h", "r")):read("*a")
local declared = bin_h:match("rover_1_manifest_bin%[(%d+)%]")
assert(declared, "rover_1_manifest_bin not found in struct_record_test_bin.h")
declared = tonumber(declared)
print(string.format("rover_1_manifest_bin declared size: %d bytes", declared))
assert(declared == 216, string.format("expected 216 B, got %d B", declared))

-- And confirm the readable C-source const packet file built without error.
assert(io.open("struct_record_test_const.h", "r")):close()

print("OK: recursive sizing + struct/array-of-struct const packet generation")
