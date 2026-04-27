-- comm_manifest.lua
-- avro_dsl schema for libcomm's deployment manifest. Generates:
--   comm_manifest.h          - C types
--   comm_manifest_bin.h      - embeddable binary blob: rover_1_manifest_bin[N]
--   comm_manifest_ffi.lua    - FFI bindings
--   comm_manifest.bin        - raw schema for cross-tool inspection
--
-- Schema locked 2026-04-26. Multi-link, multi-dongle, virtual-dongle
-- model: every slave addressable as (dongle_idx, bus_id, addr). Host
-- process is dongles[0] with all-zero UUID (HOST_INTERNAL_DONGLE).
-- See project memory.
--
-- Invoked by Makefile target `manifest`. Do not edit generated files.

local script_dir = (arg and arg[0]):match("(.*/)") or "./"
package.path = script_dir .. "../chain_tree_luajit/c_avro_packets/?.lua;" .. package.path

require("avro_dsl").export_globals()

FILE("comm_manifest")
INCLUDE_BRACKET("stdint.h")

-- A transport endpoint that hosts slaves. The host process itself is
-- declared as dongles[0] with uuid = all zeros (HOST_INTERNAL_DONGLE);
-- real dongles get unique 128-bit UUIDs baked into firmware, discovered
-- at runtime via CMD_DONGLE_HELLO/CMD_DONGLE_IDENT.
STRUCT("manifest_dongle")
    FIELD("dongle_uuid",   "uint8", 16)   -- 128-bit identity
    FIELD("bus_count",     "uint8")        -- buses owned by this dongle
    FIELD("bus_local_ids", "uint8", 8)     -- maps dongle-local idx -> manifest bus_id
END_STRUCT()

-- A logical bus. tick_period_ms here is the wire-poll cadence; chain-tree's
-- comm_rx heartbeat (CT_COMM_RX_PERIOD_MS) is independent.
STRUCT("manifest_tunables")
    FIELD("max_miss",         "uint8")
    FIELD("tick_period_ms",   "uint16")
    FIELD("join_timeout_ms",  "uint16")
END_STRUCT()

STRUCT("manifest_bus")
    FIELD("bus_id",   "uint8")
    FIELD("tunables", "manifest_tunables")
END_STRUCT()

-- A protocol participant. addr=0xFE is reserved: targets the dongle's own
-- compute (or the host's libcomm internal handler for the virtual dongle).
-- physics_model_id = FNV-1a of the slave's physics-model record; 0 is a
-- placeholder that JOIN_CONFIRM rejects (matches feedback_no_soft_faults).
STRUCT("manifest_slave")
    FIELD("mcu",              "uint8")
    FIELD("dongle_idx",       "uint8")   -- index into dongles[]
    FIELD("bus_id",           "uint8")   -- index into buses[] (must be owned by dongle)
    FIELD("addr",             "uint8")   -- 0x01..0xFC normal, 0xFE = dongle-self
    FIELD("physics_model_id", "uint32")
END_STRUCT()

RECORD("comm_manifest_v1")
    FIELD("version",      "uint8")
    FIELD("dongle_count", "uint8")            -- always >= 1 (virtual host dongle)
    FIELD("bus_count",    "uint8")
    FIELD("slave_count",  "uint8")
    FIELD("dongles",      "manifest_dongle", 4)
    FIELD("buses",        "manifest_bus",    8)
    FIELD("slaves",       "manifest_slave", 64)
END_RECORD()

-- rover_1 today: virtual host dongle only, one synthetic bus, one
-- in-process physics slave at addr=1. When real dongles ship, dongles[1..]
-- grow with discovered UUIDs and slaves[] gains entries pointing at them.
CONST_PACKET("comm_manifest_v1", "rover_1_manifest", 0)
    SET("version",      1)
    SET("dongle_count", 1)
    SET("bus_count",    1)
    SET("slave_count",  1)
    SET("dongles", {
        -- dongles[0] = virtual host. UUID all zeros = HOST_INTERNAL_DONGLE.
        { dongle_uuid = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
          bus_count = 1,
          bus_local_ids = {0, 0,0,0,0,0,0,0} },
    })
    SET("buses", {
        { bus_id = 0,
          tunables = { max_miss = 3, tick_period_ms = 20, join_timeout_ms = 500 } },
    })
    SET("slaves", {
        -- mcu=1 lives on dongles[0] (virtual host) bus 0, addr 1.
        { mcu = 1, dongle_idx = 0, bus_id = 0, addr = 1, physics_model_id = 0 },
    })
END_CONST_PACKET()

GENERATE_ALL()
