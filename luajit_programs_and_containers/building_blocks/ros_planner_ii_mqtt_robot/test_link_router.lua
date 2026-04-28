-- test_link_router.lua
-- Slice 1d unit test. comm_init wires manifest → router → link;
-- comm_attach_internal binds the rover_1 in-process slave to a
-- transport_inproc endpoint; node-state diagnostics read from the
-- link FSM (all slaves start COMM_NODE_UNKNOWN until phase-2 join).
--
-- Run from the project root:  luajit test_link_router.lua
--
-- The cdefs below mirror comm_manifest_ffi.lua's record types verbatim;
-- we don't `require` that module because its const_packets section is
-- broken by a known avro_dsl GENERATE_FFI bug (table-tostring leak).

local ffi = require("ffi")

ffi.cdef[[
typedef struct __attribute__((packed)) {
    double      timestamp;
    uint32_t    schema_hash;
    uint16_t    seq;
    uint16_t    source_node;
} comm_manifest_wire_header_t;

typedef struct __attribute__((packed)) {
    uint8_t dongle_uuid[16];
    uint8_t bus_count;
    uint8_t bus_local_ids[8];
} manifest_dongle_t;

typedef struct __attribute__((packed)) {
    uint8_t  max_miss;
    uint16_t tick_period_ms;
    uint16_t join_timeout_ms;
} manifest_tunables_t;

typedef struct __attribute__((packed)) {
    uint8_t             bus_id;
    manifest_tunables_t tunables;
} manifest_bus_t;

typedef struct __attribute__((packed)) {
    uint8_t  mcu;
    uint8_t  dongle_idx;
    uint8_t  bus_id;
    uint8_t  addr;
    uint32_t physics_model_id;
} manifest_slave_t;

typedef struct __attribute__((packed)) {
    uint8_t version;
    uint8_t dongle_count;
    uint8_t bus_count;
    uint8_t slave_count;
    manifest_dongle_t dongles[4];
    manifest_bus_t    buses[8];
    manifest_slave_t  slaves[64];
} comm_manifest_v1_wire_t;

typedef struct __attribute__((packed)) {
    comm_manifest_wire_header_t header;
    comm_manifest_v1_wire_t     data;
} comm_manifest_v1_packet_t;

typedef int32_t comm_result_t;
typedef uint8_t comm_node_state_t;

comm_result_t     comm_init           (const uint8_t *blob, size_t len);
void              comm_shutdown       (void);
comm_result_t     comm_attach_internal(uint8_t dongle_idx, uint8_t bus_id,
                                       uint8_t addr, void *physics_handle);

comm_node_state_t comm_node_state         (uint8_t mcu);
uint32_t          comm_node_physics_model (uint8_t mcu);
uint8_t           comm_node_miss_count    (uint8_t mcu);
uint32_t          comm_node_last_seen_ms  (uint8_t mcu);
]]

local C = ffi.load("./libcomm.so")

local COMM_OK              =   0
local COMM_ERR_INIT        =  -1
local COMM_ERR_BAD_MANIFEST = -3
local COMM_ERR_BAD_ARG     = -12
local COMM_NODE_UNKNOWN    =   0

local SCHEMA_HASH = 0x79046205   -- COMM_MANIFEST_V1_SCHEMA_HASH from comm_manifest.h
local PACKET_SIZE = ffi.sizeof("comm_manifest_v1_packet_t")
assert(PACKET_SIZE == 680, "expected 680-byte packet, got "..tostring(PACKET_SIZE))

local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

-- Build the rover_1 packet. Same contents as comm_manifest_bin.h's
-- ROVER_1_MANIFEST_DATA but constructed at runtime so we can corrupt
-- it for the BAD_MANIFEST cases.
local function build_rover_1_packet()
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash = SCHEMA_HASH

    pkt.data.version      = 1
    pkt.data.dongle_count = 1
    pkt.data.bus_count    = 1
    pkt.data.slave_count  = 1

    -- dongles[0] = host virtual dongle (uuid all zeros, owns bus 0).
    -- ffi.new() already zero-fills, so we only set non-zero fields.
    pkt.data.dongles[0].bus_count        = 1
    pkt.data.dongles[0].bus_local_ids[0] = 0

    pkt.data.buses[0].bus_id                   = 0
    pkt.data.buses[0].tunables.max_miss        = 3
    pkt.data.buses[0].tunables.tick_period_ms  = 20
    pkt.data.buses[0].tunables.join_timeout_ms = 500

    pkt.data.slaves[0].mcu              = 1
    pkt.data.slaves[0].dongle_idx       = 0
    pkt.data.slaves[0].bus_id           = 0
    pkt.data.slaves[0].addr             = 1
    pkt.data.slaves[0].physics_model_id = 0
    return pkt
end

------------------------------------------------------------------------
io.write("[comm_init / valid blob]\n")
do
    local pkt = build_rover_1_packet()
    local rc  = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_OK, "comm_init returns COMM_OK on a valid rover_1 packet")

    check(C.comm_node_state(1) == COMM_NODE_UNKNOWN,
          "declared mcu=1 reports COMM_NODE_UNKNOWN before join")
    check(C.comm_node_physics_model(1) == 0,
          "declared mcu=1 reports physics_model_id = 0 (manifest placeholder)")
    check(C.comm_node_miss_count(1) == 0,
          "declared mcu=1 reports miss_count = 0")
    check(C.comm_node_last_seen_ms(1) == 0,
          "declared mcu=1 reports last_seen_ms = 0")

    check(C.comm_node_state(99) == COMM_NODE_UNKNOWN,
          "undeclared mcu=99 also reports COMM_NODE_UNKNOWN")

    C.comm_shutdown()
end

------------------------------------------------------------------------
io.write("[comm_init / corrupted blob]\n")
do
    local pkt = build_rover_1_packet()
    pkt.header.schema_hash = 0xDEADBEEF
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_ERR_BAD_MANIFEST,
          "schema_hash mismatch returns COMM_ERR_BAD_MANIFEST")
    check(C.comm_node_state(1) == COMM_NODE_UNKNOWN,
          "diagnostics return COMM_NODE_UNKNOWN after failed init")
end

------------------------------------------------------------------------
io.write("[comm_init / invariant violations]\n")
do
    local pkt = build_rover_1_packet()
    pkt.data.buses[0].tunables.tick_period_ms = 10
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_ERR_BAD_MANIFEST,
          "tick_period_ms < CT_COMM_RX_PERIOD_MS rejects manifest")
end

do
    local pkt = build_rover_1_packet()
    pkt.data.slaves[0].addr = 0xFD
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_ERR_BAD_MANIFEST,
          "slave addr=0xFD (broadcast) rejects manifest")
end

do
    local pkt = build_rover_1_packet()
    pkt.data.slaves[0].bus_id = 7
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_ERR_BAD_MANIFEST,
          "slave bus_id not in dongle.bus_local_ids[] rejects manifest")
end

-- Invariant 2 (dongles[0] must be HOST_INTERNAL_DONGLE) was dropped in
-- Phase B: every dongle now carries a real (type, instance) identity,
-- so any non-zero uuid in slot 0 is legal. The all-zeros sentinel
-- remains a valid value (this test's default rover_1 manifest uses it
-- for inproc-mode tests below) but is no longer required.
do
    local pkt = build_rover_1_packet()
    pkt.data.dongles[0].dongle_uuid[0] = 0x42
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_OK,
          "dongles[0] non-zero uuid is now ACCEPTED (Phase B drop of invariant 2)")
    if rc == COMM_OK then C.comm_shutdown() end
end

------------------------------------------------------------------------
io.write("[comm_attach_internal]\n")
do
    local pkt = build_rover_1_packet()
    assert(C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE) == COMM_OK)

    local fake_handle = ffi.cast("void*", 0xC0FFEE)
    local rc = C.comm_attach_internal(0, 0, 1, fake_handle)
    check(rc == COMM_OK,
          "attach (dongle=0, bus=0, addr=1) for declared in-process slave succeeds")

    rc = C.comm_attach_internal(0, 0, 1, fake_handle)
    check(rc == COMM_ERR_BAD_ARG,
          "re-attaching the same triple returns COMM_ERR_BAD_ARG")

    rc = C.comm_attach_internal(1, 0, 1, fake_handle)
    check(rc == COMM_ERR_BAD_ARG,
          "attach to non-host dongle_idx returns COMM_ERR_BAD_ARG")

    rc = C.comm_attach_internal(0, 0, 2, fake_handle)
    check(rc == COMM_ERR_BAD_ARG,
          "attach to undeclared (dongle, bus, addr) returns COMM_ERR_BAD_ARG")

    C.comm_shutdown()
end

------------------------------------------------------------------------
io.write("[comm_init guards re-entry]\n")
do
    local pkt = build_rover_1_packet()
    assert(C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE) == COMM_OK)
    local rc = C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE)
    check(rc == COMM_ERR_INIT,
          "second comm_init without shutdown returns COMM_ERR_INIT")
    C.comm_shutdown()
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
