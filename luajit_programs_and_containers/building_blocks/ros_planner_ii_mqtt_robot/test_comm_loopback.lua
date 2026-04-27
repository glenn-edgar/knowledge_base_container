-- test_comm_loopback.lua
-- Slice 1e end-to-end test. Submits CMD_PING through the master,
-- in-process slave (inside comm_poll) responds with CMD_ACK_BARE, and
-- the response surfaces back as a poll event AND is reachable via
-- status+claim. Also verifies handle layout, depth-1 (BUS error),
-- cancel, NAK on unknown cmd, and decoder/slot lifecycle.
--
-- Run from the project root:  luajit test_comm_loopback.lua

local ffi      = require("ffi")
local comm_ffi = require("comm_ffi")
local ct_comm  = require("ct_comm")

local C = comm_ffi.C
local R = comm_ffi.RESULT
local CMD = comm_ffi.CMD

-- The same packed manifest types test_link_router uses; not re-required
-- because comm_ffi doesn't surface them.
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
]]

local SCHEMA_HASH = 0x79046205
local PACKET_SIZE = ffi.sizeof("comm_manifest_v1_packet_t")

local function build_rover_1_packet()
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash                     = SCHEMA_HASH
    pkt.data.version                            = 1
    pkt.data.dongle_count                       = 1
    pkt.data.bus_count                          = 1
    pkt.data.slave_count                        = 1
    pkt.data.dongles[0].bus_count               = 1
    pkt.data.dongles[0].bus_local_ids[0]        = 0
    pkt.data.buses[0].bus_id                    = 0
    pkt.data.buses[0].tunables.max_miss         = 3
    pkt.data.buses[0].tunables.tick_period_ms   = 20
    pkt.data.buses[0].tunables.join_timeout_ms  = 500
    pkt.data.slaves[0].mcu                      = 1
    pkt.data.slaves[0].dongle_idx               = 0
    pkt.data.slaves[0].bus_id                   = 0
    pkt.data.slaves[0].addr                     = 1
    pkt.data.slaves[0].physics_model_id         = 0
    return pkt
end

local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

-- Spin up libcomm with the rover_1 manifest and bind the in-process slave.
local function setup()
    local pkt = build_rover_1_packet()
    assert(C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE) == R.OK,
           "comm_init failed in setup")
    -- physics_handle is opaque to libcomm; libcomm just stores the pointer.
    local fake_phys = ffi.cast("void*", 0xDEAD)
    assert(C.comm_attach_internal(0, 0, 1, fake_phys) == R.OK,
           "comm_attach_internal failed in setup")
end

local function teardown()
    C.comm_shutdown()
end

------------------------------------------------------------------------
io.write("[handle layout]\n")
do
    setup()
    local h, err = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err == R.OK,                      "submit returns OK")
    check(h ~= comm_ffi.HANDLE_INVALID,     "submit returns non-zero handle")
    check(ct_comm.handle_slot(h) == 0,      "first handle's slot index is 0")
    check(ct_comm.handle_gen(h)  == 1,      "first handle's generation is 1")
    -- Decode/encode round-trip via the helpers (also confirms bit math).
    teardown()
end

------------------------------------------------------------------------
io.write("[submit-poll-claim happy path: PING -> ACK_BARE]\n")
do
    setup()
    local t0 = ct_comm.now_ms()
    local h, err = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err == R.OK and h ~= 0, "submit PING ok")

    -- Before poll, slot is PENDING.
    check(C.comm_status(h) == R.ERR_NO_DATA, "status before poll: NO_DATA")

    -- One poll() cycle drives the full loopback (m2s encode → in-process
    -- slave responds → s2m back → master matches → slot DONE → surfaced).
    local events = ct_comm.poll(8)
    check(events ~= nil and #events == 1, "poll surfaces exactly one event")
    if events and events[1] then
        local e = events[1]
        check(e.handle == h,                "event handle matches submitted handle")
        check(e.mcu == 1,                   "event mcu == 1")
        check(e.cmd == CMD.ACK_BARE,        "event.cmd == ACK_BARE")
        check(e.ack_status == 0,            "event.ack_status == 0 (no urgent)")
        check(e.payload_len == 0,           "event.payload_len == 0 (bare ack)")
        local now = ct_comm.now_ms()
        check(e.elapsed_ms <= (now - t0) + 5,"elapsed_ms is plausible (<= wall clock)")
    end

    -- Re-polling produces no new events (surfaced flag prevents duplicates).
    local empty = ct_comm.poll(8)
    check(empty ~= nil and #empty == 0, "second poll surfaces 0 events (idempotent)")

    -- Status now reads DONE; claim consumes the slot.
    check(C.comm_status(h) == R.OK,           "status after poll: OK (DONE)")
    local e2, rc2 = ct_comm.claim(h)
    check(rc2 == R.OK and e2 ~= nil,          "claim returns OK and an event")
    check(e2 and e2.cmd == CMD.ACK_BARE,      "claimed event also reports ACK_BARE")

    -- After claim, slot is freed; handle becomes stale (gen bumped).
    check(C.comm_status(h) == R.ERR_BAD_HANDLE,
          "status after claim: BAD_HANDLE (slot freed, gen bumped)")
    teardown()
end

------------------------------------------------------------------------
io.write("[handle generation bumps after free]\n")
do
    setup()
    local h1 = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    ct_comm.claim(h1)
    local h2 = ct_comm.submit(1, CMD.PING, nil, 0)
    check(ct_comm.handle_slot(h2) == 0,            "second submit reuses slot 0")
    check(ct_comm.handle_gen(h2)  == 2,            "second submit bumps generation to 2")
    check(h2 ~= h1,                                 "the handle value itself differs (ABA-safe)")
    ct_comm.poll(8); ct_comm.claim(h2)
    teardown()
end

------------------------------------------------------------------------
io.write("[depth-1 per slave: second submit before poll returns BUS]\n")
do
    setup()
    local h1, err1 = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err1 == R.OK,                            "first submit OK")
    local h2, err2 = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err2 == R.ERR_BUS and h2 == 0,           "second submit before poll returns ERR_BUS")
    -- Drain so the test ends cleanly.
    ct_comm.poll(8); ct_comm.claim(h1)
    teardown()
end

------------------------------------------------------------------------
io.write("[cancel before response]\n")
do
    setup()
    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    check(C.comm_cancel(h) == R.OK,                "cancel on PENDING returns OK")
    check(C.comm_status(h) == R.ERR_BAD_HANDLE,    "status after cancel: BAD_HANDLE")
    -- A subsequent submit to the same mcu should now succeed
    -- (link.outstanding_slot was cleared by cancel).
    local h2, err2 = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err2 == R.OK and h2 ~= 0,                "submit after cancel succeeds")
    -- Drive the now-stale m2s frame through poll; the response will
    -- come back but won't match h (cancelled) — but it WILL match h2
    -- because both shared the same wire seq? No: link.next_seq advanced.
    -- The cancelled frame is in the m2s ring; slave processes it, sends
    -- response with ack_seq=0. h2 used seq=1. So the cancelled response
    -- won't match h2. h2's frame goes through too — call poll twice
    -- to drain both round-trips.
    ct_comm.poll(8); ct_comm.poll(8)
    -- h2 should now be DONE (its own frame round-tripped).
    check(C.comm_status(h2) == R.OK,               "h2 completes despite stale cancelled frame on the wire")
    ct_comm.claim(h2)
    teardown()
end

------------------------------------------------------------------------
io.write("[NAK on unknown cmd]\n")
do
    setup()
    -- 0x0099 isn't a defined command in any class — slave NAKs.
    local h, err = ct_comm.submit(1, 0x0099, nil, 0)
    check(err == R.OK,                              "submit of unknown cmd is accepted by master")
    ct_comm.poll(8)
    check(C.comm_status(h) == R.ERR_NAK,            "status reports NAK after slave rejects")
    local e, rc = ct_comm.claim(h)
    check(rc == R.ERR_NAK,                          "claim returns ERR_NAK")
    check(e ~= nil and e.cmd == CMD.NAK,            "claimed event's cmd == CMD.NAK")
    check(e and e.payload_len == 1 and e.payload[0] == 0xFF,
                                                    "NAK payload carries reason byte 0xFF")
    teardown()
end

------------------------------------------------------------------------
io.write("[submit guards]\n")
do
    setup()
    -- Undeclared mcu.
    local h, err = ct_comm.submit(99, CMD.PING, nil, 0)
    check(err == R.ERR_BAD_ARG and h == 0,          "submit to undeclared mcu: BAD_ARG")
    -- Oversized payload.
    local big = ffi.new("uint8_t[?]", 200)
    local h2, err2 = ct_comm.submit(1, CMD.PING, big, 200)
    check(err2 == R.ERR_BAD_ARG and h2 == 0,        "submit with payload_len > 128: BAD_ARG")
    teardown()
end

------------------------------------------------------------------------
io.write("[submit before init]\n")
do
    -- Note: prior block already called teardown(). g_initialized is 0.
    local h, err = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err == R.ERR_INIT and h == 0, "submit before comm_init: ERR_INIT")
end

------------------------------------------------------------------------
io.write("[multiple slaves, isolated submit lanes]\n")
do
    -- Two slaves on same bus; each submit goes to its own transport endpoint.
    -- This also exercises that surfacing works for multiple events in one poll.
    local pkt = build_rover_1_packet()
    pkt.data.slave_count                    = 2
    pkt.data.slaves[1].mcu                  = 2
    pkt.data.slaves[1].dongle_idx           = 0
    pkt.data.slaves[1].bus_id               = 0
    pkt.data.slaves[1].addr                 = 2
    pkt.data.slaves[1].physics_model_id     = 0
    assert(C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE) == R.OK)
    assert(C.comm_attach_internal(0, 0, 1, ffi.cast("void*", 0x1)) == R.OK)
    assert(C.comm_attach_internal(0, 0, 2, ffi.cast("void*", 0x2)) == R.OK)

    local h1 = ct_comm.submit(1, CMD.PING, nil, 0)
    local h2 = ct_comm.submit(2, CMD.PING, nil, 0)
    check(h1 ~= h2 and h1 ~= 0 and h2 ~= 0,         "two parallel submits return distinct handles")

    local events = ct_comm.poll(8)
    check(events and #events == 2,                  "single poll surfaces both completions")

    local saw_h1, saw_h2 = false, false
    for _, e in ipairs(events or {}) do
        if e.handle == h1 and e.mcu == 1 then saw_h1 = true end
        if e.handle == h2 and e.mcu == 2 then saw_h2 = true end
    end
    check(saw_h1 and saw_h2, "both events match their respective handles + mcus")

    ct_comm.claim(h1); ct_comm.claim(h2)
    teardown()
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
