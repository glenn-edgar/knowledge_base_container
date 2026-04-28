-- test_comm_slave_handler.lua
-- Slice 2a — slave-handler dispatcher. Replaces the hardcoded PING/NAK
-- responder inside libcomm with a registered slave-class handler. This
-- test proves: registration + dispatch + comm_slave_respond/ack_bare/nak
-- + per-mcu handlers + auto-NAK when handler doesn't respond + clear via
-- fn=NULL falls back to the built-in default.
--
-- Run from the project root: luajit test_comm_slave_handler.lua

local ffi      = require("ffi")
local comm_ffi = require("comm_ffi")
local ct_comm  = require("ct_comm")

local C   = comm_ffi.C
local R   = comm_ffi.RESULT
local CMD = comm_ffi.CMD
local NAK = comm_ffi.NAK_REASON

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

local function build_packet(slave_count)
    slave_count = slave_count or 1
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash                     = SCHEMA_HASH
    pkt.data.version                            = 1
    pkt.data.dongle_count                       = 1
    pkt.data.bus_count                          = 1
    pkt.data.slave_count                        = slave_count
    pkt.data.dongles[0].bus_count               = 1
    pkt.data.dongles[0].bus_local_ids[0]        = 0
    pkt.data.buses[0].bus_id                    = 0
    pkt.data.buses[0].tunables.max_miss         = 3
    pkt.data.buses[0].tunables.tick_period_ms   = 20
    pkt.data.buses[0].tunables.join_timeout_ms  = 500
    for i = 0, slave_count - 1 do
        pkt.data.slaves[i].mcu              = i + 1
        pkt.data.slaves[i].dongle_idx       = 0
        pkt.data.slaves[i].bus_id           = 0
        pkt.data.slaves[i].addr             = i + 1
        pkt.data.slaves[i].physics_model_id = 0
    end
    return pkt
end

local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

local function setup(slave_count)
    slave_count = slave_count or 1
    local pkt = build_packet(slave_count)
    assert(C.comm_init(ffi.cast("const uint8_t*", pkt), PACKET_SIZE) == R.OK,
           "comm_init failed in setup")
    for i = 1, slave_count do
        local fake_phys = ffi.cast("void*", 0xDEAD0000 + i)
        assert(C.comm_attach_internal(0, 0, i, fake_phys) == R.OK,
               "comm_attach_internal failed in setup")
    end
end

local function teardown()
    C.comm_shutdown()
end

------------------------------------------------------------------------
-- Hold callback refs at file scope so LuaJIT doesn't garbage-collect
-- them while libcomm still holds the function pointer.
local cb_refs = {}

local function make_cb(fn)
    local cb = ffi.cast("comm_slave_handler_fn", fn)
    cb_refs[#cb_refs + 1] = cb  -- keep alive
    return cb
end

------------------------------------------------------------------------
io.write("[register handler then ack_bare]\n")
do
    setup()
    local seen = { mcu=nil, cmd=nil, in_seq=nil, payload_len=nil }
    local cb = make_cb(function(mcu, cmd, payload, payload_len, in_seq, ctx)
        seen.mcu         = mcu
        seen.cmd         = cmd
        seen.in_seq      = in_seq
        seen.payload_len = payload_len
        C.comm_slave_ack_bare(mcu, in_seq)
    end)
    check(C.comm_set_slave_handler(1, cb, nil) == R.OK, "register handler for mcu=1")

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    local events = ct_comm.poll(8)
    check(events ~= nil and #events == 1,        "poll surfaces one event")
    if events and events[1] then
        local e = events[1]
        check(e.handle == h,                     "handle matches submit")
        check(e.cmd == CMD.ACK_BARE,             "response is ACK_BARE (handler emitted it)")
        check(e.payload_len == 0,                "ACK_BARE payload is empty")
    end
    check(seen.mcu == 1,                         "handler was called with mcu=1")
    check(seen.cmd == CMD.PING,                  "handler was called with cmd=PING")
    check(seen.payload_len == 0,                 "handler saw payload_len=0")
    check(seen.in_seq ~= nil,                    "handler received in_seq")
    ct_comm.claim(h)
    teardown()
end

------------------------------------------------------------------------
io.write("[handler emits NAK with custom reason]\n")
do
    setup()
    local cb = make_cb(function(mcu, cmd, payload, payload_len, in_seq, ctx)
        C.comm_slave_nak(mcu, in_seq, 0x42)
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    check(C.comm_status(h) == R.ERR_NAK,         "status reports NAK")
    local e, rc = ct_comm.claim(h)
    check(rc == R.ERR_NAK,                       "claim returns ERR_NAK")
    check(e and e.payload_len == 1,              "NAK payload is 1 byte")
    check(e and e.payload[0] == 0x42,            "NAK reason byte preserved through wire")
    teardown()
end

------------------------------------------------------------------------
io.write("[handler emits custom payload via comm_slave_respond]\n")
do
    setup()
    local payload_buf = ffi.new("uint8_t[5]", 0xAA, 0xBB, 0xCC, 0xDD, 0xEE)
    local cb = make_cb(function(mcu, cmd, payload, payload_len, in_seq, ctx)
        -- Echo back a 5-byte custom payload tagged as ACK_BARE (cmd is
        -- meaningless to the dispatcher; we just want the wire round-trip).
        C.comm_slave_respond(mcu, in_seq, CMD.ACK_BARE, payload_buf, 5, 0)
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    local e, rc = ct_comm.claim(h)
    check(rc == R.OK,                            "claim OK on custom-payload response")
    check(e and e.cmd == CMD.ACK_BARE,           "response cmd preserved")
    check(e and e.payload_len == 5,              "custom payload_len preserved")
    if e then
        local ok = (e.payload[0] == 0xAA and e.payload[1] == 0xBB
                and e.payload[2] == 0xCC and e.payload[3] == 0xDD
                and e.payload[4] == 0xEE)
        check(ok,                                 "custom payload bytes round-tripped intact")
    end
    teardown()
end

------------------------------------------------------------------------
io.write("[handler with ack_status flag]\n")
do
    setup()
    local cb = make_cb(function(mcu, cmd, payload, payload_len, in_seq, ctx)
        -- Bare ACK with the URGENT flag set in ack_status.
        C.comm_slave_respond(mcu, in_seq, CMD.ACK_BARE, nil, 0,
                             comm_ffi.ACK_FLAG_URGENT)
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    local e, rc = ct_comm.claim(h)
    check(rc == R.OK,                            "claim OK")
    check(e and e.ack_status == comm_ffi.ACK_FLAG_URGENT,
          "ack_status URGENT flag round-trips through wire")
    teardown()
end

------------------------------------------------------------------------
io.write("[per-mcu handlers stay isolated]\n")
do
    setup(2)
    local seen1, seen2 = 0, 0
    local cb1 = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        seen1 = seen1 + 1
        C.comm_slave_nak(mcu, in_seq, 0x11)
    end)
    local cb2 = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        seen2 = seen2 + 1
        C.comm_slave_nak(mcu, in_seq, 0x22)
    end)
    check(C.comm_set_slave_handler(1, cb1, nil) == R.OK, "register mcu=1 handler")
    check(C.comm_set_slave_handler(2, cb2, nil) == R.OK, "register mcu=2 handler")

    local h1 = ct_comm.submit(1, CMD.PING, nil, 0)
    local h2 = ct_comm.submit(2, CMD.PING, nil, 0)
    ct_comm.poll(8)

    check(seen1 == 1, "mcu=1 handler called exactly once")
    check(seen2 == 1, "mcu=2 handler called exactly once")

    local e1, rc1 = ct_comm.claim(h1)
    local e2, rc2 = ct_comm.claim(h2)
    check(rc1 == R.ERR_NAK and e1 and e1.payload[0] == 0x11,
          "mcu=1 returned its NAK reason 0x11")
    check(rc2 == R.ERR_NAK and e2 and e2.payload[0] == 0x22,
          "mcu=2 returned its NAK reason 0x22")
    teardown()
end

------------------------------------------------------------------------
io.write("[handler with no response triggers auto-NAK NO_RESPONSE]\n")
do
    setup()
    local cb = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        -- Deliberately do nothing.
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    local e, rc = ct_comm.claim(h)
    check(rc == R.ERR_NAK,                       "no-response handler -> NAK")
    check(e and e.payload_len == 1 and e.payload[0] == NAK.NO_RESPONSE,
          "NAK reason byte is NO_RESPONSE (0xFE)")
    teardown()
end

------------------------------------------------------------------------
io.write("[clear handler with fn=NULL falls back to default]\n")
do
    setup()
    -- First register a handler, then clear it; default responder must
    -- take over (PING -> ACK_BARE).
    local cb = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        C.comm_slave_nak(mcu, in_seq, 0x99)
    end)
    C.comm_set_slave_handler(1, cb, nil)
    -- Drive one cycle to confirm the handler IS the active one.
    local h_a = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    local ea = ct_comm.claim(h_a)
    check(ea and ea.payload_len == 1 and ea.payload[0] == 0x99,
          "registered handler ran (NAK reason 0x99 visible)")

    -- Clear and re-test: PING should now ACK_BARE via the default path.
    check(C.comm_set_slave_handler(1, nil, nil) == R.OK, "clear handler")
    local h_b = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    local eb, rcb = ct_comm.claim(h_b)
    check(rcb == R.OK and eb and eb.cmd == CMD.ACK_BARE,
          "after clear, default responder handles PING -> ACK_BARE")
    teardown()
end

------------------------------------------------------------------------
io.write("[register against unknown mcu returns BAD_ARG]\n")
do
    setup()
    local cb = make_cb(function() end)
    check(C.comm_set_slave_handler(99, cb, nil) == R.ERR_BAD_ARG,
          "register against undeclared mcu: BAD_ARG")
    teardown()
end

------------------------------------------------------------------------
io.write("[comm_slave_respond outside handler context returns ERR_INIT]\n")
do
    setup()
    -- Calling response helpers when no dispatch is active.
    local rc1 = C.comm_slave_ack_bare(1, 0)
    check(rc1 == R.ERR_INIT,
          "comm_slave_ack_bare outside handler: ERR_INIT")
    local rc2 = C.comm_slave_nak(1, 0, 0x00)
    check(rc2 == R.ERR_INIT,
          "comm_slave_nak outside handler: ERR_INIT")
    local rc3 = C.comm_slave_respond(1, 0, CMD.ACK_BARE, nil, 0, 0)
    check(rc3 == R.ERR_INIT,
          "comm_slave_respond outside handler: ERR_INIT")
    teardown()
end

------------------------------------------------------------------------
io.write("[double-respond returns BAD_ARG]\n")
do
    setup()
    local saw_second_rc = nil
    local cb = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        C.comm_slave_ack_bare(mcu, in_seq)
        -- Second call must fail; capture the rc.
        saw_second_rc = C.comm_slave_nak(mcu, in_seq, 0x00)
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    check(saw_second_rc == R.ERR_BAD_ARG,
          "second comm_slave_respond inside handler: BAD_ARG")
    -- The first response (ACK_BARE) still wins.
    local e, rc = ct_comm.claim(h)
    check(rc == R.OK and e and e.cmd == CMD.ACK_BARE,
          "first response (ACK_BARE) is the one delivered")
    teardown()
end

------------------------------------------------------------------------
io.write("[comm_slave_respond with wrong mcu returns BAD_ARG]\n")
do
    setup(2)
    local saw_rc = nil
    local cb = make_cb(function(mcu, cmd, p, plen, in_seq, ctx)
        -- Try to respond as mcu=2 from mcu=1's handler — illegal.
        saw_rc = C.comm_slave_ack_bare(2, in_seq)
        -- Then respond correctly so the master gets a reply.
        C.comm_slave_ack_bare(mcu, in_seq)
    end)
    C.comm_set_slave_handler(1, cb, nil)

    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    ct_comm.poll(8)
    check(saw_rc == R.ERR_BAD_ARG, "responding as wrong mcu: BAD_ARG")
    ct_comm.claim(h)
    teardown()
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
