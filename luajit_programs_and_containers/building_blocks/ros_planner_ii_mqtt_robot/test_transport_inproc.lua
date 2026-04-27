-- test_transport_inproc.lua
-- Slice 1c unit test. Verifies the in-process transport's per-endpoint
-- isolation, bidirectional byte movement, and end-to-end SLIP frame
-- delivery through the transport (frame.c + transport_inproc.c
-- working together).
--
-- Run from the project root:  luajit test_transport_inproc.lua

local ffi = require("ffi")

local TRANSPORT_INPROC_MAX_ENDPOINTS = 32
local TRANSPORT_INPROC_RING_SIZE     = 512
local FRAME_BUFFER_MAX               = 7 + 128 + 1

ffi.cdef([[
typedef struct {
    uint8_t  *buf;
    uint32_t  mask;
    uint32_t  head;
    uint32_t  tail;
} frame_ring_t;

typedef struct {
    uint8_t  addr;
    uint16_t cmd;
    uint8_t  seq;
    uint8_t  ack_seq;
    uint8_t  ack_status;
    uint8_t  payload_len;
} frame_meta_t;

typedef struct {
    uint8_t   dir;
    uint8_t   in_escape;
    uint8_t   in_frame;
    uint8_t   pad1;
    uint16_t  len;
    uint16_t  pad2;
    uint8_t   buf[]] .. FRAME_BUFFER_MAX .. [[];
} frame_decoder_t;

typedef struct {
    uint8_t       in_use;
    uint8_t       _pad[7];
    uint8_t       backing_m2s[]] .. TRANSPORT_INPROC_RING_SIZE .. [[];
    uint8_t       backing_s2m[]] .. TRANSPORT_INPROC_RING_SIZE .. [[];
    frame_ring_t  ring_m2s;
    frame_ring_t  ring_s2m;
    void         *physics_handle;
} transport_inproc_endpoint_t;

typedef struct {
    transport_inproc_endpoint_t endpoints[]] .. TRANSPORT_INPROC_MAX_ENDPOINTS .. [[];
} transport_inproc_t;

void transport_inproc_init  (transport_inproc_t *t);
int  transport_inproc_attach(transport_inproc_t *t, void *physics_handle);
void transport_inproc_detach(transport_inproc_t *t, int idx);

int      transport_inproc_master_write_byte (transport_inproc_t *t, int idx, uint8_t b);
int      transport_inproc_master_read_byte  (transport_inproc_t *t, int idx, uint8_t *out);
uint32_t transport_inproc_master_read_drain (transport_inproc_t *t, int idx, uint8_t *dst, uint32_t max);
int      transport_inproc_slave_read_byte   (transport_inproc_t *t, int idx, uint8_t *out);
int      transport_inproc_slave_write_byte  (transport_inproc_t *t, int idx, uint8_t b);
void*    transport_inproc_physics_handle    (transport_inproc_t *t, int idx);

int frame_encode_m2s(const frame_meta_t *meta, const uint8_t *payload, frame_ring_t *ring);
void frame_decoder_init(frame_decoder_t *d, int dir);
int  frame_decoder_feed(frame_decoder_t *d, uint8_t byte,
                        frame_meta_t *out_meta, uint8_t *out_payload);
]])

local C = ffi.load("./libcomm.so")

local FRAME_DIR_M2S = 0
local DECODE_FRAME_READY = 1

local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

------------------------------------------------------------------------
-- 1. Init + attach + detach
------------------------------------------------------------------------
io.write("[init/attach/detach]\n")
do
    local t = ffi.new("transport_inproc_t")
    C.transport_inproc_init(t)

    local h1 = ffi.cast("void*", 0x1111)
    local h2 = ffi.cast("void*", 0x2222)
    local i1 = C.transport_inproc_attach(t, h1)
    local i2 = C.transport_inproc_attach(t, h2)
    check(i1 == 0 and i2 == 1, "attach allocates 0,1 in order")
    check(C.transport_inproc_physics_handle(t, i1) == h1, "physics_handle round-trips for endpoint 0")
    check(C.transport_inproc_physics_handle(t, i2) == h2, "physics_handle round-trips for endpoint 1")
    C.transport_inproc_detach(t, i1)
    local i3 = C.transport_inproc_attach(t, ffi.cast("void*", 0x3333))
    check(i3 == 0, "detach frees slot, next attach reuses it")
end

------------------------------------------------------------------------
-- 2. Per-endpoint isolation
------------------------------------------------------------------------
io.write("[isolation]\n")
do
    local t = ffi.new("transport_inproc_t")
    C.transport_inproc_init(t)
    local idx_a = C.transport_inproc_attach(t, ffi.cast("void*", 0xA))
    local idx_b = C.transport_inproc_attach(t, ffi.cast("void*", 0xB))

    -- Master writes 0xAA to endpoint A, 0xBB to endpoint B.
    C.transport_inproc_master_write_byte(t, idx_a, 0xAA)
    C.transport_inproc_master_write_byte(t, idx_b, 0xBB)

    -- Slave A should read 0xAA, slave B should read 0xBB. Crossed reads should be empty.
    local out = ffi.new("uint8_t[1]")
    check(C.transport_inproc_slave_read_byte(t, idx_a, out) == 1 and out[0] == 0xAA,
          "slave A reads its own byte 0xAA")
    check(C.transport_inproc_slave_read_byte(t, idx_b, out) == 1 and out[0] == 0xBB,
          "slave B reads its own byte 0xBB")
    check(C.transport_inproc_slave_read_byte(t, idx_a, out) == 0,
          "slave A's ring is now empty (no leak from B)")

    -- Reverse: slaves write back, master reads per endpoint.
    C.transport_inproc_slave_write_byte(t, idx_a, 0x55)
    C.transport_inproc_slave_write_byte(t, idx_b, 0x66)
    check(C.transport_inproc_master_read_byte(t, idx_a, out) == 1 and out[0] == 0x55,
          "master reads slave A's reply 0x55")
    check(C.transport_inproc_master_read_byte(t, idx_b, out) == 1 and out[0] == 0x66,
          "master reads slave B's reply 0x66")
end

------------------------------------------------------------------------
-- 3. Backpressure: filling a ring returns 0
------------------------------------------------------------------------
io.write("[backpressure]\n")
do
    local t = ffi.new("transport_inproc_t")
    C.transport_inproc_init(t)
    local i = C.transport_inproc_attach(t, ffi.cast("void*", 0))

    local ok_count = 0
    -- Ring is 512; expect 512 successful writes, then 0.
    for _ = 1, TRANSPORT_INPROC_RING_SIZE do
        if C.transport_inproc_master_write_byte(t, i, 0x00) == 1 then
            ok_count = ok_count + 1
        end
    end
    local overflow = C.transport_inproc_master_write_byte(t, i, 0xFF)
    check(ok_count == TRANSPORT_INPROC_RING_SIZE and overflow == 0,
          string.format("ring fills to %d, then refuses writes", TRANSPORT_INPROC_RING_SIZE))
end

------------------------------------------------------------------------
-- 4. Bulk drain
------------------------------------------------------------------------
io.write("[bulk drain]\n")
do
    local t = ffi.new("transport_inproc_t")
    C.transport_inproc_init(t)
    local i = C.transport_inproc_attach(t, ffi.cast("void*", 0))

    for v = 0, 9 do
        C.transport_inproc_slave_write_byte(t, i, v)
    end
    local dst = ffi.new("uint8_t[16]")
    local n = C.transport_inproc_master_read_drain(t, i, dst, 16)
    local sequential = (n == 10)
    for v = 0, 9 do if dst[v] ~= v then sequential = false end end
    check(sequential, "master_read_drain returns 10 bytes in order")
end

------------------------------------------------------------------------
-- 5. End-to-end: encode frame → push through transport → decode
------------------------------------------------------------------------
io.write("[end-to-end frame through transport]\n")
do
    local t = ffi.new("transport_inproc_t")
    C.transport_inproc_init(t)
    local idx = C.transport_inproc_attach(t, ffi.cast("void*", 0xDEAD))

    -- Master encodes a frame into the m2s ring directly. transport just
    -- holds the ring; we hand the encoder a pointer to it via FFI.
    local ep = t.endpoints[idx]
    local meta = ffi.new("frame_meta_t")
    meta.addr = 1; meta.cmd = 0x0001; meta.seq = 5; meta.payload_len = 4
    local pl = ffi.new("uint8_t[4]", {0xC0, 0xDB, 0xAA, 0x55})  -- includes END+ESC
    local enc_rc = C.frame_encode_m2s(meta, pl, ffi.cast("frame_ring_t*", ep.ring_m2s))
    check(enc_rc == 0, "frame_encode_m2s into transport's m2s ring succeeds")

    -- Slave drains bytes one at a time, feeds an incremental decoder.
    local dec = ffi.new("frame_decoder_t")
    C.frame_decoder_init(dec, FRAME_DIR_M2S)
    local out_meta = ffi.new("frame_meta_t")
    local out_pl   = ffi.new("uint8_t[?]", 128)
    local got_byte = ffi.new("uint8_t[1]")
    local saw_ready = false
    while C.transport_inproc_slave_read_byte(t, idx, got_byte) == 1 do
        local rc = C.frame_decoder_feed(dec, got_byte[0], out_meta, out_pl)
        if rc == DECODE_FRAME_READY then saw_ready = true; break end
    end
    check(saw_ready
          and out_meta.addr == 1
          and out_meta.cmd == 0x0001
          and out_meta.seq == 5
          and out_meta.payload_len == 4
          and out_pl[0] == 0xC0 and out_pl[1] == 0xDB
          and out_pl[2] == 0xAA and out_pl[3] == 0x55,
          "slave decodes frame with embedded END/ESC bytes correctly")
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
