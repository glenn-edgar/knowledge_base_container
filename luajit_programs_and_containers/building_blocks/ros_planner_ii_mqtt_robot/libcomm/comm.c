// libcomm/comm.c
// Slice 1e: end-to-end submit/poll/claim through the in-process
// transport. Master encodes m2s frames; comm_poll runs the in-process
// slave loop (PING -> ACK_BARE; everything else -> NAK), drains s2m,
// matches responses to outstanding slots, and surfaces completion
// events. JOIN handshake, urgent-pending, timeouts, and broadcast are
// phase-2 work — broadcast still returns COMM_ERR_INIT here.
//
// _POSIX_C_SOURCE makes clock_gettime + CLOCK_MONOTONIC visible under
// -std=c99 (default glibc hides them in strict mode).

#define _POSIX_C_SOURCE 200809L

#include "comm.h"
#include "frame.h"
#include "manifest.h"
#include "router.h"
#include "link.h"
#include "transport_inproc.h"
#include "transport_uart.h"

#include <string.h>
#include <time.h>

// ============ SLOT TABLE ============

typedef enum {
    COMM_SLOT_FREE      = 0,
    COMM_SLOT_PENDING   = 1,
    COMM_SLOT_DONE      = 2,
    COMM_SLOT_NAK       = 3,
    COMM_SLOT_CANCELLED = 4,
} comm_slot_state_t;

typedef struct {
    uint32_t   gen;            // bumped on free; handle = (gen << 5) | slot
    uint8_t    state;          // comm_slot_state_t
    uint8_t    mcu;
    uint8_t    seq;            // m2s wire seq used for this request
    uint8_t    ack_status;     // populated on response
    uint8_t    surfaced;       // 1 = this slot's terminal state has already been written to a poll out[] buffer
    uint8_t    _pad[3];
    comm_cmd_t cmd;            // request cmd at submit, overwritten with response cmd on completion
    uint16_t   payload_len;    // bytes in payload[] (response payload)
    uint32_t   submit_ms;
    uint8_t    payload[COMM_PAYLOAD_MAX];
} comm_slot_t;

// ============ MODULE STATE ============

typedef struct {
    comm_slave_handler_fn fn;
    void                 *ctx;
} comm_slave_handler_t;

// Which transport backs the host side.
// INPROC  = libcomm hosts physics_core slaves itself (slice 1e shape).
// DONGLES = Phase B: chain_tree opens N pre-existing pty/tty paths
//           (per dongles.json). N can be 1.
typedef enum {
    HOST_TRANSPORT_NONE    = 0,
    HOST_TRANSPORT_INPROC  = 1,
    HOST_TRANSPORT_DONGLES = 2,
} host_transport_kind_t;

// Per-dongle state for HOST_TRANSPORT_DONGLES mode. One slot per
// manifest dongle index; in_use=1 once attachment + HELLO/IDENT
// succeeds.
typedef struct {
    uint8_t            in_use;
    uint8_t            _pad[3];
    uint16_t           dongle_type;
    uint16_t           dongle_instance;
    transport_uart_t   transport;
    frame_decoder_t    master_decoder;   // s2m decoder for this bus
} uart_dongle_t;

static int                g_initialized = 0;
static host_transport_kind_t g_host_kind = HOST_TRANSPORT_NONE;
static router_t           g_router;
static link_t             g_link;
static transport_inproc_t g_host_inproc;
static uart_dongle_t      g_uart_dongles[COMM_DONGLES_MAX];

// Owned copy of the validated manifest. manifest_validate aliases the
// caller's blob, but callers can't always keep that blob alive for
// libcomm's lifetime (notably LuaJIT FFI cdata that gets GC'd after the
// init call returns). Copying into static storage makes the lib robust
// against caller memory-management mistakes — symptom was a dangling
// g_manifest causing comm_poll's bus_id lookup to read garbage and every
// response to be silently dropped.
static comm_manifest_v1_wire_t        g_manifest_storage;
static const comm_manifest_v1_wire_t *g_manifest = 0;

static comm_slot_t        g_slots[COMM_HANDLES_MAX];

// One decoder per declared slave row, in each direction. Decoder state
// (in_escape, in_frame, accumulated bytes) persists across poll calls.
static frame_decoder_t    g_slave_decoder [COMM_SLAVES_MAX];   // m2s; slave consumes
static frame_decoder_t    g_master_decoder[COMM_SLAVES_MAX];   // s2m; master consumes

// Slice 2a — registered slave-class handlers, parallel to router.slaves.
static comm_slave_handler_t g_slave_handlers[COMM_SLAVES_MAX];

// Set inside comm_poll's slave loop while a registered handler is
// running, so comm_slave_respond/ack_bare/nak can route the response to
// the right transport endpoint and detect whether the handler emitted a
// response. Cleared back to NULL/0 once the handler returns.
static router_slave_t              *g_dispatch_slave    = 0;
static transport_inproc_endpoint_t *g_dispatch_endpoint = 0;
static uint8_t                      g_dispatch_responded = 0;

static comm_logger_fn     g_logger     = 0;
static int                g_log_level  = 0;

// ============ HELPERS ============

uint32_t comm_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint32_t)((uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u);
}

static comm_handle_t make_handle(uint8_t slot, uint32_t gen)
{
    return (gen << COMM_HANDLE_GEN_SHIFT) | (uint32_t)(slot & COMM_HANDLE_SLOT_MASK);
}

static comm_slot_t *resolve_handle(comm_handle_t h)
{
    if (h == COMM_HANDLE_INVALID) return 0;
    uint8_t  slot = comm_handle_slot(h);
    uint32_t gen  = comm_handle_gen(h);
    if (slot >= COMM_HANDLES_MAX) return 0;
    comm_slot_t *s = &g_slots[slot];
    if (s->gen != gen) return 0;     // stale handle (ABA defense)
    return s;
}

static int alloc_slot(void)
{
    for (int i = 0; i < COMM_HANDLES_MAX; i++) {
        if (g_slots[i].state == COMM_SLOT_FREE) return i;
    }
    return -1;
}

static void free_slot(comm_slot_t *s)
{
    s->state = COMM_SLOT_FREE;
    s->gen  += 1;
    if (s->gen == 0) s->gen = 1;     // skip the all-zeros handle sentinel after wrap
    s->surfaced    = 0;
    s->mcu         = 0;
    s->seq         = 0;
    s->ack_status  = 0;
    s->cmd         = 0;
    s->payload_len = 0;
    s->submit_ms   = 0;
}

static void slot_table_init(void)
{
    memset(g_slots, 0, sizeof(g_slots));
    for (int i = 0; i < COMM_HANDLES_MAX; i++) {
        g_slots[i].state = COMM_SLOT_FREE;
        g_slots[i].gen   = 1;        // first issued handle is non-zero
    }
}

static void decoders_init(void)
{
    for (int i = 0; i < COMM_SLAVES_MAX; i++) {
        frame_decoder_init(&g_slave_decoder [i], FRAME_DIR_M2S);
        frame_decoder_init(&g_master_decoder[i], FRAME_DIR_S2M);
    }
}

static void handlers_init(void)
{
    for (int i = 0; i < COMM_SLAVES_MAX; i++) {
        g_slave_handlers[i].fn  = 0;
        g_slave_handlers[i].ctx = 0;
    }
    g_dispatch_slave     = 0;
    g_dispatch_endpoint  = 0;
    g_dispatch_responded = 0;
}

// Find the row index in g_router.slaves[] for a given router_slave_t* obtained
// from router_find_slave. Used to keep g_slave_handlers parallel to slaves[].
static int slave_index_of(const router_slave_t *r)
{
    if (!r) return -1;
    ptrdiff_t idx = r - &g_router.slaves[0];
    if (idx < 0 || idx >= COMM_SLAVES_MAX) return -1;
    return (int)idx;
}

static void event_from_slot(const comm_slot_t *s, uint8_t slot_idx, comm_event_t *e)
{
    e->handle      = make_handle(slot_idx, s->gen);
    e->cmd         = s->cmd;
    e->mcu         = s->mcu;
    e->ack_status  = s->ack_status;
    e->seq         = s->seq;
    e->payload_len = s->payload_len;
    e->elapsed_ms  = comm_now_ms() - s->submit_ms;
    if (s->payload_len > 0 && s->payload_len <= COMM_PAYLOAD_MAX) {
        memcpy(e->payload, s->payload, s->payload_len);
    }
}

// ============ LIFECYCLE ============

comm_result_t comm_init(const uint8_t* manifest_blob, size_t blob_len)
{
    if (g_initialized) return COMM_ERR_INIT;

    const comm_manifest_v1_wire_t *m = 0;
    comm_result_t rc = manifest_validate(manifest_blob, blob_len, &m);
    if (rc != COMM_OK) return rc;

    transport_inproc_init(&g_host_inproc);

    rc = router_build(&g_router, m, &g_host_inproc);
    if (rc != COMM_OK) return rc;

    rc = link_build(&g_link, m);
    if (rc != COMM_OK) return rc;

    slot_table_init();
    decoders_init();
    handlers_init();

    g_manifest_storage = *m;
    g_manifest         = &g_manifest_storage;
    g_host_kind        = HOST_TRANSPORT_INPROC;
    g_initialized = 1;
    return COMM_OK;
}

void comm_shutdown(void)
{
    if (!g_initialized) return;
    if (g_host_kind == HOST_TRANSPORT_DONGLES) {
        for (int i = 0; i < COMM_DONGLES_MAX; i++) {
            if (g_uart_dongles[i].in_use) {
                transport_uart_close(&g_uart_dongles[i].transport);
            }
        }
    }
    memset(&g_router,         0, sizeof(g_router));
    memset(&g_link,           0, sizeof(g_link));
    memset(&g_host_inproc,    0, sizeof(g_host_inproc));
    for (int i = 0; i < COMM_DONGLES_MAX; i++) {
        memset(&g_uart_dongles[i], 0, sizeof(g_uart_dongles[i]));
        g_uart_dongles[i].transport.master_fd = -1;
    }
    memset(g_slots,           0, sizeof(g_slots));
    memset(g_slave_decoder,   0, sizeof(g_slave_decoder));
    memset(g_master_decoder,  0, sizeof(g_master_decoder));
    memset(g_slave_handlers,  0, sizeof(g_slave_handlers));
    g_dispatch_slave     = 0;
    g_dispatch_endpoint  = 0;
    g_dispatch_responded = 0;
    g_manifest    = 0;
    g_host_kind   = HOST_TRANSPORT_NONE;
    g_initialized = 0;
}

// ============ MULTI-DONGLE INIT (Phase B) ============

// Find the manifest dongle index whose declared (type, instance) matches.
// Returns -1 if no match.
static int find_manifest_dongle_idx(const comm_manifest_v1_wire_t *m,
                                    uint16_t dongle_type,
                                    uint16_t dongle_instance)
{
    for (uint8_t i = 0; i < m->dongle_count; i++) {
        const uint8_t *uuid = m->dongles[i].dongle_uuid;
        if (comm_dongle_get_type(uuid)     == dongle_type &&
            comm_dongle_get_instance(uuid) == dongle_instance) {
            return (int)i;
        }
    }
    return -1;
}

// Synchronous HELLO/IDENT exchange on a freshly-opened transport.
// Returns COMM_OK on validated handshake, an appropriate error otherwise.
// Pumps the transport in-loop until either an IDENT decodes or
// `timeout_ms` elapses.
static comm_result_t handshake_dongle(uart_dongle_t *d,
                                      uint16_t expected_type,
                                      uint16_t expected_instance,
                                      uint32_t timeout_ms)
{
    // 1) Encode + send HELLO. host_epoch unused at this layer (slave's
    //    ack_seq carries the seq we just sent, which is the correlation
    //    we actually validate).
    uint8_t  hello_payload[4] = { 0, 0, 0, 0 };
    uint32_t epoch = comm_now_ms() ^ 0xA5A5A5A5u;
    hello_payload[0] = (uint8_t)(epoch & 0xFF);
    hello_payload[1] = (uint8_t)((epoch >> 8)  & 0xFF);
    hello_payload[2] = (uint8_t)((epoch >> 16) & 0xFF);
    hello_payload[3] = (uint8_t)((epoch >> 24) & 0xFF);

    const uint8_t hello_seq = 0x01;
    frame_meta_t hello_meta;
    memset(&hello_meta, 0, sizeof(hello_meta));
    hello_meta.addr        = COMM_ADDR_DONGLE_SELF;
    hello_meta.cmd         = COMM_CMD_DONGLE_HELLO;
    hello_meta.seq         = hello_seq;
    hello_meta.payload_len = sizeof(hello_payload);

    if (frame_encode_m2s(&hello_meta, hello_payload, &d->transport.tx_ring) != 0) {
        return COMM_ERR_OVERFLOW;
    }
    if (transport_uart_pump(&d->transport) < 0) return COMM_ERR_FAULT;

    // 2) Wait for IDENT. Spin-pump the transport, draining bytes through
    //    the master decoder. First decoded IDENT frame is the answer;
    //    anything else is a fault.
    uint32_t deadline = comm_now_ms() + timeout_ms;
    while (1) {
        if (transport_uart_pump(&d->transport) < 0) return COMM_ERR_FAULT;

        uint8_t      byte;
        frame_meta_t fm;
        uint8_t      fp[COMM_PAYLOAD_MAX];
        while (frame_ring_read_byte(&d->transport.rx_ring, &byte) == 1) {
            frame_decode_result_t dr =
                frame_decoder_feed(&d->master_decoder, byte, &fm, fp);
            if (dr != FRAME_DECODE_FRAME_READY) continue;

            if (fm.addr != COMM_ADDR_DONGLE_SELF)         return COMM_ERR_BAD_MANIFEST;
            if (fm.cmd  != COMM_CMD_DONGLE_IDENT)         return COMM_ERR_BAD_MANIFEST;
            if (fm.ack_seq != hello_seq)                  return COMM_ERR_BAD_MANIFEST;
            if (fm.payload_len < 4)                       return COMM_ERR_BAD_MANIFEST;

            uint16_t got_type     = comm_dongle_get_type(fp);
            uint16_t got_instance = comm_dongle_get_instance(fp);
            if (got_type     != expected_type)         return COMM_ERR_DONGLE_UNEXPECTED;
            if (got_instance != expected_instance)     return COMM_ERR_DONGLE_UNEXPECTED;
            return COMM_OK;
        }

        if (comm_now_ms() >= deadline) return COMM_ERR_TIMEOUT;
        // Tight enough to keep latency low without burning CPU.
        struct timespec t = { 0, 500 * 1000 };  // 0.5 ms
        nanosleep(&t, 0);
    }
}

static void close_all_dongles(void)
{
    for (int i = 0; i < COMM_DONGLES_MAX; i++) {
        if (g_uart_dongles[i].in_use) {
            transport_uart_close(&g_uart_dongles[i].transport);
        }
        memset(&g_uart_dongles[i], 0, sizeof(g_uart_dongles[i]));
        g_uart_dongles[i].transport.master_fd = -1;
    }
}

comm_result_t comm_init_with_dongles(const uint8_t              *manifest_blob,
                                     size_t                      blob_len,
                                     const comm_dongle_attach_t *specs,
                                     size_t                      n_specs)
{
    if (g_initialized)         return COMM_ERR_INIT;
    if (n_specs == 0 || !specs) return COMM_ERR_BAD_ARG;

    const comm_manifest_v1_wire_t *m = 0;
    comm_result_t rc = manifest_validate(manifest_blob, blob_len, &m);
    if (rc != COMM_OK) return rc;

    if (n_specs != m->dongle_count) return COMM_ERR_BAD_ARG;

    // Reset the multi-dongle slot table.
    for (int i = 0; i < COMM_DONGLES_MAX; i++) {
        memset(&g_uart_dongles[i], 0, sizeof(g_uart_dongles[i]));
        g_uart_dongles[i].transport.master_fd = -1;
    }

    // Build router as inproc-shaped (placeholder host_inproc kept valid),
    // then re-stamp each manifest dongle for uart and auto-bind its
    // declared slaves so submit's bind guard passes.
    transport_inproc_init(&g_host_inproc);
    rc = router_build(&g_router, m, &g_host_inproc);
    if (rc != COMM_OK) return rc;

    rc = link_build(&g_link, m);
    if (rc != COMM_OK) return rc;

    // Pull join_timeout_ms from the first bus's tunables. All buses share
    // the same handshake budget at this layer.
    uint32_t join_timeout_ms = (m->bus_count > 0)
        ? (uint32_t)m->buses[0].tunables.join_timeout_ms
        : 500u;

    // For each spec: locate the matching manifest dongle index by
    // (type, instance), open the path, exchange HELLO/IDENT, bind.
    for (size_t s = 0; s < n_specs; s++) {
        int idx = find_manifest_dongle_idx(m,
                                           specs[s].dongle_type,
                                           specs[s].dongle_instance);
        if (idx < 0) {
            close_all_dongles();
            return COMM_ERR_DONGLE_UNEXPECTED;
        }
        if (g_uart_dongles[idx].in_use) {
            // Same (type, instance) declared twice in specs[].
            close_all_dongles();
            return COMM_ERR_BAD_ARG;
        }

        uart_dongle_t *d = &g_uart_dongles[idx];
        int open_rc = transport_uart_init_open(&d->transport, specs[s].path);
        if (open_rc != 0) {
            close_all_dongles();
            return (open_rc == -2) ? COMM_ERR_BUS : COMM_ERR_BAD_ARG;
        }
        frame_decoder_init(&d->master_decoder, FRAME_DIR_S2M);
        d->dongle_type     = specs[s].dongle_type;
        d->dongle_instance = specs[s].dongle_instance;

        comm_result_t hrc = handshake_dongle(d,
                                              specs[s].dongle_type,
                                              specs[s].dongle_instance,
                                              join_timeout_ms);
        if (hrc != COMM_OK) {
            close_all_dongles();
            return hrc;
        }
        d->in_use = 1;

        // Mark every slave on this dongle as bound (sentinel 0). The actual
        // routing in submit/poll uses dongle_idx, not transport_endpoint_idx.
        for (uint8_t j = 0; j < g_router.slave_count; j++) {
            if (g_router.slaves[j].dongle_idx == idx) {
                g_router.slaves[j].transport_endpoint_idx = 0;
            }
        }
    }

    // Every manifest dongle must be bound now (n_specs == dongle_count
    // guard above, plus uniqueness check inside the loop, ensures this).
    for (uint8_t i = 0; i < m->dongle_count; i++) {
        if (!g_uart_dongles[i].in_use) {
            close_all_dongles();
            return COMM_ERR_DONGLE_MISSING;
        }
    }

    slot_table_init();
    decoders_init();
    handlers_init();

    g_manifest_storage = *m;
    g_manifest         = &g_manifest_storage;
    g_host_kind        = HOST_TRANSPORT_DONGLES;
    g_initialized = 1;
    return COMM_OK;
}

comm_result_t comm_attach_internal(uint8_t dongle_idx,
                                   uint8_t bus_id,
                                   uint8_t addr,
                                   void*   physics_handle)
{
    if (!g_initialized)                          return COMM_ERR_INIT;
    if (g_host_kind != HOST_TRANSPORT_INPROC)    return COMM_ERR_BAD_ARG;
    if (dongle_idx != 0)                         return COMM_ERR_BAD_ARG;
    if (!physics_handle)                         return COMM_ERR_BAD_ARG;

    router_slave_t *s = router_find_by_triple(&g_router, dongle_idx, bus_id, addr);
    if (!s)                                                   return COMM_ERR_BAD_ARG;
    if (s->transport_endpoint_idx != ROUTER_ENDPOINT_UNBOUND) return COMM_ERR_BAD_ARG;

    int ep = transport_inproc_attach(&g_host_inproc, physics_handle);
    if (ep < 0) return COMM_ERR_OVERFLOW;

    s->transport_endpoint_idx = ep;
    return COMM_OK;
}

// ============ ASYNC SUBMIT ============

comm_handle_t comm_submit(uint8_t mcu,
                          comm_cmd_t cmd,
                          const uint8_t* payload, uint16_t payload_len,
                          comm_result_t* err)
{
    comm_result_t local_err = COMM_OK;
    if (!err) err = &local_err;

    if (!g_initialized)               { *err = COMM_ERR_INIT;    return COMM_HANDLE_INVALID; }
    if (payload_len > COMM_PAYLOAD_MAX){ *err = COMM_ERR_BAD_ARG; return COMM_HANDLE_INVALID; }
    if (payload_len > 0 && !payload)  { *err = COMM_ERR_BAD_ARG; return COMM_HANDLE_INVALID; }

    router_slave_t *r = router_find_slave(&g_router, mcu);
    if (!r)                           { *err = COMM_ERR_BAD_ARG; return COMM_HANDLE_INVALID; }
    if (r->transport_endpoint_idx == ROUTER_ENDPOINT_UNBOUND) {
        *err = COMM_ERR_BUS;          return COMM_HANDLE_INVALID;
    }

    link_slave_state_t *l = link_find_slave(&g_link, mcu);
    if (!l)                           { *err = COMM_ERR_BAD_ARG; return COMM_HANDLE_INVALID; }
    if (l->outstanding_slot >= 0)     { *err = COMM_ERR_BUS;     return COMM_HANDLE_INVALID; }
    if (l->state == COMM_NODE_FAULTED){ *err = COMM_ERR_FAULT;   return COMM_HANDLE_INVALID; }

    int slot_idx = alloc_slot();
    if (slot_idx < 0)                 { *err = COMM_ERR_OVERFLOW;return COMM_HANDLE_INVALID; }

    uint8_t seq = l->next_seq++;

    frame_meta_t meta;
    memset(&meta, 0, sizeof(meta));
    meta.addr        = r->addr;
    meta.cmd         = cmd;
    meta.seq         = seq;
    meta.payload_len = (uint8_t)payload_len;

    if (g_host_kind == HOST_TRANSPORT_INPROC) {
        transport_inproc_endpoint_t *ep =
            &g_host_inproc.endpoints[r->transport_endpoint_idx];
        if (frame_encode_m2s(&meta, payload, &ep->ring_m2s) != 0) {
            *err = COMM_ERR_OVERFLOW;
            return COMM_HANDLE_INVALID;
        }
    } else {  // HOST_TRANSPORT_DONGLES
        uint8_t didx = r->dongle_idx;
        if (didx >= COMM_DONGLES_MAX || !g_uart_dongles[didx].in_use) {
            *err = COMM_ERR_BUS;
            return COMM_HANDLE_INVALID;
        }
        uart_dongle_t *d = &g_uart_dongles[didx];
        if (frame_encode_m2s(&meta, payload, &d->transport.tx_ring) != 0) {
            *err = COMM_ERR_OVERFLOW;
            return COMM_HANDLE_INVALID;
        }
        (void)transport_uart_pump(&d->transport);
    }

    comm_slot_t *s = &g_slots[slot_idx];
    s->state       = COMM_SLOT_PENDING;
    s->mcu         = mcu;
    s->seq         = seq;
    s->ack_status  = 0;
    s->surfaced    = 0;
    s->cmd         = cmd;
    s->payload_len = 0;
    s->submit_ms   = comm_now_ms();

    l->outstanding_slot = (int8_t)slot_idx;

    *err = COMM_OK;
    return make_handle((uint8_t)slot_idx, s->gen);
}

comm_handle_t comm_submit_broadcast(uint8_t bus_id,
                                    comm_cmd_t cmd,
                                    uint8_t responder,
                                    const uint8_t* payload, uint16_t payload_len,
                                    comm_result_t* err)
{
    (void)bus_id; (void)cmd; (void)responder; (void)payload; (void)payload_len;
    if (err) *err = COMM_ERR_INIT;
    return COMM_HANDLE_INVALID;
}

// ============ STATUS / CLAIM / CANCEL ============

comm_result_t comm_status(comm_handle_t h)
{
    if (!g_initialized) return COMM_ERR_INIT;
    comm_slot_t *s = resolve_handle(h);
    if (!s) return COMM_ERR_BAD_HANDLE;
    switch (s->state) {
        case COMM_SLOT_PENDING:   return COMM_ERR_NO_DATA;
        case COMM_SLOT_DONE:      return COMM_OK;
        case COMM_SLOT_NAK:       return COMM_ERR_NAK;
        case COMM_SLOT_CANCELLED: return COMM_ERR_BAD_HANDLE;
        case COMM_SLOT_FREE:
        default:                  return COMM_ERR_BAD_HANDLE;
    }
}

comm_result_t comm_claim(comm_handle_t h, comm_event_t* out)
{
    if (!g_initialized) return COMM_ERR_INIT;
    if (!out)           return COMM_ERR_BAD_ARG;

    comm_slot_t *s = resolve_handle(h);
    if (!s)                              return COMM_ERR_BAD_HANDLE;
    if (s->state == COMM_SLOT_PENDING)   return COMM_ERR_NO_DATA;
    if (s->state == COMM_SLOT_FREE ||
        s->state == COMM_SLOT_CANCELLED) return COMM_ERR_BAD_HANDLE;

    uint8_t slot_idx = comm_handle_slot(h);
    memset(out, 0, sizeof(*out));
    event_from_slot(s, slot_idx, out);

    comm_result_t rc = (s->state == COMM_SLOT_NAK) ? COMM_ERR_NAK : COMM_OK;
    free_slot(s);
    return rc;
}

comm_result_t comm_cancel(comm_handle_t h)
{
    if (!g_initialized) return COMM_ERR_INIT;
    comm_slot_t *s = resolve_handle(h);
    if (!s) return COMM_ERR_BAD_HANDLE;
    if (s->state == COMM_SLOT_FREE) return COMM_ERR_BAD_HANDLE;

    if (s->state == COMM_SLOT_PENDING) {
        link_slave_state_t *l = link_find_slave(&g_link, s->mcu);
        if (l && l->outstanding_slot == (int8_t)comm_handle_slot(h)) {
            l->outstanding_slot = -1;
        }
    }
    free_slot(s);
    return COMM_OK;
}

// ============ POLL: in-process slave loop + master rx + event surface ============

// Internal: emit a response frame into a known endpoint's s2m ring.
// Used by both the built-in default responder and comm_slave_respond.
static comm_result_t emit_response(router_slave_t *r,
                                   transport_inproc_endpoint_t *ep,
                                   uint8_t in_seq,
                                   comm_cmd_t cmd,
                                   const uint8_t *payload,
                                   uint16_t payload_len,
                                   uint8_t ack_status)
{
    if (payload_len > COMM_PAYLOAD_MAX)        return COMM_ERR_BAD_ARG;
    if (payload_len > 0 && !payload)           return COMM_ERR_BAD_ARG;

    frame_meta_t out_meta;
    memset(&out_meta, 0, sizeof(out_meta));
    out_meta.addr        = r->addr;
    out_meta.cmd         = cmd;
    out_meta.seq         = 0;
    out_meta.ack_seq     = in_seq;
    out_meta.ack_status  = ack_status;
    out_meta.payload_len = (uint8_t)payload_len;

    if (frame_encode_s2m(&out_meta, payload, &ep->ring_s2m) != 0) {
        return COMM_ERR_OVERFLOW;
    }
    return COMM_OK;
}

// Built-in default responder for an mcu without a registered handler.
// PING -> ACK_BARE; anything else -> NAK reason 0xFF.
static void default_responder(router_slave_t *r,
                              transport_inproc_endpoint_t *ep,
                              const frame_meta_t *in)
{
    if (in->cmd == COMM_CMD_PING) {
        (void)emit_response(r, ep, in->seq, COMM_CMD_ACK_BARE, 0, 0, 0);
    } else {
        uint8_t reason = COMM_NAK_REASON_UNKNOWN_CMD;
        (void)emit_response(r, ep, in->seq, COMM_CMD_NAK, &reason, 1, 0);
    }
}

// Slave-side handler for one decoded m2s frame. Slice 2a: dispatches to
// the registered slave-class handler if any, otherwise falls back to the
// built-in default responder so existing PING tests still work.
static void slave_handle_frame(router_slave_t *r,
                               transport_inproc_endpoint_t *ep,
                               const frame_meta_t *in,
                               const uint8_t *in_payload)
{
    int slave_idx = slave_index_of(r);
    comm_slave_handler_t *h = (slave_idx >= 0) ? &g_slave_handlers[slave_idx] : 0;

    if (h && h->fn) {
        // Set dispatch context so comm_slave_respond/ack_bare/nak know
        // which endpoint to write to and we can detect a missing reply.
        g_dispatch_slave     = r;
        g_dispatch_endpoint  = ep;
        g_dispatch_responded = 0;

        h->fn(r->mcu, in->cmd, in_payload, in->payload_len, in->seq, h->ctx);

        if (!g_dispatch_responded) {
            // Handler returned without responding: emit auto-NAK so
            // master never hangs.
            uint8_t reason = COMM_NAK_REASON_NO_RESPONSE;
            (void)emit_response(r, ep, in->seq, COMM_CMD_NAK, &reason, 1, 0);
        }

        g_dispatch_slave     = 0;
        g_dispatch_endpoint  = 0;
        g_dispatch_responded = 0;
    } else {
        default_responder(r, ep, in);
    }
}

// Master-side dispatch when one s2m frame finishes decoding.
static void master_handle_frame(router_slave_t *r,
                                const frame_meta_t *in,
                                const uint8_t *in_payload)
{
    link_slave_state_t *l = link_find_slave(&g_link, r->mcu);
    if (!l) return;

    int slot_idx = l->outstanding_slot;
    if (slot_idx < 0 || slot_idx >= COMM_HANDLES_MAX) return;
    comm_slot_t *s = &g_slots[slot_idx];
    if (s->state != COMM_SLOT_PENDING) return;
    if (s->mcu != r->mcu)              return;
    if (s->seq != in->ack_seq)         return;     // stale or unmatched

    s->ack_status  = in->ack_status;
    s->cmd         = in->cmd;
    s->payload_len = in->payload_len;
    if (in->payload_len > 0 && in_payload) {
        if (in->payload_len > COMM_PAYLOAD_MAX) return;
        memcpy(s->payload, in_payload, in->payload_len);
    }
    s->state    = (in->cmd == COMM_CMD_NAK) ? COMM_SLOT_NAK : COMM_SLOT_DONE;
    s->surfaced = 0;

    l->outstanding_slot = -1;
    l->last_seen_ms     = comm_now_ms();
    // Phase-2: link state transitions, miss_count reset, ACK_FLAG_URGENT -> CMD_DRAIN.
}

static int slot_is_terminal(uint8_t state)
{
    return state == COMM_SLOT_DONE || state == COMM_SLOT_NAK;
}

int comm_poll(comm_event_t* out, int max)
{
    if (!g_initialized) return COMM_ERR_INIT;

    if (g_host_kind == HOST_TRANSPORT_INPROC) {
        // Phase 1: drive each in-process slave (drain m2s, generate response into s2m).
        for (uint8_t i = 0; i < g_router.slave_count; i++) {
            router_slave_t *r = &g_router.slaves[i];
            if (r->dongle_idx != 0) continue;                          // only host inproc this slice
            if (r->transport_endpoint_idx == ROUTER_ENDPOINT_UNBOUND) continue;

            transport_inproc_endpoint_t *ep = &g_host_inproc.endpoints[r->transport_endpoint_idx];

            frame_meta_t  fm;
            uint8_t       fp[COMM_PAYLOAD_MAX];
            uint8_t       byte;
            while (transport_inproc_slave_read_byte(&g_host_inproc, r->transport_endpoint_idx, &byte) == 1) {
                frame_decode_result_t dr =
                    frame_decoder_feed(&g_slave_decoder[i], byte, &fm, fp);
                if (dr == FRAME_DECODE_FRAME_READY) {
                    slave_handle_frame(r, ep, &fm, fp);
                }
                // BAD_CRC / BAD_LEN / OVERFLOW: decoder auto-resets; phase-2 will fault the link.
            }
        }

        // Phase 2: drain each slave's s2m back to the master.
        for (uint8_t i = 0; i < g_router.slave_count; i++) {
            router_slave_t *r = &g_router.slaves[i];
            if (r->dongle_idx != 0) continue;
            if (r->transport_endpoint_idx == ROUTER_ENDPOINT_UNBOUND) continue;

            frame_meta_t fm;
            uint8_t      fp[COMM_PAYLOAD_MAX];
            uint8_t      byte;
            while (transport_inproc_master_read_byte(&g_host_inproc, r->transport_endpoint_idx, &byte) == 1) {
                frame_decode_result_t dr =
                    frame_decoder_feed(&g_master_decoder[i], byte, &fm, fp);
                if (dr == FRAME_DECODE_FRAME_READY) {
                    master_handle_frame(r, &fm, fp);
                }
            }
        }
    } else if (g_host_kind == HOST_TRANSPORT_DONGLES) {
        // Phase B: iterate every bound dongle. Each dongle has its own
        // pty FD + tx/rx rings + master decoder. Decoded frames route
        // by (dongle_idx, addr) — addresses are bus-local, so two
        // dongles can each have a slave at addr=1 without collision.
        // Drain → pump → drain order: empties any leftover bytes from
        // the previous tick first, then refills from the FD, then
        // empties what we just read. Keeps rx_ring from staying full
        // long enough to backpressure the slave's writes.
        for (uint8_t i = 0; i < COMM_DONGLES_MAX; i++) {
            uart_dongle_t *d = &g_uart_dongles[i];
            if (!d->in_use) continue;

            uint8_t bus_id = (g_manifest && g_manifest->bus_count > 0)
                ? g_manifest->dongles[i].bus_local_ids[0]
                : 0;

            for (int pass = 0; pass < 2; pass++) {
                if (pass == 1) (void)transport_uart_pump(&d->transport);

                frame_meta_t fm;
                uint8_t      fp[COMM_PAYLOAD_MAX];
                uint8_t      byte;
                while (frame_ring_read_byte(&d->transport.rx_ring, &byte) == 1) {
                    frame_decode_result_t dr =
                        frame_decoder_feed(&d->master_decoder, byte, &fm, fp);
                    if (dr != FRAME_DECODE_FRAME_READY) continue;
                    router_slave_t *r =
                        router_find_by_triple(&g_router, i, bus_id, fm.addr);
                    if (r) master_handle_frame(r, &fm, fp);
                }
            }
        }
    }

    // Phase 3: surface terminal-but-not-yet-surfaced slots into the caller's buffer.
    if (!out || max <= 0) return 0;

    int written = 0;
    for (uint8_t s = 0; s < COMM_HANDLES_MAX && written < max; s++) {
        comm_slot_t *slot = &g_slots[s];
        if (!slot->surfaced && slot_is_terminal(slot->state)) {
            event_from_slot(slot, s, &out[written]);
            slot->surfaced = 1;
            written++;
        }
    }
    return written;
}

// ============ SLAVE HANDLER REGISTRATION (slice 2a) ============

comm_result_t comm_set_slave_handler(uint8_t                mcu,
                                     comm_slave_handler_fn  fn,
                                     void                  *ctx)
{
    if (!g_initialized) return COMM_ERR_INIT;
    router_slave_t *r = router_find_slave(&g_router, mcu);
    if (!r) return COMM_ERR_BAD_ARG;
    int idx = slave_index_of(r);
    if (idx < 0) return COMM_ERR_BAD_ARG;
    g_slave_handlers[idx].fn  = fn;
    g_slave_handlers[idx].ctx = ctx;
    return COMM_OK;
}

comm_result_t comm_slave_respond(uint8_t        mcu,
                                 uint8_t        in_seq,
                                 comm_cmd_t     cmd,
                                 const uint8_t *payload,
                                 uint16_t       payload_len,
                                 uint8_t        ack_status)
{
    if (!g_initialized)        return COMM_ERR_INIT;
    if (!g_dispatch_slave)     return COMM_ERR_INIT;          // not in a handler context
    if (g_dispatch_slave->mcu != mcu) return COMM_ERR_BAD_ARG; // wrong mcu
    if (g_dispatch_responded)  return COMM_ERR_BAD_ARG;        // double-respond

    comm_result_t rc = emit_response(g_dispatch_slave, g_dispatch_endpoint,
                                     in_seq, cmd, payload, payload_len, ack_status);
    if (rc == COMM_OK) g_dispatch_responded = 1;
    return rc;
}

comm_result_t comm_slave_ack_bare(uint8_t mcu, uint8_t in_seq)
{
    return comm_slave_respond(mcu, in_seq, COMM_CMD_ACK_BARE, 0, 0, 0);
}

comm_result_t comm_slave_nak(uint8_t mcu, uint8_t in_seq, uint8_t reason)
{
    return comm_slave_respond(mcu, in_seq, COMM_CMD_NAK, &reason, 1, 0);
}

// ============ DIAGNOSTICS ============

comm_node_state_t comm_node_state(uint8_t mcu)
{
    if (!g_initialized) return COMM_NODE_UNKNOWN;
    const link_slave_state_t *s = link_find_slave_const(&g_link, mcu);
    if (!s) return COMM_NODE_UNKNOWN;
    return (comm_node_state_t)s->state;
}

uint32_t comm_node_physics_model(uint8_t mcu)
{
    if (!g_initialized) return 0;
    const link_slave_state_t *s = link_find_slave_const(&g_link, mcu);
    return s ? s->physics_model_id : 0u;
}

uint8_t comm_node_miss_count(uint8_t mcu)
{
    if (!g_initialized) return 0;
    const link_slave_state_t *s = link_find_slave_const(&g_link, mcu);
    return s ? s->miss_count : (uint8_t)0;
}

uint32_t comm_node_last_seen_ms(uint8_t mcu)
{
    if (!g_initialized) return 0;
    const link_slave_state_t *s = link_find_slave_const(&g_link, mcu);
    return s ? s->last_seen_ms : 0u;
}

// ============ COMMISSIONING (no-op in this port) ============

int comm_pending_commission(uint8_t out_uuid[16])
{
    (void)out_uuid;
    return 0;
}

comm_result_t comm_commission_assign(const uint8_t uuid[16],
                                     uint8_t dongle_idx,
                                     uint8_t bus_id,
                                     uint8_t addr)
{
    (void)uuid; (void)dongle_idx; (void)bus_id; (void)addr;
    return COMM_ERR_INIT;
}

comm_result_t comm_commission_clear(const uint8_t uuid[16])
{
    (void)uuid;
    return COMM_ERR_INIT;
}

// ============ LOGGING ============

void comm_set_logger(comm_logger_fn fn, int level)
{
    g_logger    = fn;
    g_log_level = level;
}
