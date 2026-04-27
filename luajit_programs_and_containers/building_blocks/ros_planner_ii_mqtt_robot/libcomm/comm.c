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

static int                g_initialized = 0;
static router_t           g_router;
static link_t             g_link;
static transport_inproc_t g_host_inproc;
static const comm_manifest_v1_wire_t *g_manifest = 0;

static comm_slot_t        g_slots[COMM_HANDLES_MAX];

// One decoder per declared slave row, in each direction. Decoder state
// (in_escape, in_frame, accumulated bytes) persists across poll calls.
static frame_decoder_t    g_slave_decoder [COMM_SLAVES_MAX];   // m2s; slave consumes
static frame_decoder_t    g_master_decoder[COMM_SLAVES_MAX];   // s2m; master consumes

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

    g_manifest    = m;
    g_initialized = 1;
    return COMM_OK;
}

void comm_shutdown(void)
{
    if (!g_initialized) return;
    memset(&g_router,      0, sizeof(g_router));
    memset(&g_link,        0, sizeof(g_link));
    memset(&g_host_inproc, 0, sizeof(g_host_inproc));
    memset(g_slots,        0, sizeof(g_slots));
    memset(g_slave_decoder,  0, sizeof(g_slave_decoder));
    memset(g_master_decoder, 0, sizeof(g_master_decoder));
    g_manifest    = 0;
    g_initialized = 0;
}

comm_result_t comm_attach_internal(uint8_t dongle_idx,
                                   uint8_t bus_id,
                                   uint8_t addr,
                                   void*   physics_handle)
{
    if (!g_initialized)            return COMM_ERR_INIT;
    if (dongle_idx != 0)           return COMM_ERR_BAD_ARG;
    if (!physics_handle)           return COMM_ERR_BAD_ARG;

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

    transport_inproc_endpoint_t *ep = &g_host_inproc.endpoints[r->transport_endpoint_idx];
    uint8_t seq = l->next_seq++;

    frame_meta_t meta;
    memset(&meta, 0, sizeof(meta));
    meta.addr        = r->addr;
    meta.cmd         = cmd;
    meta.seq         = seq;
    meta.payload_len = (uint8_t)payload_len;

    if (frame_encode_m2s(&meta, payload, &ep->ring_m2s) != 0) {
        *err = COMM_ERR_OVERFLOW;
        return COMM_HANDLE_INVALID;
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

// Slave-side handler for one decoded m2s frame. Phase-1e contract:
//   PING            -> ACK_BARE (empty payload)
//   anything else   -> NAK (1-byte reason 0xFF)
// Encoded into the s2m ring; if the ring is full the response is dropped
// (phase-2 will surface this as FAULT_LINK).
static void slave_handle_frame(router_slave_t *r,
                               transport_inproc_endpoint_t *ep,
                               const frame_meta_t *in,
                               const uint8_t *in_payload)
{
    (void)in_payload;

    frame_meta_t out_meta;
    memset(&out_meta, 0, sizeof(out_meta));
    out_meta.addr    = r->addr;     // s2m: addr identifies the responding slave
    out_meta.seq     = 0;           // slave-originated seq; phase 1e responses are ack-only
    out_meta.ack_seq = in->seq;

    if (in->cmd == COMM_CMD_PING) {
        out_meta.cmd         = COMM_CMD_ACK_BARE;
        out_meta.ack_status  = 0;
        out_meta.payload_len = 0;
        (void)frame_encode_s2m(&out_meta, 0, &ep->ring_s2m);
    } else {
        uint8_t reason = 0xFF;       // generic "unsupported cmd" until phase 2 catalogues
        out_meta.cmd         = COMM_CMD_NAK;
        out_meta.ack_status  = 0;
        out_meta.payload_len = 1;
        (void)frame_encode_s2m(&out_meta, &reason, &ep->ring_s2m);
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
