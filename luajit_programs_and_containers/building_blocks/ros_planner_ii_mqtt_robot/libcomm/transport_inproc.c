// libcomm/transport_inproc.c
// In-process transport: pure byte movement through paired rings.
// No SLIP, no framing, no routing — strictly the byte pump between
// master-side libcomm and in-process slaves.

#include "transport_inproc.h"

#include <string.h>

void transport_inproc_init(transport_inproc_t *t) {
    memset(t, 0, sizeof(*t));
    for (int i = 0; i < TRANSPORT_INPROC_MAX_ENDPOINTS; ++i) {
        frame_ring_init(&t->endpoints[i].ring_m2s,
                        t->endpoints[i].backing_m2s,
                        TRANSPORT_INPROC_RING_SIZE);
        frame_ring_init(&t->endpoints[i].ring_s2m,
                        t->endpoints[i].backing_s2m,
                        TRANSPORT_INPROC_RING_SIZE);
    }
}

int transport_inproc_attach(transport_inproc_t *t, void *physics_handle) {
    for (int i = 0; i < TRANSPORT_INPROC_MAX_ENDPOINTS; ++i) {
        transport_inproc_endpoint_t *e = &t->endpoints[i];
        if (!e->in_use) {
            e->in_use = 1;
            e->physics_handle = physics_handle;
            // Reset rings in case detach left residual counters.
            e->ring_m2s.head = 0;  e->ring_m2s.tail = 0;
            e->ring_s2m.head = 0;  e->ring_s2m.tail = 0;
            return i;
        }
    }
    return -1;
}

void transport_inproc_detach(transport_inproc_t *t, int idx) {
    if (idx < 0 || idx >= TRANSPORT_INPROC_MAX_ENDPOINTS) return;
    t->endpoints[idx].in_use         = 0;
    t->endpoints[idx].physics_handle = (void*)0;
}

static int valid_idx(transport_inproc_t *t, int idx) {
    return (idx >= 0
         && idx < TRANSPORT_INPROC_MAX_ENDPOINTS
         && t->endpoints[idx].in_use);
}

int transport_inproc_master_write_byte(transport_inproc_t *t, int idx, uint8_t b) {
    if (!valid_idx(t, idx)) return 0;
    return frame_ring_write_byte(&t->endpoints[idx].ring_m2s, b);
}

int transport_inproc_master_read_byte(transport_inproc_t *t, int idx, uint8_t *out) {
    if (!valid_idx(t, idx)) return 0;
    return frame_ring_read_byte(&t->endpoints[idx].ring_s2m, out);
}

uint32_t transport_inproc_master_read_drain(transport_inproc_t *t, int idx, uint8_t *dst, uint32_t max) {
    if (!valid_idx(t, idx)) return 0;
    return frame_ring_read_drain(&t->endpoints[idx].ring_s2m, dst, max);
}

int transport_inproc_slave_read_byte(transport_inproc_t *t, int idx, uint8_t *out) {
    if (!valid_idx(t, idx)) return 0;
    return frame_ring_read_byte(&t->endpoints[idx].ring_m2s, out);
}

int transport_inproc_slave_write_byte(transport_inproc_t *t, int idx, uint8_t b) {
    if (!valid_idx(t, idx)) return 0;
    return frame_ring_write_byte(&t->endpoints[idx].ring_s2m, b);
}

void* transport_inproc_physics_handle(transport_inproc_t *t, int idx) {
    if (!valid_idx(t, idx)) return (void*)0;
    return t->endpoints[idx].physics_handle;
}
