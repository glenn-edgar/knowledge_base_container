// libcomm/transport_inproc.h
// In-process transport: paired byte rings between master (host libcomm)
// and one or more in-process slaves (physics_core instances). No SLIP
// knowledge — that's frame.c's job. The transport is pure byte movement.
//
// Used by the virtual host dongle (dongles[0] in the manifest); real
// dongles use transport_uart.c instead. Same per-endpoint API shape so
// link.c FSM can be transport-agnostic at the byte level.

#pragma once

#include "frame.h"
#include "bus_config.h"

#ifdef __cplusplus
extern "C" {
#endif

// TRANSPORT_INPROC_MAX_ENDPOINTS and TRANSPORT_INPROC_RING_SIZE live in
// bus_config.h. The ring size must be power-of-2 (frame_ring_init).

typedef struct {
    uint8_t       in_use;
    uint8_t       _pad[7];
    uint8_t       backing_m2s[TRANSPORT_INPROC_RING_SIZE];
    uint8_t       backing_s2m[TRANSPORT_INPROC_RING_SIZE];
    frame_ring_t  ring_m2s;     // master → slave
    frame_ring_t  ring_s2m;     // slave  → master
    void         *physics_handle;
} transport_inproc_endpoint_t;

typedef struct {
    transport_inproc_endpoint_t endpoints[TRANSPORT_INPROC_MAX_ENDPOINTS];
} transport_inproc_t;

void transport_inproc_init  (transport_inproc_t *t);

// Returns endpoint index >= 0 on success, -1 if no free slot.
int  transport_inproc_attach(transport_inproc_t *t, void *physics_handle);
void transport_inproc_detach(transport_inproc_t *t, int endpoint_idx);

// Master-side I/O. write→ slave's RX ring, read← slave's TX ring.
// Single-byte API; bulk caller drives a loop.
int      transport_inproc_master_write_byte (transport_inproc_t *t, int idx, uint8_t b);
int      transport_inproc_master_read_byte  (transport_inproc_t *t, int idx, uint8_t *out);
uint32_t transport_inproc_master_read_drain (transport_inproc_t *t, int idx, uint8_t *dst, uint32_t max);

// Slave-side I/O. read← master, write→ master.
int      transport_inproc_slave_read_byte   (transport_inproc_t *t, int idx, uint8_t *out);
int      transport_inproc_slave_write_byte  (transport_inproc_t *t, int idx, uint8_t b);

// Diagnostics.
void* transport_inproc_physics_handle(transport_inproc_t *t, int idx);

#ifdef __cplusplus
}
#endif
