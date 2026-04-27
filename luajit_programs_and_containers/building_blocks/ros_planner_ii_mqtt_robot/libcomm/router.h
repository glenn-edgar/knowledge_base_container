// libcomm/router.h
// Slice 1d. Compiled from a validated manifest at comm_init time:
// the host virtual dongle (dongles[0], uuid all zeros) gets bound
// to a transport_inproc instance; real dongles[1..] stay unbound
// until phase-2 enumeration runs the CMD_DONGLE_HELLO/IDENT
// handshake.
//
// Lookup is keyed by mcu (the chain-tree-visible identity). Per-slave
// row carries the routing target (dongle_idx, bus_id, addr), the
// expected physics_model_id, and — for in-process slaves — the
// transport endpoint index assigned by comm_attach_internal.

#pragma once

#include "comm.h"
#include "manifest.h"
#include "transport_inproc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ROUTER_ENDPOINT_UNBOUND  (-1)

typedef struct {
    uint8_t  mcu;                       // 0 = unused row
    uint8_t  dongle_idx;
    uint8_t  bus_id;
    uint8_t  addr;
    uint32_t physics_model_id;          // expected hash from manifest
    int      transport_endpoint_idx;    // -1 until comm_attach_internal binds; only used for host dongle today
} router_slave_t;

typedef struct {
    uint8_t              bound;         // 1 once a transport is attached
    transport_inproc_t  *inproc;        // non-NULL for the host virtual dongle; NULL for real dongles in phase 1
    // future: transport_uart_t *uart;
} router_dongle_t;

typedef struct {
    uint8_t          dongle_count;
    uint8_t          bus_count;
    uint8_t          slave_count;
    router_dongle_t  dongles[COMM_DONGLES_MAX];
    router_slave_t   slaves[COMM_SLAVES_MAX];
} router_t;

void router_init(router_t *r);

// Build the table from a validated manifest. Binds dongles[0] to
// host_inproc; real dongles stay unbound. Caller owns the lifetime of
// host_inproc — router holds a non-owning pointer.
comm_result_t router_build(router_t *r,
                           const comm_manifest_v1_wire_t *m,
                           transport_inproc_t *host_inproc);

// O(N) lookup. Returns NULL if mcu is not declared.
router_slave_t       *router_find_slave      (router_t       *r, uint8_t mcu);
const router_slave_t *router_find_slave_const(const router_t *r, uint8_t mcu);

// O(N) lookup of the row matching an explicit (dongle_idx, bus_id, addr)
// triple. Phase 1d uses this only inside comm_attach_internal; phase 2
// link-FSM dispatch will use it to map incoming frames back to mcu.
router_slave_t *router_find_by_triple(router_t *r,
                                      uint8_t dongle_idx,
                                      uint8_t bus_id,
                                      uint8_t addr);

#ifdef __cplusplus
}
#endif
