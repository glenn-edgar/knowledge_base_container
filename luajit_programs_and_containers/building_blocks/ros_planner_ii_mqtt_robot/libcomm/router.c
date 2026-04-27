// libcomm/router.c
// Phase 1d: structural only. No frame dispatch, no transport ops —
// just the lookup table. Phase 2 wires comm_submit / link FSM through
// router_find_slave / router_find_by_triple.

#include "router.h"
#include <string.h>

void router_init(router_t *r)
{
    if (!r) return;
    memset(r, 0, sizeof(*r));
    for (int i = 0; i < COMM_SLAVES_MAX; i++) {
        r->slaves[i].transport_endpoint_idx = ROUTER_ENDPOINT_UNBOUND;
    }
}

comm_result_t router_build(router_t *r,
                           const comm_manifest_v1_wire_t *m,
                           transport_inproc_t *host_inproc)
{
    if (!r || !m || !host_inproc) return COMM_ERR_BAD_ARG;

    router_init(r);
    r->dongle_count = m->dongle_count;
    r->bus_count    = m->bus_count;
    r->slave_count  = m->slave_count;

    // Bind the virtual host dongle (dongles[0], uuid all zeros) to its
    // transport_inproc. Real dongles stay bound=0 until phase 2.
    r->dongles[0].bound  = 1;
    r->dongles[0].inproc = host_inproc;

    for (uint8_t i = 0; i < m->slave_count; i++) {
        r->slaves[i].mcu                    = m->slaves[i].mcu;
        r->slaves[i].dongle_idx             = m->slaves[i].dongle_idx;
        r->slaves[i].bus_id                 = m->slaves[i].bus_id;
        r->slaves[i].addr                   = m->slaves[i].addr;
        r->slaves[i].physics_model_id       = m->slaves[i].physics_model_id;
        r->slaves[i].transport_endpoint_idx = ROUTER_ENDPOINT_UNBOUND;
    }
    return COMM_OK;
}

router_slave_t *router_find_slave(router_t *r, uint8_t mcu)
{
    if (!r || mcu == 0) return 0;
    for (uint8_t i = 0; i < r->slave_count; i++) {
        if (r->slaves[i].mcu == mcu) return &r->slaves[i];
    }
    return 0;
}

const router_slave_t *router_find_slave_const(const router_t *r, uint8_t mcu)
{
    if (!r || mcu == 0) return 0;
    for (uint8_t i = 0; i < r->slave_count; i++) {
        if (r->slaves[i].mcu == mcu) return &r->slaves[i];
    }
    return 0;
}

router_slave_t *router_find_by_triple(router_t *r,
                                      uint8_t dongle_idx,
                                      uint8_t bus_id,
                                      uint8_t addr)
{
    if (!r) return 0;
    for (uint8_t i = 0; i < r->slave_count; i++) {
        router_slave_t *s = &r->slaves[i];
        if (s->dongle_idx == dongle_idx && s->bus_id == bus_id && s->addr == addr) {
            return s;
        }
    }
    return 0;
}
