// libcomm/link.c
// Phase 1d: structural only. comm.c reads the per-slave state for the
// diagnostic accessors; phase 2 mutates it via the join FSM.

#include "link.h"
#include <string.h>

void link_init(link_t *l)
{
    if (!l) return;
    memset(l, 0, sizeof(*l));
}

comm_result_t link_build(link_t *l, const comm_manifest_v1_wire_t *m)
{
    if (!l || !m) return COMM_ERR_BAD_ARG;
    link_init(l);
    l->slave_count = m->slave_count;
    for (uint8_t i = 0; i < m->slave_count; i++) {
        l->slaves[i].mcu              = m->slaves[i].mcu;
        l->slaves[i].state            = (uint8_t)COMM_NODE_UNKNOWN;
        l->slaves[i].miss_count       = 0;
        l->slaves[i].next_seq         = 0;
        l->slaves[i].outstanding_slot = -1;
        l->slaves[i].last_seen_ms     = 0;
        l->slaves[i].physics_model_id = m->slaves[i].physics_model_id;
    }
    return COMM_OK;
}

link_slave_state_t *link_find_slave(link_t *l, uint8_t mcu)
{
    if (!l || mcu == 0) return 0;
    for (uint8_t i = 0; i < l->slave_count; i++) {
        if (l->slaves[i].mcu == mcu) return &l->slaves[i];
    }
    return 0;
}

const link_slave_state_t *link_find_slave_const(const link_t *l, uint8_t mcu)
{
    if (!l || mcu == 0) return 0;
    for (uint8_t i = 0; i < l->slave_count; i++) {
        if (l->slaves[i].mcu == mcu) return &l->slaves[i];
    }
    return 0;
}
