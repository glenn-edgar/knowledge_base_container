/* KB0 host-test driver: create runtime, add kb0, run. */
#include <stdio.h>
#include "cfl_runtime.h"
#include "chaintree_handle.h"

static cfl_perm_t perm;
static char perm_buffer[0xffff];

int main(void) {
    const chaintree_handle_t *h = &g_chaintree_handle;
    printf("kb_count=%u node_count=%u\n", h->kb_count, h->node_count);

    cfl_runtime_create_params_t *p = cfl_runtime_create_params_create();
    p->perm = &perm;
    p->perm_buffer = perm_buffer;
    p->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    p->heap_size = 4096;
    p->max_allocator_count = cfl_calculate_arrena_number(h);
    p->total_node_count = h->node_count;
    p->allocator_0_size = 256;
    p->event_queue_high_priority_size = 8;
    p->event_queue_low_priority_size = 64;
    p->delta_time = 0.1;

    cfl_runtime_handle_t *rt = cfl_runtime_create(&perm, p, h);
    cfl_runtime_create_params_destroy(p);
    if (!rt) { printf("create failed\n"); return 1; }

    cfl_runtime_reset(rt);
    cfl_add_test_by_index(rt, 0);   /* kb0 */
    bool ok = cfl_runtime_run(rt);
    printf("run result=%d\n", ok);
    return ok ? 0 : 1;
}
