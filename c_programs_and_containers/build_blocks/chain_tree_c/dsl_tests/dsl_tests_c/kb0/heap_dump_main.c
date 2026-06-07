/* Heap-block dump: build the runtime exactly like the slow_bus firmware
 * (heap_size=8192, allocator_0=256, evq 8/64, kb0+kb1 active), let it run a few
 * ticks (the HOST_TEST stimulus terminates via goto exit, which does NOT free),
 * then walk every heap block so we can see where the bytes actually go. */
#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_heap.h"
#include "chaintree_handle.h"

static cfl_perm_t perm;
static char perm_buffer[16 * 1024];      // == firmware g_perm_buf

static int n_used, n_free; static long b_used, b_free;
static void dump_cb(void *p, uint16_t size, bool alloc, uint16_t node_id) {
    (void)p;
    printf("  %-4s  size=%5u  node_id=%u\n", alloc ? "USED" : "free", size, node_id);
    if (alloc) { n_used++; b_used += size; } else { n_free++; b_free += size; }
}

int main(void) {
    const chaintree_handle_t *h = &g_chaintree_handle;
    printf("kb_count=%u node_count=%u\n", h->kb_count, h->node_count);

    cfl_runtime_create_params_t *p = cfl_runtime_create_params_create();
    p->perm = &perm; p->perm_buffer = perm_buffer;
    p->perm_buffer_size = (uint16_t)sizeof perm_buffer;
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

    // activate every real KB by name (skip the *_functions metadata KBs)
    for (uint16_t i = 0; i < h->kb_count; i++) {
        const char *nm = h->kb_table[i].kb_name;
        if (strstr(nm, "_functions")) continue;
        cfl_add_test_by_index(rt, i);
        printf("activated %s (idx %u)\n", nm, i);
    }

    printf("\n=== heap AFTER activation, BEFORE run ===\n");
    printf("perm used=%u/%u   heap used=%u free=%u (pool %u)\n",
           cfl_perm_used_bytes(rt->perm), (unsigned)sizeof perm_buffer,
           cfl_heap_used_bytes(rt->heap), cfl_heap_free_bytes(rt->heap),
           cfl_heap_used_bytes(rt->heap) + cfl_heap_free_bytes(rt->heap));
    n_used = n_free = 0; b_used = b_free = 0;
    cfl_heap_walk(rt->heap, dump_cb);
    printf("  -> %d used blocks (%ld B data), %d free blocks (%ld B data)\n",
           n_used, b_used, n_free, b_free);

    printf("\n=== running (stimulus ticks then terminates) ===\n");
    cfl_runtime_run(rt);

    printf("\n=== heap AFTER run ===\n");
    printf("perm used=%u/%u   heap used=%u free=%u (pool %u)\n",
           cfl_perm_used_bytes(rt->perm), (unsigned)sizeof perm_buffer,
           cfl_heap_used_bytes(rt->heap), cfl_heap_free_bytes(rt->heap),
           cfl_heap_used_bytes(rt->heap) + cfl_heap_free_bytes(rt->heap));
    n_used = n_free = 0; b_used = b_free = 0;
    cfl_heap_walk(rt->heap, dump_cb);
    printf("  -> %d used blocks (%ld B data), %d free blocks (%ld B data)\n",
           n_used, b_used, n_free, b_free);
    return 0;
}
