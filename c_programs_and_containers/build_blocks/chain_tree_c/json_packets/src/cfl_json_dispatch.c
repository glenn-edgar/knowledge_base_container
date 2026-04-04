/* ========================================================================
 * cfl_json_dispatch.c — Route JSON packets by "type" field
 *
 * Uses FNV-1a hashing to match the "type" string against a route table.
 * Always has a default column (s-engine dispatch pattern).
 * ======================================================================== */

#include <string.h>
#include "cfl_json_packets.h"
#include "cfl_exception.h"

/* FNV-1a 32-bit hash — matches ChainTree convention */
static uint32_t fnv1a_hash(const char *str) {
    uint32_t hash = 0x811c9dc5u;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 0x01000193u;
    }
    return hash;
}

/* ========================================================================
 * DISPATCHER STRUCTURE
 * ======================================================================== */

struct cfl_json_dispatcher {
    cfl_json_route_t  *routes;
    int                count;
    unsigned           default_column_id;
};

/* ========================================================================
 * CREATE / FREE
 * ======================================================================== */

cfl_json_dispatcher_t *cfl_json_dispatcher_create(cfl_json_heap_interface_t *heap,
                                                   cfl_json_route_t *routes,
                                                   int count,
                                                   unsigned default_column_id) {
    if (!heap) EXCEPTION("cfl_json_dispatcher_create: NULL heap");
    if (!routes && count > 0) EXCEPTION("cfl_json_dispatcher_create: NULL routes");

    cfl_json_dispatcher_t *d = (cfl_json_dispatcher_t *)heap->alloc(heap->ctx,
                                                                     (uint16_t)sizeof(cfl_json_dispatcher_t));
    if (!d) EXCEPTION("cfl_json_dispatcher_create: alloc failed");

    /* Pre-hash all route type names */
    for (int i = 0; i < count; i++) {
        if (!routes[i].type_name) {
            EXCEPTION("cfl_json_dispatcher_create: NULL type_name in route");
        }
        routes[i].type_hash = fnv1a_hash(routes[i].type_name);
    }

    d->routes = routes;
    d->count = count;
    d->default_column_id = default_column_id;
    return d;
}

void cfl_json_dispatcher_free(cfl_json_heap_interface_t *heap,
                               cfl_json_dispatcher_t *d) {
    if (!d) return;
    if (!heap) return;
    heap->free(heap->ctx, d);
}

/* ========================================================================
 * DISPATCH
 * ======================================================================== */

unsigned cfl_json_dispatch(cfl_json_dispatcher_t *d, cfl_json_packet_t *pkt) {
    if (!d) EXCEPTION("cfl_json_dispatch: NULL dispatcher");
    if (!pkt) EXCEPTION("cfl_json_dispatch: NULL packet");

    const char *type_str = NULL;
    cfl_json_get_string(pkt, "type", &type_str);
    /* cfl_json_get_string EXCEPTIONs if "type" is missing */

    uint32_t hash = fnv1a_hash(type_str);

    for (int i = 0; i < d->count; i++) {
        if (d->routes[i].type_hash == hash) {
            return d->routes[i].column_id;
        }
    }

    /* No match — use default column */
    return d->default_column_id;
}
