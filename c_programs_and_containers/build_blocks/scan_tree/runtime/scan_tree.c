/* scan_tree.c */

#include "scan_tree.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =============================================================
 * FNV-1a
 * ============================================================= */

uint32_t st_fnv1a(const char *str)
{
    uint32_t h = 2166136261u;
    while (*str) { h ^= (uint8_t)*str++; h *= 16777619u; }
    return h;
}

/* =============================================================
 * Init: allocate all working storage from const descriptor
 * ============================================================= */

int st_init(st_handle_t *h, const st_system_desc_t *desc)
{
    memset(h, 0, sizeof(*h));
    h->desc = desc;

    /* Calculate total memory needed */
    size_t raw_struct_sz   = sizeof(st_raw_rt_t)   * desc->n_raw;
    size_t layer_struct_sz = sizeof(st_layer_rt_t)  * desc->n_layer;
    size_t node_state_sz   = desc->n_nodes;
    size_t node_dirty_sz   = desc->n_nodes;

    /* Raw buffer data: two copies per buffer */
    size_t raw_data_sz = 0;
    for (uint16_t i = 0; i < desc->n_bufs; i++) {
        if (!desc->buf_descs[i].is_layer)
            raw_data_sz += (size_t)desc->buf_descs[i].size * desc->buf_descs[i].elem_size * 2;
    }

    /* Layer buffer data: value + not_active + shadow + states per buffer */
    size_t layer_data_sz = 0;
    for (uint16_t i = 0; i < desc->n_bufs; i++) {
        if (desc->buf_descs[i].is_layer)
            layer_data_sz += (size_t)desc->buf_descs[i].size * 4;
    }

    size_t total = raw_struct_sz + layer_struct_sz + node_state_sz +
                   node_dirty_sz + raw_data_sz + layer_data_sz;

    h->mem = (uint8_t *)calloc(1, total);
    if (!h->mem) return -1;

    /* Carve out allocations */
    uint8_t *p = h->mem;

    h->raw_bufs = (st_raw_rt_t *)p;          p += raw_struct_sz;
    h->layer_bufs = (st_layer_rt_t *)p;      p += layer_struct_sz;
    h->node_state = p;                        p += node_state_sz;
    h->node_dirty = p;                        p += node_dirty_sz;

    /* Init raw buffers */
    for (uint16_t i = 0; i < desc->n_bufs; i++) {
        const st_buf_desc_t *bd = &desc->buf_descs[i];
        if (bd->is_layer) continue;

        st_raw_rt_t *r = &h->raw_bufs[bd->buf_index];
        size_t bsz = (size_t)bd->size * bd->elem_size;
        r->current  = p;       p += bsz;
        r->previous = p;       p += bsz;
        r->size      = bd->size;
        r->elem_size = bd->elem_size;
        r->changed   = 1;  /* force first eval */
    }

    /* Init layer buffers */
    for (uint16_t i = 0; i < desc->n_bufs; i++) {
        const st_buf_desc_t *bd = &desc->buf_descs[i];
        if (!bd->is_layer) continue;

        st_layer_rt_t *l = &h->layer_bufs[bd->buf_index];
        l->size       = bd->size;
        l->value      = p;  p += bd->size;
        l->not_active = p;  p += bd->size;
        l->shadow     = p;  p += bd->size;
        l->states     = (int8_t *)p;  p += bd->size;
        memset(l->value,      0, bd->size);
        memset(l->not_active, 1, bd->size);  /* all Not Active */
        memset(l->shadow,     0, bd->size);
        memset(l->states,    -1, bd->size);  /* all NOT_OP */
    }

    /* Mark all nodes dirty for first eval */
    memset(h->node_dirty, 1, desc->n_nodes);

    return 0;
}

void st_destroy(st_handle_t *h)
{
    if (h->mem) { free(h->mem); h->mem = NULL; }
}

/* =============================================================
 * Buffer access
 * ============================================================= */

void *st_raw_current(const st_handle_t *h, uint16_t buf_id)
{
    const st_buf_desc_t *bd = &h->desc->buf_descs[buf_id];
    return h->raw_bufs[bd->buf_index].current;
}

const uint8_t *st_buf_data(const st_handle_t *h, uint16_t buf_id)
{
    const st_buf_desc_t *bd = &h->desc->buf_descs[buf_id];
    if (bd->is_layer)
        return h->layer_bufs[bd->buf_index].value;
    else
        return (const uint8_t *)h->raw_bufs[bd->buf_index].current;
}

int8_t st_get_state(const st_handle_t *h, uint16_t buf_id, uint16_t pos)
{
    const st_buf_desc_t *bd = &h->desc->buf_descs[buf_id];
    if (!bd->is_layer || pos >= bd->size) return -1;
    const st_layer_rt_t *l = &h->layer_bufs[bd->buf_index];
    return l->states[pos];
}

const int8_t *st_layer_states(const st_handle_t *h, uint16_t buf_id, uint16_t *out_size)
{
    const st_buf_desc_t *bd = &h->desc->buf_descs[buf_id];
    if (!bd->is_layer) return NULL;
    const st_layer_rt_t *l = &h->layer_bufs[bd->buf_index];
    if (out_size) *out_size = l->size;
    return l->states;
}

void st_print_info(const st_handle_t *h)
{
    const st_system_desc_t *d = h->desc;
    printf("%s: %u bufs, %u nodes, %u raw, %u layer\n",
           d->name, d->n_bufs, d->n_nodes, d->n_raw, d->n_layer);
}

/* =============================================================
 * Lookup
 * ============================================================= */

int32_t st_lookup_key(const st_handle_t *h, uint32_t key)
{
    const st_lookup_entry_t *t = h->desc->lookup;
    uint16_t lo = 0, hi = h->desc->n_lookup;
    while (lo < hi) {
        uint16_t mid = lo + (hi - lo) / 2;
        if      (t[mid].key < key) lo = mid + 1;
        else if (t[mid].key > key) hi = mid;
        else return t[mid].buf_id;
    }
    return -1;
}

int32_t st_lookup_path(const st_handle_t *h, const char *path)
{
    uint32_t key = st_fnv1a(path);
    const st_lookup_entry_t *t = h->desc->lookup;
    uint16_t lo = 0, hi = h->desc->n_lookup;
    while (lo < hi) {
        uint16_t mid = lo + (hi - lo) / 2;
        if      (t[mid].key < key) lo = mid + 1;
        else if (t[mid].key > key) hi = mid;
        else {
            /* verify path */
            if (strcmp(h->desc->buf_descs[t[mid].buf_id].path, path) == 0)
                return t[mid].buf_id;
            /* linear probe for collisions */
            for (uint16_t i = mid + 1; i < hi && t[i].key == key; i++)
                if (strcmp(h->desc->buf_descs[t[i].buf_id].path, path) == 0)
                    return t[i].buf_id;
            for (uint16_t i = mid; i > lo; ) {
                i--;
                if (t[i].key != key) break;
                if (strcmp(h->desc->buf_descs[t[i].buf_id].path, path) == 0)
                    return t[i].buf_id;
            }
            return -1;
        }
    }
    return -1;
}

/* =============================================================
 * Swap raw buffers
 * ============================================================= */

void st_swap_raw(st_handle_t *h)
{
    for (uint16_t i = 0; i < h->desc->n_raw; i++) {
        st_raw_rt_t *r = &h->raw_bufs[i];
        size_t bsz = (size_t)r->size * r->elem_size;
        r->changed = (memcmp(r->current, r->previous, bsz) != 0) ? 1 : 0;
        if (r->changed)
            memcpy(r->previous, r->current, bsz);
    }
}

/* =============================================================
 * Mark dirty (bitmask-driven: only nodes affected by changed raw buffers)
 * ============================================================= */

void st_mark_dirty(st_handle_t *h)
{
    const st_system_desc_t *d = h->desc;

    /* Build a combined bitmask of which raw buffers changed */
    uint32_t changed[ST_MAX_RAW_WORDS] = {0};
    uint8_t any = 0;
    for (uint16_t r = 0; r < d->n_raw; r++) {
        if (h->raw_bufs[r].changed) {
            changed[r / 32] |= (1u << (r % 32));
            any = 1;
        }
    }
    if (!any) {
        memset(h->node_dirty, 0, d->n_nodes);
        return;
    }

    /* Mark each node dirty if its raw_deps overlap with changed set */
    for (uint16_t i = 0; i < d->n_nodes; i++) {
        uint8_t hit = 0;
        const uint32_t *deps = d->node_descs[i].raw_deps;
        uint16_t nw = (d->n_raw + 31) / 32;
        for (uint16_t w = 0; w < nw; w++) {
            if (deps[w] & changed[w]) { hit = 1; break; }
        }
        h->node_dirty[i] = hit;
    }
}

/* =============================================================
 * Evaluate
 * ============================================================= */

static inline void layer_write(st_layer_rt_t *l, uint16_t pos, uint8_t val)
{
    l->not_active[pos] = 0;
    l->value[pos] = val;
    l->states[pos] = val ? 1 : 0;
}

void st_evaluate(st_handle_t *h)
{
    const st_system_desc_t *d = h->desc;

    for (uint16_t i = 0; i < d->n_nodes; i++) {
        if (!h->node_dirty[i]) continue;
        h->node_dirty[i] = 0;

        const st_node_desc_t *nd = &d->node_descs[i];
        const st_buf_desc_t *obd = &d->buf_descs[nd->output_buf_id];
        st_layer_rt_t *out = &h->layer_bufs[obd->buf_index];
        uint16_t pos = nd->output_pos;

        uint8_t result = nd->func(&h->node_state[i], h, nd->inputs, nd->n_inputs);
        layer_write(out, pos, result);
    }

    /* Update layer shadows */
    for (uint16_t i = 0; i < d->n_layer; i++) {
        st_layer_rt_t *l = &h->layer_bufs[i];
        memcpy(l->shadow, l->value, l->size);
    }
}

void st_cycle(st_handle_t *h)
{
    st_swap_raw(h);
    st_mark_dirty(h);
    st_evaluate(h);
}

