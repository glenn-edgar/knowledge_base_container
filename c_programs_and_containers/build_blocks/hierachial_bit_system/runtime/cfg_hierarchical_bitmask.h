/* cfg_hierarchical_bitmask.h */

#ifndef CFG_HIERARCHICAL_BITMASK_H
#define CFG_HIERARCHICAL_BITMASK_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Runtime-visible bank description — matches generated layout exactly
typedef struct {
  uint16_t node_id;
  uint16_t bitspace_id;
  uint16_t bits;
  uint8_t  merge;
  uint8_t  boundary;
  uint8_t  _pad[1];  // ensure same size/alignment if needed
} cfg_bank_desc_t;

// Schema descriptor — user fills from generated tables
typedef struct {
  const cfg_bank_desc_t *banks;   // points to g_schema_banks[]
  const uint16_t        *parents; // points to g_schema_parents[]
  uint16_t               bank_count;
  uint16_t               node_count;
} cfg_schema_desc_t;

// Rest unchanged...
typedef struct cfg_hierarchical_bitmask_s* cfg_hierarchical_bitmask_t;

typedef void* (*cfg_alloc_fn_t)(void *ctx, size_t size);
typedef void  (*cfg_dealloc_fn_t)(void *ctx, void *ptr);

cfg_hierarchical_bitmask_t cfg_hierarchical_create(
    const cfg_schema_desc_t *schema,
    void *alloc_ctx,
    cfg_alloc_fn_t alloc_fn,
    void *dealloc_ctx,
    cfg_dealloc_fn_t dealloc_fn);

void cfg_hierarchical_destroy(
    cfg_hierarchical_bitmask_t handle,
    void *dealloc_ctx,
    cfg_dealloc_fn_t dealloc_fn);

void cfg_hierarchical_set(cfg_hierarchical_bitmask_t handle,
                          uint16_t bank_id, uint16_t bit_idx, bool value);

bool cfg_hierarchical_get(cfg_hierarchical_bitmask_t handle,
                          uint16_t bank_id, uint16_t bit_idx);

void cfg_hierarchical_propagate_tick(cfg_hierarchical_bitmask_t handle);

#ifdef __cplusplus
}
#endif

#endif