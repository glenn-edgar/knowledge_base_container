/* cfg_hierarchical_bitmask.c
 *
 * Generic hierarchical bitmask runtime with shadow buffering and custom allocation.
 * Fully decoupled — works with any generated schema via cfg_schema_desc_t.
 */

 #include "cfg_hierarchical_bitmask.h"
 #include <stdlib.h>
 #include <string.h>
 
 typedef struct {
   uint8_t *current;
   uint8_t *shadow;
   uint16_t bytes;
 } bank_masks_t;
 
 struct cfg_hierarchical_bitmask_s {
   const cfg_schema_desc_t *schema;
   bank_masks_t            *banks;
   void                    *alloc_ctx;
   cfg_alloc_fn_t           alloc_fn;
   void                    *dealloc_ctx;
   cfg_dealloc_fn_t         dealloc_fn;
 };
 
 /* Helper: bytes needed for N bits (ceiling) */
 static inline uint16_t bytes_from_bits(uint16_t bits) {
   return (bits + 7u) / 8u;
 }
 
 /* Default allocators */
 static void* default_alloc(void *ctx, size_t size) {
   (void)ctx;
   return malloc(size);
 }
 
 static void default_dealloc(void *ctx, void *ptr) {
   (void)ctx;
   free(ptr);
 }
 
 cfg_hierarchical_bitmask_t cfg_hierarchical_create(
     const cfg_schema_desc_t *schema,
     void *alloc_ctx,
     cfg_alloc_fn_t alloc_fn,
     void *dealloc_ctx,
     cfg_dealloc_fn_t dealloc_fn)
 {
   if (!schema || schema->bank_count == 0) return NULL;
 
   if (!alloc_fn)   alloc_fn   = default_alloc;
   if (!dealloc_fn) dealloc_fn = default_dealloc;
 
   cfg_hierarchical_bitmask_t handle = alloc_fn(alloc_ctx, sizeof(struct cfg_hierarchical_bitmask_s));
   if (!handle) return NULL;
 
   handle->schema     = schema;
   handle->banks      = alloc_fn(alloc_ctx, schema->bank_count * sizeof(bank_masks_t));
   if (!handle->banks) {
     dealloc_fn(dealloc_ctx ? dealloc_ctx : alloc_ctx, handle);
     return NULL;
   }
 
   handle->alloc_ctx  = alloc_ctx;
   handle->alloc_fn   = alloc_fn;
   handle->dealloc_ctx = dealloc_ctx;
   handle->dealloc_fn  = dealloc_fn;
 
   /* Zero-init bank metadata */
   memset(handle->banks, 0, schema->bank_count * sizeof(bank_masks_t));
 
   for (uint16_t i = 0; i < schema->bank_count; ++i) {
     uint16_t bits  = schema->banks[i].bits;
     uint16_t bytes = bytes_from_bits(bits);
 
     handle->banks[i].bytes   = bytes;
     handle->banks[i].current = alloc_fn(alloc_ctx, bytes);
     handle->banks[i].shadow  = alloc_fn(alloc_ctx, bytes);
 
     if (!handle->banks[i].current || !handle->banks[i].shadow) {
       cfg_hierarchical_destroy(handle, dealloc_ctx, dealloc_fn);
       return NULL;
     }
 
     memset(handle->banks[i].current, 0, bytes);
     memset(handle->banks[i].shadow,  0, bytes);
   }
 
   return handle;
 }
 
 void cfg_hierarchical_destroy(
     cfg_hierarchical_bitmask_t handle,
     void *dealloc_ctx,
     cfg_dealloc_fn_t dealloc_fn)
 {
   if (!handle) return;
 
   cfg_dealloc_fn_t dfn = dealloc_fn ? dealloc_fn : handle->dealloc_fn;
   void *dctx           = dealloc_ctx ? dealloc_ctx : handle->dealloc_ctx;
 
   for (uint16_t i = 0; i < handle->schema->bank_count; ++i) {
     if (handle->banks[i].current) dfn(dctx, handle->banks[i].current);
     if (handle->banks[i].shadow)  dfn(dctx, handle->banks[i].shadow);
   }
 
   if (handle->banks) dfn(dctx, handle->banks);
   dfn(dctx, handle);
 }
 
 void cfg_hierarchical_set(cfg_hierarchical_bitmask_t handle,
                           uint16_t bank_id, uint16_t bit_idx, bool value)
 {
   if (!handle || bank_id >= handle->schema->bank_count) return;
   if (bit_idx >= handle->schema->banks[bank_id].bits) return;
 
   bank_masks_t *bank = &handle->banks[bank_id];
   uint16_t byte_idx = bit_idx / 8;
   uint8_t bit = 1u << (bit_idx % 8);
 
   if (value) {
     bank->shadow[byte_idx] |= bit;
   } else {
     bank->shadow[byte_idx] &= ~bit;
   }
 }
 
 bool cfg_hierarchical_get(cfg_hierarchical_bitmask_t handle,
                           uint16_t bank_id, uint16_t bit_idx)
 {
   if (!handle || bank_id >= handle->schema->bank_count) return false;
   if (bit_idx >= handle->schema->banks[bank_id].bits) return false;
 
   bank_masks_t *bank = &handle->banks[bank_id];
   uint16_t byte_idx = bit_idx / 8;
   uint8_t bit = 1u << (bit_idx % 8);
 
   return (bank->current[byte_idx] & bit) != 0;
 }
 
 /* Merge and boundary helpers */
 static void merge_into(uint8_t *dst, const uint8_t *src, uint16_t bytes, uint8_t rule)
 {
   switch (rule) {
     case 1: /* OR */
       for (uint16_t i = 0; i < bytes; ++i) dst[i] |= src[i];
       break;
     case 2: /* AND */
       for (uint16_t i = 0; i < bytes; ++i) dst[i] &= src[i];
       break;
     case 3: /* PRIORITY (example: higher byte wins) */
       for (uint16_t i = 0; i < bytes; ++i) {
         if (src[i] > dst[i]) dst[i] = src[i];
       }
       break;
     default:
       break;
   }
 }
 
 static void apply_boundary(uint8_t *mask, uint16_t bytes, uint8_t boundary)
 {
   if (boundary == 1) { /* RESET */
     memset(mask, 0, bytes);
   }
   /* COPY = no-op, LATCH = handled externally */
 }
 
 void cfg_hierarchical_propagate_tick(cfg_hierarchical_bitmask_t handle)
{
  if (!handle) return;

  const cfg_schema_desc_t *s = handle->schema;

  /* Phase 1: Bottom-up hierarchical merge (child → parent) */
  for (uint16_t node = 1; node < s->node_count; ++node) {
    uint16_t parent = s->parents[node];
    if (parent == 0) continue;

    for (uint16_t cb = 0; cb < s->bank_count; ++cb) {
      const cfg_bank_desc_t *cdesc = &s->banks[cb];
      if (cdesc->node_id != node) continue;

      uint16_t pb = s->bank_count;
      for (uint16_t i = 0; i < s->bank_count; ++i) {
        const cfg_bank_desc_t *pdesc = &s->banks[i];
        if (pdesc->node_id == parent && pdesc->bitspace_id == cdesc->bitspace_id) {
          pb = i;
          break;
        }
      }
      if (pb >= s->bank_count) continue;

      bank_masks_t *child = &handle->banks[cb];
      bank_masks_t *par   = &handle->banks[pb];

      apply_boundary(child->shadow, child->bytes, cdesc->boundary);
      merge_into(par->shadow, child->shadow, child->bytes, cdesc->merge);
    }
  }

  /* Phase 2: Compute local "AnyActive" summary bits for ALARM banks */
  for (uint16_t b = 0; b < s->bank_count; ++b) {
    const cfg_bank_desc_t *desc = &s->banks[b];
    bank_masks_t *bank = &handle->banks[b];

    /* Change this ID to match your actual ALARM bitspace_id */
    if (desc->bitspace_id == 1) {  // ← REPLACE 1 with your ALARM bitspace_id
      bool any_active = false;

      /* Scan all bytes except possibly the last (if reserved for AnyActive) */
      uint16_t scan_bytes = bank->bytes;
      if (desc->bits % 8 != 0) scan_bytes--;  // avoid partial byte if needed

      for (uint16_t i = 0; i < scan_bytes; ++i) {
        if (bank->shadow[i] != 0) {
          any_active = true;
          break;
        }
      }

      /* Set the highest bit as AnyActive summary */
      uint16_t any_bit_idx = desc->bits - 1;  // e.g., 127 for 128-bit bank
      uint16_t byte_idx = any_bit_idx / 8;
      uint8_t bit_mask = 1u << (any_bit_idx % 8);

      if (any_active) {
        bank->shadow[byte_idx] |= bit_mask;
      } else {
        bank->shadow[byte_idx] &= ~bit_mask;
      }
    }
  }

  /* Phase 3: Swap shadow → current and clear shadow for next tick */
  for (uint16_t i = 0; i < s->bank_count; ++i) {
    bank_masks_t *b = &handle->banks[i];
    uint8_t *temp = b->current;
    b->current = b->shadow;
    b->shadow = temp;
    memset(b->shadow, 0, b->bytes);
  }
}