/*
 * bit_mask_rt_operations.h
 * Knowledge Base C Port — Atomic bit mask get/set with change_mask
 *
 * Mirrors LuaJIT bit_mask_rt_operations.lua.
 * Operations on bit_mask_store table: get/set individual bits,
 * get/set full masks, track changes via change_mask column.
 */

#ifndef BIT_MASK_RT_OPERATIONS_H
#define BIT_MASK_RT_OPERATIONS_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_bit_mask_ops kb_bit_mask_ops_t;

/* Create/destroy. database is the table name prefix. */
kb_bit_mask_ops_t *kb_bit_mask_ops_create(sqlite3 *db, const char *database);
void               kb_bit_mask_ops_destroy(kb_bit_mask_ops_t *ops);

/* Get the current value of a single bit (0 or 1) at bit_position for path */
kb_error_t kb_bit_get(kb_bit_mask_ops_t *ops, const char *path,
                      int bit_position, int *value_out);

/* Set a single bit at bit_position. Updates change_mask. */
kb_error_t kb_bit_set(kb_bit_mask_ops_t *ops, const char *path,
                      int bit_position, int value);

/* Get the full bit_mask as integer for path */
kb_error_t kb_bit_get_mask(kb_bit_mask_ops_t *ops, const char *path,
                           int64_t *mask_out);

/* Set the full bit_mask for path. Updates change_mask. */
kb_error_t kb_bit_set_mask(kb_bit_mask_ops_t *ops, const char *path,
                           int64_t mask);

/* Get the change_mask for path */
kb_error_t kb_bit_get_change_mask(kb_bit_mask_ops_t *ops, const char *path,
                                   int64_t *mask_out);

/* Clear the change_mask for path (set to 0) */
kb_error_t kb_bit_clear_change_mask(kb_bit_mask_ops_t *ops, const char *path);

#ifdef __cplusplus
}
#endif

#endif /* BIT_MASK_RT_OPERATIONS_H */
