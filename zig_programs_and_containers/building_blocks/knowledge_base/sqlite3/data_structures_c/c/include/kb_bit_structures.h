/*
 * kb_bit_structures.h
 * Knowledge Base C Port — Bit structures orchestrator
 *
 * Mirrors LuaJIT kb_bit_structures.lua.
 * Combines KB_Search (for node lookup) + bit_mask_rt_operations
 * + bit_s_expression into a single interface that resolves
 * paths via the KB and operates on bit masks.
 */

#ifndef KB_BIT_STRUCTURES_H
#define KB_BIT_STRUCTURES_H

#include "kb_common.h"
#include "kb_query_support.h"
#include "bit_mask_rt_operations.h"
#include "bit_s_expression.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_bit_structures kb_bit_structures_t;

/* Create/destroy. Takes a KB_Search for node lookups. */
kb_bit_structures_t *kb_bit_structures_create(kb_search_t *ks,
                                               const char *database);
void kb_bit_structures_destroy(kb_bit_structures_t *bs);

/*
 * Find bit mask node IDs. Uses KB_Search with label "KB_BIT_FIELD"
 * and optional name/properties/path filters.
 *
 * node_name, properties_json, node_path, data_json may be NULL.
 */
kb_error_t kb_bit_find_node_id(kb_bit_structures_t *bs,
                                const char *node_name,
                                const char *properties_json,
                                const char *node_path,
                                int *node_id_out);

/* Bit operations that resolve path via KB first */
kb_error_t kb_bit_get_by_path(kb_bit_structures_t *bs, const char *path,
                               int bit_position, int *value_out);

kb_error_t kb_bit_set_by_path(kb_bit_structures_t *bs, const char *path,
                               int bit_position, int value);

kb_error_t kb_bit_get_mask_by_path(kb_bit_structures_t *bs, const char *path,
                                    int64_t *mask_out);

kb_error_t kb_bit_set_mask_by_path(kb_bit_structures_t *bs, const char *path,
                                    int64_t mask);

/* S-expression evaluation against a path's bit data */
kb_error_t kb_bit_eval_sexpr(kb_bit_structures_t *bs, const char *path,
                              const char *expr, int *result_out);

/* Get underlying components */
kb_bit_mask_ops_t *kb_bit_structures_get_ops(kb_bit_structures_t *bs);

#ifdef __cplusplus
}
#endif

#endif /* KB_BIT_STRUCTURES_H */
