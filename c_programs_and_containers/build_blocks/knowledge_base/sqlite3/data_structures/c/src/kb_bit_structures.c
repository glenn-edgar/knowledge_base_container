/*
 * kb_bit_structures.c
 * Knowledge Base C Port — Bit structures orchestrator
 *
 * Mirrors LuaJIT kb_bit_structures.lua.
 */

#include "kb_bit_structures.h"
#include "kb_json.h"

#include <stdlib.h>
#include <string.h>

struct kb_bit_structures {
    kb_search_t       *ks;       /* borrowed, not owned */
    kb_bit_mask_ops_t *bit_ops;  /* owned */
    char              *database;
};

kb_bit_structures_t *kb_bit_structures_create(kb_search_t *ks,
                                               const char *database)
{
    if (!ks || !database) return NULL;

    kb_bit_structures_t *bs = (kb_bit_structures_t *)calloc(1, sizeof(*bs));
    if (!bs) return NULL;

    bs->ks = ks;
    bs->database = kb_strdup(database);
    bs->bit_ops = kb_bit_mask_ops_create(kb_search_get_db(ks), database);

    if (!bs->database || !bs->bit_ops) {
        kb_bit_structures_destroy(bs);
        return NULL;
    }

    return bs;
}

void kb_bit_structures_destroy(kb_bit_structures_t *bs)
{
    if (!bs) return;
    kb_bit_mask_ops_destroy(bs->bit_ops);
    free(bs->database);
    free(bs);
}

kb_bit_mask_ops_t *kb_bit_structures_get_ops(kb_bit_structures_t *bs)
{
    return bs ? bs->bit_ops : NULL;
}

/* ================================================================
 * Node ID lookup via KB_Search
 * ================================================================ */

kb_error_t kb_bit_find_node_id(kb_bit_structures_t *bs,
                                const char *node_name,
                                const char *properties_json,
                                const char *node_path,
                                int *node_id_out)
{
    if (!bs || !node_id_out) return KB_ERR_NULL_ARG;

    kb_search_clear_filters(bs->ks);
    kb_search_label(bs->ks, "KB_BIT_FIELD");

    if (node_name) {
        kb_search_name(bs->ks, node_name);
    }
    if (node_path) {
        kb_search_path(bs->ks, node_path);
    }

    /* Property filtering would need key/value extraction from JSON
     * For now, execute and match */
    kb_error_t err = kb_search_execute(bs->ks);
    if (err != KB_OK) return err;

    const kb_result_t *results = kb_search_results(bs->ks);
    if (results->count == 0) return KB_ERR_NOT_FOUND;

    *node_id_out = kb_row_get_int(results, 0, "id", 0);
    return KB_OK;
}

/* ================================================================
 * Bit operations by path
 * ================================================================ */

kb_error_t kb_bit_get_by_path(kb_bit_structures_t *bs, const char *path,
                               int bit_position, int *value_out)
{
    if (!bs) return KB_ERR_NULL_ARG;
    return kb_bit_get(bs->bit_ops, path, bit_position, value_out);
}

kb_error_t kb_bit_set_by_path(kb_bit_structures_t *bs, const char *path,
                               int bit_position, int value)
{
    if (!bs) return KB_ERR_NULL_ARG;
    return kb_bit_set(bs->bit_ops, path, bit_position, value);
}

kb_error_t kb_bit_get_mask_by_path(kb_bit_structures_t *bs, const char *path,
                                    int64_t *mask_out)
{
    if (!bs) return KB_ERR_NULL_ARG;
    return kb_bit_get_mask(bs->bit_ops, path, mask_out);
}

kb_error_t kb_bit_set_mask_by_path(kb_bit_structures_t *bs, const char *path,
                                    int64_t mask)
{
    if (!bs) return KB_ERR_NULL_ARG;
    return kb_bit_set_mask(bs->bit_ops, path, mask);
}

/* ================================================================
 * S-expression evaluation
 * ================================================================ */

kb_error_t kb_bit_eval_sexpr(kb_bit_structures_t *bs, const char *path,
                              const char *expr, int *result_out)
{
    if (!bs || !path || !expr || !result_out) return KB_ERR_NULL_ARG;

    /* Load bit_mask and change_mask for this path */
    kb_bit_data_t bit_data = { .bit_mask = 0, .change_mask = 0 };

    kb_error_t err = kb_bit_get_mask(bs->bit_ops, path, &bit_data.bit_mask);
    if (err != KB_OK) return err;

    err = kb_bit_get_change_mask(bs->bit_ops, path, &bit_data.change_mask);
    if (err != KB_OK) return err;

    return kb_sexpr_eval(expr, &bit_data, result_out);
}
