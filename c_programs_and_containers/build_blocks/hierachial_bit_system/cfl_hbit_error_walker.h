#ifndef CFL_HBIT2_ERROR_WALKER_H
#define CFL_HBIT2_ERROR_WALKER_H

#include "cfl_hbit.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================ */
/* Error Walker Types                           */
/* ============================================ */

typedef enum {
    CFL_HBIT2_WALK_OK = 0,
    CFL_HBIT2_WALK_DONE,        /* No more errors */
    CFL_HBIT2_WALK_ERR_NULL,
    CFL_HBIT2_WALK_ERR_NOT_INIT
} cfl_hbit2_walk_status_t;

typedef struct {
    cfl_hbit2_tree_t* tree;
    int32_t root;
    int16_t bs_id;
    bool use_mask;              /* true = report only unmasked errors */
    uint8_t merge_type;         /* auto-detected: 0=OR, 1=AND */
    
    /* Internal iteration state */
    int32_t current_node;       /* Current leaf being examined */
    int current_bit;            /* Current bit position in leaf */
    int leaf_index;             /* Index into leaf list */
    bool initialized;
} cfl_hbit2_error_walker_t;

/**
 * User callback - called for each error bit
 * @param tree      Tree handle
 * @param node_id   Leaf node with error
 * @param bit_id    Bit position with error
 * @param user      User context
 * @return true to continue, false to stop
 */
typedef bool (*cfl_hbit2_error_cb)(
    cfl_hbit2_tree_t* tree,
    int32_t node_id,
    int bit_id,
    void* user
);

/* ============================================ */
/* Error Walker API                             */
/* ============================================ */

/**
 * Initialize error walker.
 * Auto-detects OR/AND from bitspace merge type.
 * 
 * @param walker    Walker state (caller provides storage)
 * @param tree      Tree to walk
 * @param root      Root node to start from
 * @param bs_id     Bitspace to check
 * @param use_mask  true = skip masked errors, false = report all errors
 */
cfl_hbit2_walk_status_t cfl_hbit2_error_walker_init(
    cfl_hbit2_error_walker_t* walker,
    cfl_hbit2_tree_t* tree,
    int32_t root,
    int16_t bs_id,
    bool use_mask
);

/**
 * Get next error.
 * 
 * @param walker    Walker state
 * @param node_id   Output: leaf node with error
 * @param bit_id    Output: bit position with error
 * @return CFL_HBIT2_WALK_OK if error found
 *         CFL_HBIT2_WALK_DONE if no more errors
 */
cfl_hbit2_walk_status_t cfl_hbit2_error_walker_next(
    cfl_hbit2_error_walker_t* walker,
    int32_t* node_id,
    int* bit_id
);

/**
 * Walk all errors with callback.
 * 
 * @param walker    Walker state (must be initialized)
 * @param callback  Called for each error (NULL to just count)
 * @param user      User context passed to callback
 * @return Number of errors found
 */
int cfl_hbit2_error_walker_foreach(
    cfl_hbit2_error_walker_t* walker,
    cfl_hbit2_error_cb callback,
    void* user
);

/**
 * Reset walker to start over.
 */
void cfl_hbit2_error_walker_reset(cfl_hbit2_error_walker_t* walker);

#ifdef __cplusplus
}
#endif

#endif /* CFL_HBIT2_ERROR_WALKER_H */