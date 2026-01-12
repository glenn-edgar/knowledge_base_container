#include "cfl_hbit_error_walker.h"
#include <string.h>

/* ============================================ */
/* Internal Helpers                             */
/* ============================================ */

/**
 * Check if node is under root (is root or descendant of root)
 */
static bool is_under_root(cfl_hbit2_tree_t* tree, int32_t node, int32_t root) {
    if (node == root) return true;
    
    /* Walk up parent chain */
    int32_t current = node;
    while (current >= 0) {
        int32_t parent = cfl_hbit2_nav_parent(tree, current);
        if (parent == root) return true;
        if (parent < 0) break;
        current = parent;
    }
    return false;
}

/**
 * Get error bits for a leaf node based on merge type and mask
 */
static uint8_t get_error_bits(
    cfl_hbit2_error_walker_t* walker,
    int32_t node,
    int byte_index
) {
    const uint8_t* bank = cfl_hbit2_bank_get(walker->tree, node, walker->bs_id);
    if (!bank) return 0;
    
    uint8_t value = bank[byte_index];
    uint8_t mask = 0xFF;
    
    if (walker->use_mask) {
        const uint8_t* mask_ptr = cfl_hbit2_mask_get(walker->tree, node, walker->bs_id);
        if (mask_ptr) {
            mask = mask_ptr[byte_index];
        }
    }
    
    /* Determine error bits based on merge type */
    if (walker->merge_type == 1) {
        /* AND: 0s are errors */
        return (~value) & mask;
    } else {
        /* OR: 1s are errors */
        return value & mask;
    }
}

/**
 * Get total bits for a node in this bitspace
 */
static int get_node_bits(cfl_hbit2_error_walker_t* walker, int32_t node) {
    return cfl_hbit2_info_bits(walker->tree, node, walker->bs_id);
}

/**
 * Get leaf node at index, returns -1 if out of range
 */
static int32_t get_leaf_at_index(cfl_hbit2_error_walker_t* walker, int index) {
    /* We need to iterate through all nodes and find leaves under root */
    int leaf_count = 0;
    int32_t node_count = cfl_hbit2_info_node_count(walker->tree);
    
    for (int32_t n = 0; n < node_count; n++) {
        if (cfl_hbit2_info_is_leaf(walker->tree, n)) {
            if (is_under_root(walker->tree, n, walker->root)) {
                if (leaf_count == index) {
                    return n;
                }
                leaf_count++;
            }
        }
    }
    
    return -1;
}

/* ============================================ */
/* Public API                                   */
/* ============================================ */

cfl_hbit2_walk_status_t cfl_hbit2_error_walker_init(
    cfl_hbit2_error_walker_t* walker,
    cfl_hbit2_tree_t* tree,
    int32_t root,
    int16_t bs_id,
    bool use_mask
) {
    if (!walker || !tree) return CFL_HBIT2_WALK_ERR_NULL;
    
    memset(walker, 0, sizeof(*walker));
    
    walker->tree = tree;
    walker->root = root;
    walker->bs_id = bs_id;
    walker->use_mask = use_mask;
    
    /* Auto-detect merge type from bitspace */
    walker->merge_type = cfl_hbit2_info_merge_type(tree, bs_id);
    
    /* Initialize iteration state */
    walker->leaf_index = 0;
    walker->current_bit = 0;
    walker->current_node = -1;
    walker->initialized = true;
    
    return CFL_HBIT2_WALK_OK;
}

cfl_hbit2_walk_status_t cfl_hbit2_error_walker_next(
    cfl_hbit2_error_walker_t* walker,
    int32_t* node_id,
    int* bit_id
) {
    if (!walker) return CFL_HBIT2_WALK_ERR_NULL;
    if (!walker->initialized) return CFL_HBIT2_WALK_ERR_NOT_INIT;
    if (!node_id || !bit_id) return CFL_HBIT2_WALK_ERR_NULL;
    
    while (1) {
        /* Get current leaf or move to next */
        if (walker->current_node < 0) {
            walker->current_node = get_leaf_at_index(walker, walker->leaf_index);
            if (walker->current_node < 0) {
                /* No more leaves */
                return CFL_HBIT2_WALK_DONE;
            }
            walker->current_bit = 0;
        }
        
        /* Check bits in current leaf */
        int total_bits = get_node_bits(walker, walker->current_node);
        
        while (walker->current_bit < total_bits) {
            int byte_idx = walker->current_bit / 8;
            int bit_in_byte = walker->current_bit % 8;
            
            uint8_t errors = get_error_bits(walker, walker->current_node, byte_idx);
            
            if (errors & (1 << bit_in_byte)) {
                /* Found error */
                *node_id = walker->current_node;
                *bit_id = walker->current_bit;
                walker->current_bit++;
                return CFL_HBIT2_WALK_OK;
            }
            
            walker->current_bit++;
        }
        
        /* Move to next leaf */
        walker->leaf_index++;
        walker->current_node = -1;
        walker->current_bit = 0;
    }
}

int cfl_hbit2_error_walker_foreach(
    cfl_hbit2_error_walker_t* walker,
    cfl_hbit2_error_cb callback,
    void* user
) {
    if (!walker || !walker->initialized) return 0;
    
    /* Reset to start */
    cfl_hbit2_error_walker_reset(walker);
    
    int count = 0;
    int32_t node_id;
    int bit_id;
    
    while (cfl_hbit2_error_walker_next(walker, &node_id, &bit_id) == CFL_HBIT2_WALK_OK) {
        count++;
        
        if (callback) {
            if (!callback(walker->tree, node_id, bit_id, user)) {
                break;  /* Callback requested stop */
            }
        }
    }
    
    return count;
}

void cfl_hbit2_error_walker_reset(cfl_hbit2_error_walker_t* walker) {
    if (!walker) return;
    
    walker->leaf_index = 0;
    walker->current_bit = 0;
    walker->current_node = -1;
}