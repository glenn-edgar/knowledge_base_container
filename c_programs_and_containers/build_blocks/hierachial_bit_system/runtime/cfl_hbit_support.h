#ifndef CFL_HBIT_SUPPORT_H
#define CFL_HBIT_SUPPORT_H
#ifdef __cplusplus
extern "C" {
#endif

#include "cfl_hbit.h"


void cfl_hbit_clear_controller_latches(cfl_hbit_controller_t* ctrl);
void cfl_hbit_clear_controller_masks(cfl_hbit_controller_t* ctrl);

void cfl_hbit_print_node_state(
    cfl_hbit_instance_t* inst,
    uint16_t buf,
    uint16_t node,
    const char* label);
    


/**
 * @brief Count total error bits in a subtree with early termination
 * 
 * Uses parent nodes to prune entire subtrees that have no errors.
 * For OR buffers: if parent has no bits set, skip entire subtree
 * For AND buffers: if parent has all bits set, skip entire subtree
 * 
 * @param inst Instance pointer
 * @param root Root node to start counting from
 * @param buf Buffer index to check for errors
 * @return Total number of error bits set in all leaf nodes
 */
 uint32_t cfl_hbit_count_error_bits(
    cfl_hbit_instance_t* inst, 
    uint16_t root, 
    uint16_t buf,
    bool use_mask);

/* ============================================ */
/* Error Bit Collection                         */
/* ============================================ */

typedef struct {
    uint16_t node;              /* Node ID */
    uint16_t index;             /* Bit index within node */
    uint16_t monitoring_node;   /* Monitoring node ID or 0xFFFF for no match */
    uint8_t  value;             /* Bit value (always 1 for error collection) */
} cfl_hbit_error_bit_t;

typedef struct {
    uint32_t count;
    cfl_hbit_error_bit_t* error_bits;
} cfl_hbit_error_bits_t;

/**
 * @brief Count and collect all error bit locations with monitoring node mapping
 * @param inst Instance pointer
 * @param root Root node to start from
 * @param buf Buffer index
 * @param number_of_monitoring_nodes Number of monitoring nodes to check
 * @param monitoring_nodes Array of monitoring node indices (first match wins)
 * @param use_mask For OR_MASK buffers: if true, only collect masked bits
 * @return Allocated structure with error bits, or NULL on failure
 */
cfl_hbit_error_bits_t* cfl_hbit_count_error_bits_and_get_bits(
    cfl_hbit_instance_t* inst, 
    uint16_t root, 
    uint16_t buf,
    uint16_t number_of_monitoring_nodes,
    uint16_t* monitoring_nodes,
    bool use_mask);

/**
 * @brief Free error bits structure
 * @param inst Instance pointer (for allocator access)
 * @param error_bits Structure to free
 */
void cfl_hbit_error_bits_destroy(
    cfl_hbit_instance_t* inst,
    cfl_hbit_error_bits_t* error_bits);

    void cfl_hbit_print_error_bits_by_node(cfl_hbit_instance_t* inst, cfl_hbit_error_bits_t* errors);
#ifdef __cplusplus
}


#endif

#endif