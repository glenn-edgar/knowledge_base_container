#include "cfl_hbit_support.h"
#include <stdio.h>
#include <string.h>

void cfl_hbit_clear_controller_latches(cfl_hbit_controller_t* ctrl){
/* Clear leaf node latches */
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        cfl_hbit_clear_latch_all(ctrl->inst, ctrl->buffer_idx, leaf_node);
    }
}

void cfl_hbit_clear_controller_masks(cfl_hbit_controller_t* ctrl){
    /* Clear leaf node masks */
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        cfl_hbit_clear_mask_all(ctrl->inst, ctrl->buffer_idx, leaf_node);
    }
}

void cfl_hbit_print_node_state(
    cfl_hbit_instance_t* inst,
    uint16_t buf,
    uint16_t node,
    const char* label)
{
    if (!inst || buf >= inst->config->buffer_count || node >= inst->config->node_count) {
        printf("Invalid inst/buf/node\n");
        return;
    }
    
    const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
    uint8_t sz = bc->arena_info[node].size;
    
    if (sz == 0) {
        printf("%s: node %d has no data in buffer %d\n", label ? label : "", node, buf);
        return;
    }
    
    uint8_t current[16] = {0};
    uint8_t latched[16] = {0};
    uint8_t mask[16] = {0};
    
    cfl_hbit_read_node(inst, buf, node, current, sz);
    
    uint16_t off = bc->arena_info[node].offset;
    
    if (bc->type == CFL_HBIT_BUF_OR_LATCH && inst->latched[buf]) {
        memcpy(latched, inst->latched[buf] + off, sz);
    }
    
    if (bc->type == CFL_HBIT_BUF_OR_MASK && inst->mask[buf]) {
        memcpy(mask, inst->mask[buf] + off, sz);
    }
    
    printf("%s [node %d, buf %d, %s]:\n", 
           label ? label : "", node, buf,
           inst->config->nodes[node].is_leaf ? "leaf" : "aggregate");
    
    printf("  current: 0x");
    for (int i = sz - 1; i >= 0; i--) printf("%02X", current[i]);
    printf(" (");
    for (int i = sz * 8 - 1; i >= 0; i--) {
        printf("%c", (current[i / 8] & (1 << (i % 8))) ? '1' : '0');
        if (i > 0 && i % 8 == 0) printf(" ");
    }
    printf(")\n");
    
    if (bc->type == CFL_HBIT_BUF_OR_LATCH) {
        printf("  latched: 0x");
        for (int i = sz - 1; i >= 0; i--) printf("%02X", latched[i]);
        printf(" (");
        for (int i = sz * 8 - 1; i >= 0; i--) {
            printf("%c", (latched[i / 8] & (1 << (i % 8))) ? '1' : '0');
            if (i > 0 && i % 8 == 0) printf(" ");
        }
        printf(")\n");
    }
    
    if (bc->type == CFL_HBIT_BUF_OR_MASK) {
        printf("  mask:    0x");
        for (int i = sz - 1; i >= 0; i--) printf("%02X", mask[i]);
        printf(" (");
        for (int i = sz * 8 - 1; i >= 0; i--) {
            printf("%c", (mask[i / 8] & (1 << (i % 8))) ? '1' : '0');
            if (i > 0 && i % 8 == 0) printf(" ");
        }
        printf(")\n");
    }
}

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
 * @param use_mask For OR_MASK buffers: if true, only count bits where mask is enabled
 *                 Ignored for other buffer types
 * @return Total number of error bits set in all leaf nodes
 */
 uint32_t cfl_hbit_count_error_bits(
    cfl_hbit_instance_t* inst, 
    uint16_t root, 
    uint16_t buf,
    bool use_mask)
{
    if (!inst || root >= inst->config->node_count) return 0;
    if (buf >= inst->config->buffer_count) return 0;
    
    const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
    const cfl_hbit_node_t* node = &inst->config->nodes[root];
    
    uint16_t off = bc->arena_info[root].offset;
    uint8_t sz = bc->arena_info[root].size;
    
    // If this is a leaf, count its bits
    if (node->is_leaf) {
        if (sz == 0) return 0;
        
        const uint8_t* data = inst->current[buf] + off;
        uint32_t count = 0;
        
        // For OR_MASK buffers with use_mask=true, apply mask
        if (bc->type == CFL_HBIT_BUF_OR_MASK && use_mask && inst->mask[buf]) {
            const uint8_t* mask = inst->mask[buf] + off;
            
            for (uint8_t i = 0; i < sz; i++) {
                uint8_t masked_byte = data[i] & mask[i];
                // Count bits in masked byte
                while (masked_byte) {
                    masked_byte &= masked_byte - 1;
                    count++;
                }
            }
        } else {
            // No masking - count all bits
            for (uint8_t i = 0; i < sz; i++) {
                uint8_t byte = data[i];
                // Count bits in byte using Brian Kernighan's algorithm
                while (byte) {
                    byte &= byte - 1;
                    count++;
                }
            }
        }
        
        return count;
    }
    
    // This is a parent node - check if we can prune the subtree
    if (sz > 0) {
        const uint8_t* data = inst->current[buf] + off;
        bool has_errors = false;
        
        if (bc->type == CFL_HBIT_BUF_AND) {
            // For AND buffer: error means bit is CLEAR (0)
            // If all bits are set (0xFF...), no errors in subtree
            for (uint8_t i = 0; i < sz; i++) {
                if (data[i] != 0xFF) {
                    has_errors = true;
                    break;
                }
            }
        } else {
            // For OR/OR_LATCH/OR_MASK: error means bit is SET (1)
            // Parent already reflects mask if use_mask is true (from propagation)
            // If all bits are clear (0x00...), no errors in subtree
            for (uint8_t i = 0; i < sz; i++) {
                if (data[i] != 0x00) {
                    has_errors = true;
                    break;
                }
            }
        }
        
        // Early termination: no errors in this subtree
        if (!has_errors) {
            return 0;
        }
    }
    
    // Recurse into children
    uint32_t total = 0;
    for (uint16_t i = 0; i < inst->config->node_count; i++) {
        if (inst->config->nodes[i].parent_index == (int16_t)root) {
            total += cfl_hbit_count_error_bits(inst, i, buf, use_mask);
        }
    }
    
    return total;
}

/**
* @brief Count and collect all error bit locations with monitoring node mapping
* 
* First pass: count total error bits
* Second pass: allocate and fill error bit array with monitoring node matches
* 
* @param inst Instance pointer
* @param root Root node to start from
* @param buf Buffer index
* @param number_of_monitoring_nodes Number of monitoring nodes to check
* @param monitoring_nodes Array of monitoring node indices to match against
* @param use_mask For OR_MASK buffers: if true, only collect masked bits
* @return Allocated structure with error bits, or NULL on failure
*/
cfl_hbit_error_bits_t* cfl_hbit_count_error_bits_and_get_bits(
   cfl_hbit_instance_t* inst, 
   uint16_t root, 
   uint16_t buf,
   uint16_t number_of_monitoring_nodes,
   uint16_t* monitoring_nodes,
   bool use_mask)
{
   if (!inst || root >= inst->config->node_count) return NULL;
   if (buf >= inst->config->buffer_count) return NULL;
   if (!inst->allocator.alloc || !inst->allocator.free) return NULL;
   
   const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
   
   // First pass: count total error bits
   uint32_t total_count = 0;
   
   // Find end of subtree
   const cfl_hbit_node_t* r = &inst->config->nodes[root];
   uint16_t end = root + 1;
   while (end < inst->config->node_count && inst->config->nodes[end].depth > r->depth) {
       end++;
   }
   
   // Count bits in all leaves
   for (uint16_t i = root; i < end; i++) {
       const cfl_hbit_node_t* node = &inst->config->nodes[i];
       if (!node->is_leaf) continue;
       
       uint16_t off = bc->arena_info[i].offset;
       uint8_t sz = bc->arena_info[i].size;
       if (sz == 0) continue;
       
       const uint8_t* data = inst->current[buf] + off;
       
       // Apply mask if requested for OR_MASK buffers
       if (bc->type == CFL_HBIT_BUF_OR_MASK && use_mask && inst->mask[buf]) {
           const uint8_t* mask = inst->mask[buf] + off;
           for (uint8_t j = 0; j < sz; j++) {
               uint8_t masked_byte = data[j] & mask[j];
               while (masked_byte) {
                   masked_byte &= masked_byte - 1;
                   total_count++;
               }
           }
       } else {
           // No masking
           for (uint8_t j = 0; j < sz; j++) {
               uint8_t byte = data[j];
               while (byte) {
                   byte &= byte - 1;
                   total_count++;
               }
           }
       }
   }
   
   // Allocate header
   cfl_hbit_error_bits_t* result = (cfl_hbit_error_bits_t*)
       inst->allocator.alloc(sizeof(cfl_hbit_error_bits_t), inst->allocator.ctx);
   if (!result) return NULL;
   
   result->count = total_count;
   result->error_bits = NULL;
   
   // If no errors, return empty structure
   if (total_count == 0) {
       return result;
   }
   
   // Allocate error bit array
   result->error_bits = (cfl_hbit_error_bit_t*)
       inst->allocator.alloc(sizeof(cfl_hbit_error_bit_t) * total_count, inst->allocator.ctx);
   
   if (!result->error_bits) {
       inst->allocator.free(result, inst->allocator.ctx);
       return NULL;
   }
   
   // Second pass: fill in error bit locations
   uint32_t idx = 0;
   
   for (uint16_t i = root; i < end; i++) {
       const cfl_hbit_node_t* node = &inst->config->nodes[i];
       if (!node->is_leaf) continue;
       
       uint16_t off = bc->arena_info[i].offset;
       uint8_t sz = bc->arena_info[i].size;
       if (sz == 0) continue;
       
       const uint8_t* data = inst->current[buf] + off;
       const uint8_t* mask = NULL;
       
       if (bc->type == CFL_HBIT_BUF_OR_MASK && use_mask && inst->mask[buf]) {
           mask = inst->mask[buf] + off;
       }
       
       // Find monitoring node for this error node (first match wins)
       int16_t monitoring_node = -1;
       if (monitoring_nodes && number_of_monitoring_nodes > 0) {
           for (uint16_t m = 0; m < number_of_monitoring_nodes; m++) {
               uint16_t mon = monitoring_nodes[m];
               if (mon >= inst->config->node_count) continue;
               
               // Check if node i is a descendant of monitoring_nodes[m]
               // Walk up from i to see if we hit mon
               int16_t parent = (int16_t)i;
               while (parent >= 0) {
                   if ((uint16_t)parent == mon) {
                       monitoring_node = (int16_t)mon;
                       break;
                   }
                   parent = inst->config->nodes[parent].parent_index;
               }
               
               if (monitoring_node >= 0) break; // First match wins
           }
       }
       
       // Scan each byte
       for (uint8_t byte_idx = 0; byte_idx < sz; byte_idx++) {
           uint8_t byte = data[byte_idx];
           
           // Apply mask if needed
           if (mask) {
               byte &= mask[byte_idx];
           }
           
           // Check each bit in the byte
           for (uint8_t bit_pos = 0; bit_pos < 8; bit_pos++) {
               if (byte & (1 << bit_pos)) {
                   result->error_bits[idx].node = i;
                   result->error_bits[idx].index = byte_idx * 8 + bit_pos;
                   result->error_bits[idx].monitoring_node = (uint16_t)monitoring_node;
                   result->error_bits[idx].value = 1;
                   idx++;
               }
           }
       }
   }
   
   return result;
}

/**
 * @brief Free error bits structure
 * 
 * @param inst Instance pointer (needed for allocator)
 * @param error_bits Structure to free
 */
void cfl_hbit_error_bits_destroy(
    cfl_hbit_instance_t* inst,
    cfl_hbit_error_bits_t* error_bits)
{
    if (!inst || !error_bits) return;
    if (!inst->allocator.free) return;
    
    if (error_bits->error_bits) {
        inst->allocator.free(error_bits->error_bits, inst->allocator.ctx);
    }
    
    inst->allocator.free(error_bits, inst->allocator.ctx);
}

void cfl_hbit_print_error_bits_by_node(cfl_hbit_instance_t* inst, cfl_hbit_error_bits_t* errors)
{
    if (!errors || errors->count == 0) {
        printf("No errors found\n");
        return;
    }
    
    uint16_t current_node = 0xFFFF;
    uint16_t bits_in_node = 0;
    
    for (uint32_t i = 0; i < errors->count; i++) {
        cfl_hbit_error_bit_t* err = &errors->error_bits[i];
        
        if (err->node != current_node) {
            if (current_node != 0xFFFF) {
                printf(" (%u bits)\n", bits_in_node);
            }
            
            current_node = err->node;
            bits_in_node = 0;
            
            const cfl_hbit_node_t* node = &inst->config->nodes[err->node];
            printf("Node %u (hash 0x%08X): bits [", err->node, node->path_hash);
        }
        
        if (bits_in_node > 0) printf(", ");
        printf("%u", err->index);
        bits_in_node++;
    }
    
    if (current_node != 0xFFFF) {
        printf("] (%u bits)\n", bits_in_node);
    }
}
