/**
 * @file cfl_hbit.c
 * @brief Hierarchical Bit Map Runtime - Implementation
 */

 #include "cfl_hbit.h"
 #include <string.h>
 
 /* ============================================ */
 /* Initialization / Destruction                 */
 /* ============================================ */
 
 size_t cfl_hbit_calc_ram_size(const cfl_hbit_config_t* config)
 {
     size_t total = 0;
     
     /* Instance struct */
     total += sizeof(cfl_hbit_instance_t);
     
     /* Buffer descriptors array */
     total += sizeof(cfl_hbit_buffer_t) * config->buffer_count;
     
     /* Buffer arenas */
     for (uint16_t i = 0; i < config->buffer_count; i++) {
         const cfl_hbit_buffer_t* buf = &config->buffer_configs[i];
         total += buf->arena_size * 2;  /* current + shadow */
         
         if (buf->type == CFL_HBIT_BUF_OR_LATCH) {
             total += buf->arena_size;  /* latched */
         }
         if (buf->type == CFL_HBIT_BUF_OR_MASK) {
             total += buf->arena_size;  /* mask */
         }
     }
     
     /* Dirty tracking: 1 bit per node, rounded up */
     total += (config->node_count + 7) / 8;
     
     return total;
 }
 
 cfl_hbit_instance_t* cfl_hbit_create(
     const cfl_hbit_config_t* config,
     const cfl_hbit_allocator_t* allocator)
 {
     if (!config || !allocator || !allocator->alloc) {
         return NULL;
     }
     
     /* Calculate total size needed */
     size_t total_size = cfl_hbit_calc_ram_size(config);
     
     /* Single allocation for everything */
     uint8_t* ram = (uint8_t*)allocator->alloc(total_size, allocator->ctx);
     if (!ram) {
         return NULL;
     }
     
     /* Zero the entire block */
     memset(ram, 0, total_size);
     
     /* Layout memory */
     uint8_t* ptr = ram;
     
     /* Instance at start */
     cfl_hbit_instance_t* inst = (cfl_hbit_instance_t*)ptr;
     ptr += sizeof(cfl_hbit_instance_t);
     
     /* Store config and allocator */
     inst->config = config;
     inst->allocator = *allocator;
     inst->ram = ram;
     
     /* Buffer descriptors array */
     inst->buffers = (cfl_hbit_buffer_t*)ptr;
     ptr += sizeof(cfl_hbit_buffer_t) * config->buffer_count;
     
     /* Initialize each buffer */
     for (uint16_t i = 0; i < config->buffer_count; i++) {
         const cfl_hbit_buffer_t* cfg = &config->buffer_configs[i];
         cfl_hbit_buffer_t* buf = &inst->buffers[i];
         
         /* Copy static config */
         buf->type = cfg->type;
         buf->arena_size = cfg->arena_size;
         buf->arena_info = cfg->arena_info;
         
         /* Assign runtime storage */
         buf->current = ptr;
         ptr += buf->arena_size;
         
         buf->shadow = ptr;
         ptr += buf->arena_size;
         
         if (buf->type == CFL_HBIT_BUF_OR_LATCH) {
             buf->latched = ptr;
             ptr += buf->arena_size;
         } else {
             buf->latched = NULL;
         }
         
         if (buf->type == CFL_HBIT_BUF_OR_MASK) {
             buf->mask = ptr;
             ptr += buf->arena_size;
             /* Initialize mask to all 1s (all bits propagate) */
             memset(buf->mask, 0xFF, buf->arena_size);
         } else {
             buf->mask = NULL;
         }
     }
     
     /* Dirty tracking */
     inst->dirty_nodes = ptr;
     inst->dirty_count = 0;
     
     return inst;
 }
 
 void cfl_hbit_destroy(cfl_hbit_instance_t* inst)
 {
     if (!inst) return;
     
     /* Free the single allocation block */
     if (inst->allocator.free && inst->ram) {
         inst->allocator.free(inst->ram, inst->allocator.ctx);
     }
 }
 
 void cfl_hbit_reset(cfl_hbit_instance_t* inst)
 {
     if (!inst) return;
     
     for (uint16_t i = 0; i < inst->config->buffer_count; i++) {
         cfl_hbit_buffer_t* buf = &inst->buffers[i];
         memset(buf->current, 0, buf->arena_size);
         memset(buf->shadow, 0, buf->arena_size);
         if (buf->latched) {
             memset(buf->latched, 0, buf->arena_size);
         }
         if (buf->mask) {
             memset(buf->mask, 0xFF, buf->arena_size);
         }
     }
     memset(inst->dirty_nodes, 0, (inst->config->node_count + 7) / 8);
     inst->dirty_count = 0;
 }
 
 /* ============================================ */
 /* Node Lookup                                  */
 /* ============================================ */
 
 int16_t cfl_hbit_find_node(const cfl_hbit_instance_t* inst, uint32_t hash)
 {
     int lo = 0;
     int hi = inst->config->node_count - 1;
     
     while (lo <= hi) {
         int mid = (lo + hi) / 2;
         uint32_t mid_hash = inst->config->nodes_by_hash[mid].hash;
         
         if (mid_hash == hash) {
             return (int16_t)inst->config->nodes_by_hash[mid].tree_index;
         } else if (mid_hash < hash) {
             lo = mid + 1;
         } else {
             hi = mid - 1;
         }
     }
     
     return -1;
 }
 
 /* ============================================ */
 /* Internal: Mark ancestors dirty               */
 /* ============================================ */
 
 static void mark_ancestors_dirty(cfl_hbit_instance_t* inst, uint16_t node_idx)
 {
     int16_t parent_idx = inst->config->nodes[node_idx].parent_index;
     while (parent_idx >= 0) {
         cfl_hbit_mark_dirty(inst, (uint16_t)parent_idx);
         parent_idx = inst->config->nodes[parent_idx].parent_index;
     }
 }
 
 /* ============================================ */
 /* Leaf Node Operations                         */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_leaf_set_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node_idx].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     uint8_t* ptr = cfl_hbit_node_ptr(buf, node_idx);
     uint8_t size = cfl_hbit_node_size(buf, node_idx);
     
     if (size == 0) return CFL_HBIT_OK;  /* Node doesn't use this buffer */
     if (bit_idx >= size * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     cfl_hbit_set_bit(ptr, bit_idx);
     
     /* For OR_LATCH, also set in latched buffer */
     if (buf->type == CFL_HBIT_BUF_OR_LATCH && buf->latched) {
         uint8_t* latch_ptr = buf->latched + buf->arena_info[node_idx].offset;
         cfl_hbit_set_bit(latch_ptr, bit_idx);
     }
     
     mark_ancestors_dirty(inst, node_idx);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_clear_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node_idx].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     uint8_t* ptr = cfl_hbit_node_ptr(buf, node_idx);
     uint8_t size = cfl_hbit_node_size(buf, node_idx);
     
     if (size == 0) return CFL_HBIT_OK;
     if (bit_idx >= size * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     cfl_hbit_clear_bit(ptr, bit_idx);
     mark_ancestors_dirty(inst, node_idx);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_write(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     const uint8_t* data,
     uint8_t len)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node_idx].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     uint8_t* ptr = cfl_hbit_node_ptr(buf, node_idx);
     uint8_t size = cfl_hbit_node_size(buf, node_idx);
     
     if (size == 0) return CFL_HBIT_OK;
     if (len > size) len = size;
     
     memcpy(ptr, data, len);
     
     /* For OR_LATCH, OR into latched buffer */
     if (buf->type == CFL_HBIT_BUF_OR_LATCH && buf->latched) {
         uint8_t* latch_ptr = buf->latched + buf->arena_info[node_idx].offset;
         for (uint8_t i = 0; i < len; i++) {
             latch_ptr[i] |= data[i];
         }
     }
     
     mark_ancestors_dirty(inst, node_idx);
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Read Operations                              */
 /* ============================================ */
 
 bool cfl_hbit_read_bit(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx)
 {
     if (!inst) return false;
     if (buffer_idx >= inst->config->buffer_count) return false;
     if (node_idx >= inst->config->node_count) return false;
     
     const cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     const uint8_t* ptr = buf->current + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size == 0 || bit_idx >= size * 8) return false;
     
     return cfl_hbit_get_bit(ptr, bit_idx);
 }
 
 bool cfl_hbit_read_latched_bit(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx)
 {
     if (!inst) return false;
     if (buffer_idx >= inst->config->buffer_count) return false;
     if (node_idx >= inst->config->node_count) return false;
     
     const cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     if (buf->type != CFL_HBIT_BUF_OR_LATCH || !buf->latched) return false;
     
     const uint8_t* ptr = buf->latched + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size == 0 || bit_idx >= size * 8) return false;
     
     return cfl_hbit_get_bit(ptr, bit_idx);
 }
 
 uint8_t cfl_hbit_read_node(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t* data,
     uint8_t max_len)
 {
     if (!inst) return 0;
     if (buffer_idx >= inst->config->buffer_count) return 0;
     if (node_idx >= inst->config->node_count) return 0;
     
     const cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     const uint8_t* ptr = buf->current + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size > max_len) size = max_len;
     memcpy(data, ptr, size);
     
     return size;
 }
 
 /* ============================================ */
 /* Mask Operations (OR_MASK buffers)            */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_set_mask(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     const uint8_t* mask,
     uint8_t len)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     if (buf->type != CFL_HBIT_BUF_OR_MASK || !buf->mask) return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t* ptr = buf->mask + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (len > size) len = size;
     memcpy(ptr, mask, len);
     
     mark_ancestors_dirty(inst, node_idx);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_set_mask_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx,
     bool enabled)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     if (buf->type != CFL_HBIT_BUF_OR_MASK || !buf->mask) return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t* ptr = buf->mask + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size == 0 || bit_idx >= size * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     if (enabled) {
         cfl_hbit_set_bit(ptr, bit_idx);
     } else {
         cfl_hbit_clear_bit(ptr, bit_idx);
     }
     
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Latch Clear Operations                       */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_clear_latch_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     if (buf->type != CFL_HBIT_BUF_OR_LATCH || !buf->latched) return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t* ptr = buf->latched + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size == 0 || bit_idx >= size * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     cfl_hbit_clear_bit(ptr, bit_idx);
     mark_ancestors_dirty(inst, node_idx);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_clear_latch_all(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buffer_idx >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node_idx >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     if (buf->type != CFL_HBIT_BUF_OR_LATCH || !buf->latched) return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t* ptr = buf->latched + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     memset(ptr, 0, size);
     mark_ancestors_dirty(inst, node_idx);
     
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Propagation                                  */
 /* ============================================ */
 
 static void propagate_node_buffer(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx)
 {
     const cfl_hbit_node_t* node = &inst->config->nodes[node_idx];
     cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     
     uint8_t* dst = buf->current + buf->arena_info[node_idx].offset;
     uint8_t size = buf->arena_info[node_idx].size;
     
     if (size == 0 || node->child_count == 0) return;
     
     /* Initialize based on merge type */
     if (buf->type == CFL_HBIT_BUF_AND) {
         memset(dst, 0xFF, size);  /* Start with all 1s for AND */
     } else {
         memset(dst, 0x00, size);  /* Start with all 0s for OR */
     }
     
     /* Merge children */
     for (uint16_t i = 0; i < node->child_count; i++) {
         uint16_t child_idx = node->first_child + i;
         const uint8_t* src = buf->current + buf->arena_info[child_idx].offset;
         uint8_t child_size = buf->arena_info[child_idx].size;
         
         if (child_size == 0) continue;
         
         /* Use smaller of parent/child size */
         uint8_t merge_size = (child_size < size) ? child_size : size;
         
         switch (buf->type) {
             case CFL_HBIT_BUF_OR_LATCH:
                 /* OR merge, also include latched values */
                 for (uint8_t j = 0; j < merge_size; j++) {
                     dst[j] |= src[j];
                 }
                 if (buf->latched) {
                     const uint8_t* latch_src = buf->latched + buf->arena_info[child_idx].offset;
                     uint8_t* latch_dst = buf->latched + buf->arena_info[node_idx].offset;
                     for (uint8_t j = 0; j < merge_size; j++) {
                         latch_dst[j] |= latch_src[j];
                     }
                 }
                 break;
                 
             case CFL_HBIT_BUF_OR_MASK:
                 /* OR merge with mask */
                 if (buf->mask) {
                     const uint8_t* mask = buf->mask + buf->arena_info[child_idx].offset;
                     for (uint8_t j = 0; j < merge_size; j++) {
                         dst[j] |= (src[j] & mask[j]);
                     }
                 } else {
                     for (uint8_t j = 0; j < merge_size; j++) {
                         dst[j] |= src[j];
                     }
                 }
                 break;
                 
             case CFL_HBIT_BUF_AND:
                 /* AND merge */
                 for (uint8_t j = 0; j < merge_size; j++) {
                     dst[j] &= src[j];
                 }
                 break;
         }
     }
 }
 
 void cfl_hbit_propagate(cfl_hbit_instance_t* inst)
 {
     if (!inst || inst->dirty_count == 0) return;
     
     /* Process nodes from deepest to shallowest */
     /* Since nodes are in tree order, we can process in reverse */
     for (int16_t i = inst->config->node_count - 1; i >= 0; i--) {
         if (!cfl_hbit_is_dirty(inst, (uint16_t)i)) continue;
         
         /* Propagate all buffers for this node */
         for (uint16_t b = 0; b < inst->config->buffer_count; b++) {
             propagate_node_buffer(inst, b, (uint16_t)i);
         }
         
         cfl_hbit_clear_dirty(inst, (uint16_t)i);
     }
 }
 
 void cfl_hbit_propagate_tree(cfl_hbit_instance_t* inst, uint16_t root_idx)
 {
     if (!inst) return;
     
     const cfl_hbit_node_t* root = &inst->config->nodes[root_idx];
     
     /* Find the range of nodes in this subtree */
     uint16_t subtree_end = root_idx + 1;
     
     /* Find end of subtree by looking for next node at same or lower depth */
     while (subtree_end < inst->config->node_count) {
         if (inst->config->nodes[subtree_end].depth <= root->depth &&
             inst->config->nodes[subtree_end].parent_index != (int16_t)root_idx) {
             break;
         }
         subtree_end++;
     }
     
     /* Process in reverse (bottom-up) */
     for (int16_t i = subtree_end - 1; i >= (int16_t)root_idx; i--) {
         if (!inst->config->nodes[i].is_leaf) {
             for (uint16_t b = 0; b < inst->config->buffer_count; b++) {
                 propagate_node_buffer(inst, b, (uint16_t)i);
             }
         }
         cfl_hbit_clear_dirty(inst, (uint16_t)i);
     }
 }
 
 /* ============================================ */
 /* Tree Walking                                 */
 /* ============================================ */
 
 void cfl_hbit_walk_preorder(
     cfl_hbit_instance_t* inst,
     uint16_t root_idx,
     cfl_hbit_walk_fn callback,
     void* user_data)
 {
     if (!inst || !callback) return;
     
     const cfl_hbit_node_t* root = &inst->config->nodes[root_idx];
     uint16_t subtree_end = root_idx + 1;
     
     while (subtree_end < inst->config->node_count &&
            inst->config->nodes[subtree_end].depth > root->depth) {
         subtree_end++;
     }
     
     for (uint16_t i = root_idx; i < subtree_end; i++) {
         if (!callback(inst, i, user_data)) break;
     }
 }
 
 void cfl_hbit_walk_postorder(
     cfl_hbit_instance_t* inst,
     uint16_t root_idx,
     cfl_hbit_walk_fn callback,
     void* user_data)
 {
     if (!inst || !callback) return;
     
     const cfl_hbit_node_t* root = &inst->config->nodes[root_idx];
     uint16_t subtree_end = root_idx + 1;
     
     while (subtree_end < inst->config->node_count &&
            inst->config->nodes[subtree_end].depth > root->depth) {
         subtree_end++;
     }
     
     for (int16_t i = subtree_end - 1; i >= (int16_t)root_idx; i--) {
         if (!callback(inst, (uint16_t)i, user_data)) break;
     }
 }
 
 void cfl_hbit_foreach_child(
     cfl_hbit_instance_t* inst,
     uint16_t parent_idx,
     cfl_hbit_walk_fn callback,
     void* user_data)
 {
     if (!inst || !callback) return;
     
     const cfl_hbit_node_t* parent = &inst->config->nodes[parent_idx];
     
     for (uint16_t i = 0; i < parent->child_count; i++) {
         uint16_t child_idx = parent->first_child + i;
         if (!callback(inst, child_idx, user_data)) break;
     }
 }
 
 /* ============================================ */
 /* Controller Implementation                    */
 /* ============================================ */
 
 /**
  * Count leaves under a node
  */
 static uint16_t count_leaves_under(
     const cfl_hbit_config_t* config,
     uint16_t node_idx)
 {
     const cfl_hbit_node_t* node = &config->nodes[node_idx];
     
     if (node->is_leaf) return 1;
     
     /* Find subtree extent (tree order means subtree is contiguous) */
     uint16_t subtree_end = node_idx + 1;
     while (subtree_end < config->node_count &&
            config->nodes[subtree_end].depth > node->depth) {
         subtree_end++;
     }
     
     /* Count leaves in this range */
     uint16_t count = 0;
     for (uint16_t i = node_idx; i < subtree_end; i++) {
         if (config->nodes[i].is_leaf) {
             count++;
         }
     }
     return count;
 }
 
 /**
  * Collect leaf indices under a node into array
  */
 static uint16_t collect_leaves(
     const cfl_hbit_config_t* config,
     uint16_t node_idx,
     uint16_t* leaves,
     uint16_t offset)
 {
     const cfl_hbit_node_t* node = &config->nodes[node_idx];
     
     if (node->is_leaf) {
         leaves[offset] = node_idx;
         return 1;
     }
     
     /* Find subtree extent */
     uint16_t subtree_end = node_idx + 1;
     while (subtree_end < config->node_count &&
            config->nodes[subtree_end].depth > node->depth) {
         subtree_end++;
     }
     
     /* Collect leaves in this range */
     uint16_t count = 0;
     for (uint16_t i = node_idx + 1; i < subtree_end; i++) {
         if (config->nodes[i].is_leaf) {
             leaves[offset + count] = i;
             count++;
         }
     }
     return count;
 }
 
 cfl_hbit_controller_t* cfl_hbit_controller_create(
     cfl_hbit_instance_t* inst,
     uint16_t root_node,
     uint16_t buffer_idx,
     const cfl_hbit_allocator_t* allocator)
 {
     if (!inst || !allocator || !allocator->alloc) return NULL;
     if (root_node >= inst->config->node_count) return NULL;
     if (buffer_idx >= inst->config->buffer_count) return NULL;
     
     const cfl_hbit_config_t* config = inst->config;
     const cfl_hbit_node_t* root = &config->nodes[root_node];
     const cfl_hbit_buffer_t* buf = &inst->buffers[buffer_idx];
     
     /* Count direct children by checking parent_index */
     uint16_t child_count = 0;
     for (uint16_t i = 0; i < config->node_count; i++) {
         if (config->nodes[i].parent_index == (int16_t)root_node) {
             child_count++;
         }
     }
     
     /* Count total leaves under root */
     uint16_t leaf_count = count_leaves_under(config, root_node);
     
     /* Get bits per leaf from first leaf's arena size */
     uint8_t bits_per_leaf = 0;
     for (uint16_t i = root_node; i < config->node_count; i++) {
         if (config->nodes[i].depth <= root->depth && i != root_node) break;
         if (config->nodes[i].is_leaf) {
             bits_per_leaf = buf->arena_info[i].size * 8;
             break;
         }
     }
     
     /* Calculate total allocation size */
     size_t size = sizeof(cfl_hbit_controller_t);
     size += sizeof(cfl_hbit_child_t) * child_count;
     size += sizeof(uint16_t) * leaf_count;
     
     /* Allocate */
     uint8_t* ram = (uint8_t*)allocator->alloc(size, allocator->ctx);
     if (!ram) return NULL;
     
     memset(ram, 0, size);
     
     /* Layout memory */
     uint8_t* ptr = ram;
     
     cfl_hbit_controller_t* ctrl = (cfl_hbit_controller_t*)ptr;
     ptr += sizeof(cfl_hbit_controller_t);
     
     ctrl->children = (cfl_hbit_child_t*)ptr;
     ptr += sizeof(cfl_hbit_child_t) * child_count;
     
     ctrl->leaf_nodes = (uint16_t*)ptr;
     
     /* Fill in controller */
     ctrl->inst = inst;
     ctrl->root_node = root_node;
     ctrl->buffer_idx = buffer_idx;
     ctrl->child_count = child_count;
     ctrl->leaf_count = leaf_count;
     ctrl->bits_per_leaf = bits_per_leaf;
     ctrl->total_bits = leaf_count * bits_per_leaf;
     ctrl->allocator = *allocator;
     ctrl->ram = ram;
     
     /* Collect all leaves */
     collect_leaves(config, root_node, ctrl->leaf_nodes, 0);
     
     /* Build child descriptors by finding nodes with parent_index == root_node */
     uint16_t child_idx = 0;
     uint16_t leaf_offset = 0;
     uint16_t bit_offset = 0;
     
     for (uint16_t i = 0; i < config->node_count && child_idx < child_count; i++) {
         if (config->nodes[i].parent_index == (int16_t)root_node) {
             uint16_t child_leaves = count_leaves_under(config, i);
             uint16_t child_bits = child_leaves * bits_per_leaf;
             
             ctrl->children[child_idx].node_idx = i;
             ctrl->children[child_idx].leaf_start = leaf_offset;
             ctrl->children[child_idx].leaf_count = child_leaves;
             ctrl->children[child_idx].bit_start = bit_offset;
             ctrl->children[child_idx].bit_count = child_bits;
             
             leaf_offset += child_leaves;
             bit_offset += child_bits;
             child_idx++;
         }
     }
     
     return ctrl;
 }
 
 void cfl_hbit_controller_destroy(cfl_hbit_controller_t* ctrl)
 {
     if (!ctrl) return;
     
     if (ctrl->allocator.free && ctrl->ram) {
         ctrl->allocator.free(ctrl->ram, ctrl->allocator.ctx);
     }
 }
 
 int16_t cfl_hbit_controller_get_node_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index,
     uint8_t* bit_index)
 {
     if (!ctrl || !bit_index) return -1;
     if (child_index >= ctrl->child_count) return -1;
     
     cfl_hbit_child_t* child = &ctrl->children[child_index];
     
     if (child_bit_index >= child->bit_count) return -1;
     
     /* Which leaf within this child's range? */
     uint16_t leaf_offset = child_bit_index / ctrl->bits_per_leaf;
     uint16_t bit_within_leaf = child_bit_index % ctrl->bits_per_leaf;
     
     if (leaf_offset >= child->leaf_count) return -1;
     
     *bit_index = (uint8_t)bit_within_leaf;
     return (int16_t)ctrl->leaf_nodes[child->leaf_start + leaf_offset];
 }
 
 int16_t cfl_hbit_controller_get_bitmap_node(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index,
     uint8_t* bit_index)
 {
     if (!ctrl || !bit_index) return -1;
     if (bitmap_index >= ctrl->total_bits) return -1;
     
     /* Which leaf? */
     uint16_t leaf_offset = bitmap_index / ctrl->bits_per_leaf;
     uint16_t bit_within_leaf = bitmap_index % ctrl->bits_per_leaf;
     
     if (leaf_offset >= ctrl->leaf_count) return -1;
     
     *bit_index = (uint8_t)bit_within_leaf;
     return (int16_t)ctrl->leaf_nodes[leaf_offset];
 }
 
 cfl_hbit_error_t cfl_hbit_controller_set_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_bitmap_node(ctrl, bitmap_index, &bit_idx);
     if (node < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     return cfl_hbit_leaf_set_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_clear_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_bitmap_node(ctrl, bitmap_index, &bit_idx);
     if (node < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     return cfl_hbit_leaf_clear_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }
 
 bool cfl_hbit_controller_read_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index)
 {
     if (!ctrl) return false;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_bitmap_node(ctrl, bitmap_index, &bit_idx);
     if (node < 0) return false;
     
     return cfl_hbit_read_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_set_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_node_bit(ctrl, child_index, child_bit_index, &bit_idx);
     if (node < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     return cfl_hbit_leaf_set_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_clear_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_node_bit(ctrl, child_index, child_bit_index, &bit_idx);
     if (node < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     return cfl_hbit_leaf_clear_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }
 
 bool cfl_hbit_controller_read_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index)
 {
     if (!ctrl) return false;
     
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_node_bit(ctrl, child_index, child_bit_index, &bit_idx);
     if (node < 0) return false;
     
     return cfl_hbit_read_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)node, bit_idx);
 }