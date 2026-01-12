/**
 * @file cfl_hbit.c
 * @brief Hierarchical Bit Map Runtime - Implementation
 */

 #include "cfl_hbit.h"
 #include <string.h>
 #include <stdio.h>
 
 /* ============================================ */
 /* Create / Destroy                             */
 /* ============================================ */
 
 cfl_hbit_instance_t* cfl_hbit_create(
     const cfl_hbit_allocator_t* allocator,
     const cfl_hbit_config_t* config)
 {
     if (!allocator || !allocator->alloc || !allocator->free || !config) return NULL;
     
     /* Allocate instance */
     cfl_hbit_instance_t* inst = (cfl_hbit_instance_t*)allocator->alloc(
         sizeof(cfl_hbit_instance_t), allocator->ctx);
     if (!inst) return NULL;
     
     /* Allocate RAM */
     uint8_t* ram = (uint8_t*)allocator->alloc(config->ram_size, allocator->ctx);
     if (!ram) {
         allocator->free(inst, allocator->ctx);
         return NULL;
     }
     
     memset(ram, 0, config->ram_size);
     
     inst->config = config;
     inst->allocator = *allocator;
     inst->ram = ram;
     inst->dirty_count = 0;
     
     /* Layout: pointer arrays first, then buffers, then dirty bits */
     uint8_t* p = ram;
     uint16_t n = config->buffer_count;
     
     inst->current = (uint8_t**)p; p += sizeof(uint8_t*) * n;
     inst->shadow  = (uint8_t**)p; p += sizeof(uint8_t*) * n;
     inst->latched = (uint8_t**)p; p += sizeof(uint8_t*) * n;
     inst->mask    = (uint8_t**)p; p += sizeof(uint8_t*) * n;
     
     /* Assign buffer arenas */
     for (uint16_t i = 0; i < n; i++) {
         const cfl_hbit_buffer_config_t* bc = &config->buffer_configs[i];
         
         inst->current[i] = p; p += bc->arena_size;
         inst->shadow[i]  = p; p += bc->arena_size;
         
         if (bc->type == CFL_HBIT_BUF_OR_LATCH) {
             inst->latched[i] = p; p += bc->arena_size;
         } else {
             inst->latched[i] = NULL;
         }
         
         if (bc->type == CFL_HBIT_BUF_OR_MASK) {
             inst->mask[i] = p; p += bc->arena_size;
             memset(inst->mask[i], 0xFF, bc->arena_size);
         } else {
             inst->mask[i] = NULL;
         }
     }
     
     inst->dirty_nodes = p;
     
     return inst;
 }
 
 void cfl_hbit_destroy(cfl_hbit_instance_t* inst)
 {
     if (!inst) return;
     
     cfl_hbit_free_fn free_fn = inst->allocator.free;
     void* ctx = inst->allocator.ctx;
     uint8_t* ram = inst->ram;
     
     if (free_fn) {
         if (ram) free_fn(ram, ctx);
         free_fn(inst, ctx);
     }
 }
 
 void cfl_hbit_reset(cfl_hbit_instance_t* inst)
 {
     if (!inst) return;
     
     for (uint16_t i = 0; i < inst->config->buffer_count; i++) {
         const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[i];
         memset(inst->current[i], 0, bc->arena_size);
         memset(inst->shadow[i], 0, bc->arena_size);
         if (inst->latched[i]) memset(inst->latched[i], 0, bc->arena_size);
         if (inst->mask[i]) memset(inst->mask[i], 0xFF, bc->arena_size);
     }
     memset(inst->dirty_nodes, 0, (inst->config->node_count + 7) / 8);
     inst->dirty_count = 0;
 }
 
 /* ============================================ */
 /* Shadow Buffer Operations                     */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_shadow_set_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
     uint8_t sz = bc->arena_info[node].size;
     if (sz == 0) return CFL_HBIT_OK;
     if (bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     uint16_t off = bc->arena_info[node].offset;
     cfl_hbit_set_bit(inst->shadow[buf] + off, bit);
     cfl_hbit_mark_dirty(inst, node);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_shadow_clear_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
     uint8_t sz = bc->arena_info[node].size;
     if (sz == 0) return CFL_HBIT_OK;
     if (bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     uint16_t off = bc->arena_info[node].offset;
     cfl_hbit_clear_bit(inst->shadow[buf] + off, bit);
     cfl_hbit_mark_dirty(inst, node);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_shadow_write(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* data, uint8_t len)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
     uint8_t sz = bc->arena_info[node].size;
     if (sz == 0) return CFL_HBIT_OK;
     if (len > sz) len = sz;
     
     uint16_t off = bc->arena_info[node].offset;
     memcpy(inst->shadow[buf] + off, data, len);
     cfl_hbit_mark_dirty(inst, node);
     
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Sync (Shadow -> Current)                     */
 /* ============================================ */
 
 void cfl_hbit_sync_buffer(cfl_hbit_instance_t* inst, uint16_t buf)
 {
     if (!inst || buf >= inst->config->buffer_count) return;
     
     const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
     
     /* Copy shadow to current for all leaf nodes */
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (!inst->config->nodes[i].is_leaf) continue;
         
         uint16_t off = bc->arena_info[i].offset;
         uint8_t sz = bc->arena_info[i].size;
         if (sz == 0) continue;
         
         uint8_t* src = inst->shadow[buf] + off;
         uint8_t* dst = inst->current[buf] + off;
         
         memcpy(dst, src, sz);
         
         /* Update latched buffer if OR_LATCH */
         if (bc->type == CFL_HBIT_BUF_OR_LATCH && inst->latched[buf]) {
             uint8_t* lat = inst->latched[buf] + off;
             for (uint8_t j = 0; j < sz; j++) {
                 lat[j] |= src[j];
             }
         }
     }
 }
 
 void cfl_hbit_sync(cfl_hbit_instance_t* inst)
 {
     if (!inst) return;
     
     /* Sync all buffers */
     for (uint16_t buf = 0; buf < inst->config->buffer_count; buf++) {
         cfl_hbit_sync_buffer(inst, buf);
     }
     
     /* Mark ancestors of all dirty leaves for propagation */
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (cfl_hbit_is_dirty(inst, i)) {
             int16_t p = inst->config->nodes[i].parent_index;
             while (p >= 0) {
                 cfl_hbit_mark_dirty(inst, (uint16_t)p);
                 p = inst->config->nodes[p].parent_index;
             }
         }
     }
 }
 
 void cfl_hbit_sync_and_propagate(cfl_hbit_instance_t* inst)
 {
     cfl_hbit_sync(inst);
     cfl_hbit_propagate(inst);
 }
 
 /* ============================================ */
 /* Node Lookup                                  */
 /* ============================================ */
 
 int16_t cfl_hbit_find_node(const cfl_hbit_instance_t* inst, uint32_t hash)
 {
     int lo = 0, hi = inst->config->node_count - 1;
     while (lo <= hi) {
         int mid = (lo + hi) / 2;
         uint32_t h = inst->config->nodes_by_hash[mid].hash;
         if (h == hash) return (int16_t)inst->config->nodes_by_hash[mid].tree_index;
         if (h < hash) lo = mid + 1; else hi = mid - 1;
     }
     return -1;
 }
 
 /* FNV-1a hash - must match codegen */
 uint32_t cfl_hbit_hash_string(const char* str)
 {
     uint32_t hash = 2166136261U;
     while (*str) {
         hash ^= (uint8_t)*str++;
         hash *= 16777619U;
     }
     return hash;
 }
 
 int16_t cfl_hbit_find_node_path(const cfl_hbit_instance_t* inst, const char* fmt, ...)
 {
     if (!inst || !fmt) return -1;
     
     char path[256];
     va_list args;
     va_start(args, fmt);
     vsnprintf(path, sizeof(path), fmt, args);
     va_end(args);
     
     uint32_t hash = cfl_hbit_hash_string(path);
     return cfl_hbit_find_node(inst, hash);
 }
 
 /* ============================================ */
 /* Internal Helpers                             */
 /* ============================================ */
 
 static void mark_ancestors(cfl_hbit_instance_t* inst, uint16_t node)
 {
     int16_t p = inst->config->nodes[node].parent_index;
     while (p >= 0) {
         cfl_hbit_mark_dirty(inst, (uint16_t)p);
         p = inst->config->nodes[p].parent_index;
     }
 }
 
 static inline uint8_t* node_ptr(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node)
 {
     return inst->current[buf] + inst->config->buffer_configs[buf].arena_info[node].offset;
 }
 
 static inline uint8_t node_size(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node)
 {
     return inst->config->buffer_configs[buf].arena_info[node].size;
 }
 
 /* ============================================ */
 /* Leaf Operations                              */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_leaf_set_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0) return CFL_HBIT_OK;
     if (bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     cfl_hbit_set_bit(node_ptr(inst, buf, node), bit);
     
     if (inst->config->buffer_configs[buf].type == CFL_HBIT_BUF_OR_LATCH && inst->latched[buf]) {
         uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
         cfl_hbit_set_bit(inst->latched[buf] + off, bit);
     }
     
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_clear_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0) return CFL_HBIT_OK;
     if (bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     cfl_hbit_clear_bit(node_ptr(inst, buf, node), bit);
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_write(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* data, uint8_t len)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0) return CFL_HBIT_OK;
     if (len > sz) len = sz;
     
     memcpy(node_ptr(inst, buf, node), data, len);
     
     if (inst->config->buffer_configs[buf].type == CFL_HBIT_BUF_OR_LATCH && inst->latched[buf]) {
         uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
         uint8_t* lp = inst->latched[buf] + off;
         for (uint8_t i = 0; i < len; i++) lp[i] |= data[i];
     }
     
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_clear(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0) return CFL_HBIT_OK;
     
     memset(node_ptr(inst, buf, node), 0, sz);
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_leaf_fill(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t value)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (!inst->config->nodes[node].is_leaf) return CFL_HBIT_ERR_NOT_LEAF;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0) return CFL_HBIT_OK;
     
     memset(node_ptr(inst, buf, node), value, sz);
     
     if (inst->config->buffer_configs[buf].type == CFL_HBIT_BUF_OR_LATCH && inst->latched[buf]) {
         uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
         uint8_t* lp = inst->latched[buf] + off;
         for (uint8_t i = 0; i < sz; i++) lp[i] |= value;
     }
     
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 void cfl_hbit_clear_all_leaves(cfl_hbit_instance_t* inst, uint16_t buf)
 {
     if (!inst || buf >= inst->config->buffer_count) return;
     
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (inst->config->nodes[i].is_leaf) {
             cfl_hbit_leaf_clear(inst, buf, i);
         }
     }
 }
 
 void cfl_hbit_fill_all_leaves(cfl_hbit_instance_t* inst, uint16_t buf, uint8_t value)
 {
     if (!inst || buf >= inst->config->buffer_count) return;
     
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (inst->config->nodes[i].is_leaf) {
             cfl_hbit_leaf_fill(inst, buf, i, value);
         }
     }
 }
 
 /* ============================================ */
 /* Read Operations                              */
 /* ============================================ */
 
 bool cfl_hbit_read_bit(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst || buf >= inst->config->buffer_count || node >= inst->config->node_count) return false;
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0 || bit >= sz * 8) return false;
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     return cfl_hbit_get_bit(inst->current[buf] + off, bit);
 }
 
 bool cfl_hbit_read_latched_bit(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst || buf >= inst->config->buffer_count || node >= inst->config->node_count) return false;
     if (inst->config->buffer_configs[buf].type != CFL_HBIT_BUF_OR_LATCH || !inst->latched[buf]) return false;
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0 || bit >= sz * 8) return false;
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     return cfl_hbit_get_bit(inst->latched[buf] + off, bit);
 }
 
 uint8_t cfl_hbit_read_node(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t* data, uint8_t max_len)
 {
     if (!inst || buf >= inst->config->buffer_count || node >= inst->config->node_count) return 0;
     uint8_t sz = node_size(inst, buf, node);
     if (sz > max_len) sz = max_len;
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     memcpy(data, inst->current[buf] + off, sz);
     return sz;
 }
 
 /* ============================================ */
 /* Mask Operations                              */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_set_mask(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* mask, uint8_t len)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (inst->config->buffer_configs[buf].type != CFL_HBIT_BUF_OR_MASK || !inst->mask[buf])
         return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t sz = node_size(inst, buf, node);
     if (len > sz) len = sz;
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     memcpy(inst->mask[buf] + off, mask, len);
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_set_mask_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit, bool enabled)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (inst->config->buffer_configs[buf].type != CFL_HBIT_BUF_OR_MASK || !inst->mask[buf])
         return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0 || bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     if (enabled) cfl_hbit_set_bit(inst->mask[buf] + off, bit);
     else cfl_hbit_clear_bit(inst->mask[buf] + off, bit);
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Latch Clear                                  */
 /* ============================================ */
 
 cfl_hbit_error_t cfl_hbit_clear_latch_bit(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (inst->config->buffer_configs[buf].type != CFL_HBIT_BUF_OR_LATCH || !inst->latched[buf])
         return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t sz = node_size(inst, buf, node);
     if (sz == 0 || bit >= sz * 8) return CFL_HBIT_ERR_OUT_OF_RANGE;
     
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     cfl_hbit_clear_bit(inst->latched[buf] + off, bit);
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_error_t cfl_hbit_clear_latch_all(
     cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node)
 {
     if (!inst) return CFL_HBIT_ERR_NULL_PTR;
     if (buf >= inst->config->buffer_count) return CFL_HBIT_ERR_INVALID_BUFFER;
     if (node >= inst->config->node_count) return CFL_HBIT_ERR_INVALID_NODE;
     if (inst->config->buffer_configs[buf].type != CFL_HBIT_BUF_OR_LATCH || !inst->latched[buf])
         return CFL_HBIT_ERR_INVALID_BUFFER;
     
     uint8_t sz = node_size(inst, buf, node);
     uint16_t off = inst->config->buffer_configs[buf].arena_info[node].offset;
     memset(inst->latched[buf] + off, 0, sz);
     mark_ancestors(inst, node);
     return CFL_HBIT_OK;
 }
 
 /* ============================================ */
 /* Propagation                                  */
 /* ============================================ */
 
 static void propagate_node(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node)
 {
     const cfl_hbit_node_t* n = &inst->config->nodes[node];
     const cfl_hbit_buffer_config_t* bc = &inst->config->buffer_configs[buf];
     
     uint16_t off = bc->arena_info[node].offset;
     uint8_t sz = bc->arena_info[node].size;
     if (sz == 0 || n->child_count == 0) return;
     
     uint8_t* dst = inst->current[buf] + off;
     
     memset(dst, (bc->type == CFL_HBIT_BUF_AND) ? 0xFF : 0x00, sz);
     
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (inst->config->nodes[i].parent_index != (int16_t)node) continue;
         
         uint16_t c_off = bc->arena_info[i].offset;
         uint8_t c_sz = bc->arena_info[i].size;
         if (c_sz == 0) continue;
         
         uint8_t msz = (c_sz < sz) ? c_sz : sz;
         const uint8_t* src = inst->current[buf] + c_off;
         
         switch (bc->type) {
         case CFL_HBIT_BUF_OR_LATCH:
             for (uint8_t j = 0; j < msz; j++) dst[j] |= src[j];
             if (inst->latched[buf]) {
                 const uint8_t* ls = inst->latched[buf] + c_off;
                 uint8_t* ld = inst->latched[buf] + off;
                 for (uint8_t j = 0; j < msz; j++) ld[j] |= ls[j];
             }
             break;
         case CFL_HBIT_BUF_OR_MASK:
             if (inst->mask[buf]) {
                 const uint8_t* m = inst->mask[buf] + c_off;
                 for (uint8_t j = 0; j < msz; j++) dst[j] |= (src[j] & m[j]);
             } else {
                 for (uint8_t j = 0; j < msz; j++) dst[j] |= src[j];
             }
             break;
         case CFL_HBIT_BUF_AND:
             for (uint8_t j = 0; j < msz; j++) dst[j] &= src[j];
             break;
         }
     }
 }
 
 void cfl_hbit_propagate(cfl_hbit_instance_t* inst)
 {
     if (!inst || inst->dirty_count == 0) return;
     
     for (int16_t i = inst->config->node_count - 1; i >= 0; i--) {
         if (!cfl_hbit_is_dirty(inst, (uint16_t)i)) continue;
         for (uint16_t b = 0; b < inst->config->buffer_count; b++) {
             propagate_node(inst, b, (uint16_t)i);
         }
         cfl_hbit_clear_dirty(inst, (uint16_t)i);
     }
 }
 
 void cfl_hbit_propagate_tree(cfl_hbit_instance_t* inst, uint16_t root)
 {
     if (!inst) return;
     const cfl_hbit_node_t* r = &inst->config->nodes[root];
     
     uint16_t end = root + 1;
     while (end < inst->config->node_count && inst->config->nodes[end].depth > r->depth) end++;
     
     for (int16_t i = end - 1; i >= (int16_t)root; i--) {
         if (!inst->config->nodes[i].is_leaf) {
             for (uint16_t b = 0; b < inst->config->buffer_count; b++) {
                 propagate_node(inst, b, (uint16_t)i);
             }
         }
         cfl_hbit_clear_dirty(inst, (uint16_t)i);
     }
 }
 
 /* ============================================ */
 /* Tree Walking                                 */
 /* ============================================ */
 
 void cfl_hbit_walk_preorder(cfl_hbit_instance_t* inst, uint16_t root, cfl_hbit_walk_fn cb, void* ctx)
 {
     if (!inst || !cb) return;
     const cfl_hbit_node_t* r = &inst->config->nodes[root];
     uint16_t end = root + 1;
     while (end < inst->config->node_count && inst->config->nodes[end].depth > r->depth) end++;
     for (uint16_t i = root; i < end; i++) if (!cb(inst, i, ctx)) break;
 }
 
 void cfl_hbit_walk_postorder(cfl_hbit_instance_t* inst, uint16_t root, cfl_hbit_walk_fn cb, void* ctx)
 {
     if (!inst || !cb) return;
     const cfl_hbit_node_t* r = &inst->config->nodes[root];
     uint16_t end = root + 1;
     while (end < inst->config->node_count && inst->config->nodes[end].depth > r->depth) end++;
     for (int16_t i = end - 1; i >= (int16_t)root; i--) if (!cb(inst, (uint16_t)i, ctx)) break;
 }
 
 void cfl_hbit_foreach_child(cfl_hbit_instance_t* inst, uint16_t parent, cfl_hbit_walk_fn cb, void* ctx)
 {
     if (!inst || !cb) return;
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (inst->config->nodes[i].parent_index == (int16_t)parent) {
             if (!cb(inst, i, ctx)) break;
         }
     }
 }
 
 /* ============================================ */
 /* Controller                                   */
 /* ============================================ */
 
 static uint16_t count_leaves(const cfl_hbit_config_t* cfg, uint16_t node)
 {
     const cfl_hbit_node_t* n = &cfg->nodes[node];
     if (n->is_leaf) return 1;
     
     uint16_t end = node + 1;
     while (end < cfg->node_count && cfg->nodes[end].depth > n->depth) end++;
     
     uint16_t c = 0;
     for (uint16_t i = node; i < end; i++) if (cfg->nodes[i].is_leaf) c++;
     return c;
 }
 
 static uint16_t collect_leaves(const cfl_hbit_config_t* cfg, uint16_t node, uint16_t* out, uint16_t off)
 {
     const cfl_hbit_node_t* n = &cfg->nodes[node];
     if (n->is_leaf) { out[off] = node; return 1; }
     
     uint16_t end = node + 1;
     while (end < cfg->node_count && cfg->nodes[end].depth > n->depth) end++;
     
     uint16_t c = 0;
     for (uint16_t i = node + 1; i < end; i++) {
         if (cfg->nodes[i].is_leaf) { out[off + c] = i; c++; }
     }
     return c;
 }
 
 cfl_hbit_controller_t* cfl_hbit_controller_create(
     cfl_hbit_instance_t* inst, uint16_t root, uint16_t buf)
 {
     if (!inst || !inst->allocator.alloc) return NULL;
     if (root >= inst->config->node_count) return NULL;
     if (buf >= inst->config->buffer_count) return NULL;
     
     const cfl_hbit_allocator_t* alloc = &inst->allocator;
     
     const cfl_hbit_config_t* cfg = inst->config;
     const cfl_hbit_node_t* rn = &cfg->nodes[root];
     
     uint16_t nc = 0;
     for (uint16_t i = 0; i < cfg->node_count; i++)
         if (cfg->nodes[i].parent_index == (int16_t)root) nc++;
     
     uint16_t nl = count_leaves(cfg, root);
     
     uint8_t bpl = 0;
     uint16_t end = root + 1;
     while (end < cfg->node_count && cfg->nodes[end].depth > rn->depth) end++;
     for (uint16_t i = root; i < end; i++) {
         if (cfg->nodes[i].is_leaf) {
             bpl = cfg->buffer_configs[buf].arena_info[i].size * 8;
             break;
         }
     }
     
     size_t sz = sizeof(cfl_hbit_controller_t) + sizeof(cfl_hbit_child_t) * nc + sizeof(uint16_t) * nl;
     uint8_t* ram = (uint8_t*)alloc->alloc(sz, alloc->ctx);
     if (!ram) return NULL;
     memset(ram, 0, sz);
     
     uint8_t* p = ram;
     cfl_hbit_controller_t* ctrl = (cfl_hbit_controller_t*)p; p += sizeof(cfl_hbit_controller_t);
     ctrl->children = (cfl_hbit_child_t*)p; p += sizeof(cfl_hbit_child_t) * nc;
     ctrl->leaf_nodes = (uint16_t*)p;
     
     ctrl->inst = inst;
     ctrl->root_node = root;
     ctrl->buffer_idx = buf;
     ctrl->child_count = nc;
     ctrl->leaf_count = nl;
     ctrl->bits_per_leaf = bpl;
     ctrl->total_bits = nl * bpl;
     ctrl->allocator = *alloc;
     ctrl->ram = ram;
     
     collect_leaves(cfg, root, ctrl->leaf_nodes, 0);
     
     uint16_t ci = 0, lo = 0, bo = 0;
     for (uint16_t i = 0; i < cfg->node_count && ci < nc; i++) {
         if (cfg->nodes[i].parent_index == (int16_t)root) {
             uint16_t cl = count_leaves(cfg, i);
             uint16_t cb = cl * bpl;
             ctrl->children[ci].node_idx = i;
             ctrl->children[ci].leaf_start = lo;
             ctrl->children[ci].leaf_count = cl;
             ctrl->children[ci].bit_start = bo;
             ctrl->children[ci].bit_count = cb;
             lo += cl; bo += cb; ci++;
         }
     }
     
     return ctrl;
 }
 
 void cfl_hbit_controller_destroy(cfl_hbit_controller_t* ctrl)
 {
     if (ctrl && ctrl->allocator.free && ctrl->ram)
         ctrl->allocator.free(ctrl->ram, ctrl->allocator.ctx);
 }
 
 int16_t cfl_hbit_controller_get_node_bit(
     cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t cbit, uint8_t* bit)
 {
     if (!ctrl || !bit || child >= ctrl->child_count) return -1;
     cfl_hbit_child_t* c = &ctrl->children[child];
     if (cbit >= c->bit_count) return -1;
     *bit = (uint8_t)(cbit % ctrl->bits_per_leaf);
     return (int16_t)ctrl->leaf_nodes[c->leaf_start + cbit / ctrl->bits_per_leaf];
 }
 
 int16_t cfl_hbit_controller_get_bitmap_node(
     cfl_hbit_controller_t* ctrl, uint16_t idx, uint8_t* bit)
 {
     if (!ctrl || !bit || idx >= ctrl->total_bits) return -1;
     *bit = (uint8_t)(idx % ctrl->bits_per_leaf);
     return (int16_t)ctrl->leaf_nodes[idx / ctrl->bits_per_leaf];
 }
 
 cfl_hbit_error_t cfl_hbit_controller_set_bit(cfl_hbit_controller_t* ctrl, uint16_t idx)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     uint8_t b; int16_t n = cfl_hbit_controller_get_bitmap_node(ctrl, idx, &b);
     if (n < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     return cfl_hbit_leaf_set_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_clear_bit(cfl_hbit_controller_t* ctrl, uint16_t idx)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     uint8_t b; int16_t n = cfl_hbit_controller_get_bitmap_node(ctrl, idx, &b);
     if (n < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     return cfl_hbit_leaf_clear_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 bool cfl_hbit_controller_read_bit(cfl_hbit_controller_t* ctrl, uint16_t idx)
 {
     if (!ctrl) return false;
     uint8_t b; int16_t n = cfl_hbit_controller_get_bitmap_node(ctrl, idx, &b);
     if (n < 0) return false;
     return cfl_hbit_read_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_set_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t cbit)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     uint8_t b; int16_t n = cfl_hbit_controller_get_node_bit(ctrl, child, cbit, &b);
     if (n < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     return cfl_hbit_leaf_set_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 cfl_hbit_error_t cfl_hbit_controller_clear_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t cbit)
 {
     if (!ctrl) return CFL_HBIT_ERR_NULL_PTR;
     uint8_t b; int16_t n = cfl_hbit_controller_get_node_bit(ctrl, child, cbit, &b);
     if (n < 0) return CFL_HBIT_ERR_OUT_OF_RANGE;
     return cfl_hbit_leaf_clear_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 bool cfl_hbit_controller_read_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t cbit)
 {
     if (!ctrl) return false;
     uint8_t b; int16_t n = cfl_hbit_controller_get_node_bit(ctrl, child, cbit, &b);
     if (n < 0) return false;
     return cfl_hbit_read_bit(ctrl->inst, ctrl->buffer_idx, (uint16_t)n, b);
 }
 
 void cfl_hbit_controller_clear_all(cfl_hbit_controller_t* ctrl)
 {
     if (!ctrl) return;
     for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
         cfl_hbit_leaf_clear(ctrl->inst, ctrl->buffer_idx, ctrl->leaf_nodes[i]);
     }
 }
 
 void cfl_hbit_controller_fill_all(cfl_hbit_controller_t* ctrl, uint8_t value)
 {
     if (!ctrl) return;
     for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
         cfl_hbit_leaf_fill(ctrl->inst, ctrl->buffer_idx, ctrl->leaf_nodes[i], value);
     }
 }