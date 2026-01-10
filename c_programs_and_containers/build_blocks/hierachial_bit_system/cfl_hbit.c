/**
 * @file cfl_hbit2.c
 * @brief ChainTree Hierarchical Bit Map - Clean API v2 Implementation
 *
 * Single-file implementation containing both the core engine and clean API.
 * Include cfl_hbit2_api.h for the public interface.
 */

 #include "cfl_hbit.h"
 #include "cfl_exception.h"
 #include <string.h>
 #include <stdlib.h>
 #include <stdio.h>
 
 /* ============================================ */
 /* Constants                                    */
 /* ============================================ */
 
 #define CFL_HBIT_MAGIC 0x54494248  /* "HBIT" */
 #define CFL_HBIT_FORMAT_VERSION 1
 
 /* ============================================ */
 /* Internal Status Codes                        */
 /* ============================================ */
 
 typedef enum {
     HBIT_OK = 0,
     HBIT_ERR_NULL_PTR,
     HBIT_ERR_NOT_INITIALIZED,
     HBIT_ERR_INVALID_DESCRIPTOR,
     HBIT_ERR_PATH_NOT_FOUND,
     HBIT_ERR_BITSPACE_INVALID,
     HBIT_ERR_BIT_OUT_OF_RANGE,
     HBIT_ERR_NO_MEMORY,
     HBIT_ERR_SOURCE_ACTIVE,
     HBIT_ERR_CALLBACK_FULL,
 } hbit_internal_status_t;
 
 /* Memory info struct */
 typedef struct {
     uint32_t descriptor_size;
     uint32_t arena_size;
     uint32_t leaf_masks_size;
     uint32_t total_ram;
     uint16_t node_count;
     uint16_t leaf_count;
     uint16_t bitspace_count;
     uint16_t max_depth;
 } hbit_mem_info_t;
 
 /* ============================================ */
 /* Validation Macros                            */
 /* ============================================ */
 
 #define CHECK_TREE(tree) \
     do { if (!(tree) || !(tree)->initialized) EXCEPTION("tree not initialized"); } while(0)
 
 #define CHECK_BITSPACE(tree, bs_id) \
     do { if ((bs_id) >= (tree)->header->bitspace_count) EXCEPTION("invalid bitspace id"); } while(0)
 
 #define CHECK_NODE_IDX(tree, idx) \
     do { if ((idx) < 0 || (idx) >= (tree)->header->node_count) EXCEPTION("invalid node index"); } while(0)
 
 #define CHECK_NOT_NULL(ptr, name) \
     do { if (!(ptr)) EXCEPTION("NULL " name); } while(0)
 
 /* ============================================ */
 /* FNV-1a Hash Implementation                   */
 /* ============================================ */
 
 #define FNV_PRIME  0x01000193U
 #define FNV_OFFSET 0x811c9dc5U
 
 static uint32_t hbit_hash_string(const char* str) {
     uint32_t hash = FNV_OFFSET;
     while (*str) {
         hash ^= (uint8_t)*str++;
         hash *= FNV_PRIME;
     }
     return hash;
 }
 
 static inline uint32_t hash_char(uint32_t hash, char c) {
     hash ^= (uint8_t)c;
     hash *= FNV_PRIME;
     return hash;
 }
 
 static uint32_t hash_int(uint32_t hash, int val) {
     char buf[16];
     int len = 0;
     int neg = 0;
     
     if (val < 0) { neg = 1; val = -val; }
     if (val == 0) { buf[len++] = '0'; }
     else { while (val > 0) { buf[len++] = '0' + (val % 10); val /= 10; } }
     if (neg) hash = hash_char(hash, '-');
     for (int i = len - 1; i >= 0; i--) hash = hash_char(hash, buf[i]);
     return hash;
 }
 
 static uint32_t hash_uint(uint32_t hash, unsigned int val) {
     char buf[16];
     int len = 0;
     if (val == 0) { buf[len++] = '0'; }
     else { while (val > 0) { buf[len++] = '0' + (val % 10); val /= 10; } }
     for (int i = len - 1; i >= 0; i--) hash = hash_char(hash, buf[i]);
     return hash;
 }
 
 static uint32_t hbit_hash_vprintf(const char* fmt, va_list args) {
     uint32_t hash = FNV_OFFSET;
     const char* p = fmt;
     
     while (*p) {
         if (*p == '%' && *(p+1)) {
             p++;
             switch (*p) {
                 case 'd': case 'i': hash = hash_int(hash, va_arg(args, int)); break;
                 case 'u': hash = hash_uint(hash, va_arg(args, unsigned int)); break;
                 case 's': {
                     const char* s = va_arg(args, const char*);
                     if (s) while (*s) hash = hash_char(hash, *s++);
                     break;
                 }
                 case 'c': hash = hash_char(hash, (char)va_arg(args, int)); break;
                 case '%': hash = hash_char(hash, '%'); break;
                 default: hash = hash_char(hash, '%'); hash = hash_char(hash, *p); break;
             }
             p++;
         } else {
             hash = hash_char(hash, *p++);
         }
     }
     return hash;
 }
 
 
 /* ============================================ */
 /* Internal Helper Functions                    */
 /* ============================================ */
 
 static const cfl_hbit_node_desc_t* get_node_desc(cfl_hbit_t* tree, int32_t node_idx) {
     return (const cfl_hbit_node_desc_t*)(tree->nodes_base + node_idx * sizeof(cfl_hbit_node_desc_t));
 }
 
 static bool is_leaf_node(cfl_hbit_t* tree, int32_t node_idx) {
     return get_node_desc(tree, node_idx)->child_count == 0;
 }
 
 static uint16_t get_bank_bits(cfl_hbit_t* tree, int32_t node_idx, uint16_t bs_id) {
     const cfl_hbit_node_desc_t* nd = get_node_desc(tree, node_idx);
     uint16_t class_size = 8 + tree->header->bitspace_count * 2;
     const uint8_t* cls = tree->classes_base + nd->class_index * class_size;
     const uint16_t* banks = (const uint16_t*)(cls + 8);
     return banks[bs_id];
 }
 
 static uint32_t get_node_offset(cfl_hbit_t* tree, uint16_t bitspace_id, int32_t node_index) {
     const uint8_t* arena_base = tree->arenas_base;
     for (uint16_t bs = 0; bs < bitspace_id; bs++) {
         const cfl_hbit_arena_desc_t* ad = (const cfl_hbit_arena_desc_t*)arena_base;
         arena_base += 8 + tree->header->node_count * 4;
         (void)ad;
     }
     const cfl_hbit_arena_desc_t* ad = (const cfl_hbit_arena_desc_t*)arena_base;
     const uint32_t* offsets = (const uint32_t*)(arena_base + 8);
     (void)ad;
     return offsets[node_index];
 }
 
 static int32_t find_leaf_index(cfl_hbit_t* tree, int32_t node_idx) {
     for (uint16_t i = 0; i < tree->leaf_count; i++) {
         if (tree->leaf_node_indices[i] == node_idx) return i;
     }
     return -1;
 }
 
 /* ============================================ */
 /* Descriptor Validation                        */
 /* ============================================ */
 
 static hbit_internal_status_t validate_descriptor(const uint8_t* desc, uint32_t size) {
     if (!desc || size < sizeof(cfl_hbit_header_t)) {
         EXCEPTION("descriptor is NULL or too small");
         return HBIT_ERR_INVALID_DESCRIPTOR;
     }
     const cfl_hbit_header_t* h = (const cfl_hbit_header_t*)desc;
     if (h->magic != CFL_HBIT_MAGIC) {
         EXCEPTION("invalid descriptor magic number (corrupted or wrong file)");
         return HBIT_ERR_INVALID_DESCRIPTOR;
     }
     if (h->version != CFL_HBIT_FORMAT_VERSION) {
         EXCEPTION("descriptor version mismatch");
         return HBIT_ERR_INVALID_DESCRIPTOR;
     }
     return HBIT_OK;
 }
 
 /* ============================================ */
 /* Memory Allocation                            */
 /* ============================================ */
 
 static void* hbit_alloc(cfl_hbit_t* tree, size_t size) {
     if (tree->alloc_fn) return tree->alloc_fn(tree->alloc_handle, size);
     return malloc(size);
 }
 
 static void hbit_free(cfl_hbit_t* tree, void* ptr) {
     if (!ptr) return;
     if (tree->free_fn) tree->free_fn(tree->alloc_handle, ptr);
     else free(ptr);
 }
 
 static hbit_internal_status_t allocate_arenas(cfl_hbit_t* tree) {
     uint32_t total_size = 0;
     uint32_t leaf_mask_bytes = 0;
     
     /* Count leaves and calculate mask size */
     tree->leaf_count = 0;
     for (uint16_t n = 0; n < tree->header->node_count; n++) {
         if (is_leaf_node(tree, n)) {
             tree->leaf_count++;
             for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
                 uint16_t bits = get_bank_bits(tree, n, bs);
                 leaf_mask_bytes += (bits + 7) / 8;
             }
         }
     }
     
     /* Calculate arena sizes */
     const uint8_t* arena_ptr = tree->arenas_base;
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         const cfl_hbit_arena_desc_t* ad = (const cfl_hbit_arena_desc_t*)arena_ptr;
         total_size += ad->size_with_latch * 2 + ad->size * 2;  /* shadow, current, latch, prev */
         arena_ptr += 8 + tree->header->node_count * 4;
     }
     
     /* Add leaf mask storage and offset arrays */
     total_size += leaf_mask_bytes * 2;  /* shadow + current masks */
     total_size += tree->leaf_count * sizeof(uint32_t);  /* mask offsets */
     total_size += tree->leaf_count * sizeof(uint16_t);  /* leaf node indices */
     total_size += tree->header->bitspace_count * sizeof(cfl_hbit_arena_t);
     total_size += CFL_HBIT_DEFAULT_CALLBACK_CAPACITY * sizeof(cfl_hbit_callback_t);
     
     tree->memory_block = hbit_alloc(tree, total_size);
     if (!tree->memory_block) {
         EXCEPTION("malloc failed to allocate memory");
         return HBIT_ERR_NO_MEMORY;
     }
     tree->memory_block_size = total_size;
     memset(tree->memory_block, 0, total_size);
     
     /* Partition memory */
     uint8_t* mem = (uint8_t*)tree->memory_block;
     
     tree->arenas = (cfl_hbit_arena_t*)mem;
     mem += tree->header->bitspace_count * sizeof(cfl_hbit_arena_t);
     
     tree->callbacks = (cfl_hbit_callback_t*)mem;
     mem += CFL_HBIT_DEFAULT_CALLBACK_CAPACITY * sizeof(cfl_hbit_callback_t);
     tree->callback_capacity = CFL_HBIT_DEFAULT_CALLBACK_CAPACITY;
     
     tree->leaf_mask_offsets = (uint32_t*)mem;
     mem += tree->leaf_count * sizeof(uint32_t);
     
     tree->leaf_node_indices = (uint16_t*)mem;
     mem += tree->leaf_count * sizeof(uint16_t);
     
     tree->leaf_shadow_masks = mem;
     mem += leaf_mask_bytes;
     
     tree->leaf_current_masks = mem;
     mem += leaf_mask_bytes;
     
     tree->leaf_mask_total_bytes = leaf_mask_bytes;
     
     /* Build leaf index mapping and mask offsets */
     uint32_t mask_off = 0;
     uint16_t leaf_idx = 0;
     for (uint16_t n = 0; n < tree->header->node_count; n++) {
         if (is_leaf_node(tree, n)) {
             tree->leaf_node_indices[leaf_idx] = n;
             tree->leaf_mask_offsets[leaf_idx] = mask_off;
             for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
                 uint16_t bits = get_bank_bits(tree, n, bs);
                 mask_off += (bits + 7) / 8;
             }
             leaf_idx++;
         }
     }
     
     /* Initialize masks to 0xFF (all propagate) */
     memset(tree->leaf_shadow_masks, 0xFF, leaf_mask_bytes);
     memset(tree->leaf_current_masks, 0xFF, leaf_mask_bytes);
     
     /* Setup arenas */
     arena_ptr = tree->arenas_base;
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         const cfl_hbit_arena_desc_t* ad = (const cfl_hbit_arena_desc_t*)arena_ptr;
         cfl_hbit_arena_t* ar = &tree->arenas[bs];
         
         ar->size = ad->size;
         ar->shadow = mem; mem += ad->size;
         ar->current = mem; mem += ad->size;
         
         if (ad->size_with_latch > ad->size) {
             ar->latch = mem; mem += ad->size;
             ar->live = mem; mem += ad->size;
         }
         
         ar->prev = mem; mem += ad->size;
         
         arena_ptr += 8 + tree->header->node_count * 4;
     }
     
     return HBIT_OK;
 }
 
 
 /* ============================================ */
 /* Initialization                               */
 /* ============================================ */
 
 static hbit_internal_status_t hbit_init_internal(cfl_hbit_t* tree, const uint8_t* desc, uint32_t desc_size) {
     hbit_internal_status_t s = validate_descriptor(desc, desc_size);
     if (s != HBIT_OK) return s;
     
     tree->descriptor = desc;
     tree->descriptor_size = desc_size;
     tree->header = (const cfl_hbit_header_t*)desc;
     tree->bitspaces = (const cfl_hbit_bitspace_desc_t*)(desc + tree->header->bitspaces_offset);
     tree->classes_base = desc + tree->header->classes_offset;
     tree->nodes_base = desc + tree->header->nodes_offset;
     tree->arenas_base = desc + tree->header->arenas_offset;
     tree->config = (const cfl_hbit_config_entry_t*)(desc + tree->header->config_offset);
     tree->strings = (const char*)(desc + tree->header->strings_offset);
     
     s = allocate_arenas(tree);
     if (s != HBIT_OK) return s;
     
     tree->initialized = 1;
     return HBIT_OK;
 }
 
 static hbit_internal_status_t hbit_init_from_file_internal(cfl_hbit_t* tree, const char* path) {
     FILE* f = fopen(path, "rb");
     if (!f) return HBIT_ERR_INVALID_DESCRIPTOR;
     
     fseek(f, 0, SEEK_END);
     long size = ftell(f);
     fseek(f, 0, SEEK_SET);
     
     uint8_t* data = (uint8_t*)hbit_alloc(tree, size);
     if (!data) { fclose(f); return HBIT_ERR_NO_MEMORY; }
     
     if (fread(data, 1, size, f) != (size_t)size) {
         hbit_free(tree, data);
         fclose(f);
         return HBIT_ERR_INVALID_DESCRIPTOR;
     }
     fclose(f);
     
     hbit_internal_status_t s = hbit_init_internal(tree, data, size);
     if (s != HBIT_OK) {
         hbit_free(tree, data);
         return s;
     }
     
     tree->owns_descriptor = 1;
     return HBIT_OK;
 }
 
 static void hbit_destroy_internal(cfl_hbit_t* tree) {
     if (!tree) return;
     if (tree->owns_descriptor && tree->descriptor) {
         hbit_free(tree, (void*)tree->descriptor);
     }
     if (tree->memory_block) {
         hbit_free(tree, tree->memory_block);
     }
     memset(tree, 0, sizeof(*tree));
 }
 
 static void hbit_reset_internal(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_arena_t* ar = &tree->arenas[bs];
         memset(ar->shadow, 0, ar->size);
         memset(ar->current, 0, ar->size);
         if (ar->latch) memset(ar->latch, 0, ar->size);
         if (ar->live) memset(ar->live, 0, ar->size);
         if (ar->prev) memset(ar->prev, 0, ar->size);
     }
     memset(tree->leaf_shadow_masks, 0xFF, tree->leaf_mask_total_bytes);
     memset(tree->leaf_current_masks, 0xFF, tree->leaf_mask_total_bytes);
     tree->dirty = 0;
 }
 
 /* ============================================ */
 /* Node Lookup                                  */
 /* ============================================ */
 
 static int32_t hbit_find_node_internal(cfl_hbit_t* tree, uint32_t path_hash) {
     CHECK_TREE(tree);
     for (int32_t i = 0; i < tree->header->node_count; i++) {
         const cfl_hbit_node_desc_t* nd = get_node_desc(tree, i);
         if (nd->path_hash == path_hash) return i;
     }
     return -1;
 }
 
 /* ============================================ */
 /* Bit Operations                               */
 /* ============================================ */
 
 static hbit_internal_status_t hbit_set_bit_n(cfl_hbit_t* tree, uint16_t bs_id, uint16_t bit, bool value, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("set_bit on non-leaf node");
     }
     
     uint16_t bits = get_bank_bits(tree, node_idx, bs_id);
     if (bit >= bits) return HBIT_ERR_BIT_OUT_OF_RANGE;
     
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     uint8_t* bank = tree->arenas[bs_id].shadow + offset;
     
     if (value) bank[bit / 8] |= (1 << (bit % 8));
     else bank[bit / 8] &= ~(1 << (bit % 8));
     
     tree->dirty = 1;
     return HBIT_OK;
 }
 
 static int hbit_get_bit_n(cfl_hbit_t* tree, uint16_t bs_id, uint16_t bit, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     uint16_t bits = get_bank_bits(tree, node_idx, bs_id);
     if (bit >= bits) return -1;
     
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     const uint8_t* bank = tree->arenas[bs_id].current + offset;
     
     return (bank[bit / 8] >> (bit % 8)) & 1;
 }
 
 static int hbit_get_bit_edge_n(cfl_hbit_t* tree, uint16_t bs_id, uint16_t bit, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     uint16_t bits = get_bank_bits(tree, node_idx, bs_id);
     if (bit >= bits) return 0;
     
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     cfl_hbit_arena_t* ar = &tree->arenas[bs_id];
     
     int curr = (ar->current[offset + bit/8] >> (bit % 8)) & 1;
     int prev = (ar->prev[offset + bit/8] >> (bit % 8)) & 1;
     
     if (curr && !prev) return 1;   /* Rising */
     if (!curr && prev) return -1;  /* Falling */
     return 0;
 }
 
 static const uint8_t* hbit_get_bits_n(cfl_hbit_t* tree, uint16_t bs_id, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     return tree->arenas[bs_id].current + offset;
 }
 
 
 /* ============================================ */
 /* Navigation                                   */
 /* ============================================ */
 
 static int32_t hbit_get_parent_n(cfl_hbit_t* tree, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_NODE_IDX(tree, node_idx);
     return get_node_desc(tree, node_idx)->parent_index;
 }
 
 static int hbit_get_child_count_n(cfl_hbit_t* tree, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_NODE_IDX(tree, node_idx);
     return get_node_desc(tree, node_idx)->child_count;
 }
 
 static int hbit_get_children_n(cfl_hbit_t* tree, int32_t node_idx, int32_t* children, int max) {
     CHECK_TREE(tree);
     CHECK_NODE_IDX(tree, node_idx);
     
     int count = 0;
     for (int32_t i = 0; i < tree->header->node_count && count < max; i++) {
         if (get_node_desc(tree, i)->parent_index == node_idx) {
             children[count++] = i;
         }
     }
     return count;
 }
 
 /* ============================================ */
 /* Latch Operations                             */
 /* ============================================ */
 
 static hbit_internal_status_t hbit_clear_latch_n(cfl_hbit_t* tree, uint16_t bs_id, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_latch on non-leaf node");
     }
     
     cfl_hbit_arena_t* ar = &tree->arenas[bs_id];
     if (!ar->latch) return HBIT_OK;
     
     const cfl_hbit_bitspace_desc_t* bs = &tree->bitspaces[bs_id];
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     uint16_t bytes = (get_bank_bits(tree, node_idx, bs_id) + 7) / 8;
     
     if (bs->clear_requires_inactive) {
         for (uint16_t i = 0; i < bytes; i++) {
             if (ar->live[offset + i] & ar->latch[offset + i]) {
                 return HBIT_ERR_SOURCE_ACTIVE;
             }
         }
     }
     
     memset(ar->latch + offset, 0, bytes);
     return HBIT_OK;
 }
 
 static hbit_internal_status_t hbit_clear_latch_bits_n(cfl_hbit_t* tree, uint16_t bs_id, uint32_t bits, int32_t node_idx) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bs_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_latch_bits on non-leaf node");
     }
     
     cfl_hbit_arena_t* ar = &tree->arenas[bs_id];
     if (!ar->latch) return HBIT_OK;
     
     const cfl_hbit_bitspace_desc_t* bs = &tree->bitspaces[bs_id];
     uint32_t offset = get_node_offset(tree, bs_id, node_idx);
     
     if (bs->clear_requires_inactive) {
         uint32_t live_bits = 0;
         for (int i = 0; i < 4; i++) live_bits |= ((uint32_t)ar->live[offset + i] << (i * 8));
         if (live_bits & bits) return HBIT_ERR_SOURCE_ACTIVE;
     }
     
     for (int i = 0; i < 4; i++) {
         ar->latch[offset + i] &= ~((bits >> (i * 8)) & 0xFF);
     }
     return HBIT_OK;
 }
 
 /* ============================================ */
 /* Sync and Propagate                           */
 /* ============================================ */
 
 static void hbit_swap_internal(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     
     /* Save current to prev for edge detection BEFORE swap */
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_arena_t* ar = &tree->arenas[bs];
         memcpy(ar->prev, ar->current, ar->size);
     }
     
     /* If not dirty, copy current to shadow so we don't lose state */
     if (!tree->dirty) {
         memcpy(tree->leaf_shadow_masks, tree->leaf_current_masks, tree->leaf_mask_total_bytes);
         for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
             cfl_hbit_arena_t* ar = &tree->arenas[bs];
             memcpy(ar->shadow, ar->current, ar->size);
         }
     }
     
     /* Swap shadow <-> current for masks */
     uint8_t* tmp = tree->leaf_shadow_masks;
     tree->leaf_shadow_masks = tree->leaf_current_masks;
     tree->leaf_current_masks = tmp;
     
     /* Swap shadow <-> current for arenas */
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_arena_t* ar = &tree->arenas[bs];
         tmp = ar->shadow;
         ar->shadow = ar->current;
         ar->current = tmp;
     }
     tree->dirty = 0;
 }
 
 static void apply_merge(uint8_t* dst, const uint8_t* src, uint8_t merge_type, uint16_t bytes) {
     switch (merge_type) {
         case 0: /* OR */
             for (uint16_t i = 0; i < bytes; i++) dst[i] |= src[i];
             break;
         case 1: /* AND */
             for (uint16_t i = 0; i < bytes; i++) dst[i] &= src[i];
             break;
         default: /* OR for others */
             for (uint16_t i = 0; i < bytes; i++) dst[i] |= src[i];
             break;
     }
 }
 
 static void hbit_propagate_internal(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_arena_t* ar = &tree->arenas[bs];
         const cfl_hbit_bitspace_desc_t* bsd = &tree->bitspaces[bs];
         
         /* Apply latching at leaf level before propagation */
         if (ar->latch) {
             for (uint16_t li = 0; li < tree->leaf_count; li++) {
                 int32_t node = tree->leaf_node_indices[li];
                 uint32_t offset = get_node_offset(tree, bs, node);
                 uint16_t bytes = (get_bank_bits(tree, node, bs) + 7) / 8;
                 
                 /* Save live state */
                 memcpy(ar->live + offset, ar->current + offset, bytes);
                 
                 /* Apply latch: current |= latch */
                 for (uint16_t i = 0; i < bytes; i++) {
                     ar->latch[offset + i] |= ar->current[offset + i];
                     ar->current[offset + i] = ar->latch[offset + i];
                 }
             }
         }
         
         /* Apply masks at leaf level */
         for (uint16_t li = 0; li < tree->leaf_count; li++) {
             int32_t node = tree->leaf_node_indices[li];
             uint32_t offset = get_node_offset(tree, bs, node);
             uint16_t bytes = (get_bank_bits(tree, node, bs) + 7) / 8;
             
             /* Calculate mask offset for this leaf/bitspace */
             uint32_t mask_off = tree->leaf_mask_offsets[li];
             for (uint16_t b = 0; b < bs; b++) {
                 mask_off += (get_bank_bits(tree, node, b) + 7) / 8;
             }
             mask_off = tree->leaf_mask_offsets[li];
             for (uint16_t b = 0; b < bs; b++) {
                 uint16_t bbits = get_bank_bits(tree, tree->leaf_node_indices[li], b);
                 mask_off += (bbits + 7) / 8;
             }
             
             /* Apply mask */
             for (uint16_t i = 0; i < bytes; i++) {
                 ar->current[offset + i] &= tree->leaf_current_masks[mask_off + i];
             }
         }
         
         /* Propagate from leaves up to root */
         for (int d = tree->header->max_depth - 1; d >= 0; d--) {
             for (int32_t n = 0; n < tree->header->node_count; n++) {
                 const cfl_hbit_node_desc_t* nd = get_node_desc(tree, n);
                 if (nd->depth != d || nd->child_count == 0) continue;
                 
                 uint32_t offset = get_node_offset(tree, bs, n);
                 uint16_t bytes = (get_bank_bits(tree, n, bs) + 7) / 8;
                 
                 /* Initialize based on merge type */
                 if (bsd->merge_type == 1) { /* AND */
                     memset(ar->current + offset, 0xFF, bytes);
                 } else {
                     memset(ar->current + offset, 0, bytes);
                 }
                 
                 /* Merge children */
                 for (int32_t c = 0; c < tree->header->node_count; c++) {
                     if (get_node_desc(tree, c)->parent_index == n) {
                         uint32_t c_off = get_node_offset(tree, bs, c);
                         apply_merge(ar->current + offset, ar->current + c_off, bsd->merge_type, bytes);
                     }
                 }
             }
         }
     }
 }
 
 static void hbit_sync_internal(cfl_hbit_t* tree) {
     hbit_swap_internal(tree);
     hbit_propagate_internal(tree);
 }
 
 /* ============================================ */
 /* Memory Info                                  */
 /* ============================================ */
 
 static void hbit_get_mem_info(cfl_hbit_t* tree, hbit_mem_info_t* info) {
     memset(info, 0, sizeof(*info));
     if (!tree || !tree->initialized) return;
     
     info->descriptor_size = tree->descriptor_size;
     info->node_count = tree->header->node_count;
     info->leaf_count = tree->leaf_count;
     info->bitspace_count = tree->header->bitspace_count;
     info->max_depth = tree->header->max_depth;
     
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         info->arena_size += tree->arenas[bs].size;
     }
     info->arena_size *= 4;  /* shadow, current, prev, latch/live */
     info->leaf_masks_size = tree->leaf_mask_total_bytes * 2;
     info->total_ram = tree->memory_block_size;
 }
 
 
 /* ============================================ */
 /* PUBLIC API IMPLEMENTATION                    */
 /* ============================================ */
 
 cfl_hbit2_status_t cfl_hbit2_init(cfl_hbit2_tree_t* tree, const uint8_t* desc, uint32_t desc_size) {
     if (!tree || !desc) return CFL_HBIT2_ERR_NULL;
     memset(tree, 0, sizeof(*tree));
     
     hbit_internal_status_t s = hbit_init_internal(&tree->impl, desc, desc_size);
     switch (s) {
         case HBIT_OK: return CFL_HBIT2_OK;
         case HBIT_ERR_NO_MEMORY: return CFL_HBIT2_ERR_NO_MEMORY;
         case HBIT_ERR_INVALID_DESCRIPTOR: return CFL_HBIT2_ERR_BAD_DESCRIPTOR;
         default: return CFL_HBIT2_ERR_NOT_INIT;
     }
 }
 
 cfl_hbit2_status_t cfl_hbit2_init_file(cfl_hbit2_tree_t* tree, const char* path) {
     if (!tree || !path) return CFL_HBIT2_ERR_NULL;
     memset(tree, 0, sizeof(*tree));
     
     hbit_internal_status_t s = hbit_init_from_file_internal(&tree->impl, path);
     switch (s) {
         case HBIT_OK: return CFL_HBIT2_OK;
         case HBIT_ERR_NO_MEMORY: return CFL_HBIT2_ERR_NO_MEMORY;
         case HBIT_ERR_INVALID_DESCRIPTOR: return CFL_HBIT2_ERR_BAD_DESCRIPTOR;
         default: return CFL_HBIT2_ERR_NOT_INIT;
     }
 }
 
 void cfl_hbit2_destroy(cfl_hbit2_tree_t* tree) {
     if (tree) hbit_destroy_internal(&tree->impl);
 }
 
 void cfl_hbit2_reset(cfl_hbit2_tree_t* tree) {
     if (tree) hbit_reset_internal(&tree->impl);
 }
 
 void cfl_hbit2_mem(cfl_hbit2_tree_t* tree, cfl_hbit2_mem_t* info) {
     if (!info) return;
     memset(info, 0, sizeof(*info));
     if (!tree || !tree->impl.initialized) return;
     
     hbit_mem_info_t m;
     hbit_get_mem_info(&tree->impl, &m);
     
     info->descriptor_size = m.descriptor_size;
     info->arena_size = m.arena_size;
     info->mask_size = m.leaf_masks_size;
     info->total_ram = m.total_ram;
     info->node_count = m.node_count;
     info->leaf_count = m.leaf_count;
     info->bitspace_count = tree->impl.header->bitspace_count;
 }
 
 int32_t cfl_hbit2_node(cfl_hbit2_tree_t* tree, const char* path_fmt, ...) {
     if (!tree || !tree->impl.initialized || !path_fmt) return -1;
     
     va_list args;
     va_start(args, path_fmt);
     uint32_t hash = hbit_hash_vprintf(path_fmt, args);
     va_end(args);
     
     return hbit_find_node_internal(&tree->impl, hash);
 }
 
 int16_t cfl_hbit2_bitspace(cfl_hbit2_tree_t* tree, const char* name) {
     if (!tree || !tree->impl.initialized || !name) return -1;
     
     uint32_t hash = hbit_hash_string(name);
     for (uint16_t i = 0; i < tree->impl.header->bitspace_count; i++) {
         if (tree->impl.bitspaces[i].name_hash == hash) return (int16_t)i;
     }
     return -1;
 }
 
 bool cfl_hbit2_info_is_leaf(cfl_hbit2_tree_t* tree, int32_t node) {
     if (!tree || !tree->impl.initialized) return false;
     if (node < 0 || node >= tree->impl.header->node_count) return false;
     return is_leaf_node(&tree->impl, node);
 }
 
 int cfl_hbit2_info_bits(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree || !tree->impl.initialized) return 0;
     if (node < 0 || node >= tree->impl.header->node_count) return 0;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return 0;
     return get_bank_bits(&tree->impl, node, bs_id);
 }
 
 int cfl_hbit2_info_bytes(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     return (cfl_hbit2_info_bits(tree, node, bs_id) + 7) / 8;
 }
 
 int cfl_hbit2_info_node_count(cfl_hbit2_tree_t* tree) {
     if (!tree || !tree->impl.initialized) return 0;
     return tree->impl.header->node_count;
 }
 
 int cfl_hbit2_info_bitspace_count(cfl_hbit2_tree_t* tree) {
     if (!tree || !tree->impl.initialized) return 0;
     return tree->impl.header->bitspace_count;
 }
 
 int32_t cfl_hbit2_nav_parent(cfl_hbit2_tree_t* tree, int32_t node) {
     if (!tree) return -1;
     return hbit_get_parent_n(&tree->impl, node);
 }
 
 int cfl_hbit2_nav_children(cfl_hbit2_tree_t* tree, int32_t node, int32_t* out, int max) {
     if (!tree) return 0;
     return hbit_get_children_n(&tree->impl, node, out, max);
 }
 
 int cfl_hbit2_nav_child_count(cfl_hbit2_tree_t* tree, int32_t node) {
     if (!tree) return 0;
     return hbit_get_child_count_n(&tree->impl, node);
 }
 
 cfl_hbit2_status_t cfl_hbit2_bit_set(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, int bit, bool value) {
     if (!tree) return CFL_HBIT2_ERR_NULL;
     hbit_internal_status_t s = hbit_set_bit_n(&tree->impl, bs_id, bit, value, node);
     return (s == HBIT_OK) ? CFL_HBIT2_OK : CFL_HBIT2_ERR_BAD_NODE;
 }
 
 int cfl_hbit2_bit_get(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, int bit) {
     if (!tree) return -1;
     return hbit_get_bit_n(&tree->impl, bs_id, bit, node);
 }
 
 int cfl_hbit2_bit_edge(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, int bit) {
     if (!tree) return 0;
     return hbit_get_bit_edge_n(&tree->impl, bs_id, bit, node);
 }
 
 cfl_hbit2_status_t cfl_hbit2_bank_set(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, const uint8_t* data, int len) {
     if (!tree || !data) return CFL_HBIT2_ERR_NULL;
     if (!tree->impl.initialized) return CFL_HBIT2_ERR_NOT_INIT;
     if (node < 0 || node >= tree->impl.header->node_count) return CFL_HBIT2_ERR_BAD_NODE;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return CFL_HBIT2_ERR_BAD_BITSPACE;
     if (!is_leaf_node(&tree->impl, node)) return CFL_HBIT2_ERR_NOT_LEAF;
     
     int expected = cfl_hbit2_info_bytes(tree, node, bs_id);
     if (len != expected) return CFL_HBIT2_ERR_SIZE_MISMATCH;
     
     int bits = cfl_hbit2_info_bits(tree, node, bs_id);
     for (int i = 0; i < bits; i++) {
         bool val = (data[i / 8] >> (i % 8)) & 1;
         hbit_set_bit_n(&tree->impl, bs_id, i, val, node);
     }
     return CFL_HBIT2_OK;
 }
 
 const uint8_t* cfl_hbit2_bank_get(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree) return NULL;
     return hbit_get_bits_n(&tree->impl, bs_id, node);
 }
 
 cfl_hbit2_status_t cfl_hbit2_bank_clear(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree) return CFL_HBIT2_ERR_NULL;
     if (!tree->impl.initialized) return CFL_HBIT2_ERR_NOT_INIT;
     if (node < 0 || node >= tree->impl.header->node_count) return CFL_HBIT2_ERR_BAD_NODE;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return CFL_HBIT2_ERR_BAD_BITSPACE;
     if (!is_leaf_node(&tree->impl, node)) return CFL_HBIT2_ERR_NOT_LEAF;
     
     int bits = cfl_hbit2_info_bits(tree, node, bs_id);
     for (int i = 0; i < bits; i++) {
         hbit_set_bit_n(&tree->impl, bs_id, i, false, node);
     }
     return CFL_HBIT2_OK;
 }
 
 
 /* Mask operations */
 static int32_t get_leaf_idx(cfl_hbit2_tree_t* tree, int32_t node) {
     return find_leaf_index(&tree->impl, node);
 }
 
 static uint32_t get_mask_offset(cfl_hbit2_tree_t* tree, int32_t node, int32_t leaf_idx, int16_t bs_id) {
     uint32_t off = tree->impl.leaf_mask_offsets[leaf_idx];
     for (int16_t bs = 0; bs < bs_id; bs++) {
         off += cfl_hbit2_info_bytes(tree, node, bs);
     }
     return off;
 }
 
 cfl_hbit2_status_t cfl_hbit2_mask_set(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, const uint8_t* mask, int len) {
     if (!tree || !mask) return CFL_HBIT2_ERR_NULL;
     if (!tree->impl.initialized) return CFL_HBIT2_ERR_NOT_INIT;
     if (node < 0 || node >= tree->impl.header->node_count) return CFL_HBIT2_ERR_BAD_NODE;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return CFL_HBIT2_ERR_BAD_BITSPACE;
     if (!is_leaf_node(&tree->impl, node)) return CFL_HBIT2_ERR_NOT_LEAF;
     
     int expected = cfl_hbit2_info_bytes(tree, node, bs_id);
     if (len != expected) return CFL_HBIT2_ERR_SIZE_MISMATCH;
     
     int32_t lidx = get_leaf_idx(tree, node);
     uint32_t off = get_mask_offset(tree, node, lidx, bs_id);
     memcpy(tree->impl.leaf_shadow_masks + off, mask, len);
     tree->impl.dirty = 1;
     return CFL_HBIT2_OK;
 }
 
 cfl_hbit2_status_t cfl_hbit2_mask_clear(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree) return CFL_HBIT2_ERR_NULL;
     if (!tree->impl.initialized) return CFL_HBIT2_ERR_NOT_INIT;
     if (node < 0 || node >= tree->impl.header->node_count) return CFL_HBIT2_ERR_BAD_NODE;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return CFL_HBIT2_ERR_BAD_BITSPACE;
     if (!is_leaf_node(&tree->impl, node)) return CFL_HBIT2_ERR_NOT_LEAF;
     
     int bytes = cfl_hbit2_info_bytes(tree, node, bs_id);
     int32_t lidx = get_leaf_idx(tree, node);
     uint32_t off = get_mask_offset(tree, node, lidx, bs_id);
     memset(tree->impl.leaf_shadow_masks + off, 0xFF, bytes);
     return CFL_HBIT2_OK;
 }
 
 const uint8_t* cfl_hbit2_mask_get(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree || !tree->impl.initialized) return NULL;
     if (node < 0 || node >= tree->impl.header->node_count) return NULL;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return NULL;
     if (!is_leaf_node(&tree->impl, node)) return NULL;
     
     int32_t lidx = get_leaf_idx(tree, node);
     if (lidx < 0) return NULL;
     uint32_t off = get_mask_offset(tree, node, lidx, bs_id);
     return tree->impl.leaf_current_masks + off;
 }
 
 /* Latch operations */
 cfl_hbit2_status_t cfl_hbit2_latch_clear(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree) return CFL_HBIT2_ERR_NULL;
     hbit_internal_status_t s = hbit_clear_latch_n(&tree->impl, bs_id, node);
     switch (s) {
         case HBIT_OK: return CFL_HBIT2_OK;
         case HBIT_ERR_SOURCE_ACTIVE: return CFL_HBIT2_ERR_SOURCE_ACTIVE;
         default: return CFL_HBIT2_ERR_BAD_NODE;
     }
 }
 
 cfl_hbit2_status_t cfl_hbit2_latch_clear_bits(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id, uint32_t bits) {
     if (!tree) return CFL_HBIT2_ERR_NULL;
     hbit_internal_status_t s = hbit_clear_latch_bits_n(&tree->impl, bs_id, bits, node);
     switch (s) {
         case HBIT_OK: return CFL_HBIT2_OK;
         case HBIT_ERR_SOURCE_ACTIVE: return CFL_HBIT2_ERR_SOURCE_ACTIVE;
         default: return CFL_HBIT2_ERR_BAD_NODE;
     }
 }
 
 void cfl_hbit2_latch_clear_all(cfl_hbit2_tree_t* tree, int16_t bs_id) {
     if (!tree || !tree->impl.initialized) return;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return;
     
     cfl_hbit_arena_t* arena = &tree->impl.arenas[bs_id];
     if (arena->latch) {
         memset(arena->latch, 0, arena->size);
     }
 }
 
 const uint8_t* cfl_hbit2_latch_get(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id) {
     if (!tree || !tree->impl.initialized) return NULL;
     if (node < 0 || node >= tree->impl.header->node_count) return NULL;
     if (bs_id < 0 || bs_id >= tree->impl.header->bitspace_count) return NULL;
     if (!is_leaf_node(&tree->impl, node)) return NULL;
     
     cfl_hbit_arena_t* arena = &tree->impl.arenas[bs_id];
     if (!arena->latch) return NULL;
     
     uint32_t offset = get_node_offset(&tree->impl, bs_id, node);
     return arena->latch + offset;
 }
 
 /* Sync operations */
 void cfl_hbit2_sync(cfl_hbit2_tree_t* tree) {
     if (tree) hbit_sync_internal(&tree->impl);
 }
 
 void cfl_hbit2_swap(cfl_hbit2_tree_t* tree) {
     if (tree) hbit_swap_internal(&tree->impl);
 }
 
 void cfl_hbit2_propagate(cfl_hbit2_tree_t* tree) {
     if (tree) hbit_propagate_internal(&tree->impl);
 }