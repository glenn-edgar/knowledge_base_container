/**
 * @file cfl_hbit.c
 * @brief ChainTree Hierarchical Bit Map Runtime Implementation
 */

 #include "cfl_hbit.h"
 #include <string.h>
 #include <stdlib.h>
 #include <stdio.h>
 
 /* ============================================ */
 /* Constants                                    */
 /* ============================================ */
 
 #define CFL_HBIT_MAGIC 0x54494248  /* "HBIT" */
 #define CFL_HBIT_FORMAT_VERSION 1
 
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
 
 uint32_t cfl_hbit_hash_string(const char* str) {
     uint32_t hash = FNV_OFFSET;
     while (*str) {
         hash ^= (uint8_t)*str++;
         hash *= FNV_PRIME;
     }
     return hash;
 }
 
 /* Hash a single character into running hash */
 static inline uint32_t hash_char(uint32_t hash, char c) {
     hash ^= (uint8_t)c;
     hash *= FNV_PRIME;
     return hash;
 }
 
 /* Hash an integer (for %d format) */
 static uint32_t hash_int(uint32_t hash, int val) {
     char buf[16];
     int len = 0;
     int neg = 0;
     
     if (val < 0) {
         neg = 1;
         val = -val;
     }
     
     if (val == 0) {
         buf[len++] = '0';
     } else {
         while (val > 0) {
             buf[len++] = '0' + (val % 10);
             val /= 10;
         }
     }
     
     if (neg) {
         hash = hash_char(hash, '-');
     }
     
     /* Reverse the digits */
     for (int i = len - 1; i >= 0; i--) {
         hash = hash_char(hash, buf[i]);
     }
     
     return hash;
 }
 
 /* Hash an unsigned integer */
 static uint32_t hash_uint(uint32_t hash, unsigned int val) {
     char buf[16];
     int len = 0;
     
     if (val == 0) {
         buf[len++] = '0';
     } else {
         while (val > 0) {
             buf[len++] = '0' + (val % 10);
             val /= 10;
         }
     }
     
     for (int i = len - 1; i >= 0; i--) {
         hash = hash_char(hash, buf[i]);
     }
     
     return hash;
 }
 
 /* Hash a string */
 static uint32_t hash_str(uint32_t hash, const char* str) {
     while (*str) {
         hash = hash_char(hash, *str++);
     }
     return hash;
 }
 
 uint32_t cfl_hbit_hash_vprintf(const char* fmt, va_list args) {
     uint32_t hash = FNV_OFFSET;
     
     while (*fmt) {
         if (*fmt == '%') {
             fmt++;
             switch (*fmt) {
                 case 'd':
                 case 'i':
                     hash = hash_int(hash, va_arg(args, int));
                     break;
                 case 'u':
                     hash = hash_uint(hash, va_arg(args, unsigned int));
                     break;
                 case 's':
                     hash = hash_str(hash, va_arg(args, const char*));
                     break;
                 case 'c':
                     hash = hash_char(hash, (char)va_arg(args, int));
                     break;
                 case '%':
                     hash = hash_char(hash, '%');
                     break;
                 case '\0':
                     return hash;
                 default:
                     /* Unknown format, just hash it literally */
                     hash = hash_char(hash, '%');
                     hash = hash_char(hash, *fmt);
                     break;
             }
         } else {
             hash = hash_char(hash, *fmt);
         }
         fmt++;
     }
     
     return hash;
 }
 
 uint32_t cfl_hbit_hash_printf(const char* fmt, ...) {
     va_list args;
     va_start(args, fmt);
     uint32_t hash = cfl_hbit_hash_vprintf(fmt, args);
     va_end(args);
     return hash;
 }
 
 /* ============================================ */
 /* Binary Search                                */
 /* ============================================ */
 
 /* Binary search for node by hash */
 static int32_t find_node_by_hash(cfl_hbit_t* tree, uint32_t hash) {
     if (!tree || !tree->header) return -1;
     
     uint16_t node_count = tree->header->node_count;
     
     /* Node descriptors are now fixed size (no masks in binary) */
     size_t node_desc_size = sizeof(cfl_hbit_node_desc_t);
     
     int32_t lo = 0;
     int32_t hi = node_count - 1;
     
     while (lo <= hi) {
         int32_t mid = (lo + hi) / 2;
         const cfl_hbit_node_desc_t* node = 
             (const cfl_hbit_node_desc_t*)(tree->nodes_base + mid * node_desc_size);
         
         if (node->path_hash == hash) {
             return mid;
         } else if (node->path_hash < hash) {
             lo = mid + 1;
         } else {
             hi = mid - 1;
         }
     }
     
     return -1;
 }
 
 /* Get node descriptor by index */
 static const cfl_hbit_node_desc_t* get_node_desc(cfl_hbit_t* tree, int32_t index) {
     if (!tree || !tree->header || index < 0 || index >= tree->header->node_count) {
         return NULL;
     }
     
     /* Node descriptors are now fixed size (no masks in binary) */
     size_t node_desc_size = sizeof(cfl_hbit_node_desc_t);
     
     return (const cfl_hbit_node_desc_t*)(tree->nodes_base + index * node_desc_size);
 }
 
 /* Forward declaration */
 static uint16_t get_bank_size_bits(cfl_hbit_t* tree, uint16_t class_index, uint16_t bitspace_id);
 
 /* Check if node is a leaf (no children) */
 static bool is_leaf_node(cfl_hbit_t* tree, int32_t node_index) {
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_index);
     return node && node->child_count == 0;
 }
 
 /* Find leaf index for a node (-1 if not a leaf) */
 static int32_t find_leaf_index(cfl_hbit_t* tree, int32_t node_index) {
     if (!tree->leaf_node_indices) return -1;
     
     for (uint16_t i = 0; i < tree->leaf_count; i++) {
         if (tree->leaf_node_indices[i] == node_index) {
             return i;
         }
     }
     return -1;
 }
 
 /* Get mask byte size for a leaf node's bitspace */
 static uint16_t get_leaf_mask_bytes(cfl_hbit_t* tree, int32_t leaf_index, uint16_t bitspace_id) {
     if (leaf_index < 0 || leaf_index >= tree->leaf_count) return 0;
     
     uint16_t class_idx = get_node_desc(tree, tree->leaf_node_indices[leaf_index])->class_index;
     uint16_t bank_bits = get_bank_size_bits(tree, class_idx, bitspace_id);
     return (bank_bits + 7) / 8;
 }
 
 /* Get arena descriptor by bitspace index */
 static const cfl_hbit_arena_desc_t* get_arena_desc(cfl_hbit_t* tree, uint16_t bitspace_id) {
     if (!tree || !tree->header || bitspace_id >= tree->header->bitspace_count) {
         return NULL;
     }
     
     uint16_t node_count = tree->header->node_count;
     size_t arena_desc_size = sizeof(cfl_hbit_arena_desc_t) + (node_count * sizeof(uint32_t));
     
     return (const cfl_hbit_arena_desc_t*)(tree->arenas_base + bitspace_id * arena_desc_size);
 }
 
 /* Get node offset within arena */
 static uint32_t get_node_offset(cfl_hbit_t* tree, uint16_t bitspace_id, int32_t node_index) {
     const cfl_hbit_arena_desc_t* arena = get_arena_desc(tree, bitspace_id);
     if (!arena || node_index < 0) return 0;
     
     const uint32_t* offsets = (const uint32_t*)(((const uint8_t*)arena) + sizeof(cfl_hbit_arena_desc_t));
     return offsets[node_index];
 }
 
 /* Get class descriptor */
 static const uint8_t* get_class_desc(cfl_hbit_t* tree, uint16_t class_index) {
     if (!tree || !tree->header || class_index >= tree->header->class_count) {
         return NULL;
     }
     
     uint16_t bitspace_count = tree->header->bitspace_count;
     /* Class size = name_hash(4) + bank_count(2) + reserved(2) + bank_sizes(2*bs) */
     /* Note: default_masks removed - masks are now runtime-only for leaf nodes */
     size_t class_size = 8 + (bitspace_count * 2);
     
     return tree->classes_base + class_index * class_size;
 }
 
 /* Get bank size for a class/bitspace */
 static uint16_t get_bank_size_bits(cfl_hbit_t* tree, uint16_t class_index, uint16_t bitspace_id) {
     const uint8_t* class_desc = get_class_desc(tree, class_index);
     if (!class_desc) return 0;
     
     /* Bank sizes start at offset 8 */
     const uint16_t* bank_sizes = (const uint16_t*)(class_desc + 8);
     return bank_sizes[bitspace_id];
 }
 
 /* ============================================ */
 /* Initialization                               */
 /* ============================================ */
 
 static cfl_hbit_status_t validate_descriptor(const uint8_t* desc, uint32_t desc_size) {
     if (!desc || desc_size < sizeof(cfl_hbit_header_t)) {
         EXCEPTION("descriptor is NULL or too small");
     }
     
     const cfl_hbit_header_t* header = (const cfl_hbit_header_t*)desc;
     
     if (header->magic != CFL_HBIT_MAGIC) {
         EXCEPTION("invalid descriptor magic number (corrupted or wrong file)");
     }
     
     if (header->version != CFL_HBIT_FORMAT_VERSION) {
         EXCEPTION("descriptor version mismatch");
     }
     
     return CFL_HBIT_OK;
 }
 
 static uint32_t calculate_required_memory(cfl_hbit_t* tree) {
     uint32_t total = 0;
     
     uint16_t bitspace_count = tree->header->bitspace_count;
     uint16_t node_count = tree->header->node_count;
     
     /* Count leaf nodes and calculate mask memory */
     uint16_t leaf_count = 0;
     uint32_t leaf_mask_bytes = 0;
     
     for (int32_t n = 0; n < node_count; n++) {
         const cfl_hbit_node_desc_t* node = get_node_desc(tree, n);
         if (node && node->child_count == 0) {
             leaf_count++;
             /* Sum up mask bytes for all bitspaces */
             for (uint16_t bs = 0; bs < bitspace_count; bs++) {
                 uint16_t bank_bits = get_bank_size_bits(tree, node->class_index, bs);
                 leaf_mask_bytes += (bank_bits + 7) / 8;
             }
         }
     }
     
     /* Arena structs */
     total += bitspace_count * sizeof(cfl_hbit_arena_t);
     
     /* Callback array */
     total += CFL_HBIT_DEFAULT_CALLBACK_CAPACITY * sizeof(cfl_hbit_callback_t);
     
     for (uint16_t bs = 0; bs < bitspace_count; bs++) {
         const cfl_hbit_arena_desc_t* arena_desc = get_arena_desc(tree, bs);
         const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bs];
         
         /* shadow + current */
         total += arena_desc->size * 2;
         
         /* prev (for change detection) */
         total += arena_desc->size;
         
         /* latch + live if latching enabled */
         if (bs_desc->latch) {
             total += arena_desc->size * 2;
         }
     }
     
     /* Leaf mask management arrays */
     total += leaf_count * sizeof(uint32_t);   /* leaf_mask_offsets */
     total += leaf_count * sizeof(uint16_t);   /* leaf_node_indices */
     
     /* Leaf masks: shadow + current */
     total += leaf_mask_bytes * 2;
     
     return total;
 }
 
 static cfl_hbit_status_t allocate_arenas(cfl_hbit_t* tree, uint8_t* pool, uint32_t pool_size) {
     uint32_t required = calculate_required_memory(tree);
     
     uint8_t* mem;
     if (pool) {
         /* Static allocation from provided pool */
         if (pool_size < required) {
             EXCEPTION("provided memory pool too small");
         }
         mem = pool;
         tree->memory_block = NULL;
     } else if (tree->alloc_fn) {
         /* Custom allocator */
         mem = (uint8_t*)tree->alloc_fn(tree->alloc_handle, required);
         if (!mem) {
             EXCEPTION("custom allocator failed to allocate memory");
         }
         tree->memory_block = mem;
         tree->memory_block_size = required;
     } else {
         /* Stdlib malloc */
         mem = (uint8_t*)malloc(required);
         if (!mem) {
             EXCEPTION("malloc failed to allocate memory");
         }
         tree->memory_block = mem;
         tree->memory_block_size = required;
     }
     
     memset(mem, 0, required);
     
     uint16_t bitspace_count = tree->header->bitspace_count;
     uint16_t node_count = tree->header->node_count;
     uint32_t offset = 0;
     
     /* Allocate arena structs */
     tree->arenas = (cfl_hbit_arena_t*)(mem + offset);
     offset += bitspace_count * sizeof(cfl_hbit_arena_t);
     
     /* Allocate callback array */
     tree->callbacks = (cfl_hbit_callback_t*)(mem + offset);
     tree->callback_capacity = CFL_HBIT_DEFAULT_CALLBACK_CAPACITY;
     tree->callback_count = 0;
     offset += CFL_HBIT_DEFAULT_CALLBACK_CAPACITY * sizeof(cfl_hbit_callback_t);
     
     /* Allocate arena buffers */
     for (uint16_t bs = 0; bs < bitspace_count; bs++) {
         const cfl_hbit_arena_desc_t* arena_desc = get_arena_desc(tree, bs);
         const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bs];
         uint32_t size = arena_desc->size;
         
         tree->arenas[bs].size = size;
         tree->arenas[bs].shadow = mem + offset;
         offset += size;
         
         tree->arenas[bs].current = mem + offset;
         offset += size;
         
         tree->arenas[bs].prev = mem + offset;
         offset += size;
         
         if (bs_desc->latch) {
             tree->arenas[bs].latch = mem + offset;
             offset += size;
             
             tree->arenas[bs].live = mem + offset;
             offset += size;
         } else {
             tree->arenas[bs].latch = NULL;
             tree->arenas[bs].live = NULL;
         }
     }
     
     /* Count leaf nodes */
     uint16_t leaf_count = 0;
     for (int32_t n = 0; n < node_count; n++) {
         const cfl_hbit_node_desc_t* node = get_node_desc(tree, n);
         if (node && node->child_count == 0) {
             leaf_count++;
         }
     }
     tree->leaf_count = leaf_count;
     
     /* Allocate leaf mask management arrays */
     tree->leaf_mask_offsets = (uint32_t*)(mem + offset);
     offset += leaf_count * sizeof(uint32_t);
     
     tree->leaf_node_indices = (uint16_t*)(mem + offset);
     offset += leaf_count * sizeof(uint16_t);
     
     /* Calculate total mask bytes and assign offsets */
     uint32_t leaf_mask_total = 0;
     uint16_t leaf_idx = 0;
     for (int32_t n = 0; n < node_count; n++) {
         const cfl_hbit_node_desc_t* node = get_node_desc(tree, n);
         if (node && node->child_count == 0) {
             tree->leaf_node_indices[leaf_idx] = n;
             tree->leaf_mask_offsets[leaf_idx] = leaf_mask_total;
             
             /* Sum mask bytes for all bitspaces */
             for (uint16_t bs = 0; bs < bitspace_count; bs++) {
                 uint16_t bank_bits = get_bank_size_bits(tree, node->class_index, bs);
                 leaf_mask_total += (bank_bits + 7) / 8;
             }
             leaf_idx++;
         }
     }
     tree->leaf_mask_total_bytes = leaf_mask_total;
     
     /* Allocate shadow and current mask buffers */
     tree->leaf_shadow_masks = mem + offset;
     offset += leaf_mask_total;
     
     tree->leaf_current_masks = mem + offset;
     offset += leaf_mask_total;
     
     /* Initialize all masks to 0xFF (all bits propagate) */
     memset(tree->leaf_shadow_masks, 0xFF, leaf_mask_total);
     memset(tree->leaf_current_masks, 0xFF, leaf_mask_total);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_init(
     cfl_hbit_t* tree,
     const uint8_t* desc,
     uint32_t desc_size,
     const cfl_hbit_allocator_t* allocator
 ) {
     CHECK_NOT_NULL(tree, "tree");
     
     memset(tree, 0, sizeof(cfl_hbit_t));
     
     /* Store allocator as flat fields */
     if (allocator) {
         tree->alloc_handle = allocator->handle;
         tree->alloc_fn = allocator->alloc;
         tree->free_fn = allocator->free;
     }
     
     cfl_hbit_status_t status = validate_descriptor(desc, desc_size);
     if (status != CFL_HBIT_OK) return status;
     
     tree->descriptor = desc;
     tree->descriptor_size = desc_size;
     
     /* Parse header */
     tree->header = (const cfl_hbit_header_t*)desc;
     
     /* Set up section pointers */
     tree->bitspaces = (const cfl_hbit_bitspace_desc_t*)(desc + tree->header->bitspaces_offset);
     tree->classes_base = desc + tree->header->classes_offset;
     tree->nodes_base = desc + tree->header->nodes_offset;
     tree->arenas_base = desc + tree->header->arenas_offset;
     tree->config = (const cfl_hbit_config_entry_t*)(desc + tree->header->config_offset);
     tree->strings = (const char*)(desc + tree->header->strings_offset);
     
     /* Allocate runtime memory */
     status = allocate_arenas(tree, NULL, 0);
     if (status != CFL_HBIT_OK) return status;
     
     tree->initialized = 1;
     tree->dirty = 0;
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_init_static(
     cfl_hbit_t* tree,
     const uint8_t* desc,
     uint32_t desc_size,
     uint8_t* pool,
     uint32_t pool_size
 ) {
     CHECK_NOT_NULL(tree, "tree");
     
     memset(tree, 0, sizeof(cfl_hbit_t));
     
     /* No allocator for static init */
     tree->alloc_handle = NULL;
     tree->alloc_fn = NULL;
     tree->free_fn = NULL;
     
     cfl_hbit_status_t status = validate_descriptor(desc, desc_size);
     if (status != CFL_HBIT_OK) return status;
     
     tree->descriptor = desc;
     tree->descriptor_size = desc_size;
     
     /* Parse header */
     tree->header = (const cfl_hbit_header_t*)desc;
     
     /* Set up section pointers */
     tree->bitspaces = (const cfl_hbit_bitspace_desc_t*)(desc + tree->header->bitspaces_offset);
     tree->classes_base = desc + tree->header->classes_offset;
     tree->nodes_base = desc + tree->header->nodes_offset;
     tree->arenas_base = desc + tree->header->arenas_offset;
     tree->config = (const cfl_hbit_config_entry_t*)(desc + tree->header->config_offset);
     tree->strings = (const char*)(desc + tree->header->strings_offset);
     
     /* Allocate runtime memory from pool */
     status = allocate_arenas(tree, pool, pool_size);
     if (status != CFL_HBIT_OK) return status;
     
     tree->initialized = 1;
     tree->dirty = 0;
     
     return CFL_HBIT_OK;
 }
 
 uint32_t cfl_hbit_required_memory(
     const uint8_t* desc,
     uint32_t desc_size
 ) {
     if (!desc || desc_size < sizeof(cfl_hbit_header_t)) {
         return 0;
     }
     
     cfl_hbit_t temp;
     memset(&temp, 0, sizeof(temp));
     
     if (validate_descriptor(desc, desc_size) != CFL_HBIT_OK) {
         return 0;
     }
     
     temp.header = (const cfl_hbit_header_t*)desc;
     temp.bitspaces = (const cfl_hbit_bitspace_desc_t*)(desc + temp.header->bitspaces_offset);
     temp.arenas_base = desc + temp.header->arenas_offset;
     
     return calculate_required_memory(&temp);
 }
 
 cfl_hbit_status_t cfl_hbit_load_descriptor(
     cfl_hbit_t* tree,
     const char* path,
     uint8_t** out_data,
     uint32_t* out_size
 ) {
     if (!path || !out_data || !out_size) {
         EXCEPTION("NULL pointer passed to load_descriptor");
     }
     
     *out_data = NULL;
     *out_size = 0;
     
     FILE* f = fopen(path, "rb");
     if (!f) {
         EXCEPTION("failed to open descriptor file");
     }
     
     /* Get file size */
     fseek(f, 0, SEEK_END);
     long size = ftell(f);
     fseek(f, 0, SEEK_SET);
     
     if (size <= 0 || size > 0x7FFFFFFF) {
         fclose(f);
         EXCEPTION("invalid descriptor file size");
     }
     
     /* Allocate buffer */
     uint8_t* data;
     if (tree && tree->alloc_fn) {
         data = (uint8_t*)tree->alloc_fn(tree->alloc_handle, (size_t)size);
     } else {
         data = (uint8_t*)malloc((size_t)size);
     }
     
     if (!data) {
         fclose(f);
         EXCEPTION("failed to allocate memory for descriptor file");
     }
     
     /* Read file */
     size_t read = fread(data, 1, (size_t)size, f);
     fclose(f);
     
     if (read != (size_t)size) {
         if (tree && tree->free_fn) {
             tree->free_fn(tree->alloc_handle, data);
         } else {
             free(data);
         }
         EXCEPTION("failed to read descriptor file");
     }
     
     *out_data = data;
     *out_size = (uint32_t)size;
     
     return CFL_HBIT_OK;
 }
 
 void cfl_hbit_free_descriptor(
     cfl_hbit_t* tree,
     uint8_t* data
 ) {
     if (!data) return;
     
     if (tree && tree->free_fn) {
         tree->free_fn(tree->alloc_handle, data);
     } else {
         free(data);
     }
 }
 
 cfl_hbit_status_t cfl_hbit_init_from_file(
     cfl_hbit_t* tree,
     const char* path,
     const cfl_hbit_allocator_t* allocator
 ) {
     CHECK_NOT_NULL(tree, "tree");
     CHECK_NOT_NULL(path, "path");
     
     memset(tree, 0, sizeof(cfl_hbit_t));
     
     /* Store allocator as flat fields */
     if (allocator) {
         tree->alloc_handle = allocator->handle;
         tree->alloc_fn = allocator->alloc;
         tree->free_fn = allocator->free;
     }
     
     /* Load descriptor */
     uint8_t* desc_data;
     uint32_t desc_size;
     cfl_hbit_status_t status = cfl_hbit_load_descriptor(tree, path, &desc_data, &desc_size);
     if (status != CFL_HBIT_OK) {
         return status;
     }
     
     /* Validate */
     status = validate_descriptor(desc_data, desc_size);
     if (status != CFL_HBIT_OK) {
         cfl_hbit_free_descriptor(tree, desc_data);
         return status;
     }
     
     tree->descriptor = desc_data;
     tree->descriptor_size = desc_size;
     
     /* Parse header */
     tree->header = (const cfl_hbit_header_t*)desc_data;
     
     /* Set up section pointers */
     tree->bitspaces = (const cfl_hbit_bitspace_desc_t*)(desc_data + tree->header->bitspaces_offset);
     tree->classes_base = desc_data + tree->header->classes_offset;
     tree->nodes_base = desc_data + tree->header->nodes_offset;
     tree->arenas_base = desc_data + tree->header->arenas_offset;
     tree->config = (const cfl_hbit_config_entry_t*)(desc_data + tree->header->config_offset);
     tree->strings = (const char*)(desc_data + tree->header->strings_offset);
     
     /* Allocate runtime memory */
     status = allocate_arenas(tree, NULL, 0);
     if (status != CFL_HBIT_OK) {
         cfl_hbit_free_descriptor(tree, desc_data);
         tree->descriptor = NULL;
         return status;
     }
     
     tree->initialized = 1;
     tree->dirty = 0;
     tree->owns_descriptor = 1;
     
     return CFL_HBIT_OK;
 }
 
 void cfl_hbit_destroy(cfl_hbit_t* tree) {
     if (!tree) return;
     
     /* Free runtime memory block */
     if (tree->memory_block) {
         if (tree->free_fn) {
             tree->free_fn(tree->alloc_handle, tree->memory_block);
         } else {
             free(tree->memory_block);
         }
     }
     
     /* Free descriptor if we loaded it from file */
     if (tree->owns_descriptor && tree->descriptor) {
         if (tree->free_fn) {
             tree->free_fn(tree->alloc_handle, (void*)tree->descriptor);
         } else {
             free((void*)tree->descriptor);
         }
     }
     
     memset(tree, 0, sizeof(cfl_hbit_t));
 }
 
 void cfl_hbit_reset(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_reset_bitspace(tree, bs);
     }
 }
 
 void cfl_hbit_reset_bitspace(cfl_hbit_t* tree, uint16_t bitspace_id) {
     if (!tree || !tree->initialized || bitspace_id >= tree->header->bitspace_count) return;
     
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     
     memset(arena->shadow, 0, arena->size);
     memset(arena->current, 0, arena->size);
     memset(arena->prev, 0, arena->size);
     
     if (arena->latch) {
         memset(arena->latch, 0, arena->size);
     }
     if (arena->live) {
         memset(arena->live, 0, arena->size);
     }
 }
 
 /* ============================================ */
 /* Node Lookup Helper                           */
 /* ============================================ */
 
 static int32_t find_node_vprintf(cfl_hbit_t* tree, const char* fmt, va_list args) {
     uint32_t hash = cfl_hbit_hash_vprintf(fmt, args);
     return find_node_by_hash(tree, hash);
 }
 
 /* ============================================ */
 /* Node Lookup                                  */
 /* ============================================ */
 
 int32_t cfl_hbit_find_node_path(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return -1;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     return node_idx;
 }
 
 int32_t cfl_hbit_get_parent_n(
     cfl_hbit_t* tree,
     int32_t node_idx
 ) {
     if (!tree || !tree->initialized) return -1;
     if (node_idx < 0 || node_idx >= tree->header->node_count) return -1;
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     return node->parent_index;
 }
 
 int cfl_hbit_get_children_n(
     cfl_hbit_t* tree,
     int32_t node_idx,
     int32_t* children,
     int max_children
 ) {
     if (!tree || !tree->initialized) return 0;
     if (node_idx < 0 || node_idx >= tree->header->node_count) return 0;
     if (!children || max_children <= 0) return 0;
     
     int count = 0;
     uint16_t node_count = tree->header->node_count;
     
     /* Scan all nodes looking for children of this node */
     for (uint16_t i = 0; i < node_count && count < max_children; i++) {
         const cfl_hbit_node_desc_t* node = get_node_desc(tree, i);
         if (node->parent_index == node_idx) {
             children[count++] = i;
         }
     }
     
     return count;
 }
 
 int cfl_hbit_get_child_count_n(
     cfl_hbit_t* tree,
     int32_t node_idx
 ) {
     if (!tree || !tree->initialized) return 0;
     if (node_idx < 0 || node_idx >= tree->header->node_count) return 0;
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     return node->child_count;
 }
 
 /* ============================================ */
 /* Bit Operations - Node-indexed (_n suffix)    */
 /* ============================================ */
 
 cfl_hbit_status_t cfl_hbit_set_bit_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     bool value,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     /* Must be a leaf node - aggregate state is computed by propagate */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("set_bit called on non-leaf node (use propagate for aggregates)");
     }
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     uint16_t bank_size = get_bank_size_bits(tree, node->class_index, bitspace_id);
     
     if (bit_index >= bank_size) EXCEPTION("bit index out of range");
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     uint8_t* shadow = tree->arenas[bitspace_id].shadow;
     
     uint32_t byte_idx = bit_index / 8;
     uint8_t bit_mask = 1 << (bit_index % 8);
     
     if (value) {
         shadow[offset + byte_idx] |= bit_mask;
     } else {
         shadow[offset + byte_idx] &= ~bit_mask;
     }
     
     tree->dirty = 1;
     
     return CFL_HBIT_OK;
 }
 
 int cfl_hbit_get_bit_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     uint16_t bank_size = get_bank_size_bits(tree, node->class_index, bitspace_id);
     
     if (bit_index >= bank_size) EXCEPTION("bit index out of range");
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     const uint8_t* current = tree->arenas[bitspace_id].current;
     
     uint32_t byte_idx = bit_index / 8;
     uint8_t bit_mask = 1 << (bit_index % 8);
     
     return (current[offset + byte_idx] & bit_mask) ? 1 : 0;
 }
 
 const uint8_t* cfl_hbit_get_bits_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     return tree->arenas[bitspace_id].current + offset;
 }
 
 int cfl_hbit_get_bit_edge_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     uint16_t bank_bits = get_bank_size_bits(tree, node->class_index, bitspace_id);
     
     if (bit_index >= bank_bits) EXCEPTION("bit index out of range");
     
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     
     uint32_t byte_idx = bit_index / 8;
     uint8_t bit_mask = 1 << (bit_index % 8);
     
     uint8_t prev = arena->prev[offset + byte_idx];
     uint8_t curr = arena->current[offset + byte_idx];
     
     bool was_set = (prev & bit_mask) != 0;
     bool is_set = (curr & bit_mask) != 0;
     
     if (!was_set && is_set) return 1;   /* Rising edge */
     if (was_set && !is_set) return -1;  /* Falling edge */
     return 0;                            /* No change */
 }
 
 /* ============================================ */
 /* Bit Operations - Path-based (wrappers)       */
 /* ============================================ */
 
 cfl_hbit_status_t cfl_hbit_set_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     bool value,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     return cfl_hbit_set_bit_n(tree, bitspace_id, bit_index, value, node_idx);
 }
 
 cfl_hbit_status_t cfl_hbit_clear_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     return cfl_hbit_set_bit_n(tree, bitspace_id, bit_index, false, node_idx);
 }
 
 cfl_hbit_status_t cfl_hbit_set_bits_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t mask,
     uint32_t value,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     /* Must be a leaf node - aggregate state is computed by propagate */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("set_bits_mask called on non-leaf node");
     }
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     uint8_t* shadow = tree->arenas[bitspace_id].shadow;
     
     /* Apply mask to first 4 bytes */
     uint32_t* word = (uint32_t*)(shadow + offset);
     *word = (*word & ~mask) | (value & mask);
     
     tree->dirty = 1;
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_clear_bank(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     /* Must be a leaf node - aggregate state is computed by propagate */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_bank called on non-leaf node");
     }
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     uint16_t bank_size_bits = get_bank_size_bits(tree, node->class_index, bitspace_id);
     uint16_t bank_size_bytes = (bank_size_bits + 7) / 8;
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     uint8_t* shadow = tree->arenas[bitspace_id].shadow;
     
     memset(shadow + offset, 0, bank_size_bytes);
     
     tree->dirty = 1;
     
     return CFL_HBIT_OK;
 }
 
 void cfl_hbit_clear_bitspace(cfl_hbit_t* tree, uint16_t bitspace_id) {
     if (!tree || !tree->initialized || bitspace_id >= tree->header->bitspace_count) return;
     
     memset(tree->arenas[bitspace_id].shadow, 0, tree->arenas[bitspace_id].size);
     tree->dirty = 1;
 }
 
 /* ============================================ */
 /* Bit Operations - Read                        */
 /* ============================================ */
 
 const uint8_t* cfl_hbit_get_bits(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return NULL;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     return cfl_hbit_get_bits_n(tree, bitspace_id, node_idx);
 }
 
 int cfl_hbit_get_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return -1;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     return cfl_hbit_get_bit_n(tree, bitspace_id, bit_index, node_idx);
 }
 
 int cfl_hbit_get_bank_size(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return -1;
     if (bitspace_id >= tree->header->bitspace_count) return -1;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) return -1;
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     return get_bank_size_bits(tree, node->class_index, bitspace_id);
 }
 
 /* ============================================ */
 /* Mask Operations (leaf nodes only)            */
 /* ============================================ */
 
 cfl_hbit_status_t cfl_hbit_set_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const uint8_t* mask,
     uint16_t mask_bytes,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NOT_NULL(mask, "mask");
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     /* Must be a leaf node */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("set_mask called on non-leaf node");
     }
     
     int32_t leaf_idx = find_leaf_index(tree, node_idx);
     if (leaf_idx < 0) EXCEPTION("not a leaf node");
     
     uint16_t expected_bytes = get_leaf_mask_bytes(tree, leaf_idx, bitspace_id);
     if (mask_bytes != expected_bytes) EXCEPTION("mask size mismatch");
     
     /* Calculate offset for this bitspace's mask */
     uint32_t base_offset = tree->leaf_mask_offsets[leaf_idx];
     for (uint16_t bs = 0; bs < bitspace_id; bs++) {
         base_offset += get_leaf_mask_bytes(tree, leaf_idx, bs);
     }
     
     /* Write to shadow mask */
     memcpy(tree->leaf_shadow_masks + base_offset, mask, mask_bytes);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_clear_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     /* Must be a leaf node */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_mask called on non-leaf node");
     }
     
     int32_t leaf_idx = find_leaf_index(tree, node_idx);
     if (leaf_idx < 0) EXCEPTION("not a leaf node");
     
     uint16_t mask_bytes = get_leaf_mask_bytes(tree, leaf_idx, bitspace_id);
     
     /* Calculate offset for this bitspace's mask */
     uint32_t base_offset = tree->leaf_mask_offsets[leaf_idx];
     for (uint16_t bs = 0; bs < bitspace_id; bs++) {
         base_offset += get_leaf_mask_bytes(tree, leaf_idx, bs);
     }
     
     /* Set shadow mask to all 0xFF (all bits propagate) */
     memset(tree->leaf_shadow_masks + base_offset, 0xFF, mask_bytes);
     
     return CFL_HBIT_OK;
 }
 
 const uint8_t* cfl_hbit_get_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t* out_mask_bytes,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return NULL;
     if (bitspace_id >= tree->header->bitspace_count) return NULL;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) return NULL;
     
     /* Must be a leaf node */
     if (!is_leaf_node(tree, node_idx)) return NULL;
     
     int32_t leaf_idx = find_leaf_index(tree, node_idx);
     if (leaf_idx < 0) return NULL;
     
     uint16_t mask_bytes = get_leaf_mask_bytes(tree, leaf_idx, bitspace_id);
     if (out_mask_bytes) *out_mask_bytes = mask_bytes;
     
     /* Calculate offset for this bitspace's mask */
     uint32_t base_offset = tree->leaf_mask_offsets[leaf_idx];
     for (uint16_t bs = 0; bs < bitspace_id; bs++) {
         base_offset += get_leaf_mask_bytes(tree, leaf_idx, bs);
     }
     
     return tree->leaf_current_masks + base_offset;
 }
 
 bool cfl_hbit_is_leaf(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return false;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) return false;
     
     return is_leaf_node(tree, node_idx);
 }
 
 /* ============================================ */
 /* Latch Operations                             */
 /* ============================================ */
 
 cfl_hbit_status_t cfl_hbit_clear_latch_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bitspace_id];
     if (!bs_desc->latch) return CFL_HBIT_OK;  /* Not a latching bitspace */
     
     /* Must be a leaf node */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_latch_n called on non-leaf node");
     }
     
     const cfl_hbit_node_desc_t* node = get_node_desc(tree, node_idx);
     uint16_t bank_size_bits = get_bank_size_bits(tree, node->class_index, bitspace_id);
     uint16_t bank_size_bytes = (bank_size_bits + 7) / 8;
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     
     /* Check if source must be inactive */
     if (bs_desc->clear_requires_inactive) {
         for (uint16_t i = 0; i < bank_size_bytes; i++) {
             if (arena->live[offset + i] != 0) {
                 return CFL_HBIT_ERR_SOURCE_ACTIVE;
             }
         }
     }
     
     /* Clear the latch */
     memset(arena->latch + offset, 0, bank_size_bytes);
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_clear_latch(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     return cfl_hbit_clear_latch_n(tree, bitspace_id, node_idx);
 }
 
 cfl_hbit_status_t cfl_hbit_clear_latch_bits_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t bit_mask,
     int32_t node_idx
 ) {
     CHECK_TREE(tree);
     CHECK_BITSPACE(tree, bitspace_id);
     CHECK_NODE_IDX(tree, node_idx);
     
     const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bitspace_id];
     if (!bs_desc->latch) return CFL_HBIT_OK;
     
     /* Must be a leaf node */
     if (!is_leaf_node(tree, node_idx)) {
         EXCEPTION("clear_latch_bits_n called on non-leaf node");
     }
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     
     /* Check if source must be inactive for those bits */
     if (bs_desc->clear_requires_inactive) {
         uint32_t* live_word = (uint32_t*)(arena->live + offset);
         if ((*live_word & bit_mask) != 0) {
             return CFL_HBIT_ERR_SOURCE_ACTIVE;
         }
     }
     
     /* Clear the specified latch bits */
     uint32_t* latch_word = (uint32_t*)(arena->latch + offset);
     *latch_word &= ~bit_mask;
     
     return CFL_HBIT_OK;
 }
 
 cfl_hbit_status_t cfl_hbit_clear_latch_bits(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t bit_mask,
     const char* path_fmt,
     ...
 ) {
     CHECK_TREE(tree);
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) EXCEPTION("path not found");
     
     return cfl_hbit_clear_latch_bits_n(tree, bitspace_id, bit_mask, node_idx);
 }
 
 void cfl_hbit_clear_all_latches(cfl_hbit_t* tree, uint16_t bitspace_id) {
     if (!tree || !tree->initialized || bitspace_id >= tree->header->bitspace_count) return;
     
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     if (arena->latch) {
         memset(arena->latch, 0, arena->size);
     }
 }
 
 const uint8_t* cfl_hbit_get_latched(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return NULL;
     if (bitspace_id >= tree->header->bitspace_count) return NULL;
     
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     if (!arena->latch) return NULL;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) return NULL;
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     return arena->latch + offset;
 }
 
 const uint8_t* cfl_hbit_get_live(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return NULL;
     if (bitspace_id >= tree->header->bitspace_count) return NULL;
     
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     if (!arena->live) return NULL;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     if (node_idx < 0) return NULL;
     
     uint32_t offset = get_node_offset(tree, bitspace_id, node_idx);
     return arena->live + offset;
 }
 
 /* ============================================ */
 /* Synchronization and Propagation              */
 /* ============================================ */
 
 void cfl_hbit_swap(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         cfl_hbit_arena_t* arena = &tree->arenas[bs];
         
         /* Save current to prev for change detection */
         memcpy(arena->prev, arena->current, arena->size);
         
         /* Swap shadow and current */
         uint8_t* tmp = arena->shadow;
         arena->shadow = arena->current;
         arena->current = tmp;
         
         /* Copy current to shadow for next cycle's starting point */
         memcpy(arena->shadow, arena->current, arena->size);
     }
     
     /* Swap leaf masks: shadow <-> current */
     if (tree->leaf_mask_total_bytes > 0) {
         uint8_t* tmp = tree->leaf_shadow_masks;
         tree->leaf_shadow_masks = tree->leaf_current_masks;
         tree->leaf_current_masks = tmp;
         
         /* Copy current to shadow for next cycle's starting point */
         memcpy(tree->leaf_shadow_masks, tree->leaf_current_masks, tree->leaf_mask_total_bytes);
     }
     
     tree->dirty = 0;
 }
 
 /* Apply merge operation for a single byte */
 static uint8_t merge_byte_or(uint8_t parent, uint8_t child, uint8_t mask) {
     return parent | (child & mask);
 }
 
 static uint8_t merge_byte_and(uint8_t parent, uint8_t child, uint8_t mask) {
     /* For AND, we need to handle the logic differently */
     /* If a bit in mask is 0, we don't touch that bit in parent */
     /* If a bit in mask is 1, we AND it with the child */
     uint8_t masked_child = child | ~mask;  /* Set unmasked bits to 1 */
     return parent & masked_child;
 }
 
 /* Propagate a single bitspace */
 static void propagate_bitspace(cfl_hbit_t* tree, uint16_t bitspace_id) {
     const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bitspace_id];
     cfl_hbit_arena_t* arena = &tree->arenas[bitspace_id];
     uint16_t node_count = tree->header->node_count;
     uint16_t max_depth = tree->header->max_depth;
     
     cfl_hbit_merge_t merge_type = (cfl_hbit_merge_t)bs_desc->merge_type;
     cfl_hbit_merge_t base_merge = (cfl_hbit_merge_t)bs_desc->base_merge_type;
     
     /* For MASK merge, use the base merge type */
     if (merge_type == CFL_HBIT_MERGE_MASK) {
         merge_type = base_merge;
     }
     
     /* Handle latching - save live state and apply latch */
     if (bs_desc->latch && arena->live && arena->latch) {
         /* Copy current to live (pre-latch state) */
         memcpy(arena->live, arena->current, arena->size);
         
         /* Apply latch: latch = latch | current */
         for (uint32_t i = 0; i < arena->size; i++) {
             arena->latch[i] |= arena->current[i];
         }
         
         /* Current now reflects latched state */
         memcpy(arena->current, arena->latch, arena->size);
     }
     
     /* Bottom-up propagation: process from deepest to shallowest */
     /* Multiple passes, one for each depth level starting from max */
     for (int16_t depth = max_depth; depth >= 0; depth--) {
         for (int32_t n = 0; n < node_count; n++) {
             const cfl_hbit_node_desc_t* node = get_node_desc(tree, n);
             
             /* Only process nodes at this depth */
             if (node->depth != depth) continue;
             if (node->parent_index < 0) continue;  /* Root node */
             
             const cfl_hbit_node_desc_t* parent = get_node_desc(tree, node->parent_index);
             
             uint16_t child_class = node->class_index;
             uint16_t parent_class = parent->class_index;
             
             uint16_t child_bank_bits = get_bank_size_bits(tree, child_class, bitspace_id);
             uint16_t parent_bank_bits = get_bank_size_bits(tree, parent_class, bitspace_id);
             
             if (child_bank_bits == 0 || parent_bank_bits == 0) continue;
         
         uint16_t child_bank_bytes = (child_bank_bits + 7) / 8;
         uint16_t parent_bank_bytes = (parent_bank_bits + 7) / 8;
         uint16_t merge_bytes = (child_bank_bytes < parent_bank_bytes) ? 
                                child_bank_bytes : parent_bank_bytes;
         
         uint32_t child_offset = get_node_offset(tree, bitspace_id, n);
         uint32_t parent_offset = get_node_offset(tree, bitspace_id, node->parent_index);
         
         uint8_t* child_bits = arena->current + child_offset;
         uint8_t* parent_bits = arena->current + parent_offset;
         
         /* Get mask for leaf nodes only, non-leaves use all 0xFF */
         const uint8_t* leaf_mask = NULL;
         uint16_t leaf_mask_bytes = 0;
         
         if (node->child_count == 0) {
             /* Leaf node - get mask from current mask buffer */
             int32_t leaf_idx = find_leaf_index(tree, n);
             if (leaf_idx >= 0) {
                 uint32_t mask_offset = tree->leaf_mask_offsets[leaf_idx];
                 for (uint16_t bs = 0; bs < bitspace_id; bs++) {
                     mask_offset += get_leaf_mask_bytes(tree, leaf_idx, bs);
                 }
                 leaf_mask = tree->leaf_current_masks + mask_offset;
                 leaf_mask_bytes = get_leaf_mask_bytes(tree, leaf_idx, bitspace_id);
             }
         }
         
         switch (merge_type) {
             case CFL_HBIT_MERGE_OR:
                 for (uint16_t i = 0; i < merge_bytes; i++) {
                     uint8_t byte_mask = (leaf_mask && i < leaf_mask_bytes) ? leaf_mask[i] : 0xFF;
                     parent_bits[i] = merge_byte_or(parent_bits[i], child_bits[i], byte_mask);
                 }
                 break;
                 
             case CFL_HBIT_MERGE_AND:
                 for (uint16_t i = 0; i < merge_bytes; i++) {
                     uint8_t byte_mask = (leaf_mask && i < leaf_mask_bytes) ? leaf_mask[i] : 0xFF;
                     parent_bits[i] = merge_byte_and(parent_bits[i], child_bits[i], byte_mask);
                 }
                 break;
                 
             case CFL_HBIT_MERGE_PRIORITY:
                 /* For priority, find lowest set bit (highest priority) */
                 {
                     int child_prio = -1;
                     int parent_prio = -1;
                     
                     /* Find child's priority (lowest set bit index) */
                     for (int b = 0; b < child_bank_bits && child_prio < 0; b++) {
                         if (child_bits[b/8] & (1 << (b%8))) {
                             child_prio = b;
                         }
                     }
                     
                     /* Find parent's current priority */
                     for (int b = 0; b < parent_bank_bits && parent_prio < 0; b++) {
                         if (parent_bits[b/8] & (1 << (b%8))) {
                             parent_prio = b;
                         }
                     }
                     
                     /* If child has higher priority (lower index), update parent */
                     if (child_prio >= 0 && (parent_prio < 0 || child_prio < parent_prio)) {
                         memset(parent_bits, 0, parent_bank_bytes);
                         parent_bits[child_prio/8] |= (1 << (child_prio%8));
                     }
                 }
                 break;
                 
             default:
                 break;
         }
     }
     }  /* End depth loop */
 }
 
 void cfl_hbit_propagate(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return;
     
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         propagate_bitspace(tree, bs);
     }
 }
 
 void cfl_hbit_sync(cfl_hbit_t* tree) {
     cfl_hbit_swap(tree);
     cfl_hbit_propagate(tree);
 }
 
 /* ============================================ */
 /* Change Detection                             */
 /* ============================================ */
 
 cfl_hbit_status_t cfl_hbit_register_callback(
     cfl_hbit_t* tree,
     int16_t bitspace_id,
     cfl_hbit_change_cb_t callback,
     void* user_data
 ) {
     CHECK_TREE(tree);
     CHECK_NOT_NULL(callback, "callback");
     if (tree->callback_count >= tree->callback_capacity) EXCEPTION("callback array full");
     
     cfl_hbit_callback_t* cb = &tree->callbacks[tree->callback_count++];
     cb->callback = callback;
     cb->user_data = user_data;
     cb->bitspace_id = bitspace_id;
     
     return CFL_HBIT_OK;
 }
 
 void cfl_hbit_notify_changes(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized || tree->callback_count == 0) return;
     
     uint16_t node_count = tree->header->node_count;
     uint16_t bitspace_count = tree->header->bitspace_count;
     
     for (uint16_t bs = 0; bs < bitspace_count; bs++) {
         cfl_hbit_arena_t* arena = &tree->arenas[bs];
         
         for (int32_t n = 0; n < node_count; n++) {
             const cfl_hbit_node_desc_t* node = get_node_desc(tree, n);
             uint16_t bank_bytes = (get_bank_size_bits(tree, node->class_index, bs) + 7) / 8;
             
             if (bank_bytes == 0) continue;
             
             uint32_t offset = get_node_offset(tree, bs, n);
             
             /* Check if bits changed */
             bool changed = false;
             for (uint16_t i = 0; i < bank_bytes && !changed; i++) {
                 if (arena->current[offset + i] != arena->prev[offset + i]) {
                     changed = true;
                 }
             }
             
             if (changed) {
                 /* Fire callbacks */
                 for (uint8_t c = 0; c < tree->callback_count; c++) {
                     cfl_hbit_callback_t* cb = &tree->callbacks[c];
                     if (cb->bitspace_id == -1 || cb->bitspace_id == bs) {
                         cb->callback(
                             tree,
                             bs,
                             node->path_hash,
                             arena->prev + offset,
                             arena->current + offset,
                             bank_bytes,
                             cb->user_data
                         );
                     }
                 }
             }
         }
     }
 }
 
 int cfl_hbit_get_bit_edge(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return 0;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     return cfl_hbit_get_bit_edge_n(tree, bitspace_id, bit_index, node_idx);
 }
 
 /* ============================================ */
 /* Configuration Access                         */
 /* ============================================ */
 
 static const cfl_hbit_config_entry_t* find_config(cfl_hbit_t* tree, uint32_t hash) {
     if (!tree || !tree->header) return NULL;
     
     uint16_t count = tree->header->config_count;
     
     /* Binary search */
     int32_t lo = 0;
     int32_t hi = count - 1;
     
     while (lo <= hi) {
         int32_t mid = (lo + hi) / 2;
         const cfl_hbit_config_entry_t* entry = &tree->config[mid];
         
         if (entry->path_hash == hash) {
             return entry;
         } else if (entry->path_hash < hash) {
             lo = mid + 1;
         } else {
             hi = mid - 1;
         }
     }
     
     return NULL;
 }
 
 int32_t cfl_hbit_config_get_int(
     cfl_hbit_t* tree,
     int32_t default_val,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return default_val;
     
     va_list args;
     va_start(args, path_fmt);
     uint32_t hash = cfl_hbit_hash_vprintf(path_fmt, args);
     va_end(args);
     
     const cfl_hbit_config_entry_t* entry = find_config(tree, hash);
     if (!entry || entry->value_type != CFL_HBIT_CONFIG_INT) {
         return default_val;
     }
     
     return (int32_t)entry->value;
 }
 
 float cfl_hbit_config_get_float(
     cfl_hbit_t* tree,
     float default_val,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return default_val;
     
     va_list args;
     va_start(args, path_fmt);
     uint32_t hash = cfl_hbit_hash_vprintf(path_fmt, args);
     va_end(args);
     
     const cfl_hbit_config_entry_t* entry = find_config(tree, hash);
     if (!entry || entry->value_type != CFL_HBIT_CONFIG_FLOAT) {
         return default_val;
     }
     
     /* Reinterpret uint32 bits as float */
     union { uint32_t u; float f; } conv;
     conv.u = entry->value;
     return conv.f;
 }
 
 bool cfl_hbit_config_get_bool(
     cfl_hbit_t* tree,
     bool default_val,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return default_val;
     
     va_list args;
     va_start(args, path_fmt);
     uint32_t hash = cfl_hbit_hash_vprintf(path_fmt, args);
     va_end(args);
     
     const cfl_hbit_config_entry_t* entry = find_config(tree, hash);
     if (!entry || entry->value_type != CFL_HBIT_CONFIG_BOOL) {
         return default_val;
     }
     
     return entry->value != 0;
 }
 
 const char* cfl_hbit_config_get_string(
     cfl_hbit_t* tree,
     const char* default_val,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return default_val;
     
     va_list args;
     va_start(args, path_fmt);
     uint32_t hash = cfl_hbit_hash_vprintf(path_fmt, args);
     va_end(args);
     
     const cfl_hbit_config_entry_t* entry = find_config(tree, hash);
     if (!entry || entry->value_type != CFL_HBIT_CONFIG_STRING) {
         return default_val;
     }
     
     return tree->strings + entry->value;
 }
 
 /* ============================================ */
 /* Introspection                                */
 /* ============================================ */
 
 uint16_t cfl_hbit_node_count(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return 0;
     return tree->header->node_count;
 }
 
 uint16_t cfl_hbit_bitspace_count(cfl_hbit_t* tree) {
     if (!tree || !tree->initialized) return 0;
     return tree->header->bitspace_count;
 }
 
 bool cfl_hbit_path_exists(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 ) {
     if (!tree || !tree->initialized) return false;
     
     va_list args;
     va_start(args, path_fmt);
     int32_t node_idx = find_node_vprintf(tree, path_fmt, args);
     va_end(args);
     
     return node_idx >= 0;
 }
 
 int32_t cfl_hbit_find_node(cfl_hbit_t* tree, uint32_t path_hash) {
     return find_node_by_hash(tree, path_hash);
 }
 
 void cfl_hbit_get_mem_info(cfl_hbit_t* tree, cfl_hbit_mem_info_t* info) {
     if (!info) return;
     
     memset(info, 0, sizeof(cfl_hbit_mem_info_t));
     
     if (!tree || !tree->initialized) return;
     
     info->descriptor_size = tree->descriptor_size;
     info->node_count = tree->header->node_count;
     info->leaf_count = tree->leaf_count;
     info->bitspace_count = tree->header->bitspace_count;
     info->max_depth = tree->header->max_depth;
     
     /* Arena structs */
     info->arena_size = tree->header->bitspace_count * sizeof(cfl_hbit_arena_t);
     
     /* Callback array */
     info->arena_size += tree->callback_capacity * sizeof(cfl_hbit_callback_t);
     
     /* Calculate arena buffer sizes */
     for (uint16_t bs = 0; bs < tree->header->bitspace_count; bs++) {
         const cfl_hbit_bitspace_desc_t* bs_desc = &tree->bitspaces[bs];
         cfl_hbit_arena_t* arena = &tree->arenas[bs];
         
         /* shadow + current + prev */
         info->arena_size += arena->size * 3;
         
         /* latch + live if latching */
         if (bs_desc->latch) {
             info->arena_size += arena->size * 2;
         }
     }
     
     /* Leaf masks: shadow + current buffers + management arrays */
     info->leaf_masks_size = tree->leaf_mask_total_bytes * 2;
     info->leaf_masks_size += tree->leaf_count * sizeof(uint32_t);  /* offsets */
     info->leaf_masks_size += tree->leaf_count * sizeof(uint16_t);  /* indices */
     
     info->total_ram = info->arena_size + info->leaf_masks_size;
 }