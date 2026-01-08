/* ChainTree Binary Support Header */
#ifndef CHAINTREE_BINARY_SUPPORT_H
#define CHAINTREE_BINARY_SUPPORT_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ===== Binary Header Structure ===== */
typedef struct __attribute__((packed)) {
    uint8_t  magic[4];              /* "CTRB" */
    uint16_t version;               /* 0x0001 */
    uint16_t flags;                 /* Reserved */
    uint32_t total_size;            /* Total binary size including CRCs */
    
    /* Section offsets from binary start */
    uint32_t nodes_offset;
    uint32_t link_table_offset;
    uint32_t kb_table_offset;
    uint32_t kb_aliases_offset;
    uint32_t main_func_hashes_offset;
    uint32_t one_shot_func_hashes_offset;
    uint32_t boolean_func_hashes_offset;
    uint32_t main_func_usage_offset;
    uint32_t event_hashes_offset;
    uint32_t bitmask_hashes_offset;
    uint32_t node_data_records_offset;
    uint32_t node_data_controls_offset;
    uint32_t node_data_strings_offset;
    
    /* Counts */
    uint16_t node_count;
    uint16_t link_table_size;
    uint16_t kb_count;
    uint16_t main_function_count;
    uint16_t one_shot_function_count;
    uint16_t boolean_function_count;
    uint16_t event_count;
    uint16_t bitmask_count;
    uint16_t node_data_records_count;
    uint16_t node_data_controls_count;
    uint16_t node_data_strings_size;
    uint16_t total_aliases_count;
    
    uint32_t unique_id_hash;
    uint32_t header_hash;           /* FNV-1a hash of bytes 0..(this field - 1) */
} chaintree_binary_header_t;

/* ===== Binary Node Structure ===== */
typedef struct __attribute__((packed)) {
    uint16_t node_index;
    uint16_t parent_index;
    uint16_t depth;
    uint16_t link_start;
    uint16_t link_count;            /* Bits 0-14: count, Bit 15: auto_start */
    uint16_t main_function_index;
    uint16_t init_function_index;
    uint16_t aux_function_index;
    uint16_t term_function_index;
    uint16_t node_data_id;
} chaintree_binary_node_t;

/* ===== Binary KB Info Structure ===== */
typedef struct __attribute__((packed)) {
    uint32_t kb_name_hash;
    uint16_t root_node_index;
    uint16_t start_index;
    uint16_t node_count;
    uint16_t max_depth;
    uint16_t memory_factor;
    uint16_t alias_count;
    uint32_t aliases_offset;        /* Index into aliases array */
} chaintree_binary_kb_info_t;

/* ===== Binary Alias Structure ===== */
typedef struct __attribute__((packed)) {
    uint32_t alias_hash;
    uint16_t node_index;
    uint16_t padding;
} chaintree_binary_alias_t;

/* ===== Function Pointer Types ===== */
typedef unsigned (*main_function_t)(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);
typedef void (*one_shot_function_t)(void *handle, unsigned node_index);
typedef bool (*boolean_function_t)(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* ===== Allocator Interface ===== */
typedef void* (*ct_alloc_fn)(void *alloc_handle, size_t size);
typedef void  (*ct_free_fn)(void *alloc_handle, void *ptr);

typedef struct {
    void *handle;
    ct_alloc_fn alloc;
    ct_free_fn free;
} ct_allocator_t;

/* ===== Function Resolvers ===== */
typedef main_function_t     (*ct_resolve_main_fn)(uint32_t hash);
typedef one_shot_function_t (*ct_resolve_one_shot_fn)(uint32_t hash);
typedef boolean_function_t  (*ct_resolve_boolean_fn)(uint32_t hash);

typedef struct {
    ct_resolve_main_fn     resolve_main;
    ct_resolve_one_shot_fn resolve_one_shot;
    ct_resolve_boolean_fn  resolve_boolean;
} ct_resolver_t;

/* ===== Runtime Handle (created by loader) ===== */
typedef struct {
    /* Binary data (can be in flash) */
    const uint8_t *binary_base;
    const chaintree_binary_header_t *header;
    
    /* Resolved function tables (allocated by loader) */
    main_function_t *main_functions;
    one_shot_function_t *one_shot_functions;
    boolean_function_t *boolean_functions;
    
    /* Quick access pointers (into binary) */
    const chaintree_binary_node_t *nodes;
    const uint16_t *link_table;
    const chaintree_binary_kb_info_t *kb_table;
    const chaintree_binary_alias_t *aliases;
    const uint16_t *main_func_usage;
    const uint32_t *event_hashes;
    const uint32_t *bitmask_hashes;
} chaintree_runtime_t;

/* ===== Load Result Codes ===== */
typedef enum {
    CT_LOAD_OK = 0,
    CT_LOAD_ERR_NULL_INPUT,
    CT_LOAD_ERR_MAGIC,
    CT_LOAD_ERR_VERSION,
    CT_LOAD_ERR_SIZE,
    CT_LOAD_ERR_HEADER_HASH,
    CT_LOAD_ERR_DATA_HASH,
    CT_LOAD_ERR_ALLOC,
    CT_LOAD_ERR_RESOLVE_MAIN,
    CT_LOAD_ERR_RESOLVE_ONE_SHOT,
    CT_LOAD_ERR_RESOLVE_BOOLEAN,
} ct_load_result_t;

/* ===== API Functions ===== */

/*
 * Load binary and create runtime handle.
 */
ct_load_result_t ct_load_binary(
    const uint8_t *binary_data,
    uint32_t binary_size,
    const ct_allocator_t *allocator,
    const ct_resolver_t *resolver,
    chaintree_runtime_t **runtime_out
);

/*
 * Unload and free all resources.
 */
void ct_unload(
    chaintree_runtime_t *runtime,
    const ct_allocator_t *allocator
);

/*
 * Verify binary integrity without loading.
 */
ct_load_result_t ct_verify_binary(
    const uint8_t *binary_data,
    uint32_t binary_size
);

/*
 * Get unique_id hash from binary without full load.
 */
uint32_t ct_get_binary_id_hash(const uint8_t *binary_data);

/*
 * FNV-1a hash utility.
 */
uint32_t ct_fnv1a_hash(const char *str);

/* ===== Accessor Macros ===== */
#define CT_GET_NODE(rt, idx) (&(rt)->nodes[idx])
#define CT_GET_LINK(rt, idx) ((rt)->link_table[idx])
#define CT_GET_KB(rt, idx) (&(rt)->kb_table[idx])

#define CT_NODE_LINK_COUNT(node) ((node)->link_count & 0x7FFF)
#define CT_NODE_AUTO_START(node) (((node)->link_count & 0x8000) != 0)

#endif /* CHAINTREE_BINARY_SUPPORT_H */