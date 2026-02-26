// ============================================================================
// s_expr_binary_v5_internal.h
// S-Expression Binary Module Loader - Internal Structures
// DO NOT INCLUDE DIRECTLY - used only by s_expr_binary_v5.c
// ============================================================================

#ifndef S_EXPR_BINARY_V5_INTERNAL_H
#define S_EXPR_BINARY_V5_INTERNAL_H

#include <stdint.h>

// ============================================================================
// BINARY FORMAT CONSTANTS
// ============================================================================

#define SEXB_MAGIC      0x42584553  // "SEXB" little-endian
#define SEXB_VERSION    0x0500      // Version 5.0

#define SEXB_FLAG_64BIT 0x0001
#define SEXB_FLAG_DEBUG 0x0002

// ============================================================================
// BINARY FILE STRUCTURES
// These must match the exact binary layout produced by s_compile.lua
// ============================================================================

// Header (32 bytes, offset 0)
typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t name_hash;
    uint16_t tree_count;
    uint16_t record_count;
    uint16_t string_count;
    uint16_t const_count;
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    uint16_t reserved;
    uint32_t total_size;
} sexb_header_t;

// Directory (32 bytes, offset 32)
typedef struct {
    uint32_t tree_offset;
    uint32_t record_offset;
    uint32_t field_offset;
    uint32_t string_offset;
    uint32_t const_offset;
    uint32_t const_data_offset;
    uint32_t func_offset;
    uint32_t params_offset;
} sexb_directory_t;

// Tree entry (20 bytes)
typedef struct {
    uint32_t name_hash;
    uint32_t record_hash;
    uint16_t node_count;
    uint16_t pointer_count;
    uint32_t param_offset;
    uint16_t param_count;
    uint16_t reserved;
} sexb_tree_entry_t;

// Record entry (12 bytes)
typedef struct {
    uint32_t name_hash;
    uint16_t field_count;
    uint16_t size;
    uint32_t field_offset;
} sexb_record_entry_t;

// Field entry (12 bytes)
typedef struct {
    uint32_t name_hash;
    uint8_t  type_tag;
    uint8_t  flags;
    uint16_t offset;
    uint16_t size;
    uint16_t aux;
} sexb_field_entry_t;

// Constant entry (12 bytes)
typedef struct {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t size;
    uint32_t data_offset;
} sexb_const_entry_t;

#endif // S_EXPR_BINARY_V5_INTERNAL_H