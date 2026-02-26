// ============================================================================
// s_expr_binary_v5.h
// S-Expression Binary Module Loader - Public API
// Version 5.0 - Zero-copy direct cast from ROM
// ============================================================================
//
// Minimal API for loading binary modules and integrating with ChainTree.
// For debug/inspection functions, include s_expr_binary_v5_debug.h
//
// Usage:
//   sexb_module_t mod;
//   sexb_load(&mod, binary_data, size);
//   s_expr_module_def_t* def = sexb_build_module_def(&mod, alloc, free, alloc_ctx);
//   cfl_initialize_s_engine(handle, &def, 1);
//   // ... later ...
//   sexb_free_module_def(def, free, alloc_ctx);
//
// ============================================================================

#ifndef S_EXPR_BINARY_V5_H
#define S_EXPR_BINARY_V5_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// LOADED MODULE HANDLE
// ============================================================================

typedef struct {
    const uint8_t* data;
    uint32_t size;
    uint32_t name_hash;
    bool is_64bit;
    
    uint16_t tree_count;
    uint16_t record_count;
    uint16_t string_count;
    uint16_t const_count;
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    
    // Internal pointers (opaque)
    const void* trees;
    const void* records;
    const void* fields;
    const uint8_t* strings;
    const void* constants;
    const uint8_t* const_data;
    const uint32_t* func_hashes;
    const uint8_t* params_base;
} sexb_module_t;

// ============================================================================
// ALLOCATOR TYPES
// ============================================================================

typedef void* (*sexb_alloc_fn)(void* ctx, size_t size);
typedef void  (*sexb_free_fn)(void* ctx, void* ptr);

// ============================================================================
// PUBLIC API
// ============================================================================

// Load binary module (zero-copy)
// Returns true on success, false on validation failure
bool sexb_load(sexb_module_t* mod, const uint8_t* data, uint32_t size);

// Build s_expr_module_def_t for use with cfl_initialize_s_engine()
// Allocates wrapper structures; params point directly into ROM
s_expr_module_def_t* sexb_build_module_def(
    const sexb_module_t* mod,
    sexb_alloc_fn alloc_fn,
    sexb_free_fn free_fn,
    void* alloc_ctx
);

// Free module def (only frees wrapper, not ROM data)
void sexb_free_module_def(
    s_expr_module_def_t* def, 
    sexb_free_fn free_fn,
    void* alloc_ctx
);

// ============================================================================
// CONVENIENCE MACRO
// ============================================================================

#if MODULE_IS_64BIT
    #define SEXB_PARAM_SIZE 16
#else
    #define SEXB_PARAM_SIZE 8
#endif

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_V5_H