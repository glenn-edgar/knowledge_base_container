// ============================================================================
// s_expr_binary_file.h
// S-Expression Binary File Loader
// ============================================================================
//
// Loads binary module files from filesystem into memory.
// Works with s_expr_binary_v5.h for parsing the loaded data.
//
// Usage:
//   sexb_file_t file;
//   if (sexb_file_load(&file, "my_module_32.bin", alloc, free, ctx)) {
//       sexb_module_t mod;
//       sexb_load(&mod, file.data, file.size);
//       // ... use module ...
//       sexb_file_unload(&file, free, ctx);
//   }
//
// ============================================================================

#ifndef S_EXPR_BINARY_FILE_H
#define S_EXPR_BINARY_FILE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// TYPES
// ============================================================================

typedef void* (*sexb_file_alloc_fn)(void* ctx, size_t size);
typedef void  (*sexb_file_free_fn)(void* ctx, void* ptr);

typedef struct {
    uint8_t* data;
    uint32_t size;
} sexb_file_t;

// ============================================================================
// API
// ============================================================================

// Load binary file into memory
// Returns true on success, false on failure
bool sexb_file_load(
    sexb_file_t* file,
    const char* path,
    sexb_file_alloc_fn alloc_fn,
    void* alloc_ctx
);

// Unload file (free memory)
void sexb_file_unload(
    sexb_file_t* file,
    sexb_file_free_fn free_fn,
    void* alloc_ctx
);

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_FILE_H