// ============================================================================
// s_engine_file.h
// File Loading API (Platform-Specific)
// ============================================================================

#ifndef S_ENGINE_FILE_H
#define S_ENGINE_FILE_H

#include "s_engine_types.h"
#include "s_engine_binary.h"

// ============================================================================
// ERROR CODES
// ============================================================================

#define FILE_OK                 0
#define FILE_ERR_NULL_PATH      1
#define FILE_ERR_OPEN_FAILED    2
#define FILE_ERR_READ_FAILED    3
#define FILE_ERR_ALLOC          4
#define FILE_ERR_TOO_LARGE      5

// ============================================================================
// FILE BUFFER
// ============================================================================

typedef struct {
    uint8_t*    data;
    uint32_t    size;
    int         error_code;
} file_buffer_t;

// ============================================================================
// FILE API
// ============================================================================

// Load entire file into memory buffer
// Allocates buffer using provided allocator
// Caller must free with file_free_buffer()
file_buffer_t* file_load(
    const char* path,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id,
    uint32_t max_size          // Maximum allowed file size (0 = no limit)
);

// Free buffer allocated by file_load()
void file_free_buffer(
    file_buffer_t* buf,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

// Get error string
const char* file_error_string(int error_code);

// ============================================================================
// CONVENIENCE: Load and parse in one call
// ============================================================================

// Load .bin file and parse into module
// Returns bin_module_t ready to use with module_create()
// On success, caller must free with bin_file_free_all()
typedef struct {
    bin_module_t*   module;     // Parsed module (NULL on error)
    file_buffer_t*  buffer;     // Raw file data (kept for reference)
    int             error_code; // FILE_* or BIN_* error
    const char*     error_msg;
} bin_load_result_t;

bin_load_result_t bin_file_load(
    const char* path,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id,
    uint32_t max_size
);

// Free everything allocated by bin_file_load()
void bin_file_free_all(
    bin_load_result_t* result,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

#endif // S_ENGINE_FILE_H