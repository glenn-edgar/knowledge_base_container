// ============================================================================
// s_engine_file.c
// File Loading Implementation (POSIX/Standard C)
// ============================================================================

#include "s_engine_file.h"
#include <stdio.h>
#include <string.h>

// ============================================================================
// ERROR STRINGS
// ============================================================================

const char* file_error_string(int error_code) {
    switch (error_code) {
        case FILE_OK:            return "OK";
        case FILE_ERR_NULL_PATH: return "NULL path";
        case FILE_ERR_OPEN_FAILED: return "Failed to open file";
        case FILE_ERR_READ_FAILED: return "Failed to read file";
        case FILE_ERR_ALLOC:     return "Memory allocation failed";
        case FILE_ERR_TOO_LARGE: return "File too large";
        default:                 return "Unknown file error";
    }
}

// ============================================================================
// FILE LOAD
// ============================================================================

file_buffer_t* file_load(
    const char* path,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id,
    uint32_t max_size
) {
    if (!path || !alloc || !alloc->malloc || !alloc->free) {
        return NULL;
    }
    
    // Allocate result structure
    file_buffer_t* buf = (file_buffer_t*)alloc->malloc(
        handle, ct_node_id, sizeof(file_buffer_t)
    );
    if (!buf) {
        return NULL;
    }
    memset(buf, 0, sizeof(file_buffer_t));
    
    // Open file
    FILE* f = fopen(path, "rb");
    if (!f) {
        buf->error_code = FILE_ERR_OPEN_FAILED;
        return buf;
    }
    
    // Get file size
    if (fseek(f, 0, SEEK_END) != 0) {
        buf->error_code = FILE_ERR_READ_FAILED;
        fclose(f);
        return buf;
    }
    
    long file_size = ftell(f);
    if (file_size < 0) {
        buf->error_code = FILE_ERR_READ_FAILED;
        fclose(f);
        return buf;
    }
    
    if (fseek(f, 0, SEEK_SET) != 0) {
        buf->error_code = FILE_ERR_READ_FAILED;
        fclose(f);
        return buf;
    }
    
    // Check size limit
    if (max_size > 0 && (uint32_t)file_size > max_size) {
        buf->error_code = FILE_ERR_TOO_LARGE;
        fclose(f);
        return buf;
    }
    
    // Allocate buffer
    buf->data = (uint8_t*)alloc->malloc(handle, ct_node_id, (size_t)file_size);
    if (!buf->data) {
        buf->error_code = FILE_ERR_ALLOC;
        fclose(f);
        return buf;
    }
    
    // Read file
    size_t bytes_read = fread(buf->data, 1, (size_t)file_size, f);
    fclose(f);
    
    if (bytes_read != (size_t)file_size) {
        alloc->free(handle, ct_node_id, buf->data);
        buf->data = NULL;
        buf->error_code = FILE_ERR_READ_FAILED;
        return buf;
    }
    
    buf->size = (uint32_t)file_size;
    buf->error_code = FILE_OK;
    return buf;
}

// ============================================================================
// FILE FREE
// ============================================================================

void file_free_buffer(
    file_buffer_t* buf,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    if (!buf || !alloc || !alloc->free) return;
    
    if (buf->data) {
        alloc->free(handle, ct_node_id, buf->data);
    }
    alloc->free(handle, ct_node_id, buf);
}

// ============================================================================
// CONVENIENCE: LOAD AND PARSE
// ============================================================================

bin_load_result_t bin_file_load(
    const char* path,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id,
    uint32_t max_size
) {
    bin_load_result_t result = {0};
    
    // Load file
    result.buffer = file_load(path, alloc, handle, ct_node_id, max_size);
    if (!result.buffer) {
        result.error_code = FILE_ERR_ALLOC;
        result.error_msg = file_error_string(FILE_ERR_ALLOC);
        return result;
    }
    
    if (result.buffer->error_code != FILE_OK) {
        result.error_code = result.buffer->error_code;
        result.error_msg = file_error_string(result.buffer->error_code);
        return result;
    }
    
    // Parse binary
    result.module = bin_parse_module(
        result.buffer->data,
        result.buffer->size,
        alloc,
        handle,
        ct_node_id
    );
    
    if (!result.module) {
        result.error_code = BIN_ERR_ALLOC;
        result.error_msg = bin_error_string(BIN_ERR_ALLOC);
        return result;
    }
    
    if (result.module->error_code != BIN_OK) {
        result.error_code = result.module->error_code;
        result.error_msg = bin_error_string(result.module->error_code);
        // Don't free module - caller might want error details
        return result;
    }
    
    result.error_code = BIN_OK;
    result.error_msg = "OK";
    return result;
}

// ============================================================================
// FREE ALL
// ============================================================================

void bin_file_free_all(
    bin_load_result_t* result,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    if (!result || !alloc) return;
    
    // Free module (if allocated)
    if (result->module) {
        bin_free_module(result->module, alloc, handle, ct_node_id);
        result->module = NULL;
    }
    
    // Free file buffer (if allocated)
    if (result->buffer) {
        file_free_buffer(result->buffer, alloc, handle, ct_node_id);
        result->buffer = NULL;
    }
}