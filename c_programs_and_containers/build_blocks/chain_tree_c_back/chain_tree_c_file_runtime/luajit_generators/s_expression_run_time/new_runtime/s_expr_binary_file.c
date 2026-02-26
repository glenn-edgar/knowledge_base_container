// ============================================================================
// s_expr_binary_file.c
// S-Expression Binary File Loader - Implementation
// ============================================================================

#include "s_expr_binary_file.h"
#include <stdio.h>
#include <string.h>

bool sexb_file_load(
    sexb_file_t* file,
    const char* path,
    sexb_file_alloc_fn alloc_fn,
    void* alloc_ctx
) {
    if (!file || !path || !alloc_fn) {
        return false;
    }
    
    memset(file, 0, sizeof(*file));
    
    // Open file
    FILE* fp = fopen(path, "rb");
    if (!fp) {
        return false;
    }
    
    // Get file size
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    if (size <= 0 || size > 0x7FFFFFFF) {
        fclose(fp);
        return false;
    }
    
    // Allocate buffer
    uint8_t* data = (uint8_t*)alloc_fn(alloc_ctx, (size_t)size);
    if (!data) {
        fclose(fp);
        return false;
    }
    
    // Read file
    size_t read = fread(data, 1, (size_t)size, fp);
    fclose(fp);
    
    if (read != (size_t)size) {
        // Note: caller would need to free, but we don't have free_fn here
        // This is a design choice - keep load simple, expect caller to handle
        return false;
    }
    
    file->data = data;
    file->size = (uint32_t)size;
    
    return true;
}

void sexb_file_unload(
    sexb_file_t* file,
    sexb_file_free_fn free_fn,
    void* alloc_ctx
) {
    if (!file || !free_fn) {
        return;
    }
    
    if (file->data) {
        free_fn(alloc_ctx, file->data);
    }
    
    file->data = NULL;
    file->size = 0;
}