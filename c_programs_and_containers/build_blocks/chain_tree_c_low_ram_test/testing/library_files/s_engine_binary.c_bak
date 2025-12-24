// ============================================================================
// s_engine_binary.c
// Binary Module Parser Implementation
// ============================================================================

#include "s_engine_binary.h"
#include <string.h>

// ============================================================================
// LITTLE-ENDIAN READ HELPERS
// ============================================================================

static inline uint16_t read_u16(const uint8_t* p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline uint32_t read_u32(const uint8_t* p) {
    return (uint32_t)p[0] | 
           ((uint32_t)p[1] << 8) | 
           ((uint32_t)p[2] << 16) | 
           ((uint32_t)p[3] << 24);
}

static inline int32_t read_i32(const uint8_t* p) {
    return (int32_t)read_u32(p);
}

static inline float read_f32(const uint8_t* p) {
    uint32_t bits = read_u32(p);
    float f;
    memcpy(&f, &bits, sizeof(f));
    return f;
}

// ============================================================================
// ERROR STRINGS
// ============================================================================

const char* bin_error_string(int error_code) {
    switch (error_code) {
        case BIN_OK:            return "OK";
        case BIN_ERR_NULL_INPUT: return "NULL input pointer";
        case BIN_ERR_TOO_SMALL: return "Buffer too small for header";
        case BIN_ERR_BAD_MAGIC: return "Invalid magic number";
        case BIN_ERR_BAD_VERSION: return "Unsupported version";
        case BIN_ERR_TRUNCATED: return "Truncated data";
        case BIN_ERR_ALLOC:     return "Memory allocation failed";
        case BIN_ERR_BAD_OFFSET: return "Invalid offset in file";
        default:                return "Unknown error";
    }
}

// ============================================================================
// HEADER VALIDATION
// ============================================================================

int bin_validate_header(const uint8_t* data, uint32_t size) {
    if (!data) {
        return BIN_ERR_NULL_INPUT;
    }
    
    if (size < sizeof(bin_header_t)) {
        return BIN_ERR_TOO_SMALL;
    }
    
    uint32_t magic = read_u32(data + 0);
    if (magic != SMOD_MAGIC) {
        return BIN_ERR_BAD_MAGIC;
    }
    
    uint16_t version = read_u16(data + 4);
    if (version != SMOD_VERSION) {
        return BIN_ERR_BAD_VERSION;
    }
    
    return BIN_OK;
}

// ============================================================================
// GET MODULE INFO (without full parse)
// ============================================================================

int bin_get_module_info(
    const uint8_t* data,
    uint32_t size,
    bin_module_info_t* info
) {
    int err = bin_validate_header(data, size);
    if (err != BIN_OK) {
        return err;
    }
    
    if (!info) {
        return BIN_ERR_NULL_INPUT;
    }
    
    // Parse header
    uint16_t tree_count = read_u16(data + 8);
    uint16_t oneshot_count = read_u16(data + 10);
    uint16_t boolean_count = read_u16(data + 12);
    uint16_t main_count = read_u16(data + 14);
    uint16_t string_count = read_u16(data + 16);
    uint16_t max_node_count = read_u16(data + 18);
    uint32_t module_name_offset = read_u32(data + 20);
    uint32_t string_blob_offset = read_u32(data + 24);
    
    // Check string blob is accessible
    if (string_blob_offset >= size) {
        return BIN_ERR_BAD_OFFSET;
    }
    
    // Get module name (points into buffer, length-prefixed)
    const uint8_t* string_blob = data + string_blob_offset;
    const uint8_t* name_ptr = string_blob + module_name_offset;
    
    // Note: name points into data buffer, caller must keep buffer alive
    // Skip length prefix to get actual string
    info->name = (const char*)(name_ptr + 2);
    info->tree_count = tree_count;
    info->max_node_count = max_node_count;
    info->oneshot_count = oneshot_count;
    info->boolean_count = boolean_count;
    info->main_count = main_count;
    info->string_count = string_count;
    info->total_size = size;
    
    return BIN_OK;
}

// ============================================================================
// STRING READING HELPER
// Reads length-prefixed string from blob, allocates and copies
// ============================================================================

static char* read_string_alloc(
    const uint8_t* string_blob,
    uint32_t offset,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    const uint8_t* p = string_blob + offset;
    uint16_t len = read_u16(p);
    
    char* s = (char*)alloc->malloc(handle, ct_node_id, len + 1);
    if (!s) return NULL;
    
    memcpy(s, p + 2, len);
    s[len] = '\0';
    return s;
}

// ============================================================================
// CLEANUP HELPER
// ============================================================================

static void cleanup_partial(
    bin_module_t* bmod,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id,
    uint16_t trees_allocated,
    uint16_t oneshot_names_allocated,
    uint16_t boolean_names_allocated,
    uint16_t main_names_allocated,
    uint16_t strings_allocated
) {
    if (!bmod) return;
    
    // Free tree data
    if (bmod->nodes_alloc) {
        for (uint16_t i = 0; i < trees_allocated; i++) {
            if (bmod->nodes_alloc[i]) {
                alloc->free(handle, ct_node_id, bmod->nodes_alloc[i]);
            }
        }
        alloc->free(handle, ct_node_id, bmod->nodes_alloc);
    }
    
    if (bmod->params_alloc) {
        for (uint16_t i = 0; i < trees_allocated; i++) {
            if (bmod->params_alloc[i]) {
                alloc->free(handle, ct_node_id, bmod->params_alloc[i]);
            }
        }
        alloc->free(handle, ct_node_id, bmod->params_alloc);
    }
    
    if (bmod->trees_alloc) {
        for (uint16_t i = 0; i < trees_allocated; i++) {
            if (bmod->trees_alloc[i].name) {
                alloc->free(handle, ct_node_id, (void*)bmod->trees_alloc[i].name);
            }
        }
        alloc->free(handle, ct_node_id, bmod->trees_alloc);
    }
    
    // Free function name arrays
    if (bmod->def.oneshot_names) {
        for (uint16_t i = 0; i < oneshot_names_allocated; i++) {
            if (bmod->def.oneshot_names[i]) {
                alloc->free(handle, ct_node_id, (void*)bmod->def.oneshot_names[i]);
            }
        }
        alloc->free(handle, ct_node_id, (void*)bmod->def.oneshot_names);
    }
    
    if (bmod->def.boolean_names) {
        for (uint16_t i = 0; i < boolean_names_allocated; i++) {
            if (bmod->def.boolean_names[i]) {
                alloc->free(handle, ct_node_id, (void*)bmod->def.boolean_names[i]);
            }
        }
        alloc->free(handle, ct_node_id, (void*)bmod->def.boolean_names);
    }
    
    if (bmod->def.main_names) {
        for (uint16_t i = 0; i < main_names_allocated; i++) {
            if (bmod->def.main_names[i]) {
                alloc->free(handle, ct_node_id, (void*)bmod->def.main_names[i]);
            }
        }
        alloc->free(handle, ct_node_id, (void*)bmod->def.main_names);
    }
    
    if (bmod->def.strings) {
        for (uint16_t i = 0; i < strings_allocated; i++) {
            if (bmod->def.strings[i]) {
                alloc->free(handle, ct_node_id, (void*)bmod->def.strings[i]);
            }
        }
        alloc->free(handle, ct_node_id, (void*)bmod->def.strings);
    }
    
    // Free module name
    if (bmod->def.name) {
        alloc->free(handle, ct_node_id, (void*)bmod->def.name);
    }
    
    // Free bmod itself
    alloc->free(handle, ct_node_id, bmod);
}

// ============================================================================
// MAIN PARSER
// ============================================================================

bin_module_t* bin_parse_module(
    const uint8_t* data,
    uint32_t size,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    // Validate inputs
    if (!data || !alloc || !alloc->malloc || !alloc->free) {
        return NULL;
    }
    
    // Validate header
    int err = bin_validate_header(data, size);
    if (err != BIN_OK) {
        return NULL;
    }
    
    // Allocate bin_module_t
    bin_module_t* bmod = (bin_module_t*)alloc->malloc(handle, ct_node_id, sizeof(bin_module_t));
    if (!bmod) {
        return NULL;
    }
    memset(bmod, 0, sizeof(bin_module_t));
    
    bmod->source_data = data;
    bmod->source_size = size;
    
    // Parse header fields
    uint16_t tree_count = read_u16(data + 8);
    uint16_t oneshot_count = read_u16(data + 10);
    uint16_t boolean_count = read_u16(data + 12);
    uint16_t main_count = read_u16(data + 14);
    uint16_t string_count = read_u16(data + 16);
    uint16_t max_node_count = read_u16(data + 18);
    uint32_t module_name_offset = read_u32(data + 20);
    uint32_t string_blob_offset = read_u32(data + 24);
    uint32_t tree_dir_offset = read_u32(data + 28);
    
    // Validate offsets
    if (string_blob_offset >= size || tree_dir_offset >= size) {
        bmod->error_code = BIN_ERR_BAD_OFFSET;
        cleanup_partial(bmod, alloc, handle, ct_node_id, 0, 0, 0, 0, 0);
        return NULL;
    }
    
    const uint8_t* string_blob = data + string_blob_offset;
    
    // Tracking for cleanup on error
    uint16_t trees_allocated = 0;
    uint16_t oneshot_names_allocated = 0;
    uint16_t boolean_names_allocated = 0;
    uint16_t main_names_allocated = 0;
    uint16_t strings_allocated = 0;
    
    // Read module name
    bmod->def.name = read_string_alloc(string_blob, module_name_offset, alloc, handle, ct_node_id);
    if (!bmod->def.name) {
        bmod->error_code = BIN_ERR_ALLOC;
        cleanup_partial(bmod, alloc, handle, ct_node_id, 0, 0, 0, 0, 0);
        return NULL;
    }
    
    // Set counts
    bmod->def.tree_count = tree_count;
    bmod->def.oneshot_count = oneshot_count;
    bmod->def.boolean_count = boolean_count;
    bmod->def.main_count = main_count;
    bmod->def.string_count = string_count;
    bmod->def.max_node_count = max_node_count;
    
    // ========================================================================
    // Read string index tables (after header)
    // ========================================================================
    
    const uint8_t* p = data + sizeof(bin_header_t);
    
    // Oneshot names
    if (oneshot_count > 0) {
        const char** names = (const char**)alloc->malloc(
            handle, ct_node_id, oneshot_count * sizeof(char*)
        );
        if (!names) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0, 0, 0, 0, 0);
            return NULL;
        }
        memset(names, 0, oneshot_count * sizeof(char*));
        bmod->def.oneshot_names = names;
        
        for (uint16_t i = 0; i < oneshot_count; i++) {
            uint32_t off = read_u32(p);
            p += 4;
            names[i] = read_string_alloc(string_blob, off, alloc, handle, ct_node_id);
            if (!names[i]) {
                bmod->error_code = BIN_ERR_ALLOC;
                cleanup_partial(bmod, alloc, handle, ct_node_id, 0, i, 0, 0, 0);
                return NULL;
            }
            oneshot_names_allocated = i + 1;
        }
    }
    
    // Boolean names
    if (boolean_count > 0) {
        const char** names = (const char**)alloc->malloc(
            handle, ct_node_id, boolean_count * sizeof(char*)
        );
        if (!names) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0, 
                           oneshot_names_allocated, 0, 0, 0);
            return NULL;
        }
        memset(names, 0, boolean_count * sizeof(char*));
        bmod->def.boolean_names = names;
        
        for (uint16_t i = 0; i < boolean_count; i++) {
            uint32_t off = read_u32(p);
            p += 4;
            names[i] = read_string_alloc(string_blob, off, alloc, handle, ct_node_id);
            if (!names[i]) {
                bmod->error_code = BIN_ERR_ALLOC;
                cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                               oneshot_names_allocated, i, 0, 0);
                return NULL;
            }
            boolean_names_allocated = i + 1;
        }
    }
    
    // Main names
    if (main_count > 0) {
        const char** names = (const char**)alloc->malloc(
            handle, ct_node_id, main_count * sizeof(char*)
        );
        if (!names) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                           oneshot_names_allocated, boolean_names_allocated, 0, 0);
            return NULL;
        }
        memset(names, 0, main_count * sizeof(char*));
        bmod->def.main_names = names;
        
        for (uint16_t i = 0; i < main_count; i++) {
            uint32_t off = read_u32(p);
            p += 4;
            names[i] = read_string_alloc(string_blob, off, alloc, handle, ct_node_id);
            if (!names[i]) {
                bmod->error_code = BIN_ERR_ALLOC;
                cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                               oneshot_names_allocated, boolean_names_allocated, i, 0);
                return NULL;
            }
            main_names_allocated = i + 1;
        }
    }
    
    // Data strings
    if (string_count > 0) {
        const char** strings = (const char**)alloc->malloc(
            handle, ct_node_id, string_count * sizeof(char*)
        );
        if (!strings) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                           oneshot_names_allocated, boolean_names_allocated,
                           main_names_allocated, 0);
            return NULL;
        }
        memset(strings, 0, string_count * sizeof(char*));
        bmod->def.strings = strings;
        
        for (uint16_t i = 0; i < string_count; i++) {
            uint32_t off = read_u32(p);
            p += 4;
            strings[i] = read_string_alloc(string_blob, off, alloc, handle, ct_node_id);
            if (!strings[i]) {
                bmod->error_code = BIN_ERR_ALLOC;
                cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                               oneshot_names_allocated, boolean_names_allocated,
                               main_names_allocated, i);
                return NULL;
            }
            strings_allocated = i + 1;
        }
    }
    
    // ========================================================================
    // Read tree definitions
    // ========================================================================
    
    if (tree_count > 0) {
        // Allocate trees array
        bmod->trees_alloc = (tree_def_t*)alloc->malloc(
            handle, ct_node_id, tree_count * sizeof(tree_def_t)
        );
        if (!bmod->trees_alloc) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                           oneshot_names_allocated, boolean_names_allocated,
                           main_names_allocated, strings_allocated);
            return NULL;
        }
        memset(bmod->trees_alloc, 0, tree_count * sizeof(tree_def_t));
        bmod->def.trees = bmod->trees_alloc;
        
        // Allocate tracking arrays
        bmod->nodes_alloc = (node_t**)alloc->malloc(
            handle, ct_node_id, tree_count * sizeof(node_t*)
        );
        bmod->params_alloc = (param_t**)alloc->malloc(
            handle, ct_node_id, tree_count * sizeof(param_t*)
        );
        
        if (!bmod->nodes_alloc || !bmod->params_alloc) {
            bmod->error_code = BIN_ERR_ALLOC;
            cleanup_partial(bmod, alloc, handle, ct_node_id, 0,
                           oneshot_names_allocated, boolean_names_allocated,
                           main_names_allocated, strings_allocated);
            return NULL;
        }
        memset(bmod->nodes_alloc, 0, tree_count * sizeof(node_t*));
        memset(bmod->params_alloc, 0, tree_count * sizeof(param_t*));
        
        // Parse each tree
        const uint8_t* tree_dir = data + tree_dir_offset;
        
        for (uint16_t t = 0; t < tree_count; t++) {
            const uint8_t* entry = tree_dir + t * 16;  // sizeof(bin_tree_entry_t)
            
            uint32_t name_off = read_u32(entry + 0);
            uint16_t node_count = read_u16(entry + 4);
            uint16_t param_count = read_u16(entry + 6);
            uint32_t nodes_offset = read_u32(entry + 8);
            uint32_t params_offset = read_u32(entry + 12);
            
            // Validate offsets
            if (nodes_offset >= size || (param_count > 0 && params_offset >= size)) {
                bmod->error_code = BIN_ERR_BAD_OFFSET;
                bmod->error_offset = nodes_offset;
                cleanup_partial(bmod, alloc, handle, ct_node_id, t,
                               oneshot_names_allocated, boolean_names_allocated,
                               main_names_allocated, strings_allocated);
                return NULL;
            }
            
            // Read tree name
            bmod->trees_alloc[t].name = read_string_alloc(
                string_blob, name_off, alloc, handle, ct_node_id
            );
            if (!bmod->trees_alloc[t].name) {
                bmod->error_code = BIN_ERR_ALLOC;
                cleanup_partial(bmod, alloc, handle, ct_node_id, t,
                               oneshot_names_allocated, boolean_names_allocated,
                               main_names_allocated, strings_allocated);
                return NULL;
            }
            
            bmod->trees_alloc[t].node_count = node_count;
            bmod->trees_alloc[t].param_count = param_count;
            bmod->trees_alloc[t].root_index = 0;
            
            // Read nodes
            if (node_count > 0) {
                node_t* nodes = (node_t*)alloc->malloc(
                    handle, ct_node_id, node_count * sizeof(node_t)
                );
                if (!nodes) {
                    bmod->error_code = BIN_ERR_ALLOC;
                    cleanup_partial(bmod, alloc, handle, ct_node_id, t + 1,
                                   oneshot_names_allocated, boolean_names_allocated,
                                   main_names_allocated, strings_allocated);
                    return NULL;
                }
                bmod->nodes_alloc[t] = nodes;
                bmod->trees_alloc[t].nodes = nodes;
                
                const uint8_t* src = data + nodes_offset;
                for (uint16_t i = 0; i < node_count; i++) {
                    nodes[i].type         = src[0];
                    nodes[i].child_count  = src[1];
                    nodes[i].node_index   = read_u16(src + 2);
                    nodes[i].first_child  = read_u16(src + 4);
                    nodes[i].next_sibling = read_u16(src + 6);
                    nodes[i].fn_index     = read_u16(src + 8);
                    nodes[i].param_offset = read_u16(src + 10);
                    nodes[i].param_count  = src[12];
                    nodes[i].reserved     = src[13];
                    src += 14;  // sizeof(bin_node_t)
                }
            }
            
            // Read params
            if (param_count > 0) {
                param_t* params = (param_t*)alloc->malloc(
                    handle, ct_node_id, param_count * sizeof(param_t)
                );
                if (!params) {
                    bmod->error_code = BIN_ERR_ALLOC;
                    cleanup_partial(bmod, alloc, handle, ct_node_id, t + 1,
                                   oneshot_names_allocated, boolean_names_allocated,
                                   main_names_allocated, strings_allocated);
                    return NULL;
                }
                bmod->params_alloc[t] = params;
                bmod->trees_alloc[t].params = params;
                
                const uint8_t* src = data + params_offset;
                for (uint16_t i = 0; i < param_count; i++) {
                    params[i].type = src[0];
                    // src[1..3] reserved
                    
                    // Read value based on type
                    switch (params[i].type) {
                        case PARAM_INT32:
                            params[i].i32 = read_i32(src + 4);
                            break;
                        case PARAM_UINT32:
                            params[i].u32 = read_u32(src + 4);
                            break;
                        case PARAM_FLOAT32:
                            params[i].f32 = read_f32(src + 4);
                            break;
                        case PARAM_STRING:
                            params[i].str_index = (uint16_t)read_u32(src + 4);
                            break;
                        default:
                            params[i].u32 = read_u32(src + 4);
                            break;
                    }
                    src += 8;  // sizeof(bin_param_t)
                }
            }
            
            trees_allocated = t + 1;
        }
    }
    
    bmod->error_code = BIN_OK;
    return bmod;
}

// ============================================================================
// FREE MODULE
// ============================================================================

void bin_free_module(
    bin_module_t* bmod,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    if (!bmod || !alloc || !alloc->free) return;
    
    cleanup_partial(
        bmod, alloc, handle, ct_node_id,
        bmod->def.tree_count,
        bmod->def.oneshot_count,
        bmod->def.boolean_count,
        bmod->def.main_count,
        bmod->def.string_count
    );
}