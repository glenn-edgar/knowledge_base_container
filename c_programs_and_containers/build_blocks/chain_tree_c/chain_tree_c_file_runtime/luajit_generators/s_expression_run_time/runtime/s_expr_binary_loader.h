// ============================================================================
// s_expr_binary_loader.h
// ChainTree S-Expression Binary Module Loader
// 
// Loads binary modules from:
//   - ROM (flash) - zero-copy, in-place
//   - RAM (malloc'd) - copy + resolve
//
// ARM32/ARM64 compatible
// ============================================================================

#ifndef S_EXPR_BINARY_LOADER_H
#define S_EXPR_BINARY_LOADER_H

#include "s_expr_binary.h"
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// CONFIGURATION
// ============================================================================

#ifndef SEXB_MALLOC
#define SEXB_MALLOC(size) malloc(size)
#endif

#ifndef SEXB_FREE
#define SEXB_FREE(ptr) free(ptr)
#endif

#ifndef SEXB_REALLOC
#define SEXB_REALLOC(ptr, size) realloc(ptr, size)
#endif

// ============================================================================
// LOAD RESULT
// ============================================================================

typedef struct {
    sexb_error_t error;
    sexb_module_t* module;
    
    // Internal allocations (for cleanup)
    void* trees_alloc;
    void* records_alloc;
    void* fields_alloc;
    void* constants_alloc;
} sexb_load_result_t;

// ============================================================================
// LOADER FUNCTIONS
// ============================================================================

// Load from ROM (zero-copy where possible)
// data must remain valid for lifetime of module
static inline sexb_load_result_t sexb_load_rom(const void* data, size_t size) {
    sexb_load_result_t result = {0};
    
    result.error = sexb_validate(data, size);
    if (result.error != SEXB_OK) {
        return result;
    }
    
    const uint8_t* base = (const uint8_t*)data;
    const sexb_header_t* hdr = (const sexb_header_t*)base;
    const sexb_directory_t* dir = (const sexb_directory_t*)(base + sizeof(sexb_header_t));
    
    // Allocate module structure
    result.module = (sexb_module_t*)SEXB_MALLOC(sizeof(sexb_module_t));
    if (!result.module) {
        result.error = SEXB_ERR_ALLOC_FAILED;
        return result;
    }
    memset(result.module, 0, sizeof(sexb_module_t));
    
    sexb_module_t* mod = result.module;
    
    // Copy header info
    mod->name_hash = hdr->module_name_hash;
    mod->flags = hdr->flags;
    mod->tree_count = hdr->tree_count;
    mod->record_count = hdr->record_count;
    mod->string_count = hdr->string_count;
    mod->const_count = hdr->const_count;
    mod->oneshot_count = hdr->oneshot_count;
    mod->main_count = hdr->main_count;
    mod->pred_count = hdr->pred_count;
    mod->raw_data = base;
    mod->raw_size = size;
    
    // Allocate and resolve trees
    if (mod->tree_count > 0) {
        sexb_tree_t* trees = (sexb_tree_t*)SEXB_MALLOC(mod->tree_count * sizeof(sexb_tree_t));
        if (!trees) {
            result.error = SEXB_ERR_ALLOC_FAILED;
            SEXB_FREE(mod);
            result.module = NULL;
            return result;
        }
        result.trees_alloc = trees;
        
        const sexb_tree_def_t* tree_defs = (const sexb_tree_def_t*)(base + dir->tree_table_offset);
        
        for (uint16_t i = 0; i < mod->tree_count; i++) {
            trees[i].name_hash = tree_defs[i].name_hash;
            trees[i].record_index = tree_defs[i].record_index;
            trees[i].node_count = tree_defs[i].node_count;
            trees[i].bytecode = base + tree_defs[i].bytecode_offset;
            trees[i].bytecode_size = tree_defs[i].bytecode_size;
        }
        
        mod->trees = trees;
    }
    
    // Allocate and resolve records + fields
    if (mod->record_count > 0) {
        sexb_record_t* records = (sexb_record_t*)SEXB_MALLOC(mod->record_count * sizeof(sexb_record_t));
        if (!records) {
            result.error = SEXB_ERR_ALLOC_FAILED;
            if (result.trees_alloc) SEXB_FREE(result.trees_alloc);
            SEXB_FREE(mod);
            result.module = NULL;
            return result;
        }
        result.records_alloc = records;
        
        const sexb_record_def_t* rec_defs = (const sexb_record_def_t*)(base + dir->record_table_offset);
        
        // Count total fields
        uint32_t total_fields = 0;
        for (uint16_t i = 0; i < mod->record_count; i++) {
            total_fields += rec_defs[i].field_count;
        }
        
        sexb_field_t* all_fields = NULL;
        if (total_fields > 0) {
            all_fields = (sexb_field_t*)SEXB_MALLOC(total_fields * sizeof(sexb_field_t));
            if (!all_fields) {
                result.error = SEXB_ERR_ALLOC_FAILED;
                if (result.trees_alloc) SEXB_FREE(result.trees_alloc);
                SEXB_FREE(records);
                SEXB_FREE(mod);
                result.module = NULL;
                return result;
            }
            result.fields_alloc = all_fields;
        }
        
        // Resolve records and fields
        sexb_field_t* field_ptr = all_fields;
        for (uint16_t i = 0; i < mod->record_count; i++) {
            records[i].name_hash = rec_defs[i].name_hash;
            records[i].field_count = rec_defs[i].field_count;
            records[i].size = rec_defs[i].size;
            records[i].fields = field_ptr;
            
            const sexb_field_def_t* field_defs = (const sexb_field_def_t*)(base + rec_defs[i].field_table_offset);
            for (uint16_t j = 0; j < rec_defs[i].field_count; j++) {
                field_ptr->name_hash = field_defs[j].name_hash;
                field_ptr->type_tag = field_defs[j].type_tag;
                field_ptr->flags = field_defs[j].flags;
                field_ptr->offset = field_defs[j].offset;
                field_ptr->size = field_defs[j].size;
                field_ptr->aux = field_defs[j].aux;
                field_ptr++;
            }
        }
        
        mod->records = records;
    }
    
    // Resolve constants
    if (mod->const_count > 0) {
        sexb_const_t* constants = (sexb_const_t*)SEXB_MALLOC(mod->const_count * sizeof(sexb_const_t));
        if (!constants) {
            result.error = SEXB_ERR_ALLOC_FAILED;
            if (result.trees_alloc) SEXB_FREE(result.trees_alloc);
            if (result.records_alloc) SEXB_FREE(result.records_alloc);
            if (result.fields_alloc) SEXB_FREE(result.fields_alloc);
            SEXB_FREE(mod);
            result.module = NULL;
            return result;
        }
        result.constants_alloc = constants;
        
        const sexb_const_def_t* const_defs = (const sexb_const_def_t*)(base + dir->const_table_offset);
        
        for (uint16_t i = 0; i < mod->const_count; i++) {
            constants[i].name_hash = const_defs[i].name_hash;
            constants[i].record_index = const_defs[i].record_index;
            constants[i].data_size = const_defs[i].data_size;
            constants[i].data = base + const_defs[i].data_offset;
        }
        
        mod->constants = constants;
    }
    
    // String blob (direct pointer)
    mod->string_blob = base + dir->string_blob_offset;
    mod->string_blob_size = dir->const_table_offset - dir->string_blob_offset;
    
    // Function hash tables (direct pointers)
    const uint32_t* func_base = (const uint32_t*)(base + dir->func_table_offset);
    mod->oneshot_hashes = func_base;
    mod->main_hashes = func_base + mod->oneshot_count;
    mod->pred_hashes = func_base + mod->oneshot_count + mod->main_count;
    
    result.error = SEXB_OK;
    return result;
}

// Free loaded module
static inline void sexb_free(sexb_load_result_t* result) {
    if (!result) return;
    
    if (result->trees_alloc) SEXB_FREE(result->trees_alloc);
    if (result->records_alloc) SEXB_FREE(result->records_alloc);
    if (result->fields_alloc) SEXB_FREE(result->fields_alloc);
    if (result->constants_alloc) SEXB_FREE(result->constants_alloc);
    if (result->module) SEXB_FREE(result->module);
    
    memset(result, 0, sizeof(*result));
}

// Load from RAM buffer (makes copy)
static inline sexb_load_result_t sexb_load_copy(const void* data, size_t size) {
    sexb_load_result_t result = {0};
    
    result.error = sexb_validate(data, size);
    if (result.error != SEXB_OK) {
        return result;
    }
    
    uint8_t* copy = (uint8_t*)SEXB_MALLOC(size);
    if (!copy) {
        result.error = SEXB_ERR_ALLOC_FAILED;
        return result;
    }
    memcpy(copy, data, size);
    
    result = sexb_load_rom(copy, size);
    // Note: copy is now owned by module (via raw_data)
    
    return result;
}

// Free module loaded via sexb_load_copy
static inline void sexb_free_copy(sexb_load_result_t* result) {
    if (!result || !result->module) return;
    
    if (result->module->raw_data) {
        SEXB_FREE((void*)result->module->raw_data);
    }
    
    sexb_free(result);
}

// ============================================================================
// CONVENIENCE MACROS
// ============================================================================

#define SEXB_LOAD_STATIC(bin_array) \
    sexb_load_rom(bin_array, sizeof(bin_array))

// ============================================================================
// BYTECODE PARAMETER READER
// ============================================================================

typedef struct {
    uint8_t opcode;
    union {
        int32_t i32;
        uint32_t u32;
        float f32;
        int64_t i64;
        uint64_t u64;
        double f64;
        uint32_t str_idx;
        uint32_t field_hash;
        uint32_t const_idx;
        uint32_t result_code;
    } value;
} sexb_param_t;

static inline bool sexb_read_param(sexb_bytecode_reader_t* r, sexb_param_t* param) {
    if (sexb_reader_eof(r)) return false;
    
    param->opcode = sexb_read_u8(r);
    
    switch (param->opcode) {
        case SEXB_OP_INT:
            param->value.i32 = sexb_read_i32(r);
            break;
        case SEXB_OP_UINT:
            param->value.u32 = sexb_read_u32(r);
            break;
        case SEXB_OP_FLOAT:
            param->value.f32 = sexb_read_f32(r);
            break;
        case SEXB_OP_STR_IDX:
            param->value.str_idx = sexb_read_u32(r);
            break;
        case SEXB_OP_FIELD_REF:
        case SEXB_OP_NESTED_REF:
            param->value.field_hash = sexb_read_u32(r);
            break;
        case SEXB_OP_CONST_REF:
            param->value.const_idx = sexb_read_u32(r);
            break;
        case SEXB_OP_RESULT:
            param->value.result_code = sexb_read_u32(r);
            break;
        case SEXB_OP_INT64:
            param->value.i64 = sexb_read_i64(r);
            break;
        case SEXB_OP_UINT64:
            param->value.u64 = sexb_read_u64(r);
            break;
        case SEXB_OP_DOUBLE:
            param->value.f64 = sexb_read_f64(r);
            break;
        case SEXB_OP_LIST_START:
        case SEXB_OP_LIST_END:
        case SEXB_OP_CALL_START:
        case SEXB_OP_CALL_END:
            break;
        default:
            return false;
    }
    
    return true;
}

// ============================================================================
// FIELD RESOLUTION HELPERS
// ============================================================================

static inline int32_t sexb_resolve_field_offset(
    const sexb_module_t* mod,
    uint16_t record_index,
    uint32_t field_hash
) {
    if (!mod || record_index >= mod->record_count) return -1;
    
    const sexb_record_t* rec = &mod->records[record_index];
    const sexb_field_t* field = sexb_find_field(rec, field_hash);
    
    return field ? field->offset : -1;
}

static inline void* sexb_get_field_ptr(
    void* blackboard,
    const sexb_module_t* mod,
    uint16_t record_index,
    uint32_t field_hash
) {
    int32_t offset = sexb_resolve_field_offset(mod, record_index, field_hash);
    if (offset < 0) return NULL;
    return (uint8_t*)blackboard + offset;
}

static inline bool sexb_copy_const_to_field(
    void* blackboard,
    const sexb_module_t* mod,
    uint16_t record_index,
    uint32_t field_hash,
    uint32_t const_hash
) {
    void* field_ptr = sexb_get_field_ptr(blackboard, mod, record_index, field_hash);
    if (!field_ptr) return false;
    
    const sexb_const_t* cnst = sexb_find_const(mod, const_hash);
    if (!cnst || !cnst->data) return false;
    
    memcpy(field_ptr, cnst->data, cnst->data_size);
    return true;
}

static inline bool sexb_copy_const_full(
    void* blackboard,
    size_t bb_size,
    const sexb_module_t* mod,
    uint32_t const_hash
) {
    const sexb_const_t* cnst = sexb_find_const(mod, const_hash);
    if (!cnst || !cnst->data) return false;
    
    size_t copy_size = (cnst->data_size < bb_size) ? cnst->data_size : bb_size;
    memcpy(blackboard, cnst->data, copy_size);
    return true;
}

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_LOADER_H