// ============================================================================
// s_expr_binary_runtime.h
// ChainTree S-Expression Binary Runtime Bridge
// 
// Converts binary modules (sexb_module_t) to s_expr_module_def_t format
// for use with the existing s_engine_module.c and s_engine_eval.c runtime.
//
// CRITICAL FIX: Uses func_type directly (1-6) to select hash table, not
// S_EXPR_PARAM_* constants which may have different values.
// ============================================================================

#ifndef S_EXPR_BINARY_RUNTIME_H
#define S_EXPR_BINARY_RUNTIME_H

#include "s_expr_binary.h"
#include "s_expr_binary_loader.h"
#include "s_engine_types.h"

#ifdef SEXB_DEBUG
#include <stdio.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// BRIDGE STRUCTURES
// ============================================================================

typedef struct {
    s_expr_module_def_t def;
    
    // Internal allocations
    s_expr_tree_def_t* trees;
    s_expr_record_desc_t* records;
    s_expr_field_desc_t* fields;
    s_expr_param_t* params;
    const char** string_table;
    s_expr_hash_t* oneshot_hashes;
    s_expr_hash_t* main_hashes;
    s_expr_hash_t* pred_hashes;
    const void** constants;
    
    // Source module (must remain valid)
    const sexb_module_t* source;
    
    sexb_error_t error;
} sexb_runtime_t;

// ============================================================================
// API
// ============================================================================

static inline sexb_error_t sexb_create_runtime(
    sexb_runtime_t* runtime,
    const sexb_module_t* source
);

static inline void sexb_free_runtime(sexb_runtime_t* runtime);

static inline const s_expr_module_def_t* sexb_get_module_def(const sexb_runtime_t* runtime) {
    return runtime ? &runtime->def : NULL;
}

// Combined load + convert
typedef struct {
    sexb_load_result_t load;
    sexb_runtime_t runtime;
    sexb_error_t error;
} sexb_full_load_t;

static inline sexb_error_t sexb_load_and_create(
    sexb_full_load_t* result,
    const void* data,
    size_t size,
    bool copy_data
);

static inline void sexb_full_free(sexb_full_load_t* result, bool free_copy);

// ============================================================================
// IMPLEMENTATION
// ============================================================================

#ifndef SEXB_RUNTIME_MALLOC
#define SEXB_RUNTIME_MALLOC(size) SEXB_MALLOC(size)
#endif

#ifndef SEXB_RUNTIME_FREE
#define SEXB_RUNTIME_FREE(ptr) SEXB_FREE(ptr)
#endif

// ============================================================================
// Map binary func_type to s_expr param type + flags
// ============================================================================

static inline void sexb_map_func_type(
    uint8_t sexb_func_type,
    uint8_t* out_opcode,
    uint8_t* out_flags
) {
    *out_flags = 0;
    
    switch (sexb_func_type) {
        case SEXB_FUNC_ONESHOT:
            *out_opcode = S_EXPR_PARAM_ONESHOT;
            break;
        case SEXB_FUNC_MAIN:
            *out_opcode = S_EXPR_PARAM_MAIN;
            break;
        case SEXB_FUNC_PRED:
            *out_opcode = S_EXPR_PARAM_PRED;
            break;
        case SEXB_FUNC_PT_MAIN:
            *out_opcode = S_EXPR_PARAM_MAIN;
            *out_flags = S_EXPR_FLAG_POINTER;
            break;
        case SEXB_FUNC_INIT_ONE:
            *out_opcode = S_EXPR_PARAM_ONESHOT;
            *out_flags = S_EXPR_FLAG_SURVIVES_RESET;
            break;
        case SEXB_FUNC_BIT_PRED:
            *out_opcode = S_EXPR_PARAM_PRED;
            break;
        default:
            *out_opcode = S_EXPR_PARAM_MAIN;
            break;
    }
}

// ============================================================================
// Count params for a tree
// ============================================================================

static inline int32_t sexb_count_tree_params(const sexb_tree_t* tree) {
    if (!tree || !tree->bytecode || tree->bytecode_size == 0) return 0;
    
    int32_t total = 0;
    uint32_t pos = 0;
    
    while (pos + 8 <= tree->bytecode_size) {
        uint16_t node_size = tree->bytecode[pos + 6] | (tree->bytecode[pos + 7] << 8);
        uint8_t param_count = tree->bytecode[pos + 5];
        
        // Each node: OPEN_CALL + FUNC_REF + params + CLOSE
        total += 2 + param_count + 1;
        pos += node_size;
    }
    
    return total;
}

// ============================================================================
// Decode single node
// ============================================================================

static inline int32_t sexb_decode_node(
    const uint8_t* bytecode,
    uint32_t bytecode_size,
    const sexb_module_t* mod,
    s_expr_param_t* out_params,
    uint16_t max_params,
    uint16_t* inout_node_index,
    uint16_t* inout_pointer_index
) {
    if (!bytecode || bytecode_size < 8) return -1;
    
    sexb_bytecode_reader_t r;
    sexb_reader_init(&r, bytecode, bytecode_size);
    
    // Read node header
    uint32_t func_hash = sexb_read_u32(&r);
    uint8_t func_type = sexb_read_u8(&r);
    uint8_t param_count = sexb_read_u8(&r);
    uint16_t node_bytecode_size = sexb_read_u16(&r);
    (void)node_bytecode_size;
    
    // Map to s_expr opcode and flags
    uint8_t base_opcode, type_flags;
    sexb_map_func_type(func_type, &base_opcode, &type_flags);
    
    bool is_pointer_call = (type_flags & S_EXPR_FLAG_POINTER) != 0;
    
    // =========================================================================
    // CRITICAL: Use func_type directly to find function index
    // This uses sexb_find_func_by_type from s_expr_binary.h which maps:
    //   Types 1,5 -> oneshot_hashes
    //   Types 2,4 -> main_hashes  
    //   Types 3,6 -> pred_hashes
    // =========================================================================
    uint16_t func_index = sexb_find_func_by_type(mod, func_type, func_hash);
    
    if (func_index == 0xFFFF) {
        #ifdef SEXB_DEBUG
        const char* type_names[] = {"???", "ONESHOT", "MAIN", "PRED", "PT_MAIN", "INIT_ONE", "BIT_PRED"};
        const char* type_name = (func_type <= 6) ? type_names[func_type] : "???";
        const char* table_name = "???";
        switch (func_type) {
            case 1: case 5: table_name = "oneshot"; break;
            case 2: case 4: table_name = "main"; break;
            case 3: case 6: table_name = "pred"; break;
        }
        fprintf(stderr, "ERROR: Function not found: hash=0x%08X type=%d(%s) -> %s_hashes\n",
                func_hash, func_type, type_name, table_name);
        #endif
        return -1;
    }
    
    // Output count: OPEN_CALL + FUNC_REF + params + CLOSE
    int32_t out_count = 2 + param_count + 1;
    
    if (!out_params) return out_count;
    if (out_count > max_params) return -1;
    
    uint16_t this_node = (*inout_node_index)++;
    uint8_t this_pointer = 0;
    if (is_pointer_call) {
        this_pointer = (uint8_t)(*inout_pointer_index);
        (*inout_pointer_index)++;
    }
    
    memset(out_params, 0, out_count * sizeof(s_expr_param_t));
    
    // OPEN_CALL
    out_params[0].type = S_EXPR_PARAM_OPEN_CALL;
    out_params[0].brace_idx = out_count - 1;
    
    // Function reference
    out_params[1].type = base_opcode | type_flags;
    out_params[1].index_to_pointer = this_pointer;
    out_params[1].node_index = this_node;
    out_params[1].func_index = func_index;
    
    // Decode parameters
    uint16_t out_idx = 2;
    
    for (uint8_t i = 0; i < param_count && !sexb_reader_eof(&r); i++) {
        uint8_t op = sexb_read_u8(&r);
        s_expr_param_t* p = &out_params[out_idx++];
        
        switch (op) {
            case SEXB_OP_INT:
                p->type = S_EXPR_PARAM_INT;
                p->int_val = sexb_read_i32(&r);
                break;
            case SEXB_OP_UINT:
                p->type = S_EXPR_PARAM_UINT;
                p->uint_val = sexb_read_u32(&r);
                break;
            case SEXB_OP_FLOAT:
                p->type = S_EXPR_PARAM_FLOAT;
                p->float_val = sexb_read_f32(&r);
                break;
            case SEXB_OP_STR_IDX:
                p->type = S_EXPR_PARAM_STR_IDX;
                p->str_index = (uint16_t)sexb_read_u32(&r);
                break;
            case SEXB_OP_FIELD_REF:
                p->type = S_EXPR_PARAM_FIELD;
                p->str_hash = sexb_read_u32(&r);
                break;
            case SEXB_OP_NESTED_REF:
                p->type = S_EXPR_PARAM_FIELD;
                p->str_hash = sexb_read_u32(&r);
                break;
            case SEXB_OP_CONST_REF:
                p->type = S_EXPR_PARAM_CONST_REF;
                p->const_index = (uint16_t)sexb_read_u32(&r);
                break;
            case SEXB_OP_RESULT:
                p->type = S_EXPR_PARAM_RESULT;
                p->int_val = sexb_read_i32(&r);
                break;
            case SEXB_OP_INT64:
                p->type = S_EXPR_PARAM_INT;
                p->int_val = (ct_int_t)sexb_read_i64(&r);
                break;
            case SEXB_OP_UINT64:
                p->type = S_EXPR_PARAM_UINT;
                p->uint_val = (ct_uint_t)sexb_read_u64(&r);
                break;
            case SEXB_OP_DOUBLE:
                p->type = S_EXPR_PARAM_FLOAT;
                p->float_val = (ct_float_t)sexb_read_f64(&r);
                break;
            case SEXB_OP_LIST_START:
                p->type = S_EXPR_PARAM_OPEN;
                p->brace_idx = 0;
                break;
            case SEXB_OP_LIST_END:
                p->type = S_EXPR_PARAM_CLOSE;
                break;
            case SEXB_OP_CALL_START:
                p->type = S_EXPR_PARAM_OPEN_CALL;
                p->brace_idx = 0;
                break;
            case SEXB_OP_CALL_END:
                p->type = S_EXPR_PARAM_CLOSE;
                break;
            default:
                return -1;
        }
    }
    
    // CLOSE
    out_params[out_count - 1].type = S_EXPR_PARAM_CLOSE;
    
    return out_count;
}

// ============================================================================
// Decode entire tree
// ============================================================================

static inline int32_t sexb_decode_tree(
    const sexb_tree_t* tree,
    const sexb_module_t* mod,
    s_expr_param_t* out_params,
    uint16_t max_params,
    uint16_t* out_node_count,
    uint16_t* out_pointer_count
) {
    if (!tree || !tree->bytecode) return 0;
    
    uint16_t param_offset = 0;
    uint16_t node_index = 0;
    uint16_t pointer_index = 0;
    uint32_t pos = 0;
    
    while (pos + 8 <= tree->bytecode_size) {
        uint16_t node_size = tree->bytecode[pos + 6] | (tree->bytecode[pos + 7] << 8);
        
        int32_t decoded = sexb_decode_node(
            tree->bytecode + pos,
            node_size,
            mod,
            out_params ? out_params + param_offset : NULL,
            out_params ? max_params - param_offset : 0xFFFF,
            &node_index,
            &pointer_index
        );
        
        if (decoded < 0) return -1;
        
        param_offset += decoded;
        pos += node_size;
    }
    
    if (out_node_count) *out_node_count = node_index;
    if (out_pointer_count) *out_pointer_count = pointer_index;
    
    return param_offset;
}

// ============================================================================
// Resolve field references (hash -> offset)
// ============================================================================

static inline void sexb_resolve_fields(
    s_expr_param_t* params,
    uint16_t param_count,
    const sexb_record_t* record
) {
    if (!params || !record) return;
    
    for (uint16_t i = 0; i < param_count; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FIELD) {
            uint32_t field_hash = params[i].str_hash;
            const sexb_field_t* field = sexb_find_field(record, field_hash);
            if (field) {
                params[i].field_offset = field->offset;
                params[i].field_size = field->size;
            }
        }
    }
}

// ============================================================================
// Main conversion function
// ============================================================================

static inline sexb_error_t sexb_create_runtime(
    sexb_runtime_t* runtime,
    const sexb_module_t* source
) {
    if (!runtime || !source) {
        return SEXB_ERR_INVALID_HEADER;
    }
    
    memset(runtime, 0, sizeof(*runtime));
    runtime->source = source;
    
    // 1. Create string table
    if (source->string_count > 0) {
        runtime->string_table = (const char**)SEXB_RUNTIME_MALLOC(
            source->string_count * sizeof(const char*)
        );
        if (!runtime->string_table) {
            runtime->error = SEXB_ERR_ALLOC_FAILED;
            return SEXB_ERR_ALLOC_FAILED;
        }
        
        for (uint16_t i = 0; i < source->string_count; i++) {
            runtime->string_table[i] = sexb_get_string(source, i);
        }
    }
    
    // 2. Copy function hash arrays
    if (source->oneshot_count > 0) {
        runtime->oneshot_hashes = (s_expr_hash_t*)SEXB_RUNTIME_MALLOC(
            source->oneshot_count * sizeof(s_expr_hash_t)
        );
        if (!runtime->oneshot_hashes) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        memcpy(runtime->oneshot_hashes, source->oneshot_hashes,
               source->oneshot_count * sizeof(s_expr_hash_t));
    }
    
    if (source->main_count > 0) {
        runtime->main_hashes = (s_expr_hash_t*)SEXB_RUNTIME_MALLOC(
            source->main_count * sizeof(s_expr_hash_t)
        );
        if (!runtime->main_hashes) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        memcpy(runtime->main_hashes, source->main_hashes,
               source->main_count * sizeof(s_expr_hash_t));
    }
    
    if (source->pred_count > 0) {
        runtime->pred_hashes = (s_expr_hash_t*)SEXB_RUNTIME_MALLOC(
            source->pred_count * sizeof(s_expr_hash_t)
        );
        if (!runtime->pred_hashes) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        memcpy(runtime->pred_hashes, source->pred_hashes,
               source->pred_count * sizeof(s_expr_hash_t));
    }
    
    // 3. Create record descriptors
    uint32_t total_fields = 0;
    for (uint16_t i = 0; i < source->record_count; i++) {
        total_fields += source->records[i].field_count;
    }
    
    if (source->record_count > 0) {
        runtime->records = (s_expr_record_desc_t*)SEXB_RUNTIME_MALLOC(
            source->record_count * sizeof(s_expr_record_desc_t)
        );
        if (!runtime->records) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        
        if (total_fields > 0) {
            runtime->fields = (s_expr_field_desc_t*)SEXB_RUNTIME_MALLOC(
                total_fields * sizeof(s_expr_field_desc_t)
            );
            if (!runtime->fields) {
                sexb_free_runtime(runtime);
                return SEXB_ERR_ALLOC_FAILED;
            }
        }
        
        s_expr_field_desc_t* field_ptr = runtime->fields;
        
        for (uint16_t i = 0; i < source->record_count; i++) {
            const sexb_record_t* src_rec = &source->records[i];
            s_expr_record_desc_t* dst_rec = &runtime->records[i];
            
            dst_rec->name_hash = src_rec->name_hash;
            dst_rec->total_size = src_rec->size;
            dst_rec->field_count = src_rec->field_count;
            dst_rec->fields = field_ptr;
            
            for (uint16_t j = 0; j < src_rec->field_count; j++) {
                const sexb_field_t* src_fld = &src_rec->fields[j];
                field_ptr->name_hash = src_fld->name_hash;
                field_ptr->offset = src_fld->offset;
                field_ptr->size = src_fld->size;
                field_ptr++;
            }
        }
    }
    
    // 4. Create constant pointers
    if (source->const_count > 0) {
        runtime->constants = (const void**)SEXB_RUNTIME_MALLOC(
            source->const_count * sizeof(const void*)
        );
        if (!runtime->constants) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        
        for (uint16_t i = 0; i < source->const_count; i++) {
            runtime->constants[i] = source->constants[i].data;
        }
    }
    
    // 5. Count total params
    uint32_t total_params = 0;
    uint16_t max_node_count = 0;
    uint16_t max_pointer_count = 0;
    uint16_t max_param_count = 0;
    
    for (uint16_t i = 0; i < source->tree_count; i++) {
        int32_t tree_params = sexb_count_tree_params(&source->trees[i]);
        if (tree_params < 0) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_INVALID_BYTECODE;
        }
        total_params += tree_params;
        
        if (source->trees[i].node_count > max_node_count) {
            max_node_count = source->trees[i].node_count;
        }
        if ((uint16_t)tree_params > max_param_count) {
            max_param_count = (uint16_t)tree_params;
        }
    }
    
    // 6. Allocate params
    if (total_params > 0) {
        runtime->params = (s_expr_param_t*)SEXB_RUNTIME_MALLOC(
            total_params * sizeof(s_expr_param_t)
        );
        if (!runtime->params) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        memset(runtime->params, 0, total_params * sizeof(s_expr_param_t));
    }
    
    // 7. Create tree definitions and decode bytecode
    if (source->tree_count > 0) {
        runtime->trees = (s_expr_tree_def_t*)SEXB_RUNTIME_MALLOC(
            source->tree_count * sizeof(s_expr_tree_def_t)
        );
        if (!runtime->trees) {
            sexb_free_runtime(runtime);
            return SEXB_ERR_ALLOC_FAILED;
        }
        memset(runtime->trees, 0, source->tree_count * sizeof(s_expr_tree_def_t));
        
        uint32_t param_offset = 0;
        
        for (uint16_t i = 0; i < source->tree_count; i++) {
            const sexb_tree_t* src_tree = &source->trees[i];
            s_expr_tree_def_t* dst_tree = &runtime->trees[i];
            
            dst_tree->name_hash = src_tree->name_hash;
            
            if (src_tree->record_index < source->record_count) {
                dst_tree->record_hash = source->records[src_tree->record_index].name_hash;
            } else {
                dst_tree->record_hash = 0;
            }
            
            dst_tree->params = runtime->params + param_offset;
            
            uint16_t node_count = 0;
            uint16_t pointer_count = 0;
            
            int32_t decoded = sexb_decode_tree(
                src_tree,
                source,
                runtime->params + param_offset,
                total_params - param_offset,
                &node_count,
                &pointer_count
            );
            
            if (decoded < 0) {
                sexb_free_runtime(runtime);
                return SEXB_ERR_INVALID_BYTECODE;
            }
            
            dst_tree->param_count = (uint16_t)decoded;
            dst_tree->func_node_count = node_count;
            dst_tree->pointer_count = pointer_count;
            
            if (pointer_count > max_pointer_count) {
                max_pointer_count = pointer_count;
            }
            
            param_offset += decoded;
        }
    }
    
    // 8. Resolve field references
    for (uint16_t t = 0; t < source->tree_count; t++) {
        s_expr_tree_def_t* tree = &runtime->trees[t];
        
        const sexb_record_t* rec = NULL;
        for (uint16_t r = 0; r < source->record_count; r++) {
            if (source->records[r].name_hash == tree->record_hash) {
                rec = &source->records[r];
                break;
            }
        }
        
        if (rec) {
            sexb_resolve_fields(
                (s_expr_param_t*)tree->params,
                tree->param_count,
                rec
            );
        }
    }
    
    // 9. Fill module definition
    runtime->def.name_hash = source->name_hash;
    runtime->def.trees = runtime->trees;
    runtime->def.tree_count = source->tree_count;
    runtime->def.is_64bit = (source->flags & SEXB_FLAG_64BIT) != 0;
    
    runtime->def.oneshot_hashes = runtime->oneshot_hashes;
    runtime->def.main_hashes = runtime->main_hashes;
    runtime->def.pred_hashes = runtime->pred_hashes;
    
    runtime->def.oneshot_count = source->oneshot_count;
    runtime->def.main_count = source->main_count;
    runtime->def.pred_count = source->pred_count;
    
    runtime->def.max_func_node_count = max_node_count;
    runtime->def.max_pointer_count = max_pointer_count;
    runtime->def.max_param_count = max_param_count;
    
    runtime->def.records = runtime->records;
    runtime->def.record_count = source->record_count;
    
    runtime->def.string_table = runtime->string_table;
    runtime->def.string_count = source->string_count;
    
    runtime->def.constants = runtime->constants;
    runtime->def.const_count = source->const_count;
    
    runtime->error = SEXB_OK;
    return SEXB_OK;
}

// ============================================================================
// Free runtime
// ============================================================================

static inline void sexb_free_runtime(sexb_runtime_t* runtime) {
    if (!runtime) return;
    
    if (runtime->trees) SEXB_RUNTIME_FREE(runtime->trees);
    if (runtime->records) SEXB_RUNTIME_FREE(runtime->records);
    if (runtime->fields) SEXB_RUNTIME_FREE(runtime->fields);
    if (runtime->params) SEXB_RUNTIME_FREE(runtime->params);
    if (runtime->string_table) SEXB_RUNTIME_FREE((void*)runtime->string_table);
    if (runtime->oneshot_hashes) SEXB_RUNTIME_FREE(runtime->oneshot_hashes);
    if (runtime->main_hashes) SEXB_RUNTIME_FREE(runtime->main_hashes);
    if (runtime->pred_hashes) SEXB_RUNTIME_FREE(runtime->pred_hashes);
    if (runtime->constants) SEXB_RUNTIME_FREE((void*)runtime->constants);
    
    memset(runtime, 0, sizeof(*runtime));
}

// ============================================================================
// Combined load + convert
// ============================================================================

static inline sexb_error_t sexb_load_and_create(
    sexb_full_load_t* result,
    const void* data,
    size_t size,
    bool copy_data
) {
    if (!result) return SEXB_ERR_INVALID_HEADER;
    
    memset(result, 0, sizeof(*result));
    
    if (copy_data) {
        result->load = sexb_load_copy(data, size);
    } else {
        result->load = sexb_load_rom(data, size);
    }
    
    if (result->load.error != SEXB_OK) {
        result->error = result->load.error;
        return result->error;
    }
    
    result->error = sexb_create_runtime(&result->runtime, result->load.module);
    if (result->error != SEXB_OK) {
        if (copy_data) {
            sexb_free_copy(&result->load);
        } else {
            sexb_free(&result->load);
        }
        return result->error;
    }
    
    return SEXB_OK;
}

static inline void sexb_full_free(sexb_full_load_t* result, bool free_copy) {
    if (!result) return;
    
    sexb_free_runtime(&result->runtime);
    
    if (free_copy) {
        sexb_free_copy(&result->load);
    } else {
        sexb_free(&result->load);
    }
    
    memset(result, 0, sizeof(*result));
}

// ============================================================================
// Convenience macros
// ============================================================================

#define SEXB_LOAD_RUNTIME_STATIC(result, bin_array) \
    sexb_load_and_create(result, bin_array, sizeof(bin_array), false)

#define SEXB_GET_DEF(result) \
    (&(result)->runtime.def)

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_RUNTIME_H