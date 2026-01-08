// ============================================================================
// s_expr_binary_v5.c
// S-Expression Binary Module Loader - Implementation
// Version 5.0
// ============================================================================

#include "s_expr_binary_v5.h"
#include "s_expr_binary_v5_debug.h"
#include "s_expr_binary_v5_internal.h"
#include <string.h>

// ============================================================================
// INTERNAL CAST HELPERS
// ============================================================================

#define TREES(mod)     ((const sexb_tree_entry_t*)(mod)->trees)
#define RECORDS(mod)   ((const sexb_record_entry_t*)(mod)->records)
#define FIELDS(mod)    ((const sexb_field_entry_t*)(mod)->fields)
#define CONSTANTS(mod) ((const sexb_const_entry_t*)(mod)->constants)

// ============================================================================
// VALIDATION
// ============================================================================

bool sexb_validate(const uint8_t* data, uint32_t size) {
    if (!data || size < sizeof(sexb_header_t) + sizeof(sexb_directory_t)) {
        return false;
    }
    
    const sexb_header_t* hdr = (const sexb_header_t*)data;
    
    if (hdr->magic != SEXB_MAGIC) {
        return false;
    }
    
    if ((hdr->version & 0xFF00) != (SEXB_VERSION & 0xFF00)) {
        return false;
    }
    
    if (hdr->total_size > size) {
        return false;
    }
    
    bool bin_is_64bit = (hdr->flags & SEXB_FLAG_64BIT) != 0;
    bool target_is_64bit = (MODULE_IS_64BIT != 0);
    
    if (bin_is_64bit != target_is_64bit) {
        return false;
    }
    
    return true;
}

// ============================================================================
// LOADER
// ============================================================================

bool sexb_load(sexb_module_t* mod, const uint8_t* data, uint32_t size) {
    if (!mod) {
        return false;
    }
    
    memset(mod, 0, sizeof(*mod));
    
    if (!sexb_validate(data, size)) {
        return false;
    }
    
    const sexb_header_t* hdr = (const sexb_header_t*)data;
    const sexb_directory_t* dir = (const sexb_directory_t*)(data + sizeof(sexb_header_t));
    
    mod->data = data;
    mod->size = size;
    mod->name_hash = hdr->name_hash;
    mod->is_64bit = (hdr->flags & SEXB_FLAG_64BIT) != 0;
    
    mod->tree_count = hdr->tree_count;
    mod->record_count = hdr->record_count;
    mod->string_count = hdr->string_count;
    mod->const_count = hdr->const_count;
    mod->oneshot_count = hdr->oneshot_count;
    mod->main_count = hdr->main_count;
    mod->pred_count = hdr->pred_count;
    
    mod->trees = data + dir->tree_offset;
    mod->records = data + dir->record_offset;
    mod->fields = data + dir->field_offset;
    mod->strings = data + dir->string_offset;
    mod->constants = data + dir->const_offset;
    mod->const_data = data + dir->const_data_offset;
    mod->func_hashes = (const uint32_t*)(data + dir->func_offset);
    mod->params_base = data + dir->params_offset;
    
    return true;
}

// ============================================================================
// MODULE DEFINITION BUILDER
// ============================================================================

s_expr_module_def_t* sexb_build_module_def(
    const sexb_module_t* mod,
    sexb_alloc_fn alloc_fn,
    sexb_free_fn free_fn,
    void* alloc_ctx
) {
    if (!mod || !alloc_fn) {
        return NULL;
    }
    
    s_expr_module_def_t* def = (s_expr_module_def_t*)alloc_fn(alloc_ctx, sizeof(s_expr_module_def_t));
    if (!def) {
        return NULL;
    }
    
    memset(def, 0, sizeof(*def));
    
    def->name_hash = mod->name_hash;
    def->is_64bit = mod->is_64bit;
    def->tree_count = mod->tree_count;
    def->record_count = mod->record_count;
    def->string_count = mod->string_count;
    def->const_count = mod->const_count;
    def->oneshot_count = mod->oneshot_count;
    def->main_count = mod->main_count;
    def->pred_count = mod->pred_count;
    
    if (mod->tree_count > 0) {
        s_expr_tree_def_t* trees = (s_expr_tree_def_t*)alloc_fn(
            alloc_ctx, 
            mod->tree_count * sizeof(s_expr_tree_def_t)
        );
        if (!trees) {
            if (free_fn) free_fn(alloc_ctx, def);
            return NULL;
        }
        
        uint16_t max_node = 0, max_ptr = 0, max_param = 0;
        
        for (uint16_t i = 0; i < mod->tree_count; i++) {
            const sexb_tree_entry_t* entry = &TREES(mod)[i];
            
            trees[i].name_hash = entry->name_hash;
            trees[i].record_hash = entry->record_hash;
            trees[i].func_node_count = entry->node_count;
            trees[i].pointer_count = entry->pointer_count;
            trees[i].param_count = entry->param_count;
            trees[i].params = (const s_expr_param_t*)(mod->data + entry->param_offset);
            
            if (entry->node_count > max_node) max_node = entry->node_count;
            if (entry->pointer_count > max_ptr) max_ptr = entry->pointer_count;
            if (entry->param_count > max_param) max_param = entry->param_count;
        }
        
        def->trees = trees;
        def->max_func_node_count = max_node;
        def->max_pointer_count = max_ptr;
        def->max_param_count = max_param;
    }
    
    def->oneshot_hashes = &mod->func_hashes[0];
    def->main_hashes = &mod->func_hashes[mod->oneshot_count];
    def->pred_hashes = &mod->func_hashes[mod->oneshot_count + mod->main_count];
    
    def->records = NULL;
    def->string_table = NULL;
    def->constants = NULL;
    
    return def;
}

void sexb_free_module_def(
    s_expr_module_def_t* def, 
    sexb_free_fn free_fn,
    void* alloc_ctx
) {
    if (!def || !free_fn) {
        return;
    }
    
    if (def->trees) {
        free_fn(alloc_ctx, (void*)def->trees);
    }
    
    free_fn(alloc_ctx, def);
}

// ============================================================================
// DEBUG: TREE ACCESS
// ============================================================================

bool sexb_get_tree(const sexb_module_t* mod, uint16_t index, sexb_tree_def_t* out) {
    if (!mod || !out || index >= mod->tree_count) {
        return false;
    }
    
    const sexb_tree_entry_t* entry = &TREES(mod)[index];
    
    out->name_hash = entry->name_hash;
    out->record_hash = entry->record_hash;
    out->node_count = entry->node_count;
    out->pointer_count = entry->pointer_count;
    out->param_count = entry->param_count;
    out->params = (const s_expr_param_t*)(mod->data + entry->param_offset);
    
    return true;
}

bool sexb_find_tree(const sexb_module_t* mod, s_expr_hash_t name_hash, sexb_tree_def_t* out) {
    if (!mod || !out) {
        return false;
    }
    
    for (uint16_t i = 0; i < mod->tree_count; i++) {
        if (TREES(mod)[i].name_hash == name_hash) {
            return sexb_get_tree(mod, i, out);
        }
    }
    
    return false;
}

// ============================================================================
// DEBUG: FUNCTION HASH ACCESS
// ============================================================================

s_expr_hash_t sexb_get_oneshot_hash(const sexb_module_t* mod, uint16_t index) {
    if (!mod || index >= mod->oneshot_count) {
        return 0;
    }
    return mod->func_hashes[index];
}

s_expr_hash_t sexb_get_main_hash(const sexb_module_t* mod, uint16_t index) {
    if (!mod || index >= mod->main_count) {
        return 0;
    }
    return mod->func_hashes[mod->oneshot_count + index];
}

s_expr_hash_t sexb_get_pred_hash(const sexb_module_t* mod, uint16_t index) {
    if (!mod || index >= mod->pred_count) {
        return 0;
    }
    return mod->func_hashes[mod->oneshot_count + mod->main_count + index];
}

// ============================================================================
// DEBUG: STRING ACCESS
// ============================================================================

static const uint8_t* get_string_ptr(const sexb_module_t* mod, uint16_t index) {
    if (!mod || index >= mod->string_count) {
        return NULL;
    }
    
    const uint8_t* ptr = mod->strings;
    
    for (uint16_t i = 0; i < index; i++) {
        uint16_t len = ptr[0] | (ptr[1] << 8);
        uint16_t padded = (2 + len + 1 + 3) & ~3;
        ptr += padded;
    }
    
    return ptr;
}

const char* sexb_get_string(const sexb_module_t* mod, uint16_t index) {
    const uint8_t* ptr = get_string_ptr(mod, index);
    return ptr ? (const char*)(ptr + 2) : NULL;
}

uint16_t sexb_get_string_len(const sexb_module_t* mod, uint16_t index) {
    const uint8_t* ptr = get_string_ptr(mod, index);
    return ptr ? (ptr[0] | (ptr[1] << 8)) : 0;
}

// ============================================================================
// DEBUG: RECORD/FIELD ACCESS
// ============================================================================

static const sexb_record_entry_t* find_record_entry(const sexb_module_t* mod, s_expr_hash_t hash) {
    if (!mod) return NULL;
    
    for (uint16_t i = 0; i < mod->record_count; i++) {
        if (RECORDS(mod)[i].name_hash == hash) {
            return &RECORDS(mod)[i];
        }
    }
    return NULL;
}

uint16_t sexb_get_record_size(const sexb_module_t* mod, s_expr_hash_t record_hash) {
    const sexb_record_entry_t* rec = find_record_entry(mod, record_hash);
    return rec ? rec->size : 0;
}

bool sexb_find_field(
    const sexb_module_t* mod,
    s_expr_hash_t record_hash,
    s_expr_hash_t field_hash,
    sexb_field_info_t* out
) {
    if (!mod || !out) {
        return false;
    }
    
    const sexb_record_entry_t* rec = find_record_entry(mod, record_hash);
    if (!rec) {
        return false;
    }
    
    const sexb_field_entry_t* fields = (const sexb_field_entry_t*)(mod->data + rec->field_offset);
    
    for (uint16_t i = 0; i < rec->field_count; i++) {
        if (fields[i].name_hash == field_hash) {
            out->name_hash = fields[i].name_hash;
            out->offset = fields[i].offset;
            out->size = fields[i].size;
            out->type_tag = fields[i].type_tag;
            out->flags = fields[i].flags;
            return true;
        }
    }
    
    return false;
}

// ============================================================================
// DEBUG: CONSTANT ACCESS
// ============================================================================

const void* sexb_find_constant(
    const sexb_module_t* mod,
    s_expr_hash_t hash,
    uint16_t* out_size
) {
    if (!mod) {
        return NULL;
    }
    
    for (uint16_t i = 0; i < mod->const_count; i++) {
        if (CONSTANTS(mod)[i].name_hash == hash) {
            if (out_size) {
                *out_size = CONSTANTS(mod)[i].size;
            }
            return mod->data + CONSTANTS(mod)[i].data_offset;
        }
    }
    
    return NULL;
}