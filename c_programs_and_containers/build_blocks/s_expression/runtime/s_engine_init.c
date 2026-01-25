// ============================================================================
// s_engine_init.c
// High-level S-Expression Engine Initialization Implementation
// ============================================================================

#include "s_engine_init.h"
#include "s_engine_builtins.h"
#include "s_engine_exception.h"
#include <string.h>

// ============================================================================
// INIT FROM ROM
// ============================================================================

uint8_t s_engine_init_from_rom(
    s_engine_handle_t* handle,
    const uint8_t* binary_data,
    size_t binary_size,
    s_expr_allocator_t alloc,
    void* user_ctx
) {
    if (!handle) {
        EXCEPTION("s_engine_init_from_rom: NULL handle");
        return S_EXPR_ERR_ALLOC;
    }
    if (!binary_data || binary_size == 0) {
        EXCEPTION("s_engine_init_from_rom: invalid binary data");
        return S_EXPR_ERR_NULL_DEF;
    }
    if (!alloc.malloc || !alloc.free) {
        EXCEPTION("s_engine_init_from_rom: invalid allocator");
        return S_EXPR_ERR_ALLOC;
    }
    
    memset(handle, 0, sizeof(*handle));
    handle->alloc = alloc;
    handle->user_ctx = user_ctx;
    
    // Load binary (not owned - ROM stays valid)
    handle->loaded = s_expr_load_from_rom(binary_data, binary_size, alloc);
    if (!handle->loaded) {
        EXCEPTION("s_engine_init_from_rom: failed to load binary");
        handle->error_code = SEXB_ERR_CORRUPT;
        return SEXB_ERR_CORRUPT;
    }
    
    if (handle->loaded->error_code != SEXB_ERR_OK) {
        handle->error_code = handle->loaded->error_code;
        s_expr_unload_module(handle->loaded);
        handle->loaded = NULL;
        return handle->error_code;
    }
    
    // Initialize module
    uint8_t err = s_expr_module_init(&handle->module, &handle->loaded->def, alloc);
    if (err != S_EXPR_ERR_OK) {
        EXCEPTION("s_engine_init_from_rom: module init failed");
        s_expr_unload_module(handle->loaded);
        handle->loaded = NULL;
        handle->error_code = err;
        return err;
    }
    
    return S_EXPR_ERR_OK;
}

// ============================================================================
// INIT FROM FILE
// ============================================================================

uint8_t s_engine_init_from_file(
    s_engine_handle_t* handle,
    const char* filepath,
    s_expr_allocator_t alloc,
    void* user_ctx
) {
    if (!handle) {
        EXCEPTION("s_engine_init_from_file: NULL handle");
        return S_EXPR_ERR_ALLOC;
    }
    if (!filepath) {
        EXCEPTION("s_engine_init_from_file: NULL filepath");
        return S_EXPR_ERR_NULL_DEF;
    }
    if (!alloc.malloc || !alloc.free) {
        EXCEPTION("s_engine_init_from_file: invalid allocator");
        return S_EXPR_ERR_ALLOC;
    }
    
    memset(handle, 0, sizeof(*handle));
    handle->alloc = alloc;
    handle->user_ctx = user_ctx;
    
    // Load binary from file (owned - will be freed on unload)
    handle->loaded = s_expr_load_from_file(filepath, alloc);
    if (!handle->loaded) {
        EXCEPTION("s_engine_init_from_file: failed to load file");
        handle->error_code = SEXB_ERR_FILE_NOT_FOUND;
        return SEXB_ERR_FILE_NOT_FOUND;
    }
    
    if (handle->loaded->error_code != SEXB_ERR_OK) {
        handle->error_code = handle->loaded->error_code;
        s_expr_unload_module(handle->loaded);
        handle->loaded = NULL;
        return handle->error_code;
    }
    
    // Initialize module
    uint8_t err = s_expr_module_init(&handle->module, &handle->loaded->def, alloc);
    if (err != S_EXPR_ERR_OK) {
        EXCEPTION("s_engine_init_from_file: module init failed");
        s_expr_unload_module(handle->loaded);
        handle->loaded = NULL;
        handle->error_code = err;
        return err;
    }
    
    return S_EXPR_ERR_OK;
}

// ============================================================================
// INIT FROM COMPILE-TIME DEFINITION
// ============================================================================

uint8_t s_engine_init_from_def(
    s_engine_handle_t* handle,
    const s_expr_module_def_t* def,
    s_expr_allocator_t alloc,
    void* user_ctx
) {
    if (!handle) {
        EXCEPTION("s_engine_init_from_def: NULL handle");
        return S_EXPR_ERR_ALLOC;
    }
    if (!def) {
        EXCEPTION("s_engine_init_from_def: NULL definition");
        return S_EXPR_ERR_NULL_DEF;
    }
    if (!alloc.malloc || !alloc.free) {
        EXCEPTION("s_engine_init_from_def: invalid allocator");
        return S_EXPR_ERR_ALLOC;
    }
    
    memset(handle, 0, sizeof(*handle));
    handle->alloc = alloc;
    handle->user_ctx = user_ctx;
    handle->loaded = NULL;  // Not from binary
    
    // Initialize module directly from definition
    uint8_t err = s_expr_module_init(&handle->module, def, alloc);
    if (err != S_EXPR_ERR_OK) {
        EXCEPTION("s_engine_init_from_def: module init failed");
        handle->error_code = err;
        return err;
    }
    
    return S_EXPR_ERR_OK;
}

// ============================================================================
// FUNCTION REGISTRATION
// ============================================================================

void s_engine_register_oneshot(s_engine_handle_t* handle, const s_expr_fn_table_t* table) {
    if (!handle) {
        EXCEPTION("s_engine_register_oneshot: NULL handle");
        return;
    }
    if (!table) {
        EXCEPTION("s_engine_register_oneshot: NULL table");
        return;
    }
    s_expr_module_register_oneshot(&handle->module, table);
}

void s_engine_register_main(s_engine_handle_t* handle, const s_expr_fn_table_t* table) {
    if (!handle) {
        EXCEPTION("s_engine_register_main: NULL handle");
        return;
    }
    if (!table) {
        EXCEPTION("s_engine_register_main: NULL table");
        return;
    }
    s_expr_module_register_main(&handle->module, table);
}

void s_engine_register_pred(s_engine_handle_t* handle, const s_expr_fn_table_t* table) {
    if (!handle) {
        EXCEPTION("s_engine_register_pred: NULL handle");
        return;
    }
    if (!table) {
        EXCEPTION("s_engine_register_pred: NULL table");
        return;
    }
    s_expr_module_register_pred(&handle->module, table);
}

void s_engine_register_builtins(s_engine_handle_t* handle) {
    if (!handle) {
        EXCEPTION("s_engine_register_builtins: NULL handle");
        return;
    }
    
    const s_expr_fn_table_t* oneshot_table = s_engine_builtin_oneshot_table();
    const s_expr_fn_table_t* main_table = s_engine_builtin_main_table();
    const s_expr_fn_table_t* pred_table = s_engine_builtin_pred_table();
    
    if (oneshot_table) {
        s_expr_module_register_oneshot(&handle->module, oneshot_table);
    }
    if (main_table) {
        s_expr_module_register_main(&handle->module, main_table);
    }
    if (pred_table) {
        s_expr_module_register_pred(&handle->module, pred_table);
    }
}

uint8_t s_engine_validate(s_engine_handle_t* handle) {
    if (!handle) {
        EXCEPTION("s_engine_validate: NULL handle");
        return S_EXPR_ERR_NULL_DEF;
    }
    
    uint8_t err = s_expr_module_validate(&handle->module);
    if (err != S_EXPR_ERR_OK) {
        handle->error_code = err;
    }
    return err;
}

// ============================================================================
// TREE CREATION - Caller owns the returned tree
// ============================================================================

s_expr_tree_instance_t* s_engine_create_tree(
    s_engine_handle_t* handle,
    uint16_t tree_index,
    uint32_t node_id
) {
    if (!handle) {
        EXCEPTION("s_engine_create_tree: NULL handle");
        return NULL;
    }
    
    s_expr_tree_instance_t* tree = s_expr_tree_create(
        &handle->module, tree_index, node_id
    );
    
    if (tree) {
        // Pass external user context to tree
        // Main functions retrieve this via s_expr_tree_get_user_ctx()
        s_expr_tree_set_user_ctx(tree, handle->user_ctx);
    }
    
    return tree;
}

s_expr_tree_instance_t* s_engine_create_tree_by_hash(
    s_engine_handle_t* handle,
    s_expr_hash_t name_hash,
    uint32_t node_id
) {
    if (!handle) {
        EXCEPTION("s_engine_create_tree_by_hash: NULL handle");
        return NULL;
    }
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &handle->module, name_hash, node_id
    );
    
    if (tree) {
        s_expr_tree_set_user_ctx(tree, handle->user_ctx);
    }
    
    return tree;
}

// ============================================================================
// CLEANUP
// ============================================================================

void s_engine_free(s_engine_handle_t* handle) {
    if (!handle) return;
    
    // Free module (function tables)
    s_expr_module_free(&handle->module);
    
    // Free loaded binary (if from file/ROM)
    if (handle->loaded) {
        s_expr_unload_module(handle->loaded);
        handle->loaded = NULL;
    }
    
    // Clear handle
    handle->user_ctx = NULL;
    handle->error_code = 0;
}