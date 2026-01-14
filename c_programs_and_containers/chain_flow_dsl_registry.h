// ============================================================================
// module_registry.h
// Generated from chain_flow_registry.registry
// Version 3.0 - Hash-based lookup
// DO NOT EDIT
// ============================================================================

#ifndef MODULE_REGISTRY_H
#define MODULE_REGISTRY_H

#ifdef __cplusplus
extern "C" {
#endif

#include "s_engine_types.h"

// Module headers
#include "chain_flow_dsl_tests.h"

// Module registry table
// Note: actual name_hash is in each module's definition
static const s_expr_module_def_t* const module_registry[] = {
    &chain_flow_dsl_tests_module,
};

#define MODULE_REGISTRY_COUNT 1

// Lookup module by name hash
static inline const s_expr_module_def_t* find_module_by_hash(uint32_t name_hash) {
    for (int i = 0; i < MODULE_REGISTRY_COUNT; i++) {
        if (module_registry[i]->name_hash == name_hash) {
            return module_registry[i];
        }
    }
    return NULL;
}

// Get module by index
static inline const s_expr_module_def_t* get_module(int index) {
    if (index >= 0 && index < MODULE_REGISTRY_COUNT) {
        return module_registry[index];
    }
    return NULL;
}

// Find tree by hash across all modules
// Returns module via out_module, tree index via return value (-1 if not found)
static inline int find_tree_by_hash(
    uint32_t tree_hash,
    const s_expr_module_def_t** out_module
) {
    for (int m = 0; m < MODULE_REGISTRY_COUNT; m++) {
        const s_expr_module_def_t* mod = module_registry[m];
        for (int t = 0; t < mod->tree_count; t++) {
            if (mod->trees[t].name_hash == tree_hash) {
                if (out_module) *out_module = mod;
                return t;
            }
        }
    }
    if (out_module) *out_module = NULL;
    return -1;
}

#ifdef __cplusplus
}
#endif

#endif // MODULE_REGISTRY_H
