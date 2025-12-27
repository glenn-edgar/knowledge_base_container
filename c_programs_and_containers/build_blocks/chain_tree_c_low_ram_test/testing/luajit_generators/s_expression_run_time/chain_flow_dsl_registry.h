// ============================================================================
// module_registry.h
// Generated from chain_flow_registry.registry
// DO NOT EDIT
// ============================================================================

#ifndef MODULE_REGISTRY_H
#define MODULE_REGISTRY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>
#include "s_engine_types.h"

// Module headers
#include "chain_flow_dsl_tests.h"

// Module registry table
static const s_expr_module_def_t* const module_registry[] = {
    &chain_flow_dsl_tests_module,
};

#define MODULE_REGISTRY_COUNT 1

// Lookup module by name
static inline const s_expr_module_def_t* find_module(const char* name) {
    for (int i = 0; i < MODULE_REGISTRY_COUNT; i++) {
        if (strcmp(module_registry[i]->name, name) == 0) {
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

#ifdef __cplusplus
}
#endif

#endif // MODULE_REGISTRY_H
