// ============================================================================
// example_binary_module_usage.c
// 
// Shows how a user interacts with the v5 binary module loader
// to load S-expression modules and integrate with ChainTree runtime
// ============================================================================

#include <stdio.h>
#include <stdlib.h>

// 1. Define target mode BEFORE including headers
#define MODULE_IS_64BIT 0  // or 1 for 64-bit systems

// 2. Include the engine types and binary loader
#include "s_engine_types.h"
#include "s_expr_binary_v5.h"

// 3. Include the generated binary (from: luajit s_compile.lua my_module.lua --binary-h=my_module_bin_32.h)
#include "my_module_bin_32.h"

// Your ChainTree runtime handle (from your existing code)
// #include "cfl_runtime.h"
// #include "cfl_s_engine_interface.h"

// ============================================================================
// EXAMPLE 1: Basic Loading and Inspection
// ============================================================================

void example_basic_loading(void) {
    printf("=== Example 1: Basic Loading ===\n\n");
    
    // Load the binary module (zero-copy from ROM)
    sexb_module_t mod;
    if (!sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE)) {
        printf("ERROR: Failed to load module\n");
        return;
    }
    
    // Inspect module info
    printf("Module loaded:\n");
    printf("  Name hash: 0x%08X\n", mod.name_hash);
    printf("  Mode: %s\n", mod.is_64bit ? "64-bit" : "32-bit");
    printf("  Trees: %d\n", mod.tree_count);
    printf("  Records: %d\n", mod.record_count);
    printf("  Strings: %d\n", mod.string_count);
    printf("  Oneshot funcs: %d\n", mod.oneshot_count);
    printf("  Main funcs: %d\n", mod.main_count);
    printf("  Pred funcs: %d\n\n", mod.pred_count);
}

// ============================================================================
// EXAMPLE 2: Accessing Trees
// ============================================================================

void example_tree_access(void) {
    printf("=== Example 2: Tree Access ===\n\n");
    
    sexb_module_t mod;
    sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE);
    
    // Get tree by index
    sexb_tree_def_t tree;
    if (sexb_get_tree(&mod, 0, &tree)) {
        printf("Tree[0]:\n");
        printf("  Name hash: 0x%08X\n", tree.name_hash);
        printf("  Record hash: 0x%08X\n", tree.record_hash);
        printf("  Node count: %d\n", tree.node_count);
        printf("  Pointer count: %d\n", tree.pointer_count);
        printf("  Param count: %d\n", tree.param_count);
        printf("  Params ptr: %p (direct ROM pointer!)\n\n", (void*)tree.params);
    }
    
    // Or find tree by name hash
    s_expr_hash_t tree_hash = s_expr_hash("MyTreeName");
    if (sexb_find_tree(&mod, tree_hash, &tree)) {
        printf("Found tree by hash!\n\n");
    }
}

// ============================================================================
// EXAMPLE 3: Accessing Parameters (Zero-Copy)
// ============================================================================

void example_param_access(void) {
    printf("=== Example 3: Parameter Access (Zero-Copy) ===\n\n");
    
    sexb_module_t mod;
    sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE);
    
    sexb_tree_def_t tree;
    sexb_get_tree(&mod, 0, &tree);
    
    // Direct access to params - NO COPYING, points into ROM!
    const s_expr_param_t* params = tree.params;
    
    printf("First 5 parameters:\n");
    for (int i = 0; i < 5 && i < tree.param_count; i++) {
        const s_expr_param_t* p = &params[i];
        uint8_t opcode = p->type & S_EXPR_OPCODE_MASK;
        
        printf("  [%d] type=0x%02X", i, p->type);
        
        switch (opcode) {
            case S_EXPR_PARAM_INT:
                printf(" (INT) val=%d", p->int_val);
                break;
            case S_EXPR_PARAM_FLOAT:
                printf(" (FLOAT) val=%f", p->float_val);
                break;
            case S_EXPR_PARAM_STR_IDX:
                printf(" (STR) idx=%d", p->str_index);
                // Get actual string
                const char* str = sexb_get_string(&mod, p->str_index);
                if (str) printf(" -> \"%s\"", str);
                break;
            case S_EXPR_PARAM_OPEN_CALL:
                printf(" (OPEN_CALL)");
                break;
            case S_EXPR_PARAM_CLOSE:
                printf(" (CLOSE)");
                break;
            case S_EXPR_PARAM_MAIN:
                printf(" (MAIN) func_idx=%d", p->func_index);
                break;
            case S_EXPR_PARAM_ONESHOT:
                printf(" (ONESHOT) func_idx=%d", p->func_index);
                break;
            case S_EXPR_PARAM_PRED:
                printf(" (PRED) func_idx=%d", p->func_index);
                break;
        }
        
        // Check flags
        if (p->type & S_EXPR_FLAG_POINTER) printf(" +PTR");
        if (p->type & S_EXPR_FLAG_SURVIVES_RESET) printf(" +SURVIVES");
        
        printf("\n");
    }
    printf("\n");
}

// ============================================================================
// EXAMPLE 4: Function Hash Lookup
// ============================================================================

void example_function_hashes(void) {
    printf("=== Example 4: Function Hashes ===\n\n");
    
    sexb_module_t mod;
    sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE);
    
    // Get function hashes for registration
    printf("Oneshot functions:\n");
    for (int i = 0; i < mod.oneshot_count; i++) {
        printf("  [%d] hash=0x%08X\n", i, sexb_get_oneshot_hash(&mod, i));
    }
    
    printf("Main functions:\n");
    for (int i = 0; i < mod.main_count; i++) {
        printf("  [%d] hash=0x%08X\n", i, sexb_get_main_hash(&mod, i));
    }
    
    printf("Predicate functions:\n");
    for (int i = 0; i < mod.pred_count; i++) {
        printf("  [%d] hash=0x%08X\n", i, sexb_get_pred_hash(&mod, i));
    }
    printf("\n");
}

// ============================================================================
// EXAMPLE 5: Field Lookup (for blackboard access)
// ============================================================================

void example_field_lookup(void) {
    printf("=== Example 5: Field Lookup ===\n\n");
    
    sexb_module_t mod;
    sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE);
    
    // Get tree's record hash
    sexb_tree_def_t tree;
    sexb_get_tree(&mod, 0, &tree);
    
    // Look up a field by name
    s_expr_hash_t field_hash = s_expr_hash("value");  // field name
    sexb_field_info_t field;
    
    if (sexb_find_field(&mod, tree.record_hash, field_hash, &field)) {
        printf("Field 'value':\n");
        printf("  Offset: %d\n", field.offset);
        printf("  Size: %d\n", field.size);
        printf("  Type tag: 0x%02X\n", field.type_tag);
    } else {
        printf("Field not found\n");
    }
    
    // Get record size (for blackboard allocation)
    uint16_t rec_size = sexb_get_record_size(&mod, tree.record_hash);
    printf("Record size: %d bytes\n\n", rec_size);
}

// ============================================================================
// EXAMPLE 6: Integration with ChainTree Runtime
// ============================================================================

void example_chaintree_integration(void) {
    printf("=== Example 6: ChainTree Integration ===\n\n");
    
    // Step 1: Load binary module
    sexb_module_t mod;
    if (!sexb_load(&mod, my_module_bin_32, MY_MODULE_BIN_32_SIZE)) {
        printf("ERROR: Failed to load module\n");
        return;
    }
    
    // Step 2: Build s_expr_module_def_t for the runtime
    // This allocates wrapper structures, but params still point into ROM
    s_expr_module_def_t* def = sexb_build_module_def(&mod, malloc, free);
    if (!def) {
        printf("ERROR: Failed to build module def\n");
        return;
    }
    
    printf("Module def created:\n");
    printf("  Trees: %d\n", def->tree_count);
    printf("  Max nodes: %d\n", def->max_func_node_count);
    printf("  Max pointers: %d\n", def->max_pointer_count);
    printf("  Oneshot count: %d\n", def->oneshot_count);
    printf("  Main count: %d\n", def->main_count);
    printf("  Pred count: %d\n\n", def->pred_count);
    
    // Step 3: Create registry array (can mix static and binary modules)
    const s_expr_module_def_t* registry[] = {
        def,
        // &other_static_module_def,  // Can add static modules too
    };
    int registry_count = sizeof(registry) / sizeof(registry[0]);
    
    // Step 4: Initialize S-engine (your existing code)
    // cfl_initialize_s_engine(handle, registry, registry_count);
    // load_user_s_functions(handle);
    // cfl_s_engine_module_check(handle);
    
    printf("Ready to call: cfl_initialize_s_engine(handle, registry, %d)\n\n", registry_count);
    
    // Step 5: Cleanup when done (at system shutdown)
    // cfl_deinitialize_s_engine(handle);
    sexb_free_module_def(def, free);
    // Note: 'mod' doesn't need cleanup - it just holds pointers into ROM
}

// ============================================================================
// EXAMPLE 7: Multiple Modules
// ============================================================================

void example_multiple_modules(void) {
    printf("=== Example 7: Multiple Modules ===\n\n");
    
    // Can load multiple binary modules
    // #include "module_a_bin_32.h"
    // #include "module_b_bin_32.h"
    
    sexb_module_t mod_a, mod_b;
    // sexb_load(&mod_a, module_a_bin_32, MODULE_A_BIN_32_SIZE);
    // sexb_load(&mod_b, module_b_bin_32, MODULE_B_BIN_32_SIZE);
    
    // Build defs for each
    // s_expr_module_def_t* def_a = sexb_build_module_def(&mod_a, malloc, free);
    // s_expr_module_def_t* def_b = sexb_build_module_def(&mod_b, malloc, free);
    
    // Create combined registry
    // const s_expr_module_def_t* registry[] = { def_a, def_b };
    // cfl_initialize_s_engine(handle, registry, 2);
    
    printf("Multiple modules can be loaded and combined in registry\n\n");
}

// ============================================================================
// EXAMPLE 8: ROM Embedding (Embedded Systems)
// ============================================================================

void example_rom_embedding(void) {
    printf("=== Example 8: ROM Embedding ===\n\n");
    
    printf("For embedded systems with flash memory:\n\n");
    
    printf("// In your linker script or code:\n");
    printf("// Place binary in flash section\n");
    printf("__attribute__((section(\".rodata\"))) \n");
    printf("static const uint8_t module_rom[] = { ... };\n\n");
    
    printf("// Load from flash - true zero-copy!\n");
    printf("sexb_module_t mod;\n");
    printf("sexb_load(&mod, module_rom, sizeof(module_rom));\n\n");
    
    printf("// params point directly into flash memory\n");
    printf("// No RAM used for parameter storage!\n\n");
}

// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    printf("============================================================\n");
    printf("S-Expression Binary Module Loader v5.0 - Usage Examples\n");
    printf("============================================================\n\n");
    
    example_basic_loading();
    example_tree_access();
    example_param_access();
    example_function_hashes();
    example_field_lookup();
    example_chaintree_integration();
    example_multiple_modules();
    example_rom_embedding();
    
    printf("============================================================\n");
    printf("All examples complete!\n");
    printf("============================================================\n");
    
    return 0;
}