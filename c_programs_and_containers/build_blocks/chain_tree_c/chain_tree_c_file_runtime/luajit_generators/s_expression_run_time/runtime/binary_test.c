// ============================================================================
// test_binary_runtime.c
// Test program for binary runtime bridge
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SEXB_DEBUG 1

// Include runtime headers first
#include "s_engine_types.h"
#include "s_expr_binary.h"
#include "s_expr_binary_loader.h"
#include "s_expr_binary_runtime.h"

// Include the binary module as a C array
#include "../test_out/chain_flow_dsl_tests_bin.h"

// ============================================================================
// TEST HELPERS
// ============================================================================

#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            printf("FAIL: %s\n", msg); \
            return 1; \
        } \
    } while (0)

#define TEST_PASS(msg) \
    printf("PASS: %s\n", msg)

// ============================================================================
// TEST: Binary validation
// ============================================================================

static int test_binary_validation(void) {
    printf("\n=== Test: Binary Validation ===\n");
    
    sexb_error_t err = sexb_validate(
        chain_flow_dsl_tests_module_bin,
        CHAIN_FLOW_DSL_TESTS_MODULE_BIN_SIZE
    );
    
    TEST_ASSERT(err == SEXB_OK, "Binary validation failed");
    TEST_PASS("Binary validation successful");
    
    // Check header values
    const sexb_header_t* hdr = (const sexb_header_t*)chain_flow_dsl_tests_module_bin;
    
    printf("  Module hash: 0x%08X\n", hdr->module_name_hash);
    printf("  Trees: %d\n", hdr->tree_count);
    printf("  Records: %d\n", hdr->record_count);
    printf("  Strings: %d\n", hdr->string_count);
    printf("  Oneshots: %d\n", hdr->oneshot_count);
    printf("  Mains: %d\n", hdr->main_count);
    printf("  Preds: %d\n", hdr->pred_count);
    
    TEST_ASSERT(hdr->tree_count == 11, "Expected 11 trees");
    TEST_ASSERT(hdr->record_count == 20, "Expected 20 records");
    TEST_ASSERT(hdr->string_count == 48, "Expected 48 strings");
    
    TEST_PASS("Header values correct");
    return 0;
}

// ============================================================================
// TEST: Binary loading
// ============================================================================

static int test_binary_loading(void) {
    printf("\n=== Test: Binary Loading ===\n");
    
    sexb_load_result_t result = SEXB_LOAD_STATIC(chain_flow_dsl_tests_module_bin);
    
    TEST_ASSERT(result.error == SEXB_OK, "Binary loading failed");
    TEST_ASSERT(result.module != NULL, "Module is NULL");
    
    const sexb_module_t* mod = result.module;
    
    printf("  Trees loaded: %d\n", mod->tree_count);
    printf("  Records loaded: %d\n", mod->record_count);
    printf("  Strings loaded: %d\n", mod->string_count);
    
    // Check trees
    TEST_ASSERT(mod->trees != NULL, "Trees array is NULL");
    
    for (uint16_t i = 0; i < mod->tree_count; i++) {
        printf("  Tree[%d] hash=0x%08X nodes=%d bytecode=%d bytes\n",
               i, mod->trees[i].name_hash, mod->trees[i].node_count,
               mod->trees[i].bytecode_size);
    }
    
    // Check records
    TEST_ASSERT(mod->records != NULL, "Records array is NULL");
    
    printf("\n  Records:\n");
    for (uint16_t i = 0; i < mod->record_count && i < 5; i++) {
        printf("  Record[%d] hash=0x%08X size=%d fields=%d\n",
               i, mod->records[i].name_hash, mod->records[i].size,
               mod->records[i].field_count);
    }
    
    // Check strings
    printf("\n  First few strings:\n");
    for (uint16_t i = 0; i < mod->string_count && i < 5; i++) {
        const char* str = sexb_get_string(mod, i);
        printf("  String[%d] = \"%s\"\n", i, str ? str : "(null)");
    }
    
    TEST_PASS("Binary loaded successfully");
    
    sexb_free(&result);
    TEST_PASS("Binary freed successfully");
    
    return 0;
}

// ============================================================================
// TEST: Runtime conversion
// ============================================================================

static int test_runtime_conversion(void) {
    printf("\n=== Test: Runtime Conversion ===\n");
    
    // Load binary
    sexb_load_result_t load = SEXB_LOAD_STATIC(chain_flow_dsl_tests_module_bin);
    TEST_ASSERT(load.error == SEXB_OK, "Binary loading failed");
    
    // Convert to runtime
    sexb_runtime_t runtime;
    
    // DEBUG: Print first few hashes from each table
    printf("  DEBUG: Main hashes in module (%d total):\n", load.module->main_count);
    for (int i = 0; i < load.module->main_count && i < 5; i++) {
        printf("    main[%d] = 0x%08X\n", i, load.module->main_hashes[i]);
    }
    
    // Debug: show where function tables are in binary
    printf("  DEBUG: func_table raw bytes at first main offset:\n");
    const uint8_t* func_raw = (const uint8_t*)load.module->oneshot_hashes;
    printf("    oneshots start: ");
    for (int i = 0; i < 16; i++) printf("%02X ", func_raw[i]);
    printf("\n");
    const uint8_t* main_raw = (const uint8_t*)load.module->main_hashes;
    printf("    mains start:    ");
    for (int i = 0; i < 16; i++) printf("%02X ", main_raw[i]);
    printf("\n");
    
    printf("  DEBUG: First bytecode node analysis:\n");
    if (load.module->trees && load.module->trees[0].bytecode_size >= 8) {
        const uint8_t* bc = load.module->trees[0].bytecode;
        uint32_t hash = bc[0] | (bc[1]<<8) | (bc[2]<<16) | (bc[3]<<24);
        uint8_t ftype = bc[4];
        printf("    func_hash = 0x%08X, func_type = %d\n", hash, ftype);
    }
    
    sexb_error_t err = sexb_create_runtime(&runtime, load.module);
    
    if (err != SEXB_OK) {
        printf("  Runtime conversion error code: %d\n", err);
    }
    
    TEST_ASSERT(err == SEXB_OK, "Runtime conversion failed");
    
    // Check module definition
    const s_expr_module_def_t* def = &runtime.def;
    
    printf("  Module hash: 0x%08X\n", def->name_hash);
    printf("  Trees: %d\n", def->tree_count);
    printf("  Records: %d\n", def->record_count);
    printf("  Strings: %d\n", def->string_count);
    printf("  Oneshots: %d\n", def->oneshot_count);
    printf("  Mains: %d\n", def->main_count);
    printf("  Preds: %d\n", def->pred_count);
    
    TEST_ASSERT(def->tree_count == 11, "Expected 11 trees");
    TEST_ASSERT(def->record_count == 20, "Expected 20 records");
    
    // Check trees
    TEST_ASSERT(def->trees != NULL, "Trees array is NULL");
    
    printf("\n  Decoded trees:\n");
    for (uint16_t i = 0; i < def->tree_count; i++) {
        const s_expr_tree_def_t* tree = &def->trees[i];
        printf("  Tree[%d] hash=0x%08X record=0x%08X params=%d nodes=%d ptrs=%d\n",
               i, tree->name_hash, tree->record_hash,
               tree->param_count, tree->func_node_count, tree->pointer_count);
    }
    
    // Check string table
    TEST_ASSERT(def->string_table != NULL, "String table is NULL");
    
    printf("\n  String table:\n");
    for (uint16_t i = 0; i < def->string_count && i < 5; i++) {
        printf("  String[%d] = \"%s\"\n", i, def->string_table[i] ? def->string_table[i] : "(null)");
    }
    
    // Check function hashes
    if (def->oneshot_count > 0) {
        TEST_ASSERT(def->oneshot_hashes != NULL, "Oneshot hashes is NULL");
        printf("\n  Oneshot hashes (first 5):\n");
        for (uint16_t i = 0; i < def->oneshot_count && i < 5; i++) {
            printf("  Oneshot[%d] = 0x%08X\n", i, def->oneshot_hashes[i]);
        }
    }
    
    if (def->main_count > 0) {
        TEST_ASSERT(def->main_hashes != NULL, "Main hashes is NULL");
        printf("\n  Main hashes:\n");
        for (uint16_t i = 0; i < def->main_count; i++) {
            printf("  Main[%d] = 0x%08X\n", i, def->main_hashes[i]);
        }
    }
    
    // Check decoded params for first tree
    const s_expr_tree_def_t* tree0 = &def->trees[0];
    if (tree0->params && tree0->param_count > 0) {
        printf("\n  First tree params (first 10):\n");
        for (uint16_t i = 0; i < tree0->param_count && i < 10; i++) {
            const s_expr_param_t* p = &tree0->params[i];
            uint8_t opcode = p->type & S_EXPR_OPCODE_MASK;
            printf("  Param[%d] type=0x%02X opcode=%d", i, p->type, opcode);
            
            switch (opcode) {
                case S_EXPR_PARAM_OPEN_CALL:
                    printf(" OPEN_CALL brace_idx=%d", p->brace_idx);
                    break;
                case S_EXPR_PARAM_CLOSE:
                    printf(" CLOSE");
                    break;
                case S_EXPR_PARAM_ONESHOT:
                    printf(" ONESHOT node=%d func=%d", p->node_index, p->func_index);
                    break;
                case S_EXPR_PARAM_MAIN:
                    printf(" MAIN node=%d func=%d", p->node_index, p->func_index);
                    break;
                case S_EXPR_PARAM_INT:
                    printf(" INT val=%d", p->int_val);
                    break;
                case S_EXPR_PARAM_STR_IDX:
                    printf(" STR_IDX idx=%d", p->str_index);
                    break;
                case S_EXPR_PARAM_RESULT:
                    printf(" RESULT val=%d", p->int_val);
                    break;
                default:
                    break;
            }
            printf("\n");
        }
    }
    
    TEST_PASS("Runtime conversion successful");
    
    // Cleanup
    sexb_free_runtime(&runtime);
    sexb_free(&load);
    
    TEST_PASS("Runtime freed successfully");
    
    return 0;
}

// ============================================================================
// TEST: Combined load and create
// ============================================================================

static int test_combined_load(void) {
    printf("\n=== Test: Combined Load and Create ===\n");
    
    sexb_full_load_t result;
    sexb_error_t err = sexb_load_and_create(
        &result,
        chain_flow_dsl_tests_module_bin,
        CHAIN_FLOW_DSL_TESTS_MODULE_BIN_SIZE,
        false  // Don't copy, use ROM directly
    );
    
    TEST_ASSERT(err == SEXB_OK, "Combined load failed");
    
    const s_expr_module_def_t* def = SEXB_GET_DEF(&result);
    
    printf("  Module ready: trees=%d records=%d\n", def->tree_count, def->record_count);
    
    TEST_PASS("Combined load successful");
    
    sexb_full_free(&result, false);
    
    TEST_PASS("Combined free successful");
    
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    
    printf("============================================================================\n");
    printf("Binary Runtime Bridge Tests\n");
    printf("============================================================================\n");
    
    int failures = 0;
    
    failures += test_binary_validation();
    failures += test_binary_loading();
    failures += test_runtime_conversion();
    failures += test_combined_load();
    
    printf("\n============================================================================\n");
    if (failures == 0) {
        printf("ALL TESTS PASSED\n");
    } else {
        printf("FAILURES: %d\n", failures);
    }
    printf("============================================================================\n");
    
    return failures;
}