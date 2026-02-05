// ============================================================================
// user_dict_extract_debug.c
// Debug version with extensive logging
// ============================================================================

#include "json_test_user_functions.h"
#include "s_engine_module.h"
#include "se_dict_string.h"
#include "se_dict_hash.h"
#include <stdio.h>
#include <string.h>
#include <math.h>


// ============================================================================
// Expected values
// ============================================================================

// Pass 1 & 2: Scalar extractions
#define EXPECT_INT_1       12345
#define EXPECT_INT_2      -9876
#define EXPECT_INT_3       42

#define EXPECT_UINT_1      100
#define EXPECT_UINT_2      50000
#define EXPECT_UINT_3      255

#define EXPECT_FLOAT_1     3.14159f
#define EXPECT_FLOAT_2    -273.15f
#define EXPECT_FLOAT_3     2.71828f

#define EXPECT_BOOL_1      1
#define EXPECT_BOOL_2      0
#define EXPECT_BOOL_3      1

// Pass 3: Array elements
#define EXPECT_ARR_INT_0   10
#define EXPECT_ARR_INT_1   20
#define EXPECT_ARR_INT_2   30
#define EXPECT_ARR_INT_3   40

#define EXPECT_ARR_FLOAT_0 1.5f
#define EXPECT_ARR_FLOAT_1 2.5f
#define EXPECT_ARR_FLOAT_2 3.5f

#define EXPECT_ARR_N0_ID   100
#define EXPECT_ARR_N0_VAL  10.1f
#define EXPECT_ARR_N1_ID   200
#define EXPECT_ARR_N1_VAL  20.2f
#define EXPECT_ARR_N2_ID   300
#define EXPECT_ARR_N2_VAL  30.3f

// Pass 4: Pointer extractions (same values, different access path)
#define EXPECT_PTR_INT_POS   12345
#define EXPECT_PTR_INT_NEG  -9876
#define EXPECT_PTR_FLOAT_PI  3.14159f
#define EXPECT_PTR_FLOAT_NEG -273.15f
#define EXPECT_PTR_N0_ID     100
#define EXPECT_PTR_N0_VAL    10.1f
#define EXPECT_PTR_N1_ID     200
#define EXPECT_PTR_N1_VAL    20.2f

static const float TOL = 0.01f;

// ============================================================================
// Helpers
// ============================================================================

static const char* get_string(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* str_param
) {
    return s_expr_get_string(inst, str_param);
}

static int check_int(const char* name, int32_t got, int32_t expected, int* errors) {
    if (got == expected) {
        printf("║  ✅ %-28s = %-10d\n", name, got);
        return 0;
    }
    printf("║  ❌ %s: got %d, expected %d\n", name, got, expected);
    (*errors)++;
    return 1;
}

static int check_uint(const char* name, uint32_t got, uint32_t expected, int* errors) {
    if (got == expected) {
        printf("║  ✅ %-28s = %-10u\n", name, got);
        return 0;
    }
    printf("║  ❌ %s: got %u, expected %u\n", name, got, expected);
    (*errors)++;
    return 1;
}

static int check_float(const char* name, float got, float expected, int* errors) {
    if (fabsf(got - expected) < TOL) {
        printf("║  ✅ %-28s = %-10.5f\n", name, got);
        return 0;
    }
    printf("║  ❌ %s: got %.5f, expected %.5f\n", name, got, expected);
    (*errors)++;
    return 1;
}

static int check_hash(const char* name, uint32_t got, const char* str, int* errors) {
    uint32_t expected = s_expr_hash(str);
    if (got == expected) {
        printf("║  ✅ %-28s = 0x%08X\n", name, got);
        return 0;
    }
    printf("║  ❌ %s: got 0x%08X, expected 0x%08X\n", name, got, expected);
    (*errors)++;
    return 1;
}

// ============================================================================
// USER_PRINT_EXTRACT_RESULTS (Pass 1 & 2)
// Params: [0] STR title, [1] pass, [2-4] int, [5-7] uint, [8-10] float,
//         [11-13] bool, [14-16] hash
// ============================================================================

void user_print_extract_results(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type);
    UNUSED(event_id);
    UNUSED(event_data);
    
    if (!inst || !inst->blackboard || param_count < 17) return;
    
    uint8_t* bb = (uint8_t*)inst->blackboard;
    const char* title = get_string(inst, &params[0]);
    if (!title) title = "Unknown";
    
    int32_t pass = *(int32_t*)(bb + params[1].field_offset);
    
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║  %s (Pass %d)\n", title, pass);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  INTEGERS:                                                       ║\n");
    printf("║    int_val_1 (positive)      = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[2].field_offset), EXPECT_INT_1);
    printf("║    int_val_2 (negative)      = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[3].field_offset), EXPECT_INT_2);
    printf("║    int_val_3 (nested.deep)   = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[4].field_offset), EXPECT_INT_3);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  UNSIGNED INTEGERS:                                              ║\n");
    printf("║    uint_val_1 (small)        = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[5].field_offset), EXPECT_UINT_1);
    printf("║    uint_val_2 (medium)       = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[6].field_offset), EXPECT_UINT_2);
    printf("║    uint_val_3 (nested.deep)  = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[7].field_offset), EXPECT_UINT_3);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  FLOATS:                                                         ║\n");
    printf("║    float_val_1 (pi)          = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[8].field_offset), EXPECT_FLOAT_1);
    printf("║    float_val_2 (negative)    = %-10.2f  (expected: %.2f)\n",
        *(float*)(bb + params[9].field_offset), EXPECT_FLOAT_2);
    printf("║    float_val_3 (nested.deep) = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[10].field_offset), EXPECT_FLOAT_3);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  BOOLEANS:                                                       ║\n");
    printf("║    bool_val_1 (true_val)     = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[11].field_offset), EXPECT_BOOL_1);
    printf("║    bool_val_2 (false_val)    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[12].field_offset), EXPECT_BOOL_2);
    printf("║    bool_val_3 (nested.deep)  = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[13].field_offset), EXPECT_BOOL_3);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  HASHES:                                                         ║\n");
    printf("║    hash_val_1 (idle)         = 0x%08X  (expected: 0x%08X)\n",
        *(uint32_t*)(bb + params[14].field_offset), s_expr_hash("idle"));
    printf("║    hash_val_2 (running)      = 0x%08X  (expected: 0x%08X)\n",
        *(uint32_t*)(bb + params[15].field_offset), s_expr_hash("running"));
    printf("║    hash_val_3 (deep_hash)    = 0x%08X  (expected: 0x%08X)\n",
        *(uint32_t*)(bb + params[16].field_offset), s_expr_hash("deep_hash"));
    printf("╚══════════════════════════════════════════════════════════════════╝\n");
}

// ============================================================================
// USER_PRINT_ARRAY_RESULTS (Pass 3)
// Params: [0] STR title, [1] pass, [2-5] arr_int, [6-8] arr_float,
//         [9-10] n0 id/val, [11-12] n1 id/val, [13-14] n2 id/val
// ============================================================================

void user_print_array_results(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type);
    UNUSED(event_id);
    UNUSED(event_data);
    
    if (!inst || !inst->blackboard || param_count < 15) return;
    
    uint8_t* bb = (uint8_t*)inst->blackboard;
    const char* title = get_string(inst, &params[0]);
    if (!title) title = "Unknown";
    
    int32_t pass = *(int32_t*)(bb + params[1].field_offset);
    
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║  %s (Pass %d)\n", title, pass);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  INT ARRAY {10, 20, 30, 40}:                                     ║\n");
    printf("║    [0] = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[2].field_offset), EXPECT_ARR_INT_0);
    printf("║    [1] = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[3].field_offset), EXPECT_ARR_INT_1);
    printf("║    [2] = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[4].field_offset), EXPECT_ARR_INT_2);
    printf("║    [3] = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[5].field_offset), EXPECT_ARR_INT_3);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  FLOAT ARRAY {1.5, 2.5, 3.5}:                                   ║\n");
    printf("║    [0] = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[6].field_offset), EXPECT_ARR_FLOAT_0);
    printf("║    [1] = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[7].field_offset), EXPECT_ARR_FLOAT_1);
    printf("║    [2] = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[8].field_offset), EXPECT_ARR_FLOAT_2);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  NESTED ARRAY [{id:100,val:10.1}, {id:200,val:20.2}, ...]:       ║\n");
    printf("║    items[0].id    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[9].field_offset), EXPECT_ARR_N0_ID);
    printf("║    items[0].value = %-10.1f  (expected: %.1f)\n",
        *(float*)(bb + params[10].field_offset), EXPECT_ARR_N0_VAL);
    printf("║    items[1].id    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[11].field_offset), EXPECT_ARR_N1_ID);
    printf("║    items[1].value = %-10.1f  (expected: %.1f)\n",
        *(float*)(bb + params[12].field_offset), EXPECT_ARR_N1_VAL);
    printf("║    items[2].id    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[13].field_offset), EXPECT_ARR_N2_ID);
    printf("║    items[2].value = %-10.1f  (expected: %.1f)\n",
        *(float*)(bb + params[14].field_offset), EXPECT_ARR_N2_VAL);
    printf("╚══════════════════════════════════════════════════════════════════╝\n");
}

// ============================================================================
// USER_PRINT_POINTER_RESULTS (Pass 4)
// Params: [0] STR title, [1] pass, [2-3] int from sub_integers,
//         [4-5] float from sub_floats, [6-7] n0 id/val, [8-9] n1 id/val
// ============================================================================

void user_print_pointer_results(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type);
    UNUSED(event_id);
    UNUSED(event_data);
    
    if (!inst || !inst->blackboard || param_count < 10) return;
    
    uint8_t* bb = (uint8_t*)inst->blackboard;
    const char* title = get_string(inst, &params[0]);
    if (!title) title = "Unknown";
    
    int32_t pass = *(int32_t*)(bb + params[1].field_offset);
    
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║  %s (Pass %d)\n", title, pass);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  FROM sub_integers POINTER:                                      ║\n");
    printf("║    positive  = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[2].field_offset), EXPECT_PTR_INT_POS);
    printf("║    negative  = %-10d  (expected: %d)\n",
        *(int32_t*)(bb + params[3].field_offset), EXPECT_PTR_INT_NEG);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  FROM sub_floats POINTER:                                        ║\n");
    printf("║    pi        = %-10.5f  (expected: %.5f)\n",
        *(float*)(bb + params[4].field_offset), EXPECT_PTR_FLOAT_PI);
    printf("║    negative  = %-10.2f  (expected: %.2f)\n",
        *(float*)(bb + params[5].field_offset), EXPECT_PTR_FLOAT_NEG);
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  FROM sub_nested POINTERS (items[0], items[1]):                  ║\n");
    printf("║    items[0].id    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[6].field_offset), EXPECT_PTR_N0_ID);
    printf("║    items[0].value = %-10.1f  (expected: %.1f)\n",
        *(float*)(bb + params[7].field_offset), EXPECT_PTR_N0_VAL);
    printf("║    items[1].id    = %-10u  (expected: %u)\n",
        *(uint32_t*)(bb + params[8].field_offset), EXPECT_PTR_N1_ID);
    printf("║    items[1].value = %-10.1f  (expected: %.1f)\n",
        *(float*)(bb + params[9].field_offset), EXPECT_PTR_N1_VAL);
    printf("╚══════════════════════════════════════════════════════════════════╝\n");
}

// ============================================================================
// USER_VERIFY_RESULTS - Verify all 4 passes
// No params - reads blackboard directly by field hash
// ============================================================================

void user_verify_results(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params);
    UNUSED(param_count);
    UNUSED(event_type);
    UNUSED(event_id);
    UNUSED(event_data);
    
    if (!inst || !inst->blackboard) return;
    
    int errors = 0;
    
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║  VERIFICATION RESULTS                                            ║\n");
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  Pass 1 & 2: Scalar Extractions                                 ║\n");
    printf("╠──────────────────────────────────────────────────────────────────╣\n");
    
    // Pass 2 values remain in blackboard (last written)
    check_int("int_val_1",
        s_expr_blackboard_get_int(inst, s_expr_hash("int_val_1"), 0),
        EXPECT_INT_1, &errors);
    check_int("int_val_2",
        s_expr_blackboard_get_int(inst, s_expr_hash("int_val_2"), 0),
        EXPECT_INT_2, &errors);
    check_int("int_val_3",
        s_expr_blackboard_get_int(inst, s_expr_hash("int_val_3"), 0),
        EXPECT_INT_3, &errors);
    
    check_uint("uint_val_1",
        s_expr_blackboard_get_uint_by_string(inst, "uint_val_1", 0),
        EXPECT_UINT_1, &errors);
    check_uint("uint_val_2",
        s_expr_blackboard_get_uint_by_string(inst, "uint_val_2", 0),
        EXPECT_UINT_2, &errors);
    check_uint("uint_val_3",
        s_expr_blackboard_get_uint_by_string(inst, "uint_val_3", 0),
        EXPECT_UINT_3, &errors);
    
    check_float("float_val_1",
        s_expr_blackboard_get_float(inst, s_expr_hash("float_val_1"), 0.0f),
        EXPECT_FLOAT_1, &errors);
    check_float("float_val_2",
        s_expr_blackboard_get_float(inst, s_expr_hash("float_val_2"), 0.0f),
        EXPECT_FLOAT_2, &errors);
    check_float("float_val_3",
        s_expr_blackboard_get_float(inst, s_expr_hash("float_val_3"), 0.0f),
        EXPECT_FLOAT_3, &errors);
    
    check_uint("bool_val_1",
        s_expr_blackboard_get_uint_by_string(inst, "bool_val_1", 99),
        EXPECT_BOOL_1, &errors);
    check_uint("bool_val_2",
        s_expr_blackboard_get_uint_by_string(inst, "bool_val_2", 99),
        EXPECT_BOOL_2, &errors);
    check_uint("bool_val_3",
        s_expr_blackboard_get_uint_by_string(inst, "bool_val_3", 99),
        EXPECT_BOOL_3, &errors);
    
    check_hash("hash_val_1",
        s_expr_blackboard_get_uint_by_string(inst, "hash_val_1", 0),
        "idle", &errors);
    check_hash("hash_val_2",
        s_expr_blackboard_get_uint_by_string(inst, "hash_val_2", 0),
        "running", &errors);
    check_hash("hash_val_3",
        s_expr_blackboard_get_uint_by_string(inst, "hash_val_3", 0),
        "deep_hash", &errors);
    
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  Pass 3: Array Access                                            ║\n");
    printf("╠──────────────────────────────────────────────────────────────────╣\n");
    
    check_int("arr_int_0",
        s_expr_blackboard_get_int(inst, s_expr_hash("arr_int_0"), 0),
        EXPECT_ARR_INT_0, &errors);
    check_int("arr_int_1",
        s_expr_blackboard_get_int(inst, s_expr_hash("arr_int_1"), 0),
        EXPECT_ARR_INT_1, &errors);
    check_int("arr_int_2",
        s_expr_blackboard_get_int(inst, s_expr_hash("arr_int_2"), 0),
        EXPECT_ARR_INT_2, &errors);
    check_int("arr_int_3",
        s_expr_blackboard_get_int(inst, s_expr_hash("arr_int_3"), 0),
        EXPECT_ARR_INT_3, &errors);
    
    check_float("arr_float_0",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_float_0"), 0.0f),
        EXPECT_ARR_FLOAT_0, &errors);
    check_float("arr_float_1",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_float_1"), 0.0f),
        EXPECT_ARR_FLOAT_1, &errors);
    check_float("arr_float_2",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_float_2"), 0.0f),
        EXPECT_ARR_FLOAT_2, &errors);
    
    check_uint("arr_nested_0_id",
        s_expr_blackboard_get_uint_by_string(inst, "arr_nested_0_id", 0),
        EXPECT_ARR_N0_ID, &errors);
    check_float("arr_nested_0_val",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_nested_0_val"), 0.0f),
        EXPECT_ARR_N0_VAL, &errors);
    check_uint("arr_nested_1_id",
        s_expr_blackboard_get_uint_by_string(inst, "arr_nested_1_id", 0),
        EXPECT_ARR_N1_ID, &errors);
    check_float("arr_nested_1_val",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_nested_1_val"), 0.0f),
        EXPECT_ARR_N1_VAL, &errors);
    check_uint("arr_nested_2_id",
        s_expr_blackboard_get_uint_by_string(inst, "arr_nested_2_id", 0),
        EXPECT_ARR_N2_ID, &errors);
    check_float("arr_nested_2_val",
        s_expr_blackboard_get_float(inst, s_expr_hash("arr_nested_2_val"), 0.0f),
        EXPECT_ARR_N2_VAL, &errors);
    
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  Pass 4: Pointer Extraction                                      ║\n");
    printf("╠──────────────────────────────────────────────────────────────────╣\n");
    
    check_int("ptr_int_pos",
        s_expr_blackboard_get_int(inst, s_expr_hash("ptr_int_pos"), 0),
        EXPECT_PTR_INT_POS, &errors);
    check_int("ptr_int_neg",
        s_expr_blackboard_get_int(inst, s_expr_hash("ptr_int_neg"), 0),
        EXPECT_PTR_INT_NEG, &errors);
    check_float("ptr_float_pi",
        s_expr_blackboard_get_float(inst, s_expr_hash("ptr_float_pi"), 0.0f),
        EXPECT_PTR_FLOAT_PI, &errors);
    check_float("ptr_float_neg",
        s_expr_blackboard_get_float(inst, s_expr_hash("ptr_float_neg"), 0.0f),
        EXPECT_PTR_FLOAT_NEG, &errors);
    check_uint("ptr_n0_id",
        s_expr_blackboard_get_uint_by_string(inst, "ptr_n0_id", 0),
        EXPECT_PTR_N0_ID, &errors);
    check_float("ptr_n0_val",
        s_expr_blackboard_get_float(inst, s_expr_hash("ptr_n0_val"), 0.0f),
        EXPECT_PTR_N0_VAL, &errors);
    check_uint("ptr_n1_id",
        s_expr_blackboard_get_uint_by_string(inst, "ptr_n1_id", 0),
        EXPECT_PTR_N1_ID, &errors);
    check_float("ptr_n1_val",
        s_expr_blackboard_get_float(inst, s_expr_hash("ptr_n1_val"), 0.0f),
        EXPECT_PTR_N1_VAL, &errors);
    
    // Final tally
    int total = 15 + 13 + 8;  // Pass 1&2 + Pass 3 + Pass 4
    int passed = total - errors;
    
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    
    if (errors == 0) {
        printf("║  ✅ ALL %d TESTS PASSED                                         ║\n", total);
    } else {
        printf("║  ❌ %d/%d PASSED, %d FAILED                                     ║\n",
            passed, total, errors);
    }
    
    printf("╚══════════════════════════════════════════════════════════════════╝\n\n");
}