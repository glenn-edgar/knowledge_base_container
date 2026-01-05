// user_s_test_38.c

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_runtime.h"
#include "chain_flow_dsl_tests_records.h"
#include <string.h>
#include <stdio.h>
#include <math.h>

#define FLOAT_EQ(a, b) (fabsf((a) - (b)) < 0.0001f)

// ============================================================================
// test39_init_funcs.c
// External init and verification for TEST 39
// ============================================================================

#include <stdio.h>
#include "s_engine_types.h"
#include "s_engine_module.h"

// ============================================================================
// STRUCTURES (match DSL records)
// ============================================================================
#if 0
typedef struct {
    float kp;
    float ki;
    float kd;
} pid_gains_c_t;
#endif
// ============================================================================
// EXPECTED VALUES
// ============================================================================

#define EXPECTED_KP  2.5f
#define EXPECTED_KI  0.5f
#define EXPECTED_KD  0.1f

// ============================================================================
// GLOBAL STATE
// ============================================================================

static int g_pass = 0;
static int g_fail = 0;

// ============================================================================
// EXTERNAL INIT FUNCTION
// Called after tree create, before first tick
// ============================================================================

static void test39_init(s_expr_tree_instance_t* inst) {
    printf("test39_init: Setting up blackboard and slots...\n");
    printf("test39_init: inst: %p\n", inst);
    
    // Get pointer to gains field in blackboard
    pid_gains_c_t* gains = s_expr_blackboard_get_field_by_string(inst, "gains");
    if (!gains) {
        printf("  ERROR: Could not get gains field\n");
        return;
    }
    
    // Set gains values
    gains->kp = EXPECTED_KP;
    gains->ki = EXPECTED_KI;
    gains->kd = EXPECTED_KD;
    printf("  blackboard.gains: kp=%.1f, ki=%.1f, kd=%.1f\n",
           gains->kp, gains->ki, gains->kd);
    
    // Get pointer to gains_ptr field and set it to point to gains
    pid_gains_c_t** gains_ptr = s_expr_blackboard_get_field_by_string(inst, "gains_ptr");
    if (!gains_ptr) {
        printf("  ERROR: Could not get gains_ptr field\n");
        return;
    }
    
    *gains_ptr = gains;
    printf("  blackboard.gains_ptr -> &blackboard.gains (%p)\n", (void*)gains);
    
    printf("test39_init: Complete\n\n");
}

bool test_39_set_init_data_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)event_type; (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    if(event_id == CFL_INIT_EVENT){
        // Get pointer to the stored pointer
        s_expr_tree_instance_t** inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
        if (!inst_ptr) {
            EXCEPTION("tree instance pointer not found");
            return false;
        }
        
        printf("test39_init: inst: %p\n", *inst_ptr);
        test39_init(*inst_ptr);
        return true;
    }
    if(event_id == CFL_TERMINATE_EVENT){
        return true;
    }

    return true;
}
// ============================================================================
// TEST_39_VERIFY_GAINS
// Verifies gains field values: kp=2.5, ki=0.5, kd=0.1
// ============================================================================

void test_39_verify_gains_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    printf("TEST_39_VERIFY_GAINS: ");
    
    if (param_count < 1) {
        printf("FAIL - no params\n");
        g_fail++;
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        printf("FAIL - param not FIELD type\n");
        g_fail++;
        return;
    }
    
    pid_gains_c_t* gains = S_EXPR_GET_FIELD(inst, &params[0], pid_gains_c_t);
    if (!gains) {
        printf("FAIL - null field ptr\n");
        g_fail++;
        return;
    }
    
    printf("kp=%.1f, ki=%.1f, kd=%.1f ", gains->kp, gains->ki, gains->kd);
    
    if (gains->kp == EXPECTED_KP && 
        gains->ki == EXPECTED_KI && 
        gains->kd == EXPECTED_KD) {
        printf("PASS\n");
        g_pass++;
    } else {
        printf("FAIL - values mismatch\n");
        g_fail++;
    }
}

// ============================================================================
// TEST_39_VERIFY_POINTER
// Verifies gains_ptr points to gains field with correct values
// ============================================================================

void test_39_verify_pointer_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("TEST_39_VERIFY_POINTER: ");
    
    // Get gains field address from param for comparison
    if (param_count < 1 || (params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        printf("FAIL - no field param\n");
        g_fail++;
        return;
    }
    
    pid_gains_c_t* field_gains = S_EXPR_GET_FIELD(inst, &params[0], pid_gains_c_t);
    
    // Get gains_ptr from blackboard (it's a PTR_FIELD, not a tree slot)
    pid_gains_c_t** gains_ptr = s_expr_blackboard_get_field_by_string(inst, "gains_ptr");
    if (!gains_ptr) {
        printf("FAIL - could not get gains_ptr field\n");
        g_fail++;
        return;
    }
    
    pid_gains_c_t* ptr_gains = *gains_ptr;
    if (!ptr_gains) {
        printf("FAIL - gains_ptr is NULL\n");
        g_fail++;
        return;
    }
    
    // Verify pointer points to gains field
    if (ptr_gains != field_gains) {
        printf("FAIL - pointer mismatch (ptr=%p, field=%p)\n", 
               (void*)ptr_gains, (void*)field_gains);
        g_fail++;
        return;
    }
    
    // Verify values through pointer
    if (ptr_gains->kp == EXPECTED_KP && 
        ptr_gains->ki == EXPECTED_KI && 
        ptr_gains->kd == EXPECTED_KD) {
        printf("ptr=%p PASS\n", (void*)ptr_gains);
        g_pass++;
    } else {
        printf("FAIL - values through pointer mismatch\n");
        g_fail++;
    }
}