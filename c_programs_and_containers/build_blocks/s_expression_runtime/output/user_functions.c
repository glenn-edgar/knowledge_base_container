// ============================================================================
// s_expr_dsl_test_user_functions.c
// User function implementations for s_expr_dsl_test
// Test/stub implementations for validation
// ============================================================================

#include "s_expr_dsl_test_user_functions.h"
#include "s_expr_dsl_test_records.h"
#include "s_expr_dsl_test.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include <stdio.h>
#include <string.h>

// ============================================================================
// HELPER MACROS
// ============================================================================

#define UNUSED(x) (void)(x)

#define LOG_FUNC() printf("[%s] event=%d\n", __func__, event_type)

// ============================================================================
// ONESHOT FUNCTIONS
// ============================================================================

void init_system(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    printf("[init_system] Initializing system\n");
    
    // Initialize blackboard if available
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb) {
        bb->state = 0;
        bb->counter = 0;
        bb->enabled = true;
    }
    
}

void update_counter(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb) {
        bb->counter++;
        printf("[update_counter] counter=%d\n", bb->counter);
    }
}

void set_state(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb && param_count > 0) {
        bb->state = params[0].int_val;
        printf("[set_state] state=%d\n", bb->state);
    }
}

void set_vector(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb && param_count >= 3) {
        bb->motor.position.x = params[0].float_val;
        bb->motor.position.y = params[1].float_val;
        bb->motor.position.z = params[2].float_val;
        printf("[set_vector] pos=(%.2f, %.2f, %.2f)\n", 
               bb->motor.position.x, bb->motor.position.y, bb->motor.position.z);
    }
}

void set_float(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb && param_count > 0) {
        bb->temperature = params[0].float_val;
        printf("[set_float] temperature=%.2f\n", bb->temperature);
    }
}

void set_pid(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb && param_count >= 3) {
        bb->gains.kp = params[0].float_val;
        bb->gains.ki = params[1].float_val;
        bb->gains.kd = params[2].float_val;
        printf("[set_pid] kp=%.2f ki=%.2f kd=%.2f\n", 
               bb->gains.kp, bb->gains.ki, bb->gains.kd);
    }
}

void set_uint(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb && param_count > 0) {
        bb->flags = params[0].uint_val;
        printf("[set_uint] flags=0x%08X\n", bb->flags);
    }
}

void level_3a(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_3a] Executed\n");
}

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

s_expr_result_t process_state(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_id); UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        printf("[process_state] INIT\n");
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        printf("[process_state] TERMINATE\n");
        return SE_CONTINUE;
    }
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    if (bb) {
        printf("[process_state] TICK state=%d\n", bb->state);
    }

    
    
    return SE_CONTINUE;
}

s_expr_result_t wait_for_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
        return SE_HALT;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Wait for event 42
    if (event_id == 42) {
        printf("[wait_for_event] Got event 42!\n");
        return SE_CONTINUE;
    }
    
    return SE_HALT;
}

// TEST_PARAMS - validate all parameter types
// Expected params: int, uint, float, str, str_ptr, field_ref, nested_field_ref x2, const_ref, result
s_expr_result_t test_params(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    
    (void)event_data;
    
    int errors = 0;
    printf("event_id: %d\n", event_id);
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    printf("TEST_PARAMS: param_count=%d\n", param_count);
    
    // Param 0: int(-12345)
    if (param_count > 0) {
        uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
        int32_t val = params[0].int_val;
        if (opcode != S_EXPR_PARAM_INT || val != -12345) {
            printf("  ❌ param[0] INT: expected -12345, got %d (opcode=%d)\n", val, opcode);
            errors++;
        } else {
            printf("  ✅ param[0] INT: %d\n", val);
        }
    }
    
    // Param 1: uint(0xDEADBEEF)
    if (param_count > 1) {
        uint8_t opcode = params[1].type & S_EXPR_OPCODE_MASK;
        uint32_t val = params[1].uint_val;
        if (opcode != S_EXPR_PARAM_UINT || val != 0xDEADBEEF) {
            printf("  ❌ param[1] UINT: expected 0xDEADBEEF, got 0x%08X (opcode=%d)\n", val, opcode);
            errors++;
        } else {
            printf("  ✅ param[1] UINT: 0x%08X\n", val);
        }
    }
    
    // Param 2: flt(3.14159)
    if (param_count > 2) {
        uint8_t opcode = params[2].type & S_EXPR_OPCODE_MASK;
        float val = params[2].float_val;
        float expected = 3.14159f;
        float diff = val - expected;
        if (diff < 0) diff = -diff;
        if (opcode != S_EXPR_PARAM_FLOAT || diff > 0.0001f) {
            printf("  ❌ param[2] FLOAT: expected 3.14159, got %f (opcode=%d)\n", val, opcode);
            errors++;
        } else {
            printf("  ✅ param[2] FLOAT: %f\n", val);
        }
    }
    
    // Param 3: str("Hello, World!") - string index
    if (param_count > 3) {
        uint8_t opcode = params[3].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_STR_IDX) {
            printf("  ❌ param[3] STR_IDX: wrong opcode=%d\n", opcode);
            errors++;
        } else {
            const char* str = s_expr_param_string(inst->module->def, &params[3]);
            if (!str || strcmp(str, "Hello, World!") != 0) {
                printf("  ❌ param[3] STR_IDX: expected 'Hello, World!', got '%s'\n", str ? str : "NULL");
                errors++;
            } else {
                printf("  ✅ param[3] STR_IDX: '%s'\n", str);
            }
        }
    }
    
    // Param 4: str_ptr("This is a longer string for testing") - also string index
    if (param_count > 4) {
        uint8_t opcode = params[4].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_STR_IDX) {
            printf("  ❌ param[4] STR_PTR: wrong opcode=%d\n", opcode);
            errors++;
        } else {
            const char* str = s_expr_param_string(inst->module->def, &params[4]);
            if (!str || strcmp(str, "This is a longer string for testing") != 0) {
                printf("  ❌ param[4] STR_PTR: expected 'This is a longer string...', got '%s'\n", str ? str : "NULL");
                errors++;
            } else {
                printf("  ✅ param[4] STR_PTR: '%s'\n", str);
            }
        }
    }
    
    // Param 5: field_ref("counter") - FIELD type
    if (param_count > 5) {
        uint8_t opcode = params[5].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            printf("  ❌ param[5] FIELD_REF: wrong opcode=%d (expected %d)\n", opcode, S_EXPR_PARAM_FIELD);
            errors++;
        } else {
            printf("  ✅ param[5] FIELD_REF: offset=%d, size=%d\n", 
                   params[5].field_offset, params[5].field_size);
        }
    }
    
    // Param 6: nested_field_ref("gains.kp") - FIELD type
    if (param_count > 6) {
        uint8_t opcode = params[6].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            printf("  ❌ param[6] NESTED_FIELD_REF: wrong opcode=%d\n", opcode);
            errors++;
        } else {
            printf("  ✅ param[6] NESTED_FIELD_REF (gains.kp): offset=%d, size=%d\n",
                   params[6].field_offset, params[6].field_size);
        }
    }
    
    // Param 7: nested_field_ref("motor.position.x") - FIELD type
    if (param_count > 7) {
        uint8_t opcode = params[7].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            printf("  ❌ param[7] NESTED_FIELD_REF: wrong opcode=%d\n", opcode);
            errors++;
        } else {
            printf("  ✅ param[7] NESTED_FIELD_REF (motor.position.x): offset=%d, size=%d\n",
                   params[7].field_offset, params[7].field_size);
        }
    }
    
    // Param 8: const_ref("default_gains") - CONST_REF type
    if (param_count > 8) {
        uint8_t opcode = params[8].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_CONST_REF) {
            printf("  ❌ param[8] CONST_REF: wrong opcode=%d (expected %d)\n", opcode, S_EXPR_PARAM_CONST_REF);
            errors++;
        } else {
            printf("  ✅ param[8] CONST_REF: index=%d, size=%d\n",
                   params[8].const_index, params[8].const_size);
        }
    }
    
    // Param 9: result(SE_CONTINUE)
    if (param_count > 9) {
        uint8_t opcode = params[9].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_RESULT) {
            printf("  ❌ param[9] RESULT: wrong opcode=%d (expected %d)\n", opcode, S_EXPR_PARAM_RESULT);
            errors++;
        } else {
            printf("  ✅ param[9] RESULT: %d\n", (int)params[9].int_val);
        }
    }
    
    printf("TEST_PARAMS: %d errors\n", errors);
    
    return (errors == 0) ? SE_CONTINUE : SE_TERMINATE;
}

s_expr_result_t test_continue(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_CONTINUE;
}

s_expr_result_t test_terminate(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_TERMINATE;
}

s_expr_result_t test_reset(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_RESET;
}

s_expr_result_t test_disable(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_DISABLE;
}

s_expr_result_t test_halt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_HALT;
}

s_expr_result_t test_skip_continue(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_SKIP_CONTINUE;
}

s_expr_result_t test_function_halt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_FUNCTION_HALT;
}

s_expr_result_t test_function_reset(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_FUNCTION_RESET;
}

s_expr_result_t test_function_terminate(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return SE_FUNCTION_TERMINATE;
}

// Protothread simulations
s_expr_result_t protothread_1(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_id); UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
        printf("[protothread_1] INIT\n");
    }
    
    uint8_t state = s_expr_get_state(inst);
    printf("[protothread_1] state=%d\n", state);
    
    if (state < 3) {
        s_expr_set_state(inst, state + 1);
        return SE_HALT;
    }
    
    return SE_CONTINUE;
}

s_expr_result_t protothread_2(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_id); UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
    }
    
    uint8_t state = s_expr_get_state(inst);
    if (state < 2) {
        s_expr_set_state(inst, state + 1);
        return SE_HALT;
    }
    
    return SE_CONTINUE;
}

s_expr_result_t protothread_3(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_id); UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
    }
    
    uint8_t state = s_expr_get_state(inst);
    if (state < 1) {
        s_expr_set_state(inst, state + 1);
        return SE_HALT;
    }
    
    return SE_CONTINUE;
}

s_expr_result_t outer_func(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[outer_func] Executed\n");
    return SE_CONTINUE;
}

s_expr_result_t nested_protothread(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_id); UNUSED(event_data);
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
    }
    
    uint8_t state = s_expr_get_state(inst);
    if (state < 2) {
        s_expr_set_state(inst, state + 1);
        return SE_HALT;
    }
    
    return SE_CONTINUE;
}

// Nested levels
s_expr_result_t level_1(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_1] Executed\n");
    return SE_CONTINUE;
}

s_expr_result_t level_2a(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_2a] Executed\n");
    return SE_CONTINUE;
}

s_expr_result_t level_2b(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_2b] Executed\n");
    return SE_CONTINUE;
}

s_expr_result_t level_3b(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_3b] Executed\n");
    return SE_CONTINUE;
}

s_expr_result_t level_4(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    printf("[level_4] Executed\n");
    return SE_CONTINUE;
}

// ============================================================================
// PREDICATE FUNCTIONS
// ============================================================================

bool check_enabled(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(params); UNUSED(param_count);
    UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);

    return bb ? bb->enabled : false;
}

// Simple test predicates that always return true
bool pred_a(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_b(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_c(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool pred_d(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_e(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_f(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool pred_g(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool pred_h(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_i(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_j(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool pred_k(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

// Sensor/hardware predicates
bool sensor_a_ready(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool sensor_b_ready(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool timeout_expired(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool retry_available(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool flag_1(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool flag_2(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool flag_3(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool flag_4(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool check_condition(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool another_condition(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool monitor_state(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(inst);
    return bb ? (bb->state > 0) : false;
}

bool button_pressed(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return false;
}

bool sensor_active(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool custom_pred(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool deep_pred_1(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool deep_pred_2(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}

bool deep_pred_3(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data) {
    UNUSED(inst); UNUSED(params); UNUSED(param_count); UNUSED(event_type); UNUSED(event_id); UNUSED(event_data);
    return true;
}