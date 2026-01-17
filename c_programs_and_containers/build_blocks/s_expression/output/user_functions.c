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
#include "s_engine_builtins.h"
#include "cfl_exception.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
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
    if (param_count < 2) {
        printf("[set_state] param_count < 2\n");
        EXCEPTION("[set_state] param_count < 2");
        return;
    }
    // Get field offset from first param
    uint16_t offset = params[0].field_offset;
    
    
    // Get value from second param
    int32_t new_value = params[1].int_val;
    
    // Write to blackboard
    int32_t* field_ptr = (int32_t*)((uint8_t*)inst->blackboard + offset);
    *field_ptr = new_value;
    
    printf("[set_state] offset=%d, value=%d\n", offset, new_value);
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

/*

  list and dictionary processing functions


*/

// ============================================================================
// s_expr_dsl_test_new_stubs.c
// Stub implementations for new structure tests (trees 13-24)
// ============================================================================

#include "s_expr_dsl_test.h"
#include "s_expr_dsl_test_user_functions.h"
#include "s_engine_types.h"
#include <stdio.h>

// ============================================================================
// TEST TREE 13: Basic List Functions
// ============================================================================

// SE_PROCESS_INT_LIST - process a list of integers
// params: [OPEN list...integers... CLOSE] [RESULT]
s_expr_result_t process_int_list(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data; (void)inst;
    
    // Handle lifecycle events
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (param_count < 1){
        EXCEPTION("se_process_int_list: expected at least 1 parameter");
        return SE_CONTINUE;
    }
    
    // First param should be OPEN (the list)
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN) {
        EXCEPTION("se_process_int_list: expected OPEN for list");
        return SE_CONTINUE;
    }
    
    // Get list bounds using brace_idx
    uint16_t list_end_idx = params[0].brace_idx;  // Index of CLOSE relative to OPEN
    
    // Iterate through list contents (skip OPEN at 0, stop before CLOSE)
    for (uint16_t i = 1; i < list_end_idx; i++) {
        uint8_t elem_opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (elem_opcode == S_EXPR_PARAM_INT) {
            int32_t value = (int32_t)params[i].int_val;
            
            // Do something with the integer
            printf("Processing int: %d\n", value);
        }
        else if (elem_opcode == S_EXPR_PARAM_UINT) {
            uint32_t value = (uint32_t)params[i].uint_val;
            printf("Processing uint: %u\n", value);
        }
        // Handle nested structures if needed
        else if (elem_opcode == S_EXPR_PARAM_OPEN) {
            // Skip nested list
            i += params[i].brace_idx;
        }
    }
    
    // Find result code at end of params
    s_expr_result_t result = s_expr_find_result(params, param_count);
    printf("result: %d\n", result);
    
    return result;
}

// Maximum lists we'll track
#define MAX_LISTS 16

typedef struct {
    uint16_t param_idx;      // Index into params where OPEN is
    uint16_t element_count;  // Number of elements in list
} list_info_t;

s_expr_result_t multi_list_func(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // PASS 1: Identify all lists and store their locations
    // =========================================================================
    list_info_t lists[MAX_LISTS];
    uint16_t list_count = 0;
    uint16_t idx = 0;
    
    while (idx < param_count && list_count < MAX_LISTS) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            lists[list_count].param_idx = idx;
            lists[list_count].element_count = params[idx].brace_idx - 1;  // Exclude OPEN/CLOSE
            list_count++;
        }
        
        idx = s_expr_skip_param(params, idx);
    }
    
    printf("Identified %u lists:\n", list_count);
    for (uint16_t i = 0; i < list_count; i++) {
        printf("  List %u: starts at param[%u], %u elements\n", 
               i + 1, lists[i].param_idx, lists[i].element_count);
    }
    
    // =========================================================================
    // PASS 2: Process each list using stored info
    // =========================================================================
    for (uint16_t list_idx = 0; list_idx < list_count; list_idx++) {
        uint16_t open_idx = lists[list_idx].param_idx;
        
        uint16_t content_count;
        const s_expr_param_t* contents = s_expr_brace_contents(params, open_idx, &content_count);
        
        printf("\nProcessing List %u:\n", list_idx + 1);
        
        // Sum floats as example processing
        float sum = 0.0f;
        for (uint16_t i = 0; i < content_count; i++) {
            if ((contents[i].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
                sum += contents[i].float_val;
                printf("  + %.2f\n", (double)contents[i].float_val);
            }
        }
        printf("  = %.2f (sum)\n", (double)sum);
    }
    
    return s_expr_find_result(params, param_count);
}

#define MAX_NESTING_DEPTH 8

typedef struct {
    const s_expr_param_t* params;  // Pointer to list contents
    uint16_t count;                 // Number of elements
    uint16_t current_idx;           // Current position
} list_frame_t;

s_expr_result_t nested_lists(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Find the outer list
    if (param_count < 1){
        EXCEPTION("nested_lists_iterative: expected at least 1 parameter");
        return SE_CONTINUE;
    }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_OPEN) {
        return s_expr_find_result(params, param_count);
    }
    
    // Stack for tracking nested list traversal
    list_frame_t stack[MAX_NESTING_DEPTH];
    int16_t stack_depth = 0;
    
    // Push initial list onto stack
    uint16_t initial_count;
    const s_expr_param_t* initial_contents = s_expr_brace_contents(params, 0, &initial_count);
    
    stack[0].params = initial_contents;
    stack[0].count = initial_count;
    stack[0].current_idx = 0;
    stack_depth = 1;
    
    printf("=== Iterative Nested List Traversal ===\n");
    printf("ENTER list at depth 0\n");
    
    // Process until stack is empty
    while (stack_depth > 0) {
        list_frame_t* frame = &stack[stack_depth - 1];
        
        // Check if current list is exhausted
        if (frame->current_idx >= frame->count) {
            // Pop this list
            stack_depth--;
            printf("%*sEXIT list at depth %d\n", (stack_depth) * 2, "", stack_depth);
            continue;
        }
        
        // Get current element
        const s_expr_param_t* elem = &frame->params[frame->current_idx];
        uint8_t opcode = elem->type & S_EXPR_OPCODE_MASK;
        uint16_t depth = stack_depth - 1;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            // Nested list - push onto stack
            if (stack_depth >= MAX_NESTING_DEPTH) {
                printf("%*sERROR: Max nesting depth exceeded!\n", depth * 2, "");
                frame->current_idx = s_expr_skip_param(frame->params, frame->current_idx);
                continue;
            }
            
            uint16_t nested_count;
            const s_expr_param_t* nested = s_expr_brace_contents(
                frame->params, frame->current_idx, &nested_count
            );
            
            // Advance past this list in current frame
            frame->current_idx = s_expr_skip_param(frame->params, frame->current_idx);
            
            // Push new frame
            stack[stack_depth].params = nested;
            stack[stack_depth].count = nested_count;
            stack[stack_depth].current_idx = 0;
            stack_depth++;
            
            printf("%*sENTER list at depth %d (%u elements)\n", 
                   depth * 2, "", depth, nested_count);
        }
        else {
            // Regular element - process it
            printf("%*s", depth * 2, "");
            
            switch (opcode) {
                case S_EXPR_PARAM_INT:
                    printf("INT: %d\n", (int32_t)elem->int_val);
                    break;
                case S_EXPR_PARAM_UINT:
                    printf("UINT: %u\n", (uint32_t)elem->uint_val);
                    break;
                case S_EXPR_PARAM_FLOAT:
                    printf("FLOAT: %.4f\n", (double)elem->float_val);
                    break;
                default:
                    printf("<opcode 0x%02X>\n", opcode);
                    break;
            }
            
            // Advance to next element
            frame->current_idx = s_expr_skip_param(frame->params, frame->current_idx);
        }
    }
    
    printf("=== Done ===\n");
    return s_expr_find_result(params, param_count);
}
// ============================================================================
// TEST TREE 14: Dictionary Basic
// ============================================================================


// ============================================================================
// TEST TREE 15: Dictionary with Actions
// ============================================================================

s_expr_result_t state_machine_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("STATE_MACHINE_DISPATCH: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

void log_msg(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("LOG_MSG: param_count=%u\n", param_count);
    exit(0);
}

void set_counter(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("SET_COUNTER: param_count=%u\n", param_count);
    exit(0);
}

// ============================================================================
// TEST TREE 16: Array Basic
// ============================================================================

s_expr_result_t array_access(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("ARRAY_ACCESS: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

s_expr_result_t field_array(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("FIELD_ARRAY: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

s_expr_result_t matrix_2d(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("MATRIX_2D: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

// ============================================================================
// TEST TREE 17: Tuple Basic
// ============================================================================

s_expr_result_t process_tuple(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("PROCESS_TUPLE: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

s_expr_result_t tuple_table(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("TUPLE_TABLE: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

s_expr_result_t complex_tuple(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("COMPLEX_TUPLE: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

// ============================================================================
// TEST TREE 18: Named State Machine
// ============================================================================

void set_state_hash(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("SET_STATE_HASH: param_count=%u\n", param_count);
    exit(0);
}

bool check_start_condition(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("CHECK_START_CONDITION: param_count=%u\n", param_count);
    return true;
    exit(0);
}

// ============================================================================
// TEST TREE 19: Dict Event Dispatch
// ============================================================================

void increment_counter(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("INCREMENT_COUNTER: param_count=%u\n", param_count);
    exit(0);
}

void toggle_enabled(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("TOGGLE_ENABLED: param_count=%u\n", param_count);
    exit(0);
}

void read_sensor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("READ_SENSOR: param_count=%u\n", param_count);
    exit(0);
}

// ============================================================================
// TEST TREE 20: Complex Structures
// ============================================================================

s_expr_result_t load_config(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("LOAD_CONFIG: param_count=%u\n", param_count);
    exit(0);
    
    return SE_CONTINUE;
}

// ============================================================================
// TEST TREE 21: Alist Style
// ============================================================================

s_expr_result_t process_alist(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("PROCESS_ALIST: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

// ============================================================================
// TEST TREE 22: Plist Style
// ============================================================================

s_expr_result_t process_plist(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("PROCESS_PLIST: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

// ============================================================================
// TEST TREE 23: Mixed Dispatch
// ============================================================================

s_expr_result_t mixed_structure_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("MIXED_STRUCTURE_DISPATCH: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}

void action_1(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("ACTION_1\n");
    exit(0);
}

void action_2(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("ACTION_2\n");
    exit(0);
}

void action_3(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("ACTION_3\n");\
    exit(0);
}

void sub_action_a(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("SUB_ACTION_A\n");
    exit(0);
}

void sub_action_b(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("SUB_ACTION_B\n");
    exit(0);
}

// ============================================================================
// TEST TREE 24: Brace Navigation
// ============================================================================

s_expr_result_t brace_test(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)event_type; (void)event_id; (void)event_data;
    printf("BRACE_TEST: param_count=%u\n", param_count);
    exit(0);
    return SE_CONTINUE;
}