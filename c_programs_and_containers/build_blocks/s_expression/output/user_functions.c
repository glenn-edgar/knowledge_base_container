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
#include "s_engine_node.h"
#include  "s_engine_types.h"
#include "s_engine_list_dictionary_support.h"
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

/// SE_PROCESS_INT_LIST - process a list of integers
// params: [OPEN list...integers... CLOSE]
// Returns SE_CONTINUE after processing
s_expr_result_t process_int_list(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data; (void)inst;
    
    // Structure:
    // Child 0: OPEN list of integers CLOSE - not callable
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    // TICK: Process the integer list
    if (param_count < 1) {
        EXCEPTION("se_process_int_list: expected at least 1 parameter");
        return SE_CONTINUE;
    }
    
    // First param should be OPEN (the list)
    uint16_t list_phys_idx = s_expr_child_index(params, param_count, 0);
    if (list_phys_idx == UINT16_MAX) {
        EXCEPTION("se_process_int_list: no list found");
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[list_phys_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN) {
        EXCEPTION("se_process_int_list: expected OPEN for list");
        return SE_CONTINUE;
    }
    
    // Get list bounds using brace_idx
    uint16_t list_end_idx = list_phys_idx + params[list_phys_idx].brace_idx;
    
    // Iterate through list contents (skip OPEN, stop before CLOSE)
    for (uint16_t i = list_phys_idx + 1; i < list_end_idx; i++) {
        uint8_t elem_opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (elem_opcode == S_EXPR_PARAM_INT) {
            int32_t value = (int32_t)params[i].int_val;
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
    
    return SE_CONTINUE;
}
// ============================================================================
// multi_list_func - Process multiple lists (sum floats)
// NEW FORMAT: Uses proper event handling
// ============================================================================

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
    
    // Handle lifecycle events
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK: Process the lists
    
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
            lists[list_count].element_count = params[idx].brace_idx - 1;
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
        
        // Sum floats
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


// ============================================================================
// nested_lists - Iterative nested list traversal
// NEW FORMAT: Uses proper event handling
// ============================================================================

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
    
    // Handle lifecycle events
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK: Process nested lists
    
    // Validate: need at least one parameter
    if (param_count < 1) {
        EXCEPTION("nested_lists: expected at least 1 parameter");
        return SE_CONTINUE;
    }
    
    // Check if first param is a list
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
            stack_depth--;
            printf("%*sEXIT list at depth %d\n", stack_depth * 2, "", stack_depth);
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

// ============================================================================
// Test Array Functions - New Format
// ============================================================================


// ============================================================================
// FIELD_ARRAY - Array of field references
// params: [OPEN_ARRAY field_refs...] [result]
// ============================================================================
s_expr_result_t field_array(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("field_array: need array");
            return SE_CONTINUE;
        }
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK
    // =========================================================================
    
    printf("=== FIELD_ARRAY DEBUG ===\n");
    printf("Blackboard ptr: %p, size: %u\n", inst->blackboard, inst->blackboard_size);
    
    if (inst->blackboard) {
        printf("Blackboard dump (expected: state=1, command=2, counter=5, gains.kp=1.0, motor.position.x=100.0):\n");
        uint8_t* bb = (uint8_t*)inst->blackboard;
        
        // Show key offsets based on record layout
        printf("  offset  0 (state):    int=%d\n", *(int32_t*)(bb + 0));
        printf("  offset  4 (hash):     uint=0x%08X\n", *(uint32_t*)(bb + 4));
        printf("  offset  8 (command):  int=%d\n", *(int32_t*)(bb + 8));
        printf("  offset 12 (event_id): int=%d\n", *(int32_t*)(bb + 12));
        printf("  offset 16 (counter):  uint=%u\n", *(uint32_t*)(bb + 16));
        printf("  offset 20 (temp):     float=%f\n", *(float*)(bb + 20));
        printf("  offset 24 (enabled):  bool=%d\n", *(uint8_t*)(bb + 24));
        // gains is at offset 108, kp is first field
        printf("  offset 108 (gains.kp): float=%f\n", *(float*)(bb + 108));
        printf("  offset 112 (gains.ki): float=%f\n", *(float*)(bb + 112));
        printf("  offset 116 (gains.kd): float=%f\n", *(float*)(bb + 116));
        // motor is at offset 76, position is first (vec3), x is first
        printf("  offset 76 (motor.position.x): float=%f\n", *(float*)(bb + 76));
        printf("  offset 80 (motor.position.y): float=%f\n", *(float*)(bb + 80));
        printf("  offset 84 (motor.position.z): float=%f\n", *(float*)(bb + 84));
    }
    
    // Find array
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    printf("\nparams[0] opcode: 0x%02X (expected OPEN_ARRAY=0x%02X)\n", opcode, S_EXPR_PARAM_OPEN_ARRAY);
    
    if (opcode != S_EXPR_PARAM_OPEN_ARRAY) {
        EXCEPTION("field_array: expected OPEN_ARRAY");
        return s_expr_find_result(params, param_count);
    }
    
    uint16_t array_end_idx = params[0].brace_idx;
    printf("Array brace_idx (end): %u\n", array_end_idx);
    
    printf("\nIterating array - checking field params:\n");
    
    uint16_t element_num = 0;
    uint16_t idx = 1;
    
    while (idx < array_end_idx) {
        uint8_t elem_opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (elem_opcode == S_EXPR_PARAM_FIELD) {
            printf("  [%u] params[%u]: FIELD\n", element_num, idx);
            printf("       field_offset=%u, field_size=%u\n", 
                   params[idx].field_offset, params[idx].field_size);
            printf("       uint_val=0x%08X (raw 32-bit)\n", params[idx].uint_val);
            
            void* val_ptr = S_EXPR_GET_FIELD(inst, &params[idx], void);
            if (val_ptr) {
                int32_t as_int = *(int32_t*)val_ptr;
                float as_float = *(float*)val_ptr;
                printf("       -> as_int=%d, as_float=%f\n", as_int, as_float);
            } else {
                printf("       -> NULL\n");
            }
            element_num++;
        } else if (elem_opcode == S_EXPR_PARAM_CLOSE_ARRAY) {
            printf("  params[%u]: CLOSE_ARRAY\n", idx);
            break;
        } else {
            printf("  params[%u]: opcode=0x%02X\n", idx, elem_opcode);
        }
        
        idx = s_expr_skip_param(params, idx);
    }
    
    printf("Total fields: %u\n", element_num);
    printf("=== END DEBUG ===\n");
    
    return s_expr_find_result(params, param_count);
}
// ============================================================================
// MATRIX_2D - Nested 2D array (matrix)
// params: [OPEN_ARRAY rows...] [result]
// ============================================================================
s_expr_result_t matrix_2d(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_id; (void)event_data;
    
    // =========================================================================
    // TERMINATE
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("se_matrix_2d: need array");
            return SE_CONTINUE;
        }
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK
    // =========================================================================
    
    // Find outer array (rows)
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_ARRAY) {
        EXCEPTION("se_matrix_2d: expected OPEN_ARRAY");
        return s_expr_find_result(params, param_count);
    }
    
    // Get outer array contents
    uint16_t outer_count;
    const s_expr_param_t* outer = s_expr_brace_contents(params, 0, &outer_count);
    
    uint16_t row_count = s_expr_child_count(outer, outer_count);
    
    printf("Matrix %u rows:\n", row_count);
    
    for (uint16_t row = 0; row < row_count; row++) {
        uint16_t row_idx = s_expr_child_index(outer, outer_count, row);
        if (row_idx == UINT16_MAX) {
            continue;
        }
        
        const s_expr_param_t* row_param = &outer[row_idx];
        uint8_t row_opcode = row_param->type & S_EXPR_OPCODE_MASK;
        
        if (row_opcode != S_EXPR_PARAM_OPEN_ARRAY) {
            printf("  Row %u: not an array\n", row);
            continue;
        }
        
        // Get inner array contents (columns)
        uint16_t inner_count;
        const s_expr_param_t* inner = s_expr_brace_contents(outer, row_idx, &inner_count);
        
        uint16_t col_count = s_expr_child_count(inner, inner_count);
        
        printf("  Row %u: [", row);
        
        for (uint16_t col = 0; col < col_count; col++) {
            uint16_t col_idx = s_expr_child_index(inner, inner_count, col);
            if (col_idx == UINT16_MAX) {
                continue;
            }
            
            const s_expr_param_t* elem = &inner[col_idx];
            uint8_t elem_opcode = elem->type & S_EXPR_OPCODE_MASK;
            
            if (col > 0) printf(", ");
            
            switch (elem_opcode) {
                case S_EXPR_PARAM_INT:
                    printf("%d", (int32_t)elem->int_val);
                    break;
                case S_EXPR_PARAM_UINT:
                    printf("%u", (uint32_t)elem->uint_val);
                    break;
                case S_EXPR_PARAM_FLOAT:
                    printf("%.1f", (double)elem->float_val);
                    break;
                default:
                    printf("?");
                    break;
            }
        }
        
        printf("]\n");
    }
    
    return SE_CONTINUE;
}

// ============================================================================
// ARRAY_ACCESS - Access integer array by index
// params: [OPEN_ARRAY ints...] [index] [result]
// ============================================================================
s_expr_result_t array_access(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_id; (void)event_data;
    
    (void)inst; (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_ARRAY) {
        EXCEPTION("se_array_access: expected OPEN_ARRAY");
        return SE_FUNCTION_TERMINATE;
    }
    
    // Get array contents
    uint16_t content_count;
    const s_expr_param_t* contents = s_expr_brace_contents(params, 0, &content_count);
    
    
    // Get index parameter (after array)
    uint16_t index_idx = s_expr_skip_param(params, 0);

    
    if (index_idx >= param_count) {
        EXCEPTION("se_array_access: missing index");
        return SE_FUNCTION_TERMINATE;
    }
    
    uint16_t access_index = (uint16_t)params[index_idx].int_val;
    
    
    uint16_t element_count = s_expr_child_count(contents, content_count);
    
    
    if (access_index >= element_count) {
        EXCEPTION("se_array_access: index out of bounds");
        return SE_FUNCTION_TERMINATE;
    }
    
    // Get element at index
    uint16_t elem_idx = s_expr_child_index(contents, content_count, access_index);
    if (elem_idx == UINT16_MAX) {
        EXCEPTION("se_array_access: failed to find element");
        return SE_FUNCTION_TERMINATE;
    }
    
    const s_expr_param_t* elem = &contents[elem_idx];
    uint8_t elem_opcode = elem->type & S_EXPR_OPCODE_MASK;
    
    printf("Array access [%u]: ", access_index);
    switch (elem_opcode) {
        case S_EXPR_PARAM_INT:
            printf("INT %d\n", (int32_t)elem->int_val);
            break;
        case S_EXPR_PARAM_UINT:
            printf("UINT %u\n", (uint32_t)elem->uint_val);
            break;
        case S_EXPR_PARAM_FLOAT:
            printf("FLOAT %.4f\n", (double)elem->float_val);
            break;
        default:
            printf("<opcode 0x%02X>\n", elem_opcode);
            break;
    }
    
    return SE_CONTINUE;
}




// ============================================================================
// TEST FUNCTIONS
// ============================================================================

s_expr_result_t process_tuple(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;  (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }

    printf("PROCESS_TUPLE: param_count=%u\n", param_count);
    
    if (param_count < 1 || !S_EXPR_PARAM_IS_OPEN_TUPLE(params[0].type)) {
        printf("  ERROR: expected OPEN_TUPLE at params[0]\n");
        return SE_CONTINUE;
    }
    
    uint16_t tuple_count;
    const s_expr_param_t* tuple_params = s_expr_tuple_contents(&params[0], &tuple_count);
    if (!tuple_params) {
        printf("  ERROR: failed to get tuple contents\n");
        return SE_CONTINUE;
    }
    
    printf("  tuple t1: %u elements\n", tuple_count);
    
    if (tuple_count >= 3) {
        if (S_EXPR_PARAM_IS_STR_IDX(tuple_params[0].type)) {
            const char* str = s_expr_param_string(inst->module->def, &tuple_params[0]);
            printf("    [0] str: \"%s\"\n", str ? str : "(null)");
        }
        if ((tuple_params[1].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_INT) {
            printf("    [1] int: %d\n", (int)tuple_params[1].int_val);
        }
        if ((tuple_params[2].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
            printf("    [2] float: %f\n", (double)tuple_params[2].float_val);
        }
    }
    
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
    (void)event_id;  (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("TUPLE_TABLE: param_count=%u\n", param_count);
    
    if (param_count < 1 || !S_EXPR_PARAM_IS_OPEN_ARRAY(params[0].type)) {
        printf("  ERROR: expected OPEN_ARRAY at params[0]\n");
        return SE_CONTINUE;
    }
    
    uint16_t array_count;
    const s_expr_param_t* array_params = s_expr_array_contents(&params[0], &array_count);
    if (!array_params) {
        printf("  ERROR: failed to get array contents\n");
        return SE_CONTINUE;
    }
    
    printf("  array a1: %u elements (raw)\n", array_count);
    
    uint16_t idx = 0;
    int tuple_num = 0;
    
    while (idx < array_count) {
        if (S_EXPR_PARAM_IS_OPEN_TUPLE(array_params[idx].type)) {
            uint16_t tuple_count;
            const s_expr_param_t* tuple_params = s_expr_tuple_contents(&array_params[idx], &tuple_count);
            
            if (tuple_params) {
                printf("  tuple[%d]: %u elements\n", tuple_num, tuple_count);
                
                if (tuple_count >= 4) {
                    if (S_EXPR_PARAM_IS_STR_IDX(tuple_params[0].type)) {
                        const char* str = s_expr_param_string(inst->module->def, &tuple_params[0]);
                        printf("    name: \"%s\"\n", str ? str : "(null)");
                    }
                    if ((tuple_params[1].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_INT) {
                        printf("    id: %d\n", (int)tuple_params[1].int_val);
                    }
                    if ((tuple_params[2].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
                        printf("    value: %f\n", (double)tuple_params[2].float_val);
                    }
                    if ((tuple_params[3].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_UINT) {
                        printf("    flags: 0x%02X\n", (unsigned)tuple_params[3].uint_val);
                    }
                }
                tuple_num++;
            }
            
            // Skip past this tuple
            idx += array_params[idx].brace_idx + 1;
        } else {
            idx++;
        }
    }
    
    printf("  total tuples: %d\n", tuple_num);
    
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
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("COMPLEX_TUPLE: param_count=%u\n", param_count);
    
    if (param_count < 1 || !S_EXPR_PARAM_IS_OPEN_TUPLE(params[0].type)) {
        printf("  ERROR: expected OPEN_TUPLE at params[0]\n");
        return SE_CONTINUE;
    }
    
    uint16_t tuple_count;
    const s_expr_param_t* tuple_params = s_expr_tuple_contents(&params[0], &tuple_count);
    if (!tuple_params) {
        printf("  ERROR: failed to get tuple contents\n");
        return SE_CONTINUE;
    }
    
    printf("  tuple t4: %u raw elements\n", tuple_count);
    
    uint16_t idx = 0;
    
    // Element 0: str_idx("motor_config")
    if (idx < tuple_count && S_EXPR_PARAM_IS_STR_IDX(tuple_params[idx].type)) {
        const char* str = s_expr_param_string(inst->module->def, &tuple_params[idx]);
        printf("    name: \"%s\"\n", str ? str : "(null)");
        idx++;
    }
    
    // Element 1: nested dict d1
    if (idx < tuple_count && S_EXPR_PARAM_IS_OPEN_DICT(tuple_params[idx].type)) {
        printf("    nested dict:\n");
        
        const s_expr_param_t* dict_param = &tuple_params[idx];
        
        // Look up "max_speed" by hash
        const s_expr_param_t* max_speed_val = s_expr_dict_find_key(dict_param, s_expr_hash("max_speed"));
        if (max_speed_val && (max_speed_val->type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
            printf("      max_speed: %f\n", (double)max_speed_val->float_val);
        } else {
            printf("      max_speed: NOT FOUND\n");
        }
        
        // Look up "acceleration" by hash
        const s_expr_param_t* accel_val = s_expr_dict_find_key(dict_param, s_expr_hash("acceleration"));
        if (accel_val && (accel_val->type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
            printf("      acceleration: %f\n", (double)accel_val->float_val);
        } else {
            printf("      acceleration: NOT FOUND\n");
        }
        
        // Skip past dict in tuple
        idx += tuple_params[idx].brace_idx + 1;
    }
    
    // Element 2: nested array a2
    if (idx < tuple_count && S_EXPR_PARAM_IS_OPEN_ARRAY(tuple_params[idx].type)) {
        printf("    nested array (limits):\n");
        
        uint16_t arr_count;
        const s_expr_param_t* arr_params = s_expr_array_contents(&tuple_params[idx], &arr_count);
        
        if (arr_params) {
            for (uint16_t i = 0; i < arr_count; i++) {
                if ((arr_params[i].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FLOAT) {
                    printf("      [%u]: %f\n", i, (double)arr_params[i].float_val);
                }
            }
        }
    }
    
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
s_expr_result_t increment_counter(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK: Increment the counter field
    if (param_count < 1) {
        EXCEPTION("increment_counter: need field_ref");
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("increment_counter: expected field_ref");
        return SE_CONTINUE;
    }
    
    // Get pointer to the counter field in blackboard
    int32_t* counter_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!counter_ptr) {
        EXCEPTION("increment_counter: NULL field pointer");
        return SE_CONTINUE;
    }
    
    // Increment
    int32_t old_val = *counter_ptr;
    (*counter_ptr)++;
    
    printf("INCREMENT_COUNTER: %d -> %d\n", old_val, *counter_ptr);
    
    return SE_CONTINUE;  // Event dispatch handles lifecycle
}


s_expr_result_t toggle_enabled(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK: Toggle the boolean field
    if (param_count < 1) {
        EXCEPTION("toggle_enabled: need field_ref");
        return SE_DISABLE;
    }
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("toggle_enabled: expected field_ref");
        return SE_DISABLE;
    }
    
    // Get pointer to the boolean field in blackboard
    bool* enabled_ptr = S_EXPR_GET_FIELD(inst, &params[0], bool);
    if (!enabled_ptr) {
        EXCEPTION("toggle_enabled: NULL field pointer");
        return SE_DISABLE;
    }
    
    // Toggle
    *enabled_ptr = !(*enabled_ptr);
    
    printf("TOGGLE_ENABLED: %s -> %s\n", 
           *enabled_ptr ? "false" : "true",   // was
           *enabled_ptr ? "true" : "false");  // now
    
    return SE_DISABLE;  // One-shot action, done
}

s_expr_result_t read_sensor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK: Read sensor and store in field
    if (param_count < 1) {
        EXCEPTION("read_sensor: need field_ref");
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("read_sensor: expected field_ref");
        return SE_CONTINUE;
    }
    
    // Get pointer to the temperature field in blackboard
    int32_t* temp_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!temp_ptr) {
        EXCEPTION("read_sensor: NULL field pointer");
        return SE_CONTINUE;
    }
    
    // Simulate sensor read - increment by small random-ish amount
    // In real system this would read actual hardware
    int32_t old_val = *temp_ptr;
    *temp_ptr = old_val + (old_val % 3) + 1;  // Simple deterministic "variation"
    
    printf("READ_SENSOR: temperature %d -> %d\n", old_val, *temp_ptr);
    
    return SE_CONTINUE;  // Event dispatch handles lifecycle
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
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (param_count < 1) {
        EXCEPTION("load_config: need dict");
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("load_config: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    const s_expr_param_t* config_dict = &params[0];
    
    printf("=== LOAD_CONFIG: Parsing configuration ===\n");
    
    // =========================================================================
    // Parse "sensors" - array of tuples (name, id, min, max)
    // =========================================================================
    uint32_t sensors_hash = s_expr_hash("sensors");
    const s_expr_param_t* sensors_content = s_expr_dict_find_key(config_dict, sensors_hash);
    
    if (sensors_content) {
        printf("\n[sensors]\n");
        
        uint16_t array_count;
        const s_expr_param_t* array_items = s_expr_array_contents(sensors_content, &array_count);
        
        if (array_items) {
            const s_expr_param_t* p = array_items;
            int sensor_idx = 0;
            
            while (p < sensors_content + sensors_content->brace_idx) {
                if (S_EXPR_PARAM_IS_OPEN_TUPLE(p->type)) {
                    uint16_t tuple_count;
                    const s_expr_param_t* tuple_items = s_expr_tuple_contents(p, &tuple_count);
                    
                    if (tuple_items && tuple_count >= 4) {
                        const char* name = inst->module->def->string_table[tuple_items[0].str_index];
                        int32_t id = tuple_items[1].int_val;
                        float min_val = tuple_items[2].float_val;
                        float max_val = tuple_items[3].float_val;
                        
                        printf("  sensor[%d]: name=\"%s\" id=%d range=[%.1f, %.1f]\n",
                               sensor_idx, name, id, min_val, max_val);
                    }
                    
                    p += p->brace_idx + 1;
                    sensor_idx++;
                } else {
                    p++;
                }
            }
        }
    }
    
    // =========================================================================
    // Parse "actuators" - array of tuples (name, id, dict{min,max,default})
    // =========================================================================
    uint32_t actuators_hash = s_expr_hash("actuators");
    const s_expr_param_t* actuators_content = s_expr_dict_find_key(config_dict, actuators_hash);
    
    if (actuators_content) {
        printf("\n[actuators]\n");
        
        uint16_t array_count;
        const s_expr_param_t* array_items = s_expr_array_contents(actuators_content, &array_count);
        
        if (array_items) {
            const s_expr_param_t* p = array_items;
            int actuator_idx = 0;
            
            while (p < actuators_content + actuators_content->brace_idx) {
                if (S_EXPR_PARAM_IS_OPEN_TUPLE(p->type)) {
                    uint16_t tuple_count;
                    const s_expr_param_t* tuple_items = s_expr_tuple_contents(p, &tuple_count);
                    
                    if (tuple_items && tuple_count >= 3) {
                        const char* name = inst->module->def->string_table[tuple_items[0].str_index];
                        int32_t id = tuple_items[1].int_val;
                        
                        printf("  actuator[%d]: name=\"%s\" id=%d", actuator_idx, name, id);
                        
                        const s_expr_param_t* limits_dict = &tuple_items[2];
                        if (S_EXPR_PARAM_IS_OPEN_DICT(limits_dict->type)) {
                            uint32_t min_hash = s_expr_hash("min");
                            uint32_t max_hash = s_expr_hash("max");
                            uint32_t default_hash = s_expr_hash("default");
                            
                            const s_expr_param_t* min_val = s_expr_dict_find_key(limits_dict, min_hash);
                            const s_expr_param_t* max_val = s_expr_dict_find_key(limits_dict, max_hash);
                            const s_expr_param_t* def_val = s_expr_dict_find_key(limits_dict, default_hash);
                            
                            printf(" limits={");
                            if (min_val) printf("min=%.1f", min_val->float_val);
                            if (max_val) printf(", max=%.1f", max_val->float_val);
                            if (def_val) printf(", default=%.1f", def_val->float_val);
                            printf("}");
                        }
                        printf("\n");
                    }
                    
                    p += p->brace_idx + 1;
                    actuator_idx++;
                } else {
                    p++;
                }
            }
        }
    }
    
    // =========================================================================
    // Parse "timing" - dict with tick_rate, watchdog_ms, startup_delay
    // =========================================================================
    uint32_t timing_hash = s_expr_hash("timing");
    const s_expr_param_t* timing_content = s_expr_dict_find_key(config_dict, timing_hash);
    
    if (timing_content) {
        printf("\n[timing]\n");
        
        if (S_EXPR_PARAM_IS_OPEN_DICT(timing_content->type)) {
            uint32_t tick_rate_hash = s_expr_hash("tick_rate");
            uint32_t watchdog_hash = s_expr_hash("watchdog_ms");
            uint32_t startup_hash = s_expr_hash("startup_delay");
            
            const s_expr_param_t* tick_rate = s_expr_dict_find_key(timing_content, tick_rate_hash);
            const s_expr_param_t* watchdog = s_expr_dict_find_key(timing_content, watchdog_hash);
            const s_expr_param_t* startup = s_expr_dict_find_key(timing_content, startup_hash);
            
            if (tick_rate) printf("  tick_rate: %d Hz\n", tick_rate->int_val);
            if (watchdog) printf("  watchdog_ms: %d ms\n", watchdog->int_val);
            if (startup) printf("  startup_delay: %.2f s\n", startup->float_val);
        }
    }
    
    // =========================================================================
    // Parse "flags" - list of uint flags
    // =========================================================================
    uint32_t flags_hash = s_expr_hash("flags");
    const s_expr_param_t* flags_content = s_expr_dict_find_key(config_dict, flags_hash);
    
    if (flags_content) {
        printf("\n[flags]\n");
        
        uint16_t list_count;
        const s_expr_param_t* list_items = s_expr_list_contents(flags_content, &list_count);
        
        if (list_items) {
            uint32_t combined_flags = 0;
            printf("  flags: ");
            for (uint16_t i = 0; i < list_count; i++) {
                uint32_t flag = list_items[i].uint_val;
                combined_flags |= flag;
                printf("0x%02X ", flag);
            }
            printf("\n  combined: 0x%02X\n", combined_flags);
            
            printf("  decoded: ");
            if (combined_flags & 0x01) printf("LOGGING ");
            if (combined_flags & 0x02) printf("WATCHDOG ");
            if (combined_flags & 0x04) printf("SAFETY ");
            if (combined_flags & 0x08) printf("DEBUG ");
            printf("\n");
        }
    }
    
    printf("\n=== Configuration loaded ===\n");
    
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
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (param_count < 1 || !S_EXPR_PARAM_IS_OPEN(params[0].type)) {
        EXCEPTION("process_alist: need list");
        return SE_CONTINUE;
    }
    
    const s_expr_param_t* alist = &params[0];
    const s_expr_module_def_t* def = inst->module->def;
    
    printf("=== PROCESS_ALIST ===\n");
    
    // Clean typed access with defaults
    const char* name = s_expr_alist_str(def, alist, s_expr_hash("name"), "unknown");
    int32_t version = s_expr_alist_int(alist, s_expr_hash("version"), 0);
    bool enabled = s_expr_alist_bool(alist, s_expr_hash("enabled"), false);
    float timeout = s_expr_alist_float(alist, s_expr_hash("timeout"), 1.0f);
    
    printf("  name:    \"%s\"\n", name);
    printf("  version: %d\n", version);
    printf("  enabled: %s\n", enabled ? "true" : "false");
    printf("  timeout: %.2f\n", timeout);
    
    printf("=== Done ===\n");
    
    return SE_CONTINUE;
}


//============================================================================
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
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (param_count < 1 || !S_EXPR_PARAM_IS_OPEN(params[0].type)) {
        EXCEPTION("process_plist: need list");
        return SE_CONTINUE;
    }
    
    const s_expr_param_t* plist = &params[0];
    const s_expr_module_def_t* def = inst->module->def;
    
    printf("=== PROCESS_PLIST ===\n");
    
    // Clean typed access with defaults
    const char* name = s_expr_plist_str(def, plist, s_expr_hash("name"), "unknown");
    int32_t channel = s_expr_plist_int(plist, s_expr_hash("channel"), -1);
    float gain = s_expr_plist_float(plist, s_expr_hash("gain"), 1.0f);
    bool enabled = s_expr_plist_bool(plist, s_expr_hash("enabled"), false);
    s_expr_hash_t mode = s_expr_plist_hash(plist, s_expr_hash("mode"), 0);
    
    // Decode mode hash for display
    const char* mode_str = "unknown";
    if (mode == s_expr_hash("auto")) mode_str = "auto";
    else if (mode == s_expr_hash("manual")) mode_str = "manual";
    else if (mode == s_expr_hash("off")) mode_str = "off";
    
    printf("  name:    \"%s\"\n", name);
    printf("  channel: %d\n", channel);
    printf("  gain:    %.2f\n", gain);
    printf("  enabled: %s\n", enabled ? "true" : "false");
    printf("  mode:    %s (0x%08X)\n", mode_str, mode);
    
    printf("=== Done ===\n");
    
    return SE_CONTINUE;
}

// ============================================================================
// TEST BITMAP CONTEXT
// Stored in inst->user_ctx as uint32_t*
// ============================================================================

// Predicate: Test if bit N is set
bool test_bit(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("test_bit: need bit index");
        return false;
    }
    
    uint32_t* bitmap = (uint32_t*)inst->user_ctx;
    if (!bitmap) {
        EXCEPTION("test_bit: no bitmap in user_ctx");
        return false;
    }
    
    int32_t bit_index = params[0].int_val;
    if (bit_index < 0 || bit_index > 31) {
        EXCEPTION("test_bit: bit index out of range");
        return false;
    }
    
    bool result = (*bitmap & (1U << bit_index)) != 0;
    return result;
}

// ============================================================================
// TRIGGER ACTIONS - Print and record what happened
// ============================================================================

static uint32_t g_trigger_events = 0;  // Bitmask of which events fired

#define EVENT_BIT0_RISE     (1U << 0)
#define EVENT_BIT0_FALL     (1U << 1)
#define EVENT_BITS12_RISE   (1U << 2)
#define EVENT_BITS12_FALL   (1U << 3)
#define EVENT_BITS34_RISE   (1U << 4)
#define EVENT_BITS34_FALL   (1U << 5)
#define EVENT_BIT5_CLEAR    (1U << 6)
#define EVENT_BIT5_SET      (1U << 7)

void reset_trigger_events(void) {
    g_trigger_events = 0;
}

uint32_t get_trigger_events(void) {
    return g_trigger_events;
}

void on_bit0_rise(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BIT0_RISE\n");
    g_trigger_events |= EVENT_BIT0_RISE;
}

void on_bit0_fall(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BIT0_FALL\n");
    g_trigger_events |= EVENT_BIT0_FALL;
}

void on_bits_12_rise(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BITS_12_RISE (bit1 AND bit2)\n");
    g_trigger_events |= EVENT_BITS12_RISE;
}

void on_bits_12_fall(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BITS_12_FALL (bit1 AND bit2)\n");
    g_trigger_events |= EVENT_BITS12_FALL;
}

void on_bits_34_rise(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BITS_34_RISE (bit3 OR bit4)\n");
    g_trigger_events |= EVENT_BITS34_RISE;
}

void on_bits_34_fall(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BITS_34_FALL (bit3 OR bit4)\n");
    g_trigger_events |= EVENT_BITS34_FALL;
}

void on_bit5_clear(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BIT5_CLEAR (NOT bit5 went true)\n");
    g_trigger_events |= EVENT_BIT5_CLEAR;
}

void on_bit5_set(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("  >> ON_BIT5_SET (NOT bit5 went false)\n");
    g_trigger_events |= EVENT_BIT5_SET;
}

// ============================================================================
// SEQUENCE/FORK TRACKING - Unified tracker for test verification
// ============================================================================

// ============================================================================
// SEQUENCE/FORK TRACKING - Unified tracker for test verification
// ============================================================================

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#define MAX_TRACKED_STEPS 32
static int g_tracked_steps[MAX_TRACKED_STEPS];
static int g_tracked_count = 0;

// ============================================================================
// C API - For test driver assertions
// ============================================================================

int test_tracker_get_count(void) {
    return g_tracked_count;
}

int test_tracker_get_step(int index) {
    if (index >= 0 && index < g_tracked_count) {
        return g_tracked_steps[index];
    }
    return -1;
}

void test_tracker_reset(void) {
    g_tracked_count = 0;
    memset(g_tracked_steps, 0, sizeof(g_tracked_steps));
}

// ============================================================================
// RESET_SEQUENCE_TRACKER - Clears all recorded steps (oneshot)
// ============================================================================

void reset_sequence_tracker(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    test_tracker_reset();
    printf("  SEQUENCE TRACKER: Reset\n");
}

// ============================================================================
// TRACK_STEP - Records a step number in execution order (oneshot)
// Param: int - step number to record
// ============================================================================

void track_step(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    int step = 0;
    if (param_count > 0 && (params[0].type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_INT) {
        step = params[0].int_val;
    }
    
    if (g_tracked_count < MAX_TRACKED_STEPS) {
        g_tracked_steps[g_tracked_count++] = step;
        printf("  SEQUENCE TRACKER: Step %d recorded (total: %d)\n", step, g_tracked_count);
    }
}

// ============================================================================
// VERIFY_SEQUENCE_ORDER - Checks steps are in sequential order (oneshot)
// ============================================================================

void verify_sequence_order(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("  SEQUENCE TRACKER: Verifying order - ");
    bool in_order = true;
    for (int i = 0; i < g_tracked_count; i++) {
        printf("%d ", g_tracked_steps[i]);
        if (g_tracked_steps[i] != i) {
            in_order = false;
        }
    }
    printf("- %s\n", in_order ? "IN ORDER" : "OUT OF ORDER");
}

// ============================================================================
// VERIFY_FORK_ORDER - Checks parallel execution pattern (oneshot)
// ============================================================================

void verify_fork_order(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("  FORK TRACKER: Recorded steps - ");
    for (int i = 0; i < g_tracked_count; i++) {
        printf("%d ", g_tracked_steps[i]);
    }
    printf("\n");
}