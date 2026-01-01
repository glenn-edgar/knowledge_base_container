#include "cfl_s_boolean_functions.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"

// ============================================================================
// BOOLEAN FUNCTION IMPLEMENTATIONS
// ============================================================================

// ============================================================================
// CFL_READ_BIT: Read a bit from runtime bitmask
// Params: [0] = bit index (int/uint, 0-31)
// Returns: true if bit is set
// ============================================================================

static bool cfl_read_bit_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_READ_BIT: requires 1 parameter");
        return false;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_READ_BIT: param[0] must be INT or UINT");
        return false;
    }
    
    uint32_t bit_index = (uint32_t)s_expr_param_uint(&params[0]);
    if (bit_index >= 32) {
        EXCEPTION("CFL_READ_BIT: bit index out of range");
        return false;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_READ_BIT: no runtime handle");
        return false;
    }
    
    uint32_t bit_mask = 1U << bit_index;
    return (runtime_handle->bitmask & bit_mask) != 0;
}

// ============================================================================
// CFL_TRUE: Always returns true
// Params: none
// ============================================================================

static bool cfl_true_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    return true;
}

// ============================================================================
// CFL_FALSE: Always returns false
// Params: none
// ============================================================================

static bool cfl_false_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    return false;
}

// ============================================================================
// Bit operation modes
// ============================================================================

typedef enum {
    BIT_OP_OR,    // Short-circuit on first true, default false
    BIT_OP_AND,   // Short-circuit on first false, default true
    BIT_OP_NOR,   // NOT(OR) - true only if all false
    BIT_OP_NAND,  // NOT(AND) - false only if all true
    BIT_OP_XOR    // True if odd number of true inputs
} bit_op_mode_t;

// ============================================================================
// Evaluate a single parameter - returns true/false
// ============================================================================

static inline bool cfl_s_bit_eval_param(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx,
    uint32_t bitmask
) {
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
        unsigned bit_index = (unsigned)s_expr_param_uint(&params[idx]);
        return (bitmask & (1U << bit_index)) != 0;
    }
    
    if (opcode == S_EXPR_PARAM_PRED || opcode == S_EXPR_PARAM_OPEN_CALL) {
        return s_expr_invoke_pred(inst, params, idx);
    }
    
    EXCEPTION("cfl_s_bit: Invalid parameter type");
    return false;
}

// ============================================================================
// Validate all parameters during INIT
// ============================================================================

static inline bool cfl_s_bit_validate(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
) {
    uint16_t idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        switch (opcode) {
            case S_EXPR_PARAM_INT:
            case S_EXPR_PARAM_UINT:
                // Valid - integer bit index
                break;
                
            case S_EXPR_PARAM_PRED:
            case S_EXPR_PARAM_OPEN_CALL:
                // Valid - invoke to validate nested params
                s_expr_invoke_pred(inst, params, idx);
                break;
                
            default:
                EXCEPTION("cfl_s_bit: Invalid parameter type");
                return false;
        }
        idx = s_expr_skip_param(params, idx);
    }
    return true;
}

// ============================================================================
// Generic bit operation evaluator
// ============================================================================

static inline bool cfl_s_bit_eval(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    uint32_t bitmask,
    bit_op_mode_t mode
) {
    uint16_t idx = 0;
    bool accumulator = false;  // For XOR
    
    while (idx < param_count) {
        bool result = cfl_s_bit_eval_param(inst, params, idx, bitmask);
        
        switch (mode) {
            case BIT_OP_OR:
                if (result) return true;
                break;
                
            case BIT_OP_NOR:
                if (result) return false;  // Any true → NOR is false
                break;
                
            case BIT_OP_AND:
                if (!result) return false;
                break;
                
            case BIT_OP_NAND:
                if (!result) return true;  // Any false → NAND is true
                break;
                
            case BIT_OP_XOR:
                accumulator ^= result;     // Toggle on each true
                break;
        }
        
        idx = s_expr_skip_param(params, idx);
    }
    
    // Default returns
    switch (mode) {
        case BIT_OP_OR:   return false;  // No true found
        case BIT_OP_NOR:  return true;   // All were false
        case BIT_OP_AND:  return true;   // No false found
        case BIT_OP_NAND: return false;  // All were true
        case BIT_OP_XOR:  return accumulator;
        default:          {
            EXCEPTION("cfl_s_bit: Invalid mode");
            return false;
        }
    }
}

// ============================================================================
// CFL_S_BIT_OR: Short-circuit OR on bits/predicates
// ============================================================================

static bool cfl_s_bit_or_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        cfl_s_bit_validate(inst, params, param_count);
        return true;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("CFL_S_BIT_OR: no runtime handle");
        return false;
    }
    
    return cfl_s_bit_eval(inst, params, param_count, runtime->bitmask, BIT_OP_OR);
}

// ============================================================================
// CFL_S_BIT_AND: Short-circuit AND on bits/predicates
// ============================================================================

static bool cfl_s_bit_and_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        cfl_s_bit_validate(inst, params, param_count);
        return true;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("CFL_S_BIT_AND: no runtime handle");
        return false;
    }
    
    return cfl_s_bit_eval(inst, params, param_count, runtime->bitmask, BIT_OP_AND);
}

// ============================================================================
// CFL_S_BIT_NOR: NOR on bits/predicates (true only if all false)
// ============================================================================

static bool cfl_s_bit_nor_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        cfl_s_bit_validate(inst, params, param_count);
        return true;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("CFL_S_BIT_NOR: no runtime handle");
        return false;
    }
    
    return cfl_s_bit_eval(inst, params, param_count, runtime->bitmask, BIT_OP_NOR);
}

// ============================================================================
// CFL_S_BIT_NAND: NAND on bits/predicates (false only if all true)
// ============================================================================

static bool cfl_s_bit_nand_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        cfl_s_bit_validate(inst, params, param_count);
        return true;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("CFL_S_BIT_NAND: no runtime handle");
        return false;
    }
    
    return cfl_s_bit_eval(inst, params, param_count, runtime->bitmask, BIT_OP_NAND);
}

// ============================================================================
// CFL_S_BIT_XOR: XOR on bits/predicates (true if odd number true)
// ============================================================================

static bool cfl_s_bit_xor_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        cfl_s_bit_validate(inst, params, param_count);
        return true;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("CFL_S_BIT_XOR: no runtime handle");
        return false;
    }
    
    return cfl_s_bit_eval(inst, params, param_count, runtime->bitmask, BIT_OP_XOR);
}


// ============================================================================
// CFL_CHECK_EVENT: Check if event_id matches any parameter value
// Params: list of event IDs (int/uint)
// Returns: true if current event_id matches any parameter
//
// DSL usage:
//   p_call("CFL_CHECK_EVENT") int(1) int(2) int(10) end_call(...)
// ============================================================================

static bool cfl_check_event_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT: Validate all parameters are integers
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        uint16_t idx = 0;
        while (idx < param_count) {
            uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
            if (opcode != S_EXPR_PARAM_INT && opcode != S_EXPR_PARAM_UINT) {
                EXCEPTION("CFL_CHECK_EVENT: All parameters must be INT or UINT");
                return false;
            }
            idx = s_expr_skip_param(params, idx);
        }
        return true;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return true;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Check if event_id matches any parameter
    // -------------------------------------------------------------------------
    uint16_t idx = 0;
    while (idx < param_count) {
        uint32_t param_value = (uint32_t)s_expr_param_int(&params[idx]);
        
        if (event_id == param_value) {
            return true;  // Found match
        }
        
        idx = s_expr_skip_param(params, idx);
    }
    
    return false;  // No match found
}
// ============================================================================
// FUNCTION TABLE
// ============================================================================

// ============================================================================
// SYSTEM PREDICATE ENTRIES (named for readability)
// ============================================================================

static const s_expr_fn_entry_named_t system_pred_entries_named[] = {
    { "CFL_READ_BIT",       (void*)cfl_read_bit_boolean },
    { "CFL_TRUE",           (void*)cfl_true_boolean },
    { "CFL_FALSE",          (void*)cfl_false_boolean },
    { "CFL_S_BIT_OR",       (void*)cfl_s_bit_or_boolean },
    { "CFL_S_BIT_AND",      (void*)cfl_s_bit_and_boolean },
    { "CFL_S_BIT_NOR",      (void*)cfl_s_bit_nor_boolean },
    { "CFL_S_BIT_NAND",     (void*)cfl_s_bit_nand_boolean },
    { "CFL_S_BIT_XOR",      (void*)cfl_s_bit_xor_boolean },
    { "CFL_CHECK_EVENT",    (void*)cfl_check_event_boolean },
    // Add more system predicate functions here
};

// ============================================================================
// HASH TABLE (populated at runtime)
// ============================================================================

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

static s_expr_fn_entry_t system_pred_entries[ARRAY_COUNT(system_pred_entries_named)];
static s_expr_fn_table_t system_pred_table;

// ============================================================================
// INITIALIZE AND LOAD
// ============================================================================

#if 0
void cfl_load_boolean_s_functions(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        EXCEPTION("ERROR: cfl_load_pred_s_functions called with NULL handle");
        return;
    }
    
    // Initialize hash table once
    static bool initialized = false;
    if (!initialized) {
        s_expr_build_fn_table(
            system_pred_entries_named,
            system_pred_entries,
            ARRAY_COUNT(system_pred_entries_named)
        );
        
        system_pred_table.entries = system_pred_entries;
        system_pred_table.count = ARRAY_COUNT(system_pred_entries);
        
        initialized = true;
    }
    
    // Register to all modules
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {m
        s_expr_module_register_pred(modules[i], &system_pred_table);
    }
}
#endif

void cfl_load_boolean_s_functions(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        printf("ERROR: cfl_load_pred_s_functions called with invalid handle\n");
        return;
    }
    
// Initialize hash table once

    s_expr_build_fn_table(
        system_pred_entries_named,
        system_pred_entries,
        ARRAY_COUNT(system_pred_entries_named)
    );
    
    system_pred_table.entries = system_pred_entries;
    system_pred_table.count = ARRAY_COUNT(system_pred_entries);
    
    
    
   

    
    // Register to all modules
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_register_pred(modules[i], &system_pred_table);
    }
}
