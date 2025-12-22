// ============================================================================
// s_engine_eval.h
// S-Expression Evaluator API
// Version 2.2
// ============================================================================

#ifndef S_ENGINE_EVAL_H
#define S_ENGINE_EVAL_H

#include "s_engine_types.h"

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// Execute one tick of the active tree
// Returns control code (cfl_code_t)
cfl_code_t module_tick(
    module_runtime_t* mod,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// NODE EVALUATORS (exposed for testing/extension)
// ============================================================================

// Evaluate node at index, return control code
cfl_code_t eval_node(
    module_runtime_t* mod,
    uint16_t node_index
);

// Evaluate boolean node, return true/false
bool eval_bool(
    module_runtime_t* mod,
    uint16_t node_index
);

// ============================================================================
// S-EXPRESSION PARAMETER EVALUATION (v2.2)
// ============================================================================

// Evaluate a callable S-expression in params array
// Returns result of function call
// open_idx points to PARAM_OPEN_CALL
cfl_code_t eval_sexpr(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    const param_t* params,
    uint16_t open_idx
);

// Skip over a parameter (handles braces)
// Returns index of next parameter after this one
uint16_t skip_param(
    const param_t* params,
    uint16_t idx
);

#endif // S_ENGINE_EVAL_H