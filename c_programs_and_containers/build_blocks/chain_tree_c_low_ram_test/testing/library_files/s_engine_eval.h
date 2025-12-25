// ============================================================================
// s_engine_eval.h
// S-Expression Evaluator API
// Version 2.5 - Two-tier architecture: operates on tree instances
// ============================================================================

#ifndef S_ENGINE_EVAL_H
#define S_ENGINE_EVAL_H

#include "s_engine_types.h"

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// Execute one tick of the tree instance
// Sets execution context (event_id, event_data) and evaluates from root
s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// NODE EVALUATORS (exposed for testing/extension)
// ============================================================================

// Evaluate node at index, return control code
s_expr_result_t s_expr_eval_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
);

// Evaluate boolean node, return true/false
// Handles: AND, OR, NOT, XOR, NAND, NOR
bool s_expr_eval_bool(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
);

// ============================================================================
// S-EXPRESSION PARAMETER EVALUATION
// ============================================================================

// Evaluate a callable S-expression in params array
// open_idx points to PARAM_OPEN_CALL
s_expr_result_t s_expr_eval_sexpr(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint16_t open_idx
);

// Skip over a parameter (handles braces)
// Returns index of next parameter after this one
uint16_t s_expr_skip_param(
    const s_expr_param_t* params,
    uint16_t idx
);

#endif // S_ENGINE_EVAL_H