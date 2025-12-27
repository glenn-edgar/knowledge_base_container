// ============================================================================
// s_engine_eval.h
// S-Expression Evaluator API
// Version 2.7 - Added lifecycle events (init/terminate), SE_RESET handling
// ============================================================================

#ifndef S_ENGINE_EVAL_H
#define S_ENGINE_EVAL_H

#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// Execute one tick of the tree instance
// Sets execution context (event_id, event_data) and evaluates from root
// Handles SE_RESET by terminating all nodes and resetting state
s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// LIFECYCLE MANAGEMENT
// ============================================================================

// Terminate all active+initialized nodes in reverse order (children before parents)
// Sends S_EXPR_EVENT_TERMINATE to each node, then clears all flags
void s_expr_tree_terminate(s_expr_tree_instance_t* inst);

// Reset tree: terminate all nodes, then set all to ACTIVE but not INITIALIZED
// Called automatically when SE_RESET is returned from evaluation
void s_expr_tree_reset(s_expr_tree_instance_t* inst);

// Initialize tree state (all nodes ACTIVE, none INITIALIZED)
// Called automatically during tree creation, can be called manually
void s_expr_tree_init_states(s_expr_tree_instance_t* inst);

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

// Skip over a parameter (handles braces and slots)
// Returns index of next parameter after this one
uint16_t s_expr_skip_param(
    const s_expr_param_t* params,
    uint16_t idx
);

// ============================================================================
// PARAMETER ITERATION HELPERS
// ============================================================================

// Find first parameter of given type in params array
// Returns index or param_count if not found
uint16_t s_expr_find_param_type(
    const s_expr_param_t* params,
    uint8_t param_count,
    uint8_t param_type
);

// Find first slot parameter in params array
// Returns index or param_count if not found
static inline uint16_t s_expr_find_slot_param(
    const s_expr_param_t* params,
    uint8_t param_count
) {
    return s_expr_find_param_type(params, param_count, S_EXPR_PARAM_SLOT);
}

// Count parameters of given type
uint8_t s_expr_count_param_type(
    const s_expr_param_t* params,
    uint8_t param_count,
    uint8_t param_type
);

uint8_t s_expr_count_params(
    const s_expr_param_t* params,
    uint8_t param_count
)''

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_EVAL_H