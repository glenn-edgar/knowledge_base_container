// ============================================================================
// s_engine_eval.h
// S-Expression Evaluator API
// ============================================================================

#ifndef S_ENGINE_EVAL_H
#define S_ENGINE_EVAL_H

#include "s_engine_types.h"

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// Execute one tick of the active tree
// Returns control code
uint8_t module_tick(
    module_runtime_t* mod,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// INTERNAL EVALUATORS (exposed for testing)
// ============================================================================

// Evaluate node at index, return control code
uint8_t eval_node(
    module_runtime_t* mod,
    uint16_t node_index
);

// Evaluate boolean node, return true/false
bool eval_bool(
    module_runtime_t* mod,
    uint16_t node_index
);

#endif // S_ENGINE_EVAL_H