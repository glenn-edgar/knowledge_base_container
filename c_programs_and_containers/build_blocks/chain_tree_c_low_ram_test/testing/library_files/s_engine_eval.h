// ============================================================================
// s_engine_eval.h
// S-Expression Evaluator API
// Flat parameter walker, hash-based dispatch
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

// Execute one tick of the tree
// Walks parameter array, dispatches functions, handles control flow
s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// LIFECYCLE MANAGEMENT
// ============================================================================

// Reset tree: clear INITIALIZED flags, preserve EVER_INIT
// Oneshots (o_call) will re-run, init-once (io_call) will not
void s_expr_tree_reset(s_expr_tree_instance_t* inst);

// Terminate tree: send TERMINATE event to all initialized main nodes
// Then clear all flags
void s_expr_tree_terminate(s_expr_tree_instance_t* inst);

// Full reset: terminate + clear EVER_INIT
// Everything re-runs including io_call
void s_expr_tree_full_reset(s_expr_tree_instance_t* inst);

// Initialize states (called automatically by create)
void s_expr_tree_init_states(s_expr_tree_instance_t* inst);

// ============================================================================
// CALLABLE INVOKERS
// For use by user functions to invoke nested callables in params
// ============================================================================

// Invoke a main callable at params[idx]
// idx can point to OPEN_CALL or bare MAIN ref
s_expr_result_t s_expr_invoke_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
);

// Invoke an oneshot callable at params[idx]
void s_expr_invoke_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
);

// Invoke a predicate callable at params[idx]
bool s_expr_invoke_pred(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
);

// Auto-dispatch based on function type
// Returns SE_CONTINUE for oneshot, bool->result for pred, result for main
s_expr_result_t s_expr_invoke_any(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
);

// ============================================================================
// PARAMETER ITERATION HELPERS
// ============================================================================

// Count logical parameters (braced expressions count as 1)
uint16_t s_expr_count_params(const s_expr_param_t* params, uint16_t count);

// Find first parameter of given opcode
// Returns index or UINT16_MAX if not found
uint16_t s_expr_find_param(const s_expr_param_t* params, uint16_t count, uint8_t opcode);

// Iterate over params, calling callback for each logical param
// Callback receives index of param start
// Return false from callback to stop iteration
typedef bool (*s_expr_param_iter_fn)(
    const s_expr_param_t* params,
    uint16_t idx,
    void* ctx
);

void s_expr_iterate_params(
    const s_expr_param_t* params,
    uint16_t count,
    s_expr_param_iter_fn callback,
    void* ctx
);

// Restart actions: send TERMINATE, then reset flags so INIT fires again
void s_expr_restart_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
);

// Enable actions: reset flags so INIT fires (no TERMINATE sent)
void s_expr_enable_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
);

// ============================================================================
// NODE STATE ACCESSORS
// Access per-node state for the current executing node
// ============================================================================

// State value (8-bit, for state machines) - stored in node_states[]
uint8_t s_expr_get_state(s_expr_tree_instance_t* inst);
void s_expr_set_state(s_expr_tree_instance_t* inst, uint8_t state);

// User flags (16-bit, for dispatch tracking) - stored in node_states[]
uint16_t s_expr_get_user_flags(s_expr_tree_instance_t* inst);
void s_expr_set_user_flags(s_expr_tree_instance_t* inst, uint16_t flags);

// 64-bit storage - stored in pointer_array[]
// IMPORTANT: These accessors ONLY work for pt_m_call functions!
// Regular m_call functions do not have pointer storage allocated.
// The DSL must declare functions using these as pt_m_call:
//   pt_m_call("CFL_TIME_DELAY") flt(1.5) end_call(...)
uint64_t s_expr_get_user_u64(s_expr_tree_instance_t* inst);
void s_expr_set_user_u64(s_expr_tree_instance_t* inst, uint64_t value);

double s_expr_get_user_f64(s_expr_tree_instance_t* inst);
void s_expr_set_user_f64(s_expr_tree_instance_t* inst, double value);

// ============================================================================
// USER CONTEXT
// ============================================================================

void* s_expr_tree_get_user_ctx(s_expr_tree_instance_t* inst);
void s_expr_tree_set_user_ctx(s_expr_tree_instance_t* inst, void* ctx);

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_EVAL_H