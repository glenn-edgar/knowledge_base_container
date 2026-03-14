// ============================================================================
// se_lua53_bridge.h
// Lua 5.3 function bridge for S-expression engine
//
// REQUIRES: Add this field to s_expr_tree_instance_t in s_engine_types.h:
//     s_expr_hash_t current_func_hash;
// The evaluator must set it before dispatching to any function pointer.
// ============================================================================

#ifndef SE_LUA53_BRIDGE_H
#define SE_LUA53_BRIDGE_H

#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Registry key for the Lua function lookup table
#define SE_LUA_FUNC_REGISTRY_KEY "se_lua_functions"

// ============================================================================
// User handle — must match the struct defined by the application (main.c)
// The bridge extracts lua_State* from inst->module->alloc.ctx
// ============================================================================

typedef struct {
    lua_State*  lua_state;
    // Additional user fields follow
} se_lua_user_handle_t;

// ============================================================================
// Helper: extract lua_State* from a tree instance
// ============================================================================

static inline lua_State* se_lua_get_state(s_expr_tree_instance_t *inst) {
    if (!inst || !inst->module) return NULL;
    se_lua_user_handle_t *uh = (se_lua_user_handle_t *)inst->module->alloc.ctx;
    return uh ? uh->lua_state : NULL;
}

// ============================================================================
// Initialization
// ============================================================================

// Call once at startup. Creates the registry tables that map
// function_hash -> lua_function for each call type (oneshot, main, pred).
void se_lua_bridge_init(lua_State *L);

// Register a Lua function (already on top of stack) for a given hash + type.
// type_str: "oneshot", "main", or "pred"
// Pops the function from the stack.
void se_lua_bridge_register(lua_State *L, s_expr_hash_t func_hash, const char *type_str);

// ============================================================================
// Convenience: register from C by name (hashes the name with FNV-1a)
// ============================================================================

static inline void se_lua_bridge_register_by_name(
    lua_State *L, const char *func_name, const char *type_str)
{
    s_expr_hash_t hash = s_expr_fnv1a_hash(func_name);
    se_lua_bridge_register(L, hash, type_str);
}

// ============================================================================
// Trampoline functions — register these in the C function tables
//
// The engine dispatches by hash, hits the trampoline, trampoline reads
// inst->current_func_hash (set by the evaluator) and looks up the
// matching Lua function in the bridge registry.
// ============================================================================

// Oneshot trampoline: void return
void se_lua_oneshot_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data
);

// Main trampoline: returns s_expr_result_t
s_expr_result_t se_lua_main_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data
);

// Pred trampoline: returns bool
bool se_lua_pred_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data
);

#ifdef __cplusplus
}
#endif

#endif // SE_LUA53_BRIDGE_H