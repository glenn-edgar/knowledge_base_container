// ============================================================================
// se_lua53_bridge.c
// Lua 5.3 function bridge for S-expression engine
//
// REQUIRES: s_expr_tree_instance_t must have:
//     s_expr_hash_t current_func_hash;
// Set by the evaluator before dispatching to any function pointer.
// ============================================================================

#include "se_lua53_bridge.h"
#include "s_engine_exception.h"
#include "se_dict_string.h"
#include "se_dict_hash.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

// ============================================================================
// Push a single s_expr_param_t as a Lua table with type-appropriate fields
// ============================================================================

static void push_one_param(lua_State *L, const s_expr_param_t *p)
{
    uint8_t opcode = p->type & S_EXPR_OPCODE_MASK;

    lua_createtable(L, 0, 4);

    // Always include the raw type byte and opcode
    lua_pushinteger(L, p->type);
    lua_setfield(L, -2, "type");

    lua_pushinteger(L, opcode);
    lua_setfield(L, -2, "opcode");

    switch (opcode) {
        // Numeric values
        case S_EXPR_PARAM_INT:
            lua_pushinteger(L, (lua_Integer)p->int_val);
            lua_setfield(L, -2, "int_val");
            break;

        case S_EXPR_PARAM_UINT:
            lua_pushinteger(L, (lua_Integer)p->uint_val);
            lua_setfield(L, -2, "uint_val");
            break;

        case S_EXPR_PARAM_FLOAT:
            lua_pushnumber(L, (lua_Number)p->float_val);
            lua_setfield(L, -2, "float_val");
            break;

        case S_EXPR_PARAM_STR_HASH:
            lua_pushinteger(L, (lua_Integer)p->str_hash);
            lua_setfield(L, -2, "str_hash");
            break;

        // Function references
        case S_EXPR_PARAM_ONESHOT:
        case S_EXPR_PARAM_MAIN:
        case S_EXPR_PARAM_PRED:
            lua_pushinteger(L, p->node_index);
            lua_setfield(L, -2, "node_index");
            lua_pushinteger(L, p->func_index);
            lua_setfield(L, -2, "func_index");
            lua_pushinteger(L, p->index_to_pointer);
            lua_setfield(L, -2, "index_to_pointer");
            break;

        // Field references
        case S_EXPR_PARAM_FIELD:
            lua_pushinteger(L, p->field_offset);
            lua_setfield(L, -2, "field_offset");
            lua_pushinteger(L, p->field_size);
            lua_setfield(L, -2, "field_size");
            break;

        // String table index
        case S_EXPR_PARAM_STR_IDX:
            lua_pushinteger(L, p->str_index);
            lua_setfield(L, -2, "str_index");
            lua_pushinteger(L, p->str_len);
            lua_setfield(L, -2, "str_len");
            break;

        // Constant reference
        case S_EXPR_PARAM_CONST_REF:
            lua_pushinteger(L, p->const_index);
            lua_setfield(L, -2, "const_index");
            lua_pushinteger(L, p->const_size);
            lua_setfield(L, -2, "const_size");
            break;

        // Result code
        case S_EXPR_PARAM_RESULT:
            lua_pushinteger(L, (lua_Integer)p->int_val);
            lua_setfield(L, -2, "result_code");
            break;

        // Brace tokens (OPEN, OPEN_CALL, CLOSE, OPEN_DICT, etc.)
        case S_EXPR_PARAM_OPEN:
        case S_EXPR_PARAM_OPEN_CALL:
        case S_EXPR_PARAM_CLOSE:
        case S_EXPR_PARAM_OPEN_DICT:
        case S_EXPR_PARAM_CLOSE_DICT:
        case S_EXPR_PARAM_OPEN_KEY:
        case S_EXPR_PARAM_CLOSE_KEY:
        case S_EXPR_PARAM_OPEN_ARRAY:
        case S_EXPR_PARAM_CLOSE_ARRAY:
        case S_EXPR_PARAM_OPEN_TUPLE:
        case S_EXPR_PARAM_CLOSE_TUPLE:
            lua_pushinteger(L, p->brace_idx);
            lua_setfield(L, -2, "brace_idx");
            lua_pushinteger(L, p->parent_offset);
            lua_setfield(L, -2, "parent_offset");
            break;

        // Stack operations
        case S_EXPR_PARAM_STACK_TOS:
        case S_EXPR_PARAM_STACK_LOCAL:
            lua_pushinteger(L, p->stack_offset);
            lua_setfield(L, -2, "stack_offset");
            break;

        case S_EXPR_PARAM_NULL:
        case S_EXPR_PARAM_STACK_PUSH:
        case S_EXPR_PARAM_STACK_POP:
            // No extra data
            break;

        // Slot (legacy)
        case S_EXPR_PARAM_SLOT:
            lua_pushinteger(L, p->pool_id);
            lua_setfield(L, -2, "pool_id");
            lua_pushinteger(L, p->slot_index);
            lua_setfield(L, -2, "slot_index");
            break;

        default:
            // Unknown opcode — push raw int_val as fallback
            lua_pushinteger(L, (lua_Integer)p->int_val);
            lua_setfield(L, -2, "raw_value");
            break;
    }
}

// ============================================================================
// Push the params array as a Lua table (1-indexed)
// ============================================================================

static void push_params_table(lua_State *L,
                              const s_expr_param_t *params,
                              uint16_t param_count)
{
    lua_createtable(L, param_count, 0);
    for (uint16_t i = 0; i < param_count; i++) {
        push_one_param(L, &params[i]);
        lua_rawseti(L, -2, i + 1);
    }
}

// ============================================================================
// Push a lightweight inst userdata so Lua can call back into C
// ============================================================================

static void push_inst_userdata(lua_State *L, s_expr_tree_instance_t *inst)
{
    s_expr_tree_instance_t **ud = (s_expr_tree_instance_t **)
        lua_newuserdata(L, sizeof(s_expr_tree_instance_t *));
    *ud = inst;

    luaL_getmetatable(L, "se_tree_instance");
    lua_setmetatable(L, -2);
}

// ============================================================================
// Instance metatable methods
// These are what Lua calls via inst:read_i32(), inst:write_f32(), etc.
// ============================================================================

static s_expr_tree_instance_t* check_inst(lua_State *L, int idx)
{
    s_expr_tree_instance_t **ud = (s_expr_tree_instance_t **)
        luaL_checkudata(L, idx, "se_tree_instance");
    return *ud;
}

// inst:read_i32(offset) -> integer
static int l_inst_read_i32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);

    if (!inst->blackboard || offset < 0) {
        lua_pushinteger(L, 0);
        return 1;
    }

    int32_t val;
    memcpy(&val, (uint8_t*)inst->blackboard + offset, sizeof(int32_t));
    lua_pushinteger(L, val);
    return 1;
}

// inst:write_i32(offset, value)
static int l_inst_write_i32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);
    int32_t val = (int32_t)luaL_checkinteger(L, 3);

    if (inst->blackboard && offset >= 0) {
        memcpy((uint8_t*)inst->blackboard + offset, &val, sizeof(int32_t));
    }

    return 0;
}

// inst:read_u32(offset) -> integer
static int l_inst_read_u32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);

    if (!inst->blackboard || offset < 0) {
        lua_pushinteger(L, 0);
        return 1;
    }

    uint32_t val;
    memcpy(&val, (uint8_t*)inst->blackboard + offset, sizeof(uint32_t));
    lua_pushinteger(L, (lua_Integer)val);
    return 1;
}

// inst:write_u32(offset, value)
static int l_inst_write_u32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);
    uint32_t val = (uint32_t)luaL_checkinteger(L, 3);

    if (inst->blackboard && offset >= 0) {
        memcpy((uint8_t*)inst->blackboard + offset, &val, sizeof(uint32_t));
    }

    return 0;
}

// inst:read_f32(offset) -> number
static int l_inst_read_f32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);

    if (!inst->blackboard || offset < 0) {
        lua_pushnumber(L, 0.0);
        return 1;
    }

    float val;
    memcpy(&val, (uint8_t*)inst->blackboard + offset, sizeof(float));
    lua_pushnumber(L, (lua_Number)val);
    return 1;
}

// inst:write_f32(offset, value)
static int l_inst_write_f32(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int offset = (int)luaL_checkinteger(L, 2);
    float val = (float)luaL_checknumber(L, 3);

    if (inst->blackboard && offset >= 0) {
        memcpy((uint8_t*)inst->blackboard + offset, &val, sizeof(float));
    }

    return 0;
}

// inst:read_f32_ptr(ptr) -> number
// Interprets light userdata (void*) as a blackboard offset, reads float there.
// Use for event_data that encodes a blackboard offset as a pointer.
static int l_inst_read_f32_ptr(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    void *ptr = lua_touserdata(L, 2);

    if (!inst->blackboard || !ptr) {
        lua_pushnumber(L, 0.0);
        return 1;
    }

    uint16_t offset = (uint16_t)(uintptr_t)ptr;
    float val;
    memcpy(&val, (uint8_t*)inst->blackboard + offset, sizeof(float));
    lua_pushnumber(L, (lua_Number)val);
    return 1;
}

// inst:read_i32_ptr(ptr) -> integer
// Same pattern for int32
static int l_inst_read_i32_ptr(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    void *ptr = lua_touserdata(L, 2);

    if (!inst->blackboard || !ptr) {
        lua_pushinteger(L, 0);
        return 1;
    }

    uint16_t offset = (uint16_t)(uintptr_t)ptr;
    int32_t val;
    memcpy(&val, (uint8_t*)inst->blackboard + offset, sizeof(int32_t));
    lua_pushinteger(L, val);
    return 1;
}

// inst:ptr_to_offset(ptr) -> integer
// Converts light userdata back to an integer offset for general use
static int l_inst_ptr_to_offset(lua_State *L)
{
    void *ptr = lua_touserdata(L, 2);
    lua_pushinteger(L, (lua_Integer)(uintptr_t)ptr);
    return 1;
}

// inst:get_node_count() -> integer
static int l_inst_get_node_count(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    lua_pushinteger(L, inst->node_count);
    return 1;
}

static int l_inst_get_string(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int idx = (int)luaL_checkinteger(L, 2);

    const s_expr_module_def_t *def = inst->module->def;
    if (def && def->string_table && idx >= 0 && idx < def->string_count) {
        lua_pushstring(L, def->string_table[idx]);
    } else {
        lua_pushnil(L);
    }
    return 1;
}

static int l_inst_dict_extract_hash_str(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int dict_offset = (int)luaL_checkinteger(L, 2);
    const char *path = luaL_checkstring(L, 3);
    int dest_offset = (int)luaL_checkinteger(L, 4);

    if (!inst->blackboard) {
        lua_pushboolean(L, 0);
        return 1;
    }

    const s_expr_param_t *dict = se_dicts_from_blackboard(
        inst->blackboard, dict_offset);
    if (!dict) {
        lua_pushboolean(L, 0);
        return 1;
    }

    const s_expr_param_t *found = se_dicts_resolve(
        dict, inst->module->def, path, NULL);
    if (!found) {
        lua_pushboolean(L, 0);
        return 1;
    }

    s_expr_hash_t hash_val = se_dicts_get_hash(
        dict, inst->module->def, path, 0);

    memcpy((uint8_t*)inst->blackboard + dest_offset,
           &hash_val, sizeof(uint32_t));

    lua_pushboolean(L, 1);
    return 1;
}

static int l_inst_dict_extract_hash_keys(lua_State *L)
{
    s_expr_tree_instance_t *inst = check_inst(L, 1);
    int dict_offset = (int)luaL_checkinteger(L, 2);
    luaL_checktype(L, 3, LUA_TTABLE);
    int dest_offset = (int)luaL_checkinteger(L, 4);

    if (!inst->blackboard) {
        lua_pushboolean(L, 0);
        return 1;
    }

    const s_expr_param_t *dict = se_dicth_from_blackboard(
        inst->blackboard, dict_offset);
    if (!dict) {
        lua_pushboolean(L, 0);
        return 1;
    }

    int key_count = (int)luaL_len(L, 3);
    if (key_count <= 0 || key_count > 16) {
        lua_pushboolean(L, 0);
        return 1;
    }

    s_expr_hash_t keys[16];
    for (int i = 0; i < key_count; i++) {
        lua_rawgeti(L, 3, i + 1);
        keys[i] = (s_expr_hash_t)lua_tointeger(L, -1);
        lua_pop(L, 1);
    }

    s_expr_hash_t hash_val = se_dicth_get_hash(
        dict, keys, (uint16_t)key_count,
        inst->module->def, 0);

    memcpy((uint8_t*)inst->blackboard + dest_offset,
           &hash_val, sizeof(uint32_t));

    lua_pushboolean(L, 1);
    return 1;
}

static const luaL_Reg inst_methods[] = {
    {"read_i32",                l_inst_read_i32},
    {"write_i32",               l_inst_write_i32},
    {"read_u32",                l_inst_read_u32},
    {"write_u32",               l_inst_write_u32},
    {"read_f32",                l_inst_read_f32},
    {"write_f32",               l_inst_write_f32},
    {"read_f32_ptr",            l_inst_read_f32_ptr},
    {"read_i32_ptr",            l_inst_read_i32_ptr},
    {"ptr_to_offset",           l_inst_ptr_to_offset},
    {"get_node_count",          l_inst_get_node_count},
    {"get_string",              l_inst_get_string},
    {"dict_extract_hash_str",   l_inst_dict_extract_hash_str},
    {"dict_extract_hash_keys",  l_inst_dict_extract_hash_keys},
    {NULL, NULL}
};
// ============================================================================
// Look up the Lua function for a given hash + type
// Returns true if found (function is on top of Lua stack)
// ============================================================================

static bool lookup_lua_func(lua_State *L, s_expr_hash_t func_hash, const char *type_str)
{
    lua_getfield(L, LUA_REGISTRYINDEX, SE_LUA_FUNC_REGISTRY_KEY);
    if (!lua_istable(L, -1)) { lua_pop(L, 1); return false; }

    lua_getfield(L, -1, type_str);
    if (!lua_istable(L, -1)) { lua_pop(L, 2); return false; }

    lua_pushinteger(L, (lua_Integer)func_hash);
    lua_gettable(L, -2);

    if (!lua_isfunction(L, -1)) {
        lua_pop(L, 3);
        return false;
    }

    // Keep the function, remove intermediate tables
    lua_remove(L, -2);
    lua_remove(L, -2);
    return true;
}

// ============================================================================
// Lua-callable: se_bridge.register(hash, type_str, func)
// ============================================================================

static int l_bridge_register(lua_State *L)
{
    s_expr_hash_t hash    = (s_expr_hash_t)luaL_checkinteger(L, 1);
    const char   *type_str = luaL_checkstring(L, 2);
    luaL_checktype(L, 3, LUA_TFUNCTION);

    // Push a copy of the function for se_lua_bridge_register (which pops it)
    lua_pushvalue(L, 3);
    se_lua_bridge_register(L, hash, type_str);

    return 0;
}

// ============================================================================
// Public API: Initialization
// ============================================================================

void se_lua_bridge_init(lua_State *L)
{
    // Create the master registry table with sub-tables per type
    lua_newtable(L);

    lua_newtable(L);
    lua_setfield(L, -2, "oneshot");

    lua_newtable(L);
    lua_setfield(L, -2, "main");

    lua_newtable(L);
    lua_setfield(L, -2, "pred");

    lua_setfield(L, LUA_REGISTRYINDEX, SE_LUA_FUNC_REGISTRY_KEY);

    // Create the instance metatable with __index pointing to itself
    luaL_newmetatable(L, "se_tree_instance");
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_setfuncs(L, inst_methods, 0);
    lua_pop(L, 1);

    // Expose "se_bridge" global table to Lua with .register() method
    lua_newtable(L);
    lua_pushcfunction(L, l_bridge_register);
    lua_setfield(L, -2, "register");
    lua_setglobal(L, "se_bridge");
}

// ============================================================================
// Public API: Register a Lua function by hash
// Expects function on top of Lua stack. Pops the function.
// ============================================================================

void se_lua_bridge_register(lua_State *L, s_expr_hash_t func_hash, const char *type_str)
{
    if (!lua_isfunction(L, -1)) {
        luaL_error(L, "se_lua_bridge_register: expected function on top of stack");
        return;
    }

    lua_getfield(L, LUA_REGISTRYINDEX, SE_LUA_FUNC_REGISTRY_KEY);
    lua_getfield(L, -1, type_str);

    lua_pushinteger(L, (lua_Integer)func_hash);
    lua_pushvalue(L, -4);   // copy the function
    lua_settable(L, -3);    // type_table[hash] = func

    lua_pop(L, 2);  // pop type_table and registry_table
    lua_pop(L, 1);  // pop the original function
}

// ============================================================================
// Trampolines
//
// Each trampoline:
//   1. Extracts lua_State* from the user handle via inst->module->alloc.ctx
//   2. Reads inst->current_func_hash (set by evaluator before dispatch)
//   3. Looks up the Lua function in the bridge registry
//   4. Pushes inst userdata + params table + event_type + event_id + event_data
//   5. Calls the Lua function (5 args), maps return value
//   6. On lookup failure: EXCEPTION (fatal)
//   7. On Lua runtime error: EXCEPTION (fatal)
// ============================================================================

void se_lua_oneshot_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data)
{
    lua_State *L = se_lua_get_state(inst);
    if (!L) {
        EXCEPTION("se_lua: oneshot trampoline called with NULL lua_State");
        return;
    }

    s_expr_hash_t func_hash = inst->current_func_hash;

    if (!lookup_lua_func(L, func_hash, "oneshot")) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "se_lua: oneshot function " S_EXPR_HASH_FMT " not found", func_hash);
        EXCEPTION(buf);
        return;
    }

    push_inst_userdata(L, inst);
    push_params_table(L, params, param_count);
    lua_pushinteger(L, (lua_Integer)event_type);
    lua_pushinteger(L, (lua_Integer)event_id);
    lua_pushlightuserdata(L, event_data);

    if (lua_pcall(L, 5, 0, 0) != LUA_OK) {
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "se_lua oneshot error: %s", lua_tostring(L, -1));
        lua_pop(L, 1);
        EXCEPTION(buf);
    }
}

s_expr_result_t se_lua_main_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data)
{
    lua_State *L = se_lua_get_state(inst);
    if (!L) {
        EXCEPTION("se_lua: main trampoline called with NULL lua_State");
        return SE_HALT;
    }

    s_expr_hash_t func_hash = inst->current_func_hash;

    if (!lookup_lua_func(L, func_hash, "main")) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "se_lua: main function " S_EXPR_HASH_FMT " not found", func_hash);
        EXCEPTION(buf);
        return SE_HALT;
    }

    push_inst_userdata(L, inst);
    push_params_table(L, params, param_count);
    lua_pushinteger(L, (lua_Integer)event_type);
    lua_pushinteger(L, (lua_Integer)event_id);
    lua_pushlightuserdata(L, event_data);

    if (lua_pcall(L, 5, 1, 0) != LUA_OK) {
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "se_lua main error: %s", lua_tostring(L, -1));
        lua_pop(L, 1);
        EXCEPTION(buf);
        return SE_HALT;
    }

    s_expr_result_t result = (s_expr_result_t)lua_tointeger(L, -1);
    lua_pop(L, 1);
    return result;
}

bool se_lua_pred_trampoline(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t   *params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void                   *event_data)
{
    lua_State *L = se_lua_get_state(inst);
    if (!L) {
        EXCEPTION("se_lua: pred trampoline called with NULL lua_State");
        return false;
    }

    s_expr_hash_t func_hash = inst->current_func_hash;

    if (!lookup_lua_func(L, func_hash, "pred")) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "se_lua: pred function " S_EXPR_HASH_FMT " not found", func_hash);
        EXCEPTION(buf);
        return false;
    }

    push_inst_userdata(L, inst);
    push_params_table(L, params, param_count);
    lua_pushinteger(L, (lua_Integer)event_type);
    lua_pushinteger(L, (lua_Integer)event_id);
    lua_pushlightuserdata(L, event_data);

    if (lua_pcall(L, 5, 1, 0) != LUA_OK) {
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "se_lua pred error: %s", lua_tostring(L, -1));
        lua_pop(L, 1);
        EXCEPTION(buf);
        return false;
    }

    bool result = lua_toboolean(L, -1);
    lua_pop(L, 1);
    return result;
}