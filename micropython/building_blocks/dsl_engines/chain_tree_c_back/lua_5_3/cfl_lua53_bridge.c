/*
 * cfl_lua53_bridge.c - Lua 5.3+ bridge for ChainTree CFL engine
 *
 * Trampoline-based dispatch: each registered Lua function gets a
 * unique C function pointer from a pre-generated table. At dispatch
 * time the trampoline retrieves lua_State* from handle->user_handle
 * and calls the Lua function via its registry reference.
 */

#include "cfl_lua53_bridge.h"
#include "cfl_runtime.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"
#include "cfl_blackboard.h"
#include <stdio.h>
#include <string.h>

/* ========================================================================
 * Bridge State (module-level)
 * ======================================================================== */

static int g_main_refs[CFL_LUA_MAX_MAIN];
static int g_main_count = 0;

static int g_os_refs[CFL_LUA_MAX_ONESHOT];
static int g_os_count = 0;

static int g_bool_refs[CFL_LUA_MAX_BOOLEAN];
static int g_bool_count = 0;

/* ========================================================================
 * Dispatch Functions — called by trampolines with slot index
 * ======================================================================== */

static lua_State *get_L(void *handle) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    return (lua_State *)rt->user_handle;
}

static unsigned cfl_lua_main_dispatch(
    int slot, void *handle, unsigned bool_fn_idx,
    unsigned node_index, unsigned event_type,
    unsigned event_id, void *event_data)
{
    lua_State *L = get_L(handle);
    if (!L) return CFL_HALT;

    lua_rawgeti(L, LUA_REGISTRYINDEX, g_main_refs[slot]);
    lua_pushlightuserdata(L, handle);
    lua_pushinteger(L, (lua_Integer)bool_fn_idx);
    lua_pushinteger(L, (lua_Integer)node_index);
    lua_pushinteger(L, (lua_Integer)event_type);
    lua_pushinteger(L, (lua_Integer)event_id);
    lua_pushlightuserdata(L, event_data);

    if (lua_pcall(L, 6, 1, 0) != LUA_OK) {
        fprintf(stderr, "cfl_lua main[%d] error: %s\n", slot, lua_tostring(L, -1));
        lua_pop(L, 1);
        return CFL_HALT;
    }

    unsigned result = (unsigned)lua_tointeger(L, -1);
    lua_pop(L, 1);
    return result;
}

static void cfl_lua_os_dispatch(int slot, void *handle, unsigned node_index)
{
    lua_State *L = get_L(handle);
    if (!L) return;

    lua_rawgeti(L, LUA_REGISTRYINDEX, g_os_refs[slot]);
    lua_pushlightuserdata(L, handle);
    lua_pushinteger(L, (lua_Integer)node_index);

    if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
        fprintf(stderr, "cfl_lua oneshot[%d] error: %s\n", slot, lua_tostring(L, -1));
        lua_pop(L, 1);
    }
}

static bool cfl_lua_bool_dispatch(
    int slot, void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    lua_State *L = get_L(handle);
    if (!L) return false;

    lua_rawgeti(L, LUA_REGISTRYINDEX, g_bool_refs[slot]);
    lua_pushlightuserdata(L, handle);
    lua_pushinteger(L, (lua_Integer)node_index);
    lua_pushinteger(L, (lua_Integer)event_type);
    lua_pushinteger(L, (lua_Integer)event_id);
    lua_pushlightuserdata(L, event_data);

    if (lua_pcall(L, 5, 1, 0) != LUA_OK) {
        fprintf(stderr, "cfl_lua boolean[%d] error: %s\n", slot, lua_tostring(L, -1));
        lua_pop(L, 1);
        return false;
    }

    bool result = lua_toboolean(L, -1);
    lua_pop(L, 1);
    return result;
}

/* ========================================================================
 * Trampoline Generation via X-Macro
 *
 * Each slot N gets a unique C function that forwards to the dispatch
 * function with N as the slot argument.
 * ======================================================================== */

#define SLOT_LIST \
    X(0)  X(1)  X(2)  X(3)  X(4)  X(5)  X(6)  X(7)  \
    X(8)  X(9)  X(10) X(11) X(12) X(13) X(14) X(15) \
    X(16) X(17) X(18) X(19) X(20) X(21) X(22) X(23) \
    X(24) X(25) X(26) X(27) X(28) X(29) X(30) X(31)

/* --- Main trampolines --- */
#define X(n) \
static unsigned main_tramp_##n(void *h, unsigned b, unsigned ni, \
    unsigned et, unsigned ei, void *ed) { \
    return cfl_lua_main_dispatch(n, h, b, ni, et, ei, ed); \
}
SLOT_LIST
#undef X

#define X(n) main_tramp_##n,
static cfl_main_function_t g_main_tramp_table[] = { SLOT_LIST };
#undef X

/* --- One-shot trampolines --- */
#define X(n) \
static void os_tramp_##n(void *h, unsigned ni) { \
    cfl_lua_os_dispatch(n, h, ni); \
}
SLOT_LIST
#undef X

#define X(n) os_tramp_##n,
static cfl_one_shot_function_t g_os_tramp_table[] = { SLOT_LIST };
#undef X

/* --- Boolean trampolines --- */
#define X(n) \
static bool bool_tramp_##n(void *h, unsigned ni, \
    unsigned et, unsigned ei, void *ed) { \
    return cfl_lua_bool_dispatch(n, h, ni, et, ei, ed); \
}
SLOT_LIST
#undef X

#define X(n) bool_tramp_##n,
static cfl_boolean_function_t g_bool_tramp_table[] = { SLOT_LIST };
#undef X

/* ========================================================================
 * Registration API
 * ======================================================================== */

int cfl_lua_bridge_register_main(
    cfl_image_loader_t *img, const char *name, lua_State *L)
{
    if (g_main_count >= CFL_LUA_MAX_MAIN) {
        fprintf(stderr, "cfl_lua: main trampoline table full\n");
        lua_pop(L, 1);
        return -1;
    }
    if (!lua_isfunction(L, -1)) {
        fprintf(stderr, "cfl_lua: expected function on stack for '%s'\n", name);
        lua_pop(L, 1);
        return -1;
    }
    int slot = g_main_count++;
    g_main_refs[slot] = luaL_ref(L, LUA_REGISTRYINDEX); /* pops function */
    return cfl_image_register_main(img, name, g_main_tramp_table[slot]);
}

int cfl_lua_bridge_register_one_shot(
    cfl_image_loader_t *img, const char *name, lua_State *L)
{
    if (g_os_count >= CFL_LUA_MAX_ONESHOT) {
        fprintf(stderr, "cfl_lua: oneshot trampoline table full\n");
        lua_pop(L, 1);
        return -1;
    }
    if (!lua_isfunction(L, -1)) {
        fprintf(stderr, "cfl_lua: expected function on stack for '%s'\n", name);
        lua_pop(L, 1);
        return -1;
    }
    int slot = g_os_count++;
    g_os_refs[slot] = luaL_ref(L, LUA_REGISTRYINDEX); /* pops function */
    return cfl_image_register_one_shot(img, name, g_os_tramp_table[slot]);
}

int cfl_lua_bridge_register_boolean(
    cfl_image_loader_t *img, const char *name, lua_State *L)
{
    if (g_bool_count >= CFL_LUA_MAX_BOOLEAN) {
        fprintf(stderr, "cfl_lua: boolean trampoline table full\n");
        lua_pop(L, 1);
        return -1;
    }
    if (!lua_isfunction(L, -1)) {
        fprintf(stderr, "cfl_lua: expected function on stack for '%s'\n", name);
        lua_pop(L, 1);
        return -1;
    }
    int slot = g_bool_count++;
    g_bool_refs[slot] = luaL_ref(L, LUA_REGISTRYINDEX); /* pops function */
    return cfl_image_register_boolean(img, name, g_bool_tramp_table[slot]);
}

/* ========================================================================
 * Lua-callable C functions exposed as "cfl" module
 *
 * These give Lua user functions access to the CFL runtime:
 *   cfl.json_extract_string(handle, node_index, path) -> string
 *   cfl.json_extract_int32(handle, node_index, path) -> integer
 *   cfl.json_extract_float(handle, node_index, path) -> number
 *   cfl.json_extract_bool(handle, node_index, path) -> boolean
 *   cfl.json_print_node(handle, node_index)
 *   cfl.bb_get_int32(handle, field_name) -> integer
 *   cfl.bb_set_int32(handle, field_name, value)
 *   cfl.bb_get_float(handle, field_name) -> number
 *   cfl.bb_set_float(handle, field_name, value)
 * ======================================================================== */

static int l_json_extract_string(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    unsigned node_index = (unsigned)luaL_checkinteger(L, 2);
    const char *path = luaL_checkstring(L, 3);

    json_decoder_init_from_runtime(rt, node_index);
    const char *result = NULL;
    json_extract_string_runtime(rt, path, &result);

    if (result)
        lua_pushstring(L, result);
    else
        lua_pushnil(L);
    return 1;
}

static int l_json_extract_int32(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    unsigned node_index = (unsigned)luaL_checkinteger(L, 2);
    const char *path = luaL_checkstring(L, 3);

    json_decoder_init_from_runtime(rt, node_index);
    int32_t result = 0;
    json_extract_int32_runtime(rt, path, &result);

    lua_pushinteger(L, (lua_Integer)result);
    return 1;
}

static int l_json_extract_float(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    unsigned node_index = (unsigned)luaL_checkinteger(L, 2);
    const char *path = luaL_checkstring(L, 3);

    json_decoder_init_from_runtime(rt, node_index);
    float result = 0.0f;
    json_extract_float32_runtime(rt, path, &result);

    lua_pushnumber(L, (lua_Number)result);
    return 1;
}

static int l_json_extract_bool(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    unsigned node_index = (unsigned)luaL_checkinteger(L, 2);
    const char *path = luaL_checkstring(L, 3);

    json_decoder_init_from_runtime(rt, node_index);
    bool result = false;
    json_extract_bool_runtime(rt, path, &result);

    lua_pushboolean(L, result);
    return 1;
}

static int l_json_print_node(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    unsigned node_index = (unsigned)luaL_checkinteger(L, 2);

    json_print_node_data_runtime(rt, node_index);
    return 0;
}

/* --- Blackboard accessors --- */

static int l_bb_get_int32(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    int32_t val = cfl_bb_get_int32(rt, cfl_bb_hash(field), 0);
    lua_pushinteger(L, (lua_Integer)val);
    return 1;
}

static int l_bb_set_int32(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    int32_t val = (int32_t)luaL_checkinteger(L, 3);
    cfl_bb_set_int32(rt, cfl_bb_hash(field), val);
    return 0;
}

static int l_bb_get_float(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    float val = cfl_bb_get_float(rt, cfl_bb_hash(field), 0.0f);
    lua_pushnumber(L, (lua_Number)val);
    return 1;
}

static int l_bb_set_float(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    float val = (float)luaL_checknumber(L, 3);
    cfl_bb_set_float(rt, cfl_bb_hash(field), val);
    return 0;
}

static int l_bb_get_uint32(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    uint32_t val = cfl_bb_get_uint32(rt, cfl_bb_hash(field), 0);
    lua_pushinteger(L, (lua_Integer)val);
    return 1;
}

static int l_bb_set_uint32(lua_State *L)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)lua_touserdata(L, 1);
    const char *field = luaL_checkstring(L, 2);
    uint32_t val = (uint32_t)luaL_checkinteger(L, 3);
    cfl_bb_set_uint32(rt, cfl_bb_hash(field), val);
    return 0;
}

/* ========================================================================
 * Module table
 * ======================================================================== */

static const luaL_Reg cfl_lib[] = {
    /* JSON extraction */
    {"json_extract_string",  l_json_extract_string},
    {"json_extract_int32",   l_json_extract_int32},
    {"json_extract_float",   l_json_extract_float},
    {"json_extract_bool",    l_json_extract_bool},
    {"json_print_node",      l_json_print_node},
    /* Blackboard */
    {"bb_get_int32",         l_bb_get_int32},
    {"bb_set_int32",         l_bb_set_int32},
    {"bb_get_float",         l_bb_get_float},
    {"bb_set_float",         l_bb_set_float},
    {"bb_get_uint32",        l_bb_get_uint32},
    {"bb_set_uint32",        l_bb_set_uint32},
    {NULL, NULL}
};

/* ========================================================================
 * Public API
 * ======================================================================== */

void cfl_lua_bridge_init(lua_State *L)
{
    /* Reset bridge state */
    g_main_count = 0;
    g_os_count = 0;
    g_bool_count = 0;
    memset(g_main_refs, 0, sizeof(g_main_refs));
    memset(g_os_refs, 0, sizeof(g_os_refs));
    memset(g_bool_refs, 0, sizeof(g_bool_refs));

    /* Create "cfl" module table */
    luaL_newlib(L, cfl_lib);

    /* Add constants */
    lua_pushinteger(L, CFL_CONTINUE);   lua_setfield(L, -2, "CONTINUE");
    lua_pushinteger(L, CFL_HALT);       lua_setfield(L, -2, "HALT");
    lua_pushinteger(L, CFL_TERMINATE);  lua_setfield(L, -2, "TERMINATE");
    lua_pushinteger(L, CFL_RESET);      lua_setfield(L, -2, "RESET");
    lua_pushinteger(L, CFL_DISABLE);    lua_setfield(L, -2, "DISABLE");
    lua_pushinteger(L, CFL_SKIP_CONTINUE); lua_setfield(L, -2, "SKIP_CONTINUE");

    /* Event constants */
    lua_pushinteger(L, CFL_INIT_EVENT);            lua_setfield(L, -2, "INIT_EVENT");
    lua_pushinteger(L, CFL_TERMINATE_EVENT);       lua_setfield(L, -2, "TERMINATE_EVENT");
    lua_pushinteger(L, CFL_START_TESTS);           lua_setfield(L, -2, "START_TESTS");
    lua_pushinteger(L, CFL_RAISE_EXCEPTION_EVENT); lua_setfield(L, -2, "RAISE_EXCEPTION_EVENT");
    lua_pushinteger(L, CFL_CHANGE_STATE_EVENT);    lua_setfield(L, -2, "CHANGE_STATE_EVENT");

    lua_setglobal(L, "cfl");
}

void cfl_lua_bridge_attach(cfl_runtime_handle_t *handle, lua_State *L)
{
    cfl_set_user_handle(handle, (void *)L);
}
