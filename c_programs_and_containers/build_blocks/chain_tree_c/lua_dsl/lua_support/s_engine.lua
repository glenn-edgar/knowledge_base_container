local ColumnFlow = require("lua_support.column_flow")

local SEngine = setmetatable({}, { __index = ColumnFlow })
SEngine.__index = SEngine

function SEngine.new(ctb)
    local self = ColumnFlow.new(ctb)
    return setmetatable(self, SEngine)
end

-- =========================================================================
-- se_module_load: Single leaf node that loads an S-Engine module
--
-- Init one-shot registers builtins → cfl → user functions, then validates.
-- Main is null (CFL_CONTINUE). Term unloads the module from the registry.
-- The user_register_fn is the node's boolean function — called after
-- builtins/cfl layers, before validate. Pass "CFL_NULL" if no user
-- functions needed.
--
-- Parameters:
--   module_name        - string name of the s-engine module
--   user_register_fn   - string name of user registration boolean function
-- =========================================================================

function SEngine:se_module_load(module_name, user_register_fn)
    user_register_fn = user_register_fn or "CFL_NULL"

    if type(module_name) ~= "string" then
        error("se_module_load: module_name must be a string")
    end
    if type(user_register_fn) ~= "string" then
        error("se_module_load: user_register_fn must be a string")
    end

    local node_data = {
        module_name = module_name,
    }

    return self:define_column_link(
        "CFL_SE_MODULE_LOAD_MAIN",
        "CFL_SE_MODULE_LOAD_INIT",
        user_register_fn,
        "CFL_SE_MODULE_LOAD_TERM",
        node_data,
        "SE_MOD_LOAD"
    )
end

-- =========================================================================
-- se_tree_load: Single leaf node that creates an S-Engine tree instance
--
-- Init one-shot looks up module in registry, creates tree instance,
-- stores pointer in blackboard uint64 slot.
-- Main is null (CFL_CONTINUE). Term terminates tree, frees, clears slot.
--
-- The boolean function controls blackboard loading:
--   returns true  → skip default blackboard loading (user handles it)
--   returns false → use default blackboard loading
-- Default is "CFL_FALSE" (always use default loading).
--
-- Parameters:
--   module_name       - string name of the module (registry lookup by hash)
--   tree_name         - string name of the tree within the module
--   bb_field_name     - string name of the blackboard uint64 field
--   custom_bb_load_fn - string name of boolean function for custom
--                       blackboard loading (nil → "CFL_FALSE")
-- =========================================================================

function SEngine:se_tree_load(module_name, tree_name, bb_field_name, custom_bb_load_fn)
    custom_bb_load_fn = custom_bb_load_fn or "CFL_NULL"

    if type(module_name) ~= "string" then
        error("se_tree_load: module_name must be a string")
    end
    if type(tree_name) ~= "string" then
        error("se_tree_load: tree_name must be a string")
    end
    if type(bb_field_name) ~= "string" then
        error("se_tree_load: bb_field_name must be a string")
    end
    if type(custom_bb_load_fn) ~= "string" then
        error("se_tree_load: custom_bb_load_fn must be a string")
    end

    local node_data = {
        module_name = module_name,
        tree_name = tree_name,
        bb_field_name = bb_field_name,
    }

    return self:define_column_link(
        "CFL_SE_TREE_LOAD_MAIN",
        "CFL_SE_TREE_LOAD_INIT",
        custom_bb_load_fn,
        "CFL_SE_TREE_LOAD_TERM",
        node_data,
        "SE_TREE_LOAD"
    )
end

-- =========================================================================
-- se_tick: Single leaf node that ticks an S-Engine tree each ChainTree tick
--
-- Init retrieves the tree instance from a blackboard uint64 slot and
-- resets it. Main ticks the tree each ChainTree tick, processes the
-- s-engine event queue, and maps SE return codes to CFL return codes.
-- CFL_TIMER_EVENT is mapped to SE_EVENT_TICK; all others pass through.
--
-- Parameters:
--   tree_bb_field - string name of the blackboard uint64 field holding
--                   the s-engine tree instance pointer
-- =========================================================================

function SEngine:se_tick(tree_bb_field)
    if type(tree_bb_field) ~= "string" then
        error("se_tick: tree_bb_field must be a string")
    end

    local node_data = {
        tree_bb_field = tree_bb_field,
    }

    return self:define_column_link(
        "CFL_SE_TICK_MAIN",
        "CFL_SE_TICK_INIT",
        "CFL_NULL",
        "CFL_SE_TICK_TERM",
        node_data,
        "SE_TICK"
    )
end

return SEngine
