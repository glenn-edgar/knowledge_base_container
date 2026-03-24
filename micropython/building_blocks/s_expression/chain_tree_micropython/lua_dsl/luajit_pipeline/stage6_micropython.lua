--[[
  stage6_micropython.lua - Generate MicroPython module from pipeline data.

  Generates a single _module_mpy.py file per handle, containing:
    - main_funcs    : tuple of main function names
    - pred_funcs    : tuple of predicate/boolean function names
    - records       : tuple of (name, fields) for blackboard init
    - tree_order    : tuple of tree names
    - trees         : tuple of tree tuples with nested node tuples
    - string_table  : tuple of event strings
    - string_index  : tuple of (name, index) pairs
    - events        : tuple of event entries (sorted by index)

  Node tuple format (matches ct_runtime.py):
    (func_name, func_index, node_index, guard_index, node_data, children)

  Tree tuple format:
    (tree_name, tree_name_hash, node_count, record_name, root_node)
--]]

local MicroPythonCodeGenerator = {}
MicroPythonCodeGenerator.__index = MicroPythonCodeGenerator

function MicroPythonCodeGenerator.new(opts)
    local self = setmetatable({}, MicroPythonCodeGenerator)
    self.output_dir = opts.output_dir
    self.handle_name = opts.handle_name
    self.unique_id = opts.unique_id
    self.handle = opts.handle
    self.node_builder = opts.node_builder
    self.function_builder = opts.function_builder
    self.link_builder = opts.link_builder
    self.data_encoder = opts.data_encoder
    self.main_function_usage = opts.main_function_usage
    return self
end

-- =========================================================================
-- FNV-1a 32-bit hash (shift-based mul32, matches C runtime)
-- =========================================================================
-- Resolve fnv1a module from lua_support/ (sibling of luajit_pipeline/)
local _script_dir = arg and arg[0] and arg[0]:match("^(.*)/") or "."
package.path = _script_dir .. "/../lua_support/?.lua;" .. package.path

local fnv1a_mod = require("fnv1a")
local fnv1a_32 = fnv1a_mod.fnv1a_32

-- =========================================================================
-- Helpers
-- =========================================================================

local function sorted_pairs_by_value(tbl)
    local items = {}
    for k, v in pairs(tbl) do items[#items + 1] = { k, v } end
    table.sort(items, function(a, b) return a[2] < b[2] end)
    local i = 0
    return function()
        i = i + 1
        if items[i] then return items[i][1], items[i][2] end
    end
end

local function sorted_set_keys(set_tbl)
    local keys = {}
    for k in pairs(set_tbl) do keys[#keys + 1] = k end
    table.sort(keys)
    return keys
end

local function table_len(t)
    local n = 0
    for _ in pairs(t) do n = n + 1 end
    return n
end

-- Filter executable KBs (skip function-mapping and event-table KBs)
local function filter_executable_kbs(handle, node_builder)
    local all_names = handle:get_kb_names()
    local executable = {}
    for _, kb in ipairs(all_names) do
        if not (kb:match("_test_functions$") or kb:match("_functions$")
                or kb == "complete_functions_kb"
                or kb == "event_string_table_kb"
                or kb == "bitmask_table_kb") then
            executable[#executable + 1] = kb
        end
    end
    -- Sort by start_index
    table.sort(executable, function(a, b)
        local sa = node_builder:get_kb_range(a)
        local sb = node_builder:get_kb_range(b)
        return sa < sb
    end)
    return executable
end

-- Python string escaping
local function py_str(s)
    if s == nil then return "None" end
    -- Escape backslashes, then quotes
    s = s:gsub("\\", "\\\\")
    s = s:gsub('"', '\\"')
    s = s:gsub("\n", "\\n")
    s = s:gsub("\r", "\\r")
    return '"' .. s .. '"'
end

-- Format a Python value (string, number, bool, nil)
local function py_val(v)
    if v == nil then return "None"
    elseif type(v) == "string" then return py_str(v)
    elseif type(v) == "boolean" then return v and "True" or "False"
    elseif type(v) == "number" then
        if v == math.floor(v) then
            return tostring(math.floor(v))
        else
            return tostring(v)
        end
    else
        return tostring(v)
    end
end

-- =========================================================================
-- Build main_funcs and pred_funcs lists from the function builder
-- =========================================================================

function MicroPythonCodeGenerator:_build_function_tables()
    local fb = self.function_builder

    -- Main functions (skip CFL_NULL at index 0)
    local main_funcs = {}
    local main_count = fb.main_indexer:get_count()
    for i = 0, main_count - 1 do
        local name = fb.main_indexer:get_function(i)
        main_funcs[i] = name
    end

    -- Boolean/pred functions (skip CFL_NULL at index 0)
    local pred_funcs = {}
    local pred_count = fb.boolean_indexer:get_count()
    for i = 0, pred_count - 1 do
        local name = fb.boolean_indexer:get_function(i)
        pred_funcs[i] = name
    end

    self._main_funcs = main_funcs
    self._main_count = main_count
    self._pred_funcs = pred_funcs
    self._pred_count = pred_count
end

-- =========================================================================
-- Build node_data tuple from the JSON IR node_dict
-- =========================================================================

local function is_array(t)
    if type(t) ~= "table" then return false end
    local count = 0
    for _ in pairs(t) do count = count + 1 end
    if count == 0 then return true end
    for i = 1, count do
        if t[i] == nil then return false end
    end
    return true
end

-- Convert a node_dict to a Python tuple representation string
local function node_dict_to_py(nd)
    if nd == nil or (type(nd) == "table" and not next(nd)) then
        return "()"
    end

    if type(nd) ~= "table" then
        return "(" .. py_val(nd) .. ",)"
    end

    -- For arrays, emit as tuple
    if is_array(nd) then
        if #nd == 0 then return "()" end
        local parts = {}
        for _, v in ipairs(nd) do
            if type(v) == "table" then
                parts[#parts + 1] = node_dict_to_py(v)
            else
                parts[#parts + 1] = py_val(v)
            end
        end
        if #parts == 1 then
            return "(" .. parts[1] .. ",)"
        end
        return "(" .. table.concat(parts, ", ") .. ")"
    end

    -- For dicts, emit as tuple of key-value pairs
    -- But check for known patterns first
    local parts = {}
    local keys = {}
    for k in pairs(nd) do keys[#keys + 1] = k end
    table.sort(keys)

    for _, k in ipairs(keys) do
        local v = nd[k]
        if type(v) == "table" then
            parts[#parts + 1] = py_str(k)
            parts[#parts + 1] = node_dict_to_py(v)
        else
            parts[#parts + 1] = py_str(k)
            parts[#parts + 1] = py_val(v)
        end
    end

    if #parts == 0 then return "()" end
    return "(" .. table.concat(parts, ", ") .. ")"
end

-- =========================================================================
-- Recursive node emission
-- =========================================================================

function MicroPythonCodeGenerator:_emit_node(lines, ltree_name, indent_level)
    local indent = string.rep("    ", indent_level)
    local node_data = self.handle:get_node_data(ltree_name)
    if not node_data then
        lines[#lines + 1] = indent .. '("UNKNOWN", 0, 0, -1, (), ()),'
        return
    end

    local functions = self.handle:get_node_functions(ltree_name)
    local main_fn = functions.main or "CFL_NULL"
    local aux_fn = functions.aux or "CFL_NULL"

    -- Get main function index
    local main_idx = 0
    local ok, idx = pcall(self.function_builder.main_indexer.get_index,
                          self.function_builder.main_indexer, main_fn)
    if ok then main_idx = idx end

    -- Get guard/pred index (-1 if no guard)
    local guard_idx = -1
    if aux_fn and aux_fn ~= "CFL_NULL" then
        local ok2, idx2 = pcall(self.function_builder.boolean_indexer.get_index,
                                self.function_builder.boolean_indexer, aux_fn)
        if ok2 then guard_idx = idx2 end
    end

    -- Get node index
    local node_index = self.node_builder.ltree_to_final_index[ltree_name] or 0

    -- Build node_data from node_dict
    local nd = node_data.node_dict
    -- Strip auto_start from node_dict (it's a link-table concept)
    if nd and type(nd) == "table" then
        local filtered = {}
        for k, v in pairs(nd) do
            if k ~= "auto_start" then
                filtered[k] = v
            end
        end
        nd = filtered
        if not next(nd) then nd = nil end
    end
    local nd_str = node_dict_to_py(nd)

    -- Get children
    local children = self.handle:get_node_children(ltree_name)
    -- Filter out metadata children
    local real_children = {}
    for _, child in ipairs(children) do
        if not self.node_builder.filtered_nodes[child] then
            real_children[#real_children + 1] = child
        end
    end

    if #real_children == 0 then
        -- Leaf node - single line
        lines[#lines + 1] = string.format('%s(%s, %d, %d, %d, %s, ()),',
            indent, py_str(main_fn), main_idx, node_index, guard_idx, nd_str)
    else
        -- Composite node with children
        lines[#lines + 1] = string.format('%s(%s, %d, %d, %d, %s,',
            indent, py_str(main_fn), main_idx, node_index, guard_idx, nd_str)
        lines[#lines + 1] = indent .. "    ("

        for _, child in ipairs(real_children) do
            self:_emit_node(lines, child, indent_level + 2)
        end

        lines[#lines + 1] = indent .. "    ),"
        lines[#lines + 1] = indent .. "),"
    end
end

-- =========================================================================
-- Generate the _module_mpy.py file
-- =========================================================================

function MicroPythonCodeGenerator:generate_all()
    self:_build_function_tables()

    local lines = {}
    local handle_name = self.handle_name

    -- Header
    lines[#lines + 1] = "# =========================================================================="
    lines[#lines + 1] = string.format("# %s_module_mpy.py", handle_name)
    lines[#lines + 1] = "# Auto-generated by ChainTree Pipeline (LuaJIT) - MicroPython backend"
    lines[#lines + 1] = "#"
    lines[#lines + 1] = "# Node tuple: (func_name, func_index, node_index, guard_index, node_data, children)"
    lines[#lines + 1] = "# =========================================================================="
    lines[#lines + 1] = ""
    lines[#lines + 1] = "from micropython import const"
    lines[#lines + 1] = ""

    -- main_funcs
    lines[#lines + 1] = "main_funcs = ("
    for i = 0, self._main_count - 1 do
        lines[#lines + 1] = string.format('    %s,%s',
            py_str(self._main_funcs[i]),
            string.format("  # %d", i))
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""

    -- pred_funcs
    if self._pred_count <= 1 then
        -- Only CFL_NULL or empty
        lines[#lines + 1] = "pred_funcs = ()"
    else
        lines[#lines + 1] = "pred_funcs = ("
        for i = 0, self._pred_count - 1 do
            lines[#lines + 1] = string.format('    %s,%s',
                py_str(self._pred_funcs[i]),
                string.format("  # %d", i))
        end
        lines[#lines + 1] = ")"
    end
    lines[#lines + 1] = ""

    -- records (from blackboard)
    self:_emit_records(lines)

    -- tree_order and trees
    local kb_names = filter_executable_kbs(self.handle, self.node_builder)
    self:_emit_trees(lines, kb_names)

    -- string_table and string_index (from event_string_table)
    self:_emit_string_table(lines)

    -- events
    self:_emit_events(lines)

    -- Write file
    local filename = handle_name .. "_module_mpy.py"
    local path = self.output_dir .. "/" .. filename
    local f = io.open(path, "w")
    if not f then error("Cannot open for writing: " .. path) end
    f:write(table.concat(lines, "\n") .. "\n")
    f:close()
    print("  Generated: " .. path)
end

-- =========================================================================
-- Records (blackboard)
-- =========================================================================

function MicroPythonCodeGenerator:_emit_records(lines)
    local bb = self.handle.blackboard
    if not bb or not bb.record then
        lines[#lines + 1] = "records = ()"
        lines[#lines + 1] = ""
        return
    end

    local rec = bb.record
    lines[#lines + 1] = "records = ("
    lines[#lines + 1] = "    ("
    lines[#lines + 1] = string.format("        %s,", py_str(rec.name))
    lines[#lines + 1] = "        ("

    local fields = rec.fields or {}
    for _, field in ipairs(fields) do
        lines[#lines + 1] = string.format('            (%s, %s, %s),',
            py_str(field.name), py_str(field.type), py_val(field.default))
    end

    lines[#lines + 1] = "        ),"
    lines[#lines + 1] = "    ),"
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""
end

-- =========================================================================
-- Trees
-- =========================================================================

function MicroPythonCodeGenerator:_emit_trees(lines, kb_names)
    -- tree_order
    lines[#lines + 1] = "tree_order = ("
    for _, kb_name in ipairs(kb_names) do
        lines[#lines + 1] = string.format('    %s,', py_str(kb_name))
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""

    -- trees
    lines[#lines + 1] = "trees = ("
    for _, kb_name in ipairs(kb_names) do
        local root_ltree = self.handle:get_kb_root_node(kb_name)
        if not root_ltree then
            print("  Warning: no root node for KB: " .. kb_name)
            goto continue_kb
        end

        local name_hash = fnv1a_32(kb_name)
        local node_count = self.handle:get_kb_node_count(kb_name)

        -- Determine blackboard record name
        local record_name = "None"
        local bb = self.handle.blackboard
        if bb and bb.record then
            record_name = py_str(bb.record.name)
        end

        lines[#lines + 1] = "    ("
        lines[#lines + 1] = string.format("        %s,  # T_NAME", py_str(kb_name))
        lines[#lines + 1] = string.format("        0x%08X,  # T_NAME_HASH", name_hash)
        lines[#lines + 1] = string.format("        %d,  # T_NODE_COUNT", node_count)
        lines[#lines + 1] = string.format("        %s,  # T_RECORD", record_name)
        lines[#lines + 1] = "        # T_ROOT"

        -- Emit root node recursively
        self:_emit_node(lines, root_ltree, 2)

        lines[#lines + 1] = "    ),"

        ::continue_kb::
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""
end

-- =========================================================================
-- String table (event strings)
-- =========================================================================

function MicroPythonCodeGenerator:_emit_string_table(lines)
    local est = self.handle:get_event_string_table()
    if table_len(est) == 0 then
        lines[#lines + 1] = "string_table = ()"
        lines[#lines + 1] = "string_index = ()"
        lines[#lines + 1] = ""
        return
    end

    -- Build sorted-by-index list
    local by_index = {}
    for name, idx in pairs(est) do
        by_index[#by_index + 1] = { name = name, index = idx }
    end
    table.sort(by_index, function(a, b) return a.index < b.index end)

    lines[#lines + 1] = "string_table = ("
    for _, entry in ipairs(by_index) do
        lines[#lines + 1] = string.format("    %s,  # %d", py_str(entry.name), entry.index)
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""

    -- string_index: alphabetically sorted (name, index) pairs
    local by_name = {}
    for name, idx in pairs(est) do
        by_name[#by_name + 1] = { name = name, index = idx }
    end
    table.sort(by_name, function(a, b) return a.name < b.name end)

    lines[#lines + 1] = "string_index = ("
    for _, entry in ipairs(by_name) do
        lines[#lines + 1] = string.format("    (%s, %d),", py_str(entry.name), entry.index)
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""
end

-- =========================================================================
-- Events
-- =========================================================================

function MicroPythonCodeGenerator:_emit_events(lines)
    local est = self.handle:get_event_string_table()
    if table_len(est) == 0 then
        lines[#lines + 1] = "events = ()"
        lines[#lines + 1] = ""
        return
    end

    -- Events sorted by index
    local by_index = {}
    for name, idx in pairs(est) do
        by_index[#by_index + 1] = { name = name, index = idx }
    end
    table.sort(by_index, function(a, b) return a.index < b.index end)

    lines[#lines + 1] = "events = ("
    for _, entry in ipairs(by_index) do
        lines[#lines + 1] = string.format("    (%s, %d),", py_str(entry.name), entry.index)
    end
    lines[#lines + 1] = ")"
    lines[#lines + 1] = ""
end

return MicroPythonCodeGenerator
