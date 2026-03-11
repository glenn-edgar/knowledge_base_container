#!/usr/bin/env luajit
-- ============================================================================
-- s_compile.lua
-- S-Expression Engine DSL Compiler - Version 5.3
--
-- Generates:
--   - C headers (.h) for records, module, user functions
--   - C registration code (.c)
--   - Binary module files (.bin) - direct s_expr_param_t, zero-copy
--   - Binary as C header (_bin.h)
--   - Debug parameter dump header (_dump.h)
--   - Lua module file (_module.lua) - pre-serialized module_data for LuaJIT runtime
--   - Python module file (_module.py) - pre-serialized module_data for Python runtime
--
-- VERSION 5.3 CHANGES:
--   - Added --python / --python=<file> flag to serialize module_data as Python
--   - --all-bin now also generates _module.py automatically
--   - serialize_module_python() produces a zero-dependency static Python file
--
-- VERSION 5.2 CHANGES:
--   - Added --lua / --lua=<file> flag to serialize module_data as a Lua table
--   - --all-bin now also generates _module.lua automatically
--   - serialize_module_lua() produces a zero-dependency static Lua file
--
-- VERSION 5.1 CHANGES:
--   - Renamed p_call_bit to p_call_composite for generic predicate composition
--   - Updated result codes for proper caller/engine separation
--   - Standalone library (no ChainTree dependencies in core)
--   - Added parameter dump header for debugging
--
-- Usage: luajit s_compile.lua <input.lua> [options]
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")
jit.off()

-- ============================================================================
-- ARGUMENT PARSING
-- ============================================================================

local function parse_args(args)
    local opts = {
        input = nil,
        header = nil,
        user_header = nil,
        registration = nil,
        records_header = nil,
        debug_header = nil,
        binary_file = nil,
        binary_header = nil,
        dump_header = nil,
        lua_module = nil,
        python_module = nil,
        dump = false,
        all = false,
        all_bin = false,
        outdir = ".",
        pointer_size = 4,
        helpers = {},
    }

    for i, arg in ipairs(args) do
        if arg:match("^%-%-header=") then
            opts.header = arg:match("^%-%-header=(.+)$")
        elseif arg:match("^%-%-user=") then
            opts.user_header = arg:match("^%-%-user=(.+)$")
        elseif arg:match("^%-%-reg=") then
            opts.registration = arg:match("^%-%-reg=(.+)$")
        elseif arg:match("^%-%-records=") then
            opts.records_header = arg:match("^%-%-records=(.+)$")
        elseif arg:match("^%-%-debug=") then
            opts.debug_header = arg:match("^%-%-debug=(.+)$")
        elseif arg:match("^%-%-binary=") then
            opts.binary_file = arg:match("^%-%-binary=(.+)$")
        elseif arg:match("^%-%-binary%-h=") then
            opts.binary_header = arg:match("^%-%-binary%-h=(.+)$")
        elseif arg:match("^%-%-dump%-h=") then
            opts.dump_header = arg:match("^%-%-dump%-h=(.+)$")
        elseif arg:match("^%-%-lua=") then
            opts.lua_module = arg:match("^%-%-lua=(.+)$")
        elseif arg == "--lua" then
            opts.lua_module = true
        elseif arg:match("^%-%-python=") then
            opts.python_module = arg:match("^%-%-python=(.+)$")
        elseif arg == "--python" then
            opts.python_module = true
        elseif arg:match("^%-%-outdir=") then
            opts.outdir = arg:match("^%-%-outdir=(.+)$")
        elseif arg:match("^%-%-helpers=") then
            local helpers_file = arg:match("^%-%-helpers=(.+)$")
            table.insert(opts.helpers, helpers_file)
        elseif arg == "--dump" then
            opts.dump = true
        elseif arg == "--all" then
            opts.all = true
        elseif arg == "--all-bin" then
            opts.all_bin = true
        elseif arg == "--32bit" then
            opts.pointer_size = 4
        elseif arg == "--64bit" then
            opts.pointer_size = 8
        elseif arg == "--help" or arg == "-h" then
            return nil
        elseif arg:match("^%-") then
            io.stderr:write("Unknown option: " .. arg .. "\n")
            os.exit(1)
        else
            if not opts.input then
                opts.input = arg
            else
                io.stderr:write("Multiple input files not supported\n")
                os.exit(1)
            end
        end
    end

    return opts
end

local function print_usage()
    print([[
S-Expression Engine DSL Compiler v5.3

Usage: luajit s_compile.lua <input.lua> [options]

Options:
  --header=<file>      Generate main C header (default: <base>.h)
  --user=<file>        Generate user function header
  --reg=<file>         Generate user registration code
  --records=<file>     Generate records header (standalone structures)
  --debug=<file>       Generate debug header with hash->name mappings
  --binary=<file>      Generate binary module file (.bin)
  --binary-h=<file>    Generate binary header (const uint8_t array)
  --dump-h=<file>      Generate parameter dump header (human-readable params)
  --lua[=<file>]       Generate Lua module file for LuaJIT runtime
                       (default filename: <base>_module.lua)
  --python[=<file>]    Generate Python module file for Python runtime
                       (default filename: <base>_module.py)
  --helpers=<file>     Load helper functions (can specify multiple times)
  --dump               Print debug dump of module to stdout
  --all                Generate all text outputs
  --all-bin            Generate all outputs including binary, Lua, and Python
  --outdir=<dir>       Output directory (default: current)
  --32bit              Force 32-bit mode (default)
  --64bit              Force 64-bit mode
  --help, -h           Show this help

Generated files with --all:
  <base>_records.h           - Standalone record structures
  <base>.h                   - Module header (includes records)
  <base>_debug.h             - Debug hash reference
  <base>_user_functions.h    - User function prototypes
  <base>_user_registration.c - Function registration code

Generated files with --all-bin (includes --all plus):
  <base>_32.bin or <base>_64.bin       - Binary module for runtime loading
  <base>_bin_32.h or <base>_bin_64.h   - Binary as C array for ROM embedding
  <base>_dump_32.h or <base>_dump_64.h - Human-readable parameter dump
  <base>_module.lua                    - Pre-serialized module_data for LuaJIT runtime
  <base>_module.py                     - Pre-serialized module_data for Python runtime

Examples:
  luajit s_compile.lua my_module.lua --all-bin --outdir=generated/
  luajit s_compile.lua my_module.lua --all-bin --64bit
  luajit s_compile.lua my_module.lua --lua --outdir=generated/
  luajit s_compile.lua my_module.lua --python --outdir=generated/
  luajit s_compile.lua my_module.lua --python=my_runtime_module.py
  luajit s_compile.lua my_module.lua --helpers=s_engine_helpers.lua --all-bin
]])
end

-- ============================================================================
-- FILE UTILITIES
-- ============================================================================

local function file_exists(path)
    local f = io.open(path, "r")
    if f then
        f:close()
        return true
    end
    return false
end

local function find_file(filename, search_paths)
    for _, path in ipairs(search_paths) do
        local full_path = path .. "/" .. filename
        if file_exists(full_path) then
            return full_path
        end
    end
    if file_exists(filename) then
        return filename
    end
    return nil
end

local function write_file(path, content)
    local f = io.open(path, "w")
    if not f then
        io.stderr:write("Error: Cannot write to " .. path .. "\n")
        os.exit(1)
    end
    f:write(content)
    f:write("\n")
    f:close()
    print("Generated: " .. path)
end

local function write_binary(path, bytes)
    local f = io.open(path, "wb")
    if not f then
        io.stderr:write("Error: Cannot write to " .. path .. "\n")
        os.exit(1)
    end
    for _, b in ipairs(bytes) do
        f:write(string.char(b))
    end
    f:close()
    print("Generated: " .. path .. " (" .. #bytes .. " bytes)")
end

local function make_path(outdir, filename)
    if outdir == "." then
        return filename
    else
        os.execute("mkdir -p " .. outdir)
        return outdir .. "/" .. filename
    end
end

-- ============================================================================
-- LUA MODULE SERIALIZER
-- ============================================================================

local function serialize_module_lua(module_data)
    local lines = {}
    local mod = module_data

    local function ser_string_list(arr)
        local parts = {}
        for _, v in ipairs(arr) do
            table.insert(parts, string.format('%q', v))
        end
        return "{ " .. table.concat(parts, ", ") .. " }"
    end

    local function ser_param(p)
        local t = p.type
        local v = p.value
        local vstr

        if t == "int" or t == "uint" or t == "float" or t == "result"
           or t == "stack_tos" or t == "stack_local"
           or t == "stack_push" or t == "stack_pop"
           or t == "null_param" then
            vstr = tostring(v)

        elseif t == "str_idx" or t == "str_ptr"
               or t == "field_ref" or t == "nested_field_ref"
               or t == "const_ref"
               or t == "list_start"  or t == "list_end"
               or t == "dict_start"  or t == "dict_end"
               or t == "dict_key"    or t == "end_dict_key"
               or t == "array_start" or t == "array_end"
               or t == "tuple_start" or t == "tuple_end" then
            vstr = string.format('%q', tostring(v))

        elseif t == "str_hash" then
            vstr = string.format('{hash=0x%08X,str=%q}', v.hash, v.str)

        elseif t == "dict_key_hash" then
            vstr = tostring(v)

        else
            vstr = tostring(v)
        end

        return string.format(
            '{type=%q,value=%s,order=%d}',
            t, vstr, p.order or 0
        )
    end

    -- Recursive node serializer, returns array of line strings
    local function ser_node(node, indent)
        local ind  = string.rep("  ", indent)
        local ind2 = string.rep("  ", indent + 1)
        local parts = {}

        table.insert(parts, ind .. "{")
        table.insert(parts, ind2 .. string.format('func_name=%q,', node.func_name))
        table.insert(parts, ind2 .. string.format('func_hash=0x%08X,', node.func_hash))
        table.insert(parts, ind2 .. string.format('call_type=%q,', node.call_type))
        table.insert(parts, ind2 .. string.format('order=%d,', node.order or 0))
        table.insert(parts, ind2 .. string.format('param_count=%d,', node.param_count or 0))

        if node.pointer_index ~= nil then
            table.insert(parts, ind2 .. string.format('pointer_index=%d,', node.pointer_index))
        else
            table.insert(parts, ind2 .. 'pointer_index=nil,')
        end

        -- params
        if #node.params > 0 then
            table.insert(parts, ind2 .. "params={")
            for _, p in ipairs(node.params) do
                table.insert(parts, ind2 .. "  " .. ser_param(p) .. ",")
            end
            table.insert(parts, ind2 .. "},")
        else
            table.insert(parts, ind2 .. "params={},")
        end

        -- children (recursive)
        if #node.children > 0 then
            table.insert(parts, ind2 .. "children={")
            for _, child in ipairs(node.children) do
                local child_lines = ser_node(child, indent + 2)
                for _, cl in ipairs(child_lines) do
                    table.insert(parts, cl)
                end
                -- trailing comma after each child table
                -- (already closed by ser_node, add comma on same line as closing brace)
                parts[#parts] = parts[#parts] .. ","
            end
            table.insert(parts, ind2 .. "},")
        else
            table.insert(parts, ind2 .. "children={},")
        end

        table.insert(parts, ind .. "}")
        return parts
    end

    -- Header
    table.insert(lines, "-- ============================================================================")
    table.insert(lines, "-- " .. mod.name .. "_module.lua")
    table.insert(lines, "-- Pre-serialized module_data for LuaJIT runtime")
    table.insert(lines, "-- DO NOT EDIT - Generated by s_compile.lua v5.3")
    table.insert(lines, "-- ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "local M = {}")
    table.insert(lines, "")

    -- Scalar metadata
    table.insert(lines, string.format('M.name         = %q', mod.name))
    table.insert(lines, string.format('M.name_hash    = 0x%08X', mod.name_hash))
    table.insert(lines, string.format('M.pointer_size = %d', mod.pointer_size))
    table.insert(lines, string.format('M.debug        = %s', tostring(mod.debug or false)))
    table.insert(lines, "")

    -- Function tables (order determines 0-based func_index in binary)
    table.insert(lines, "M.oneshot_funcs = " .. ser_string_list(mod.oneshot_funcs))
    table.insert(lines, "M.main_funcs    = " .. ser_string_list(mod.main_funcs))
    table.insert(lines, "M.pred_funcs    = " .. ser_string_list(mod.pred_funcs))
    table.insert(lines, "")

    -- String table
    table.insert(lines, "M.string_table = " .. ser_string_list(mod.string_table))
    table.insert(lines, "M.string_index = {")
    -- Sort for deterministic output
    local skeys = {}
    for s, _ in pairs(mod.string_index) do table.insert(skeys, s) end
    table.sort(skeys)
    for _, s in ipairs(skeys) do
        table.insert(lines, string.format('  [%q] = %d,', s, mod.string_index[s]))
    end
    table.insert(lines, "}")
    table.insert(lines, "")

    -- Trees
    table.insert(lines, "M.tree_order = " .. ser_string_list(mod.tree_order))
    table.insert(lines, "M.trees = {}")
    table.insert(lines, "")

    for _, tname in ipairs(mod.tree_order) do
        local tree = mod.trees[tname]
        table.insert(lines, string.format('M.trees[%q] = {', tname))
        table.insert(lines, string.format('  name          = %q,', tree.name))
        table.insert(lines, string.format('  name_hash     = 0x%08X,', tree.name_hash))
        table.insert(lines, string.format('  node_count    = %d,', tree.node_count))
        table.insert(lines, string.format('  pointer_count = %d,', tree.pointer_count or 0))
        if tree.record_name then
            table.insert(lines, string.format('  record_name   = %q,', tree.record_name))
        else
            table.insert(lines, '  record_name   = nil,')
        end
        table.insert(lines, string.format('  defaults_index = 0x%04X,', tree.defaults_index or 0xFFFF))
        table.insert(lines, "  nodes = {")
        for _, node in ipairs(tree.nodes) do
            local nlines = ser_node(node, 2)
            for _, nl in ipairs(nlines) do
                table.insert(lines, nl)
            end
            lines[#lines] = lines[#lines] .. ","
        end
        table.insert(lines, "  },")
        table.insert(lines, "}")
        table.insert(lines, "")
    end

    -- Records (may be empty but must be present)
    table.insert(lines, "M.record_order = " .. ser_string_list(mod.record_order))
    table.insert(lines, "M.records      = {}")
    if #mod.record_order > 0 then
        for _, rname in ipairs(mod.record_order) do
            local rec = mod.records[rname]
            table.insert(lines, string.format('M.records[%q] = {', rname))
            table.insert(lines, string.format('  name      = %q,', rec.name))
            table.insert(lines, string.format('  name_hash = 0x%08X,', rec.name_hash))
            table.insert(lines, string.format('  size      = %d,', rec.size))
            table.insert(lines, string.format('  align     = %d,', rec.align))
            table.insert(lines, "  fields    = {")
            for _, f in ipairs(rec.fields) do
                table.insert(lines, string.format(
                    '    {name=%q,name_hash=0x%08X,type=%q,offset=%d,size=%d,' ..
                    'is_pointer=%s,is_char_array=%s,is_embedded=%s},',
                    f.name, f.name_hash, f.type or "",
                    f.offset, f.size,
                    tostring(f.is_pointer or false),
                    tostring(f.is_char_array or false),
                    tostring(f.is_embedded or false)
                ))
            end
            table.insert(lines, "  },")
            table.insert(lines, "}")
        end
        table.insert(lines, "")
    end

    -- Constants
    table.insert(lines, "M.const_order = " .. ser_string_list(mod.const_order))
    table.insert(lines, "M.constants   = {}")
    table.insert(lines, "")

    -- Events
    table.insert(lines, "M.events      = {}")
    table.insert(lines, "M.event_names = {}")
    if mod.events and #mod.events > 0 then
        for _, evt in ipairs(mod.events) do
            table.insert(lines, string.format(
                'table.insert(M.events, {name=%q, id=0x%04X})', evt.name, evt.id))
            table.insert(lines, string.format(
                'M.event_names[%q] = 0x%04X', evt.name, evt.id))
        end
        table.insert(lines, "")
    end

    table.insert(lines, "return M")

    return table.concat(lines, "\n")
end

-- ============================================================================
-- PYTHON MODULE SERIALIZER
-- ============================================================================

local function serialize_module_python(module_data)
    local lines = {}
    local mod = module_data

    -- Python string escaping (handles backslash, quotes, newlines)
    local function py_str(s)
        local escaped = s:gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"):gsub("\r", "\\r")
        return '"' .. escaped .. '"'
    end

    local function py_string_list(arr)
        local parts = {}
        for _, v in ipairs(arr) do
            table.insert(parts, py_str(v))
        end
        return "[" .. table.concat(parts, ", ") .. "]"
    end

    local function py_bool(v)
        if v then return "True" else return "False" end
    end

    local function py_none_or(v, fmt)
        if v == nil then return "None" end
        if fmt then return string.format(fmt, v) end
        return tostring(v)
    end

    local function ser_param(p, indent)
        local ind = string.rep("    ", indent)
        local t = p.type
        local v = p.value
        local vstr

        if t == "int" or t == "uint" or t == "float" or t == "result"
           or t == "stack_tos" or t == "stack_local"
           or t == "stack_push" or t == "stack_pop"
           or t == "null_param" then
            vstr = tostring(v)

        elseif t == "str_idx" or t == "str_ptr"
               or t == "field_ref" or t == "nested_field_ref"
               or t == "const_ref"
               or t == "list_start"  or t == "list_end"
               or t == "dict_start"  or t == "dict_end"
               or t == "dict_key"    or t == "end_dict_key"
               or t == "array_start" or t == "array_end"
               or t == "tuple_start" or t == "tuple_end" then
            vstr = py_str(tostring(v))

        elseif t == "str_hash" then
            vstr = string.format('{"hash": 0x%08X, "str": %s}', v.hash, py_str(v.str))

        elseif t == "dict_key_hash" then
            vstr = tostring(v)

        else
            vstr = tostring(v)
        end

        return string.format(
            '%s{"type": %s, "value": %s, "order": %d}',
            ind, py_str(t), vstr, p.order or 0
        )
    end

    -- Recursive node serializer
    local function ser_node(node, indent)
        local ind  = string.rep("    ", indent)
        local ind2 = string.rep("    ", indent + 1)
        local parts = {}

        table.insert(parts, ind .. "{")
        table.insert(parts, ind2 .. string.format('"func_name": %s,', py_str(node.func_name)))
        table.insert(parts, ind2 .. string.format('"func_hash": 0x%08X,', node.func_hash))
        table.insert(parts, ind2 .. string.format('"call_type": %s,', py_str(node.call_type)))
        table.insert(parts, ind2 .. string.format('"order": %d,', node.order or 0))
        table.insert(parts, ind2 .. string.format('"param_count": %d,', node.param_count or 0))
        table.insert(parts, ind2 .. string.format('"pointer_index": %s,',
            py_none_or(node.pointer_index)))

        -- params
        if #node.params > 0 then
            table.insert(parts, ind2 .. '"params": [')
            for i, p in ipairs(node.params) do
                local comma = (i < #node.params) and "," or ""
                table.insert(parts, ser_param(p, indent + 2) .. comma)
            end
            table.insert(parts, ind2 .. "],")
        else
            table.insert(parts, ind2 .. '"params": [],')
        end

        -- children (recursive)
        if #node.children > 0 then
            table.insert(parts, ind2 .. '"children": [')
            for i, child in ipairs(node.children) do
                local child_lines = ser_node(child, indent + 2)
                for j, cl in ipairs(child_lines) do
                    if j == #child_lines and i < #node.children then
                        table.insert(parts, cl .. ",")
                    else
                        table.insert(parts, cl)
                    end
                end
            end
            table.insert(parts, ind2 .. "],")
        else
            table.insert(parts, ind2 .. '"children": [],')
        end

        table.insert(parts, ind .. "}")
        return parts
    end

    -- Header
    table.insert(lines, '"""')
    table.insert(lines, mod.name .. "_module.py")
    table.insert(lines, "Pre-serialized module_data for Python runtime")
    table.insert(lines, "DO NOT EDIT - Generated by s_compile.lua v5.3")
    table.insert(lines, '"""')
    table.insert(lines, "")

    -- Scalar metadata
    table.insert(lines, string.format('name = %s', py_str(mod.name)))
    table.insert(lines, string.format('name_hash = 0x%08X', mod.name_hash))
    table.insert(lines, string.format('pointer_size = %d', mod.pointer_size))
    table.insert(lines, string.format('debug = %s', py_bool(mod.debug or false)))
    table.insert(lines, "")

    -- Function tables
    table.insert(lines, "oneshot_funcs = " .. py_string_list(mod.oneshot_funcs))
    table.insert(lines, "main_funcs = " .. py_string_list(mod.main_funcs))
    table.insert(lines, "pred_funcs = " .. py_string_list(mod.pred_funcs))
    table.insert(lines, "")

    -- String table
    table.insert(lines, "string_table = " .. py_string_list(mod.string_table))
    table.insert(lines, "string_index = {")
    local skeys = {}
    for s, _ in pairs(mod.string_index) do table.insert(skeys, s) end
    table.sort(skeys)
    for _, s in ipairs(skeys) do
        table.insert(lines, string.format('    %s: %d,', py_str(s), mod.string_index[s]))
    end
    table.insert(lines, "}")
    table.insert(lines, "")

    -- Trees
    table.insert(lines, "tree_order = " .. py_string_list(mod.tree_order))
    table.insert(lines, "trees = {}")
    table.insert(lines, "")

    for _, tname in ipairs(mod.tree_order) do
        local tree = mod.trees[tname]
        table.insert(lines, string.format('trees[%s] = {', py_str(tname)))
        table.insert(lines, string.format('    "name": %s,', py_str(tree.name)))
        table.insert(lines, string.format('    "name_hash": 0x%08X,', tree.name_hash))
        table.insert(lines, string.format('    "node_count": %d,', tree.node_count))
        table.insert(lines, string.format('    "pointer_count": %d,', tree.pointer_count or 0))
        table.insert(lines, string.format('    "record_name": %s,',
            tree.record_name and py_str(tree.record_name) or "None"))
        table.insert(lines, string.format('    "defaults_index": 0x%04X,', tree.defaults_index or 0xFFFF))
        table.insert(lines, '    "nodes": [')
        for i, node in ipairs(tree.nodes) do
            local nlines = ser_node(node, 2)
            for j, nl in ipairs(nlines) do
                if j == #nlines and i < #tree.nodes then
                    table.insert(lines, nl .. ",")
                else
                    table.insert(lines, nl)
                end
            end
        end
        table.insert(lines, "    ],")
        table.insert(lines, "}")
        table.insert(lines, "")
    end

    -- Records
    table.insert(lines, "record_order = " .. py_string_list(mod.record_order))
    table.insert(lines, "records = {}")
    if #mod.record_order > 0 then
        for _, rname in ipairs(mod.record_order) do
            local rec = mod.records[rname]
            table.insert(lines, string.format('records[%s] = {', py_str(rname)))
            table.insert(lines, string.format('    "name": %s,', py_str(rec.name)))
            table.insert(lines, string.format('    "name_hash": 0x%08X,', rec.name_hash))
            table.insert(lines, string.format('    "size": %d,', rec.size))
            table.insert(lines, string.format('    "align": %d,', rec.align))
            table.insert(lines, '    "fields": [')
            for i, f in ipairs(rec.fields) do
                local comma = (i < #rec.fields) and "," or ""
                table.insert(lines, string.format(
                    '        {"name": %s, "name_hash": 0x%08X, "type": %s, '..
                    '"offset": %d, "size": %d, '..
                    '"is_pointer": %s, "is_char_array": %s, "is_embedded": %s}%s',
                    py_str(f.name), f.name_hash, py_str(f.type or ""),
                    f.offset, f.size,
                    py_bool(f.is_pointer or false),
                    py_bool(f.is_char_array or false),
                    py_bool(f.is_embedded or false),
                    comma
                ))
            end
            table.insert(lines, "    ],")
            table.insert(lines, "}")
        end
        table.insert(lines, "")
    end

    -- Constants
    table.insert(lines, "const_order = " .. py_string_list(mod.const_order))
    table.insert(lines, "constants = {}")
    table.insert(lines, "")

    -- Events
    table.insert(lines, "events = []")
    table.insert(lines, "event_names = {}")
    if mod.events and #mod.events > 0 then
        for _, evt in ipairs(mod.events) do
            table.insert(lines, string.format(
                'events.append({"name": %s, "id": 0x%04X})', py_str(evt.name), evt.id))
            table.insert(lines, string.format(
                'event_names[%s] = 0x%04X', py_str(evt.name), evt.id))
        end
        table.insert(lines, "")
    end

    return table.concat(lines, "\n")
end

-- ============================================================================
-- MAIN
-- ============================================================================

local function main()
    local opts = parse_args(arg)

    if not opts then
        print_usage()
        os.exit(0)
    end

    if not opts.input then
        print_usage()
        os.exit(1)
    end

    -- Determine script directory
    local script_path = arg[0]
    local script_dir = script_path:match("(.*/)")
    if not script_dir then
        script_dir = "./"
    end

    local search_paths = { script_dir, ".", "./lua", "./scripts" }

    -- Set pointer size before loading DSL
    _G._pointer_size = opts.pointer_size

    -- Load the DSL library
    local dsl_file = find_file("s_expr_dsl.lua", search_paths)
    if not dsl_file then
        io.stderr:write("Error: Cannot find s_expr_dsl.lua\n")
        io.stderr:write("Searched in: " .. table.concat(search_paths, ", ") .. "\n")
        os.exit(1)
    end

    local dsl = dofile(dsl_file)
    if not dsl or not dsl.ModuleGenerator then
        io.stderr:write("Error: Failed to load DSL library\n")
        os.exit(1)
    end

    -- Load explicitly specified helper files
    for _, helper in ipairs(opts.helpers) do
        local helper_path = find_file(helper, search_paths)
        if not helper_path then
            io.stderr:write("Warning: Cannot find helper file: " .. helper .. "\n")
        else
            local ok, err = pcall(dofile, helper_path)
            if not ok then
                io.stderr:write("Error loading helper " .. helper .. ": " .. tostring(err) .. "\n")
                os.exit(1)
            end
        end
    end

    -- Auto-detect helpers based on input file location
    local input_dir = opts.input:match("(.*/)")
    if input_dir then
        table.insert(search_paths, 1, input_dir)
    end

    -- Try to load s_engine_helpers.lua if it exists and was not already loaded
    local engine_helpers = find_file("s_engine_helpers.lua", search_paths)
    if engine_helpers then
        local ok, err = pcall(dofile, engine_helpers)
        if not ok then
            io.stderr:write("Warning: Error loading s_engine_helpers.lua: " .. tostring(err) .. "\n")
        end
    end

    -- Check input file
    if not file_exists(opts.input) then
        io.stderr:write("Error: Cannot open input file: " .. opts.input .. "\n")
        os.exit(1)
    end

    -- Run the input DSL file
    local ok, result = pcall(dofile, opts.input)
    if not ok then
        io.stderr:write("Error executing DSL file: " .. tostring(result) .. "\n")
        os.exit(1)
    end

    local module_data = result

    if not module_data then
        io.stderr:write("Error: DSL file did not return module data\n")
        io.stderr:write("Make sure your DSL file ends with:\n")
        io.stderr:write("  return end_module(mod)\n")
        os.exit(1)
    end

    if type(module_data) ~= "table" or not module_data.name then
        io.stderr:write("Error: Invalid module data returned\n")
        os.exit(1)
    end

    -- Create generators
    local gen = dsl.ModuleGenerator.new(module_data)
    local is_64bit = (opts.pointer_size == 8)
    local bin_gen = dsl.BinaryModuleGenerator.new(module_data, is_64bit)

    -- Determine base name
    local base_name = module_data.name:lower():gsub("[^%w_]", "_")

    -- Mode suffix for binary files
    local mode_suffix = is_64bit and "_64" or "_32"

    -- Handle --all flags
    if opts.all or opts.all_bin then
        if not opts.header      then opts.header      = base_name .. ".h" end
        if not opts.user_header then opts.user_header = base_name .. "_user_functions.h" end
        if not opts.registration then opts.registration = base_name .. "_user_registration.c" end
        if not opts.debug_header then opts.debug_header = base_name .. "_debug.h" end
        if not opts.records_header and #module_data.record_order > 0 then
            opts.records_header = base_name .. "_records.h"
        end
    end

    if opts.all_bin then
        if not opts.binary_file   then opts.binary_file   = base_name .. mode_suffix .. ".bin" end
        if not opts.binary_header then opts.binary_header = base_name .. "_bin" .. mode_suffix .. ".h" end
        if not opts.dump_header   then opts.dump_header   = base_name .. "_dump" .. mode_suffix .. ".h" end
        if not opts.lua_module    then opts.lua_module    = true end
        if not opts.python_module then opts.python_module = true end
    end

    -- Resolve --lua auto-name
    if opts.lua_module == true then
        opts.lua_module = base_name .. "_module.lua"
    end

    -- Resolve --python auto-name
    if opts.python_module == true then
        opts.python_module = base_name .. "_module.py"
    end

    -- ========================================================================
    -- GENERATE OUTPUTS
    -- ========================================================================

    if opts.records_header and #module_data.record_order > 0 then
        local content = gen:to_c_records_header(base_name)
        write_file(make_path(opts.outdir, opts.records_header), content)
    end

    if opts.header then
        local content = gen:to_c_header(base_name)
        write_file(make_path(opts.outdir, opts.header), content)
    end

    if opts.debug_header then
        local content = gen:to_c_debug_header(base_name)
        write_file(make_path(opts.outdir, opts.debug_header), content)
    end

    if opts.user_header then
        local content = gen:to_c_user_header(base_name)
        write_file(make_path(opts.outdir, opts.user_header), content)
    end

    if opts.registration then
        local content = gen:to_c_user_registration(base_name)
        write_file(make_path(opts.outdir, opts.registration), content)
    end

    if opts.binary_file then
        local bytes, size = bin_gen:generate()
        write_binary(make_path(opts.outdir, opts.binary_file), bytes)
    end

    if opts.binary_header then
        local content = bin_gen:to_c_header(base_name)
        write_file(make_path(opts.outdir, opts.binary_header), content)
    end

    if opts.dump_header then
        local content = bin_gen:to_debug_dump(base_name)
        write_file(make_path(opts.outdir, opts.dump_header), content)
    end

    if opts.lua_module then
        local content = serialize_module_lua(module_data)
        write_file(make_path(opts.outdir, opts.lua_module), content)
    end

    if opts.python_module then
        local content = serialize_module_python(module_data)
        write_file(make_path(opts.outdir, opts.python_module), content)
    end

    if opts.dump then
        print(gen:dump())
    end

    -- Default output if nothing specified
    if not opts.header and not opts.user_header and not opts.registration and
       not opts.records_header and not opts.binary_file and not opts.binary_header and
       not opts.debug_header and not opts.dump_header and not opts.lua_module and
       not opts.python_module and not opts.dump then
        if #module_data.record_order > 0 then
            local content = gen:to_c_records_header(base_name)
            write_file(make_path(opts.outdir, base_name .. "_records.h"), content)
        end
        local content = gen:to_c_header(base_name)
        write_file(make_path(opts.outdir, base_name .. ".h"), content)
    end

    -- Print summary
    print("")
    print("Module: " .. module_data.name)
    print("  Trees:             " .. #module_data.tree_order)
    print("  Records:           " .. #module_data.record_order)
    print("  Constants:         " .. #module_data.const_order)
    print("  Strings:           " .. #module_data.string_table)
    print("  Oneshot functions: " .. #module_data.oneshot_funcs)
    print("  Main functions:    " .. #module_data.main_funcs)
    print("  Pred functions:    " .. #module_data.pred_funcs)
    print("  Mode:              " .. (is_64bit and "64-bit" or "32-bit"))
    print("  Binary format:     v5.3 (direct s_expr_param_t, zero-copy)")
end

-- Run
local ok, err = pcall(main)
if not ok then
    io.stderr:write("Error: " .. tostring(err) .. "\n")
    os.exit(1)
end