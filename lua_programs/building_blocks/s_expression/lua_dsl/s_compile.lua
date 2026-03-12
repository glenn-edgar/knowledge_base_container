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
        micropython_module = nil,
        zig_module = nil,
        lua53_module = nil,
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
        elseif arg:match("^%-%-micropython=") then
            opts.micropython_module = arg:match("^%-%-micropython=(.+)$")
        elseif arg == "--micropython" then
            opts.micropython_module = true
        elseif arg:match("^%-%-zig=") then
            opts.zig_module = arg:match("^%-%-zig=(.+)$")
        elseif arg == "--zig" then
            opts.zig_module = true
        elseif arg:match("^%-%-lua53=") then
            opts.lua53_module = arg:match("^%-%-lua53=(.+)$")
        elseif arg == "--lua53" then
            opts.lua53_module = true
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
  --micropython[=<file>]  Generate MicroPython module file (RAM-optimized)
                       (default filename: <base>_module_mpy.py)
  --zig[=<file>]       Generate Zig module file (compiled, no GC)
                       (default filename: <base>_module.zig)
  --lua53[=<file>]     Generate Lua 5.3+ module file (native int64, no FFI)
                       (default filename: <base>_module_53.lua)
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
  <base>_module_mpy.py                 - Pre-serialized module_data for MicroPython (RAM-optimized)
  <base>_module.zig                    - Zig module (flattened param stream, comptime dispatch)
  <base>_module_53.lua                 - Lua 5.3+ module (native int64, string.pack)

Examples:
  luajit s_compile.lua my_module.lua --all-bin --outdir=generated/
  luajit s_compile.lua my_module.lua --all-bin --64bit
  luajit s_compile.lua my_module.lua --lua --outdir=generated/
  luajit s_compile.lua my_module.lua --lua53 --outdir=generated/
  luajit s_compile.lua my_module.lua --python --outdir=generated/
  luajit s_compile.lua my_module.lua --micropython --outdir=generated/
  luajit s_compile.lua my_module.lua --zig --outdir=generated/
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
-- LUA 5.3+ MODULE SERIALIZER
-- ============================================================================
-- Differences from the LuaJIT version:
--   - Lua 5.3 distinguishes integer and float types at the language level.
--     All hash/offset/index values are emitted as integers (no ".0").
--     Float params are emitted with explicit decimal point (e.g. "1.0").
--   - Includes an inline FNV-1a using native bitwise ops (& | ~ << >>)
--     so the runtime doesn't need the ffi/bit library.
--   - No dependency on LuaJIT ffi or bit.* library.
--   - Adds a version guard: assert(_VERSION >= "Lua 5.3")
-- ============================================================================

local function serialize_module_lua53(module_data)
    local lines = {}
    local mod = module_data

    local function ser_string_list(arr)
        local parts = {}
        for _, v in ipairs(arr) do
            table.insert(parts, string.format('%q', v))
        end
        return "{ " .. table.concat(parts, ", ") .. " }"
    end

    -- Format a numeric value preserving integer/float distinction for Lua 5.3
    local function ser_number(v, force_float)
        if force_float then
            -- Ensure decimal point for 5.3 float type
            local s = string.format("%.17g", v)
            if not s:find("[%.eE]") then
                s = s .. ".0"
            end
            return s
        else
            -- Integer — use %d for small, hex for hashes
            if v == math.floor(v) then
                return string.format("%d", v)
            else
                local s = string.format("%.17g", v)
                if not s:find("[%.eE]") then
                    s = s .. ".0"
                end
                return s
            end
        end
    end

    local function ser_param(p)
        local t = p.type
        local v = p.value
        local vstr

        if t == "float" then
            -- Preserve float type in 5.3
            vstr = ser_number(v, true)

        elseif t == "int" or t == "uint" or t == "result"
               or t == "stack_tos" or t == "stack_local"
               or t == "stack_push" or t == "stack_pop"
               or t == "null_param" then
            vstr = ser_number(v, false)

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
            vstr = ser_number(v, false)

        else
            vstr = tostring(v)
        end

        return string.format(
            '{type=%q,value=%s,order=%d}',
            t, vstr, p.order or 0
        )
    end

    -- Recursive node serializer
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
                parts[#parts] = parts[#parts] .. ","
            end
            table.insert(parts, ind2 .. "},")
        else
            table.insert(parts, ind2 .. "children={},")
        end

        table.insert(parts, ind .. "}")
        return parts
    end

    -- ====================================================================
    -- File header
    -- ====================================================================
    table.insert(lines, "-- ============================================================================")
    table.insert(lines, "-- " .. mod.name .. "_module_53.lua")
    table.insert(lines, "-- Pre-serialized module_data for Lua 5.3+ runtime")
    table.insert(lines, "-- DO NOT EDIT - Generated by s_compile.lua v5.3")
    table.insert(lines, "--")
    table.insert(lines, "-- Lua 5.3 features used by this file and its runtime:")
    table.insert(lines, "--   - Native 64-bit integers (hash values, offsets)")
    table.insert(lines, "--   - Integer/float type distinction (1 ~= 1.0)")
    table.insert(lines, "--   - Native bitwise operators (& | ~ << >>)")
    table.insert(lines, "--   - string.pack / string.unpack for binary I/O")
    table.insert(lines, "-- ============================================================================")
    table.insert(lines, "")
    table.insert(lines, 'assert(_VERSION >= "Lua 5.3", "This module requires Lua 5.3 or later")')
    table.insert(lines, "")
    table.insert(lines, "local M = {}")
    table.insert(lines, "")

    -- ====================================================================
    -- Inline FNV-1a using Lua 5.3 native bitwise ops
    -- ====================================================================
    table.insert(lines, "-- FNV-1a 32-bit hash (Lua 5.3 native bitwise ops)")
    table.insert(lines, "function M.fnv1a_32(str)")
    table.insert(lines, "    local hash = 0x811c9dc5")
    table.insert(lines, "    for i = 1, #str do")
    table.insert(lines, "        hash = hash ~ str:byte(i)")
    table.insert(lines, "        hash = (hash * 0x01000193) & 0xFFFFFFFF")
    table.insert(lines, "    end")
    table.insert(lines, "    return hash")
    table.insert(lines, "end")
    table.insert(lines, "")

    -- ====================================================================
    -- Scalar metadata
    -- ====================================================================
    table.insert(lines, string.format('M.name         = %q', mod.name))
    table.insert(lines, string.format('M.name_hash    = 0x%08X', mod.name_hash))
    table.insert(lines, string.format('M.pointer_size = %d', mod.pointer_size))
    table.insert(lines, string.format('M.debug        = %s', tostring(mod.debug or false)))
    table.insert(lines, "")

    -- Function tables
    table.insert(lines, "M.oneshot_funcs = " .. ser_string_list(mod.oneshot_funcs))
    table.insert(lines, "M.main_funcs    = " .. ser_string_list(mod.main_funcs))
    table.insert(lines, "M.pred_funcs    = " .. ser_string_list(mod.pred_funcs))
    table.insert(lines, "")

    -- String table
    table.insert(lines, "M.string_table = " .. ser_string_list(mod.string_table))
    table.insert(lines, "M.string_index = {")
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

    -- Records
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
-- MICROPYTHON MODULE SERIALIZER
-- ============================================================================
-- MicroPython optimizations vs CPython:
--   - Tuples instead of lists for all static data (frozen into ROM/flash)
--   - const() for module-level integer constants (inlined by MicroPython compiler)
--   - Positional tuples with named index constants (avoids dict overhead per entry)
--   - No docstrings (saves RAM)
--   - Param = (type, value, order) tuple
--   - Node  = (func_name, func_hash, call_type, order, param_count,
--              pointer_index, params_tuple, children_tuple)
--   - Field = (name, name_hash, type, offset, size, is_pointer,
--              is_char_array, is_embedded)
-- ============================================================================

local function serialize_module_micropython(module_data)
    local lines = {}
    local mod = module_data

    local function mpy_str(s)
        local escaped = s:gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"):gsub("\r", "\\r")
        return '"' .. escaped .. '"'
    end

    -- Tuple of strings: ("a", "b", "c")
    local function mpy_string_tuple(arr)
        if #arr == 0 then return "()" end
        local parts = {}
        for _, v in ipairs(arr) do
            table.insert(parts, mpy_str(v))
        end
        -- Single-element tuple needs trailing comma
        if #parts == 1 then
            return "(" .. parts[1] .. ",)"
        end
        return "(" .. table.concat(parts, ", ") .. ")"
    end

    local function mpy_bool(v)
        if v then return "True" else return "False" end
    end

    -- Serialize a param as a positional tuple: (type, value, order)
    local function ser_param_tuple(p, indent)
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
            vstr = mpy_str(tostring(v))

        elseif t == "str_hash" then
            -- (hash, str) tuple instead of dict
            vstr = string.format('(0x%08X, %s)', v.hash, mpy_str(v.str))

        elseif t == "dict_key_hash" then
            vstr = tostring(v)

        else
            vstr = tostring(v)
        end

        return string.format('%s(%s, %s, %d)', ind, mpy_str(t), vstr, p.order or 0)
    end

    -- Recursive node serializer returning lines
    -- Node tuple: (func_name, func_hash, call_type, order, param_count,
    --              pointer_index, params, children)
    local function ser_node_tuple(node, indent)
        local ind  = string.rep("    ", indent)
        local ind2 = string.rep("    ", indent + 1)
        local parts = {}

        table.insert(parts, ind .. "(")
        table.insert(parts, ind2 .. mpy_str(node.func_name) .. ",  # N_FUNC_NAME")
        table.insert(parts, ind2 .. string.format("0x%08X,  # N_FUNC_HASH", node.func_hash))
        table.insert(parts, ind2 .. mpy_str(node.call_type) .. ",  # N_CALL_TYPE")
        table.insert(parts, ind2 .. string.format("%d,  # N_ORDER", node.order or 0))
        table.insert(parts, ind2 .. string.format("%d,  # N_PARAM_COUNT", node.param_count or 0))

        if node.pointer_index ~= nil then
            table.insert(parts, ind2 .. string.format("%d,  # N_POINTER_INDEX", node.pointer_index))
        else
            table.insert(parts, ind2 .. "None,  # N_POINTER_INDEX")
        end

        -- params as tuple of tuples
        if #node.params > 0 then
            table.insert(parts, ind2 .. "(  # N_PARAMS")
            for i, p in ipairs(node.params) do
                table.insert(parts, ser_param_tuple(p, indent + 2) .. ",")
            end
            table.insert(parts, ind2 .. "),")
        else
            table.insert(parts, ind2 .. "(),  # N_PARAMS")
        end

        -- children (recursive) as tuple of tuples
        if #node.children > 0 then
            table.insert(parts, ind2 .. "(  # N_CHILDREN")
            for i, child in ipairs(node.children) do
                local child_lines = ser_node_tuple(child, indent + 2)
                for j, cl in ipairs(child_lines) do
                    table.insert(parts, cl)
                end
                parts[#parts] = parts[#parts] .. ","
            end
            table.insert(parts, ind2 .. "),")
        else
            table.insert(parts, ind2 .. "(),  # N_CHILDREN")
        end

        table.insert(parts, ind .. ")")
        return parts
    end

    -- ====================================================================
    -- Header
    -- ====================================================================
    table.insert(lines, "# " .. string.rep("=", 74))
    table.insert(lines, "# " .. mod.name .. "_module_mpy.py")
    table.insert(lines, "# Pre-serialized module_data for MicroPython runtime (RAM-optimized)")
    table.insert(lines, "# DO NOT EDIT - Generated by s_compile.lua v5.3")
    table.insert(lines, "#")
    table.insert(lines, "# All static data uses tuples (frozen into ROM/flash).")
    table.insert(lines, "# Access positional fields via the index constants below.")
    table.insert(lines, "# " .. string.rep("=", 74))
    table.insert(lines, "")
    table.insert(lines, "from micropython import const")
    table.insert(lines, "")

    -- ====================================================================
    -- Index constants for tuple access  (const() = inlined by compiler)
    -- ====================================================================
    table.insert(lines, "# --- Node tuple indices ---")
    table.insert(lines, "N_FUNC_NAME     = const(0)")
    table.insert(lines, "N_FUNC_HASH     = const(1)")
    table.insert(lines, "N_CALL_TYPE     = const(2)")
    table.insert(lines, "N_ORDER         = const(3)")
    table.insert(lines, "N_PARAM_COUNT   = const(4)")
    table.insert(lines, "N_POINTER_INDEX = const(5)")
    table.insert(lines, "N_PARAMS        = const(6)")
    table.insert(lines, "N_CHILDREN      = const(7)")
    table.insert(lines, "")
    table.insert(lines, "# --- Param tuple indices ---")
    table.insert(lines, "P_TYPE  = const(0)")
    table.insert(lines, "P_VALUE = const(1)")
    table.insert(lines, "P_ORDER = const(2)")
    table.insert(lines, "")
    table.insert(lines, "# --- Field tuple indices ---")
    table.insert(lines, "F_NAME          = const(0)")
    table.insert(lines, "F_NAME_HASH     = const(1)")
    table.insert(lines, "F_TYPE          = const(2)")
    table.insert(lines, "F_OFFSET        = const(3)")
    table.insert(lines, "F_SIZE          = const(4)")
    table.insert(lines, "F_IS_POINTER    = const(5)")
    table.insert(lines, "F_IS_CHAR_ARRAY = const(6)")
    table.insert(lines, "F_IS_EMBEDDED   = const(7)")
    table.insert(lines, "")

    -- ====================================================================
    -- Scalar metadata
    -- ====================================================================
    table.insert(lines, "# --- Module metadata ---")
    table.insert(lines, string.format("name = %s", mpy_str(mod.name)))
    table.insert(lines, string.format("name_hash = const(0x%08X)", mod.name_hash))
    table.insert(lines, string.format("pointer_size = const(%d)", mod.pointer_size))
    table.insert(lines, string.format("debug = %s", mpy_bool(mod.debug or false)))
    table.insert(lines, "")

    -- ====================================================================
    -- Function tables (tuples — frozen into ROM)
    -- ====================================================================
    table.insert(lines, "# --- Function tables ---")
    table.insert(lines, "oneshot_funcs = " .. mpy_string_tuple(mod.oneshot_funcs))
    table.insert(lines, "main_funcs = " .. mpy_string_tuple(mod.main_funcs))
    table.insert(lines, "pred_funcs = " .. mpy_string_tuple(mod.pred_funcs))
    table.insert(lines, "")

    -- ====================================================================
    -- String table
    -- ====================================================================
    table.insert(lines, "# --- String table ---")
    table.insert(lines, "string_table = " .. mpy_string_tuple(mod.string_table))
    table.insert(lines, "string_index = {")
    local skeys = {}
    for s, _ in pairs(mod.string_index) do table.insert(skeys, s) end
    table.sort(skeys)
    for _, s in ipairs(skeys) do
        table.insert(lines, string.format("    %s: %d,", mpy_str(s), mod.string_index[s]))
    end
    table.insert(lines, "}")
    table.insert(lines, "")

    -- ====================================================================
    -- Trees
    -- ====================================================================
    table.insert(lines, "# --- Trees ---")
    table.insert(lines, "tree_order = " .. mpy_string_tuple(mod.tree_order))
    table.insert(lines, "trees = {}")
    table.insert(lines, "")

    for _, tname in ipairs(mod.tree_order) do
        local tree = mod.trees[tname]
        table.insert(lines, string.format("trees[%s] = {", mpy_str(tname)))
        table.insert(lines, string.format('    "name": %s,', mpy_str(tree.name)))
        table.insert(lines, string.format('    "name_hash": 0x%08X,', tree.name_hash))
        table.insert(lines, string.format('    "node_count": %d,', tree.node_count))
        table.insert(lines, string.format('    "pointer_count": %d,', tree.pointer_count or 0))
        table.insert(lines, string.format('    "record_name": %s,',
            tree.record_name and mpy_str(tree.record_name) or "None"))
        table.insert(lines, string.format('    "defaults_index": 0x%04X,', tree.defaults_index or 0xFFFF))

        -- nodes as tuple of node-tuples
        table.insert(lines, '    "nodes": (')
        for i, node in ipairs(tree.nodes) do
            local nlines = ser_node_tuple(node, 2)
            for j, nl in ipairs(nlines) do
                table.insert(lines, nl)
            end
            lines[#lines] = lines[#lines] .. ","
        end
        table.insert(lines, "    ),")
        table.insert(lines, "}")
        table.insert(lines, "")
    end

    -- ====================================================================
    -- Records
    -- ====================================================================
    table.insert(lines, "# --- Records ---")
    table.insert(lines, "record_order = " .. mpy_string_tuple(mod.record_order))
    table.insert(lines, "records = {}")
    if #mod.record_order > 0 then
        for _, rname in ipairs(mod.record_order) do
            local rec = mod.records[rname]
            table.insert(lines, string.format("records[%s] = {", mpy_str(rname)))
            table.insert(lines, string.format('    "name": %s,', mpy_str(rec.name)))
            table.insert(lines, string.format('    "name_hash": 0x%08X,', rec.name_hash))
            table.insert(lines, string.format('    "size": %d,', rec.size))
            table.insert(lines, string.format('    "align": %d,', rec.align))
            -- Fields as tuple of tuples: (name, name_hash, type, offset, size, is_ptr, is_char, is_embed)
            table.insert(lines, '    "fields": (')
            for i, f in ipairs(rec.fields) do
                local comma = ","
                table.insert(lines, string.format(
                    '        (%s, 0x%08X, %s, %d, %d, %s, %s, %s)%s',
                    mpy_str(f.name), f.name_hash, mpy_str(f.type or ""),
                    f.offset, f.size,
                    mpy_bool(f.is_pointer or false),
                    mpy_bool(f.is_char_array or false),
                    mpy_bool(f.is_embedded or false),
                    comma
                ))
            end
            table.insert(lines, "    ),")
            table.insert(lines, "}")
        end
        table.insert(lines, "")
    end

    -- ====================================================================
    -- Constants
    -- ====================================================================
    table.insert(lines, "# --- Constants ---")
    table.insert(lines, "const_order = " .. mpy_string_tuple(mod.const_order))
    table.insert(lines, "constants = {}")
    table.insert(lines, "")

    -- ====================================================================
    -- Events
    -- ====================================================================
    table.insert(lines, "# --- Events ---")
    if mod.events and #mod.events > 0 then
        -- Build events as a single tuple of (name, id) pairs
        table.insert(lines, "events = (")
        for _, evt in ipairs(mod.events) do
            table.insert(lines, string.format(
                '    (%s, 0x%04X),', mpy_str(evt.name), evt.id))
        end
        table.insert(lines, ")")
        table.insert(lines, "event_names = {")
        for _, evt in ipairs(mod.events) do
            table.insert(lines, string.format(
                '    %s: 0x%04X,', mpy_str(evt.name), evt.id))
        end
        table.insert(lines, "}")
    else
        table.insert(lines, "events = ()")
        table.insert(lines, "event_names = {}")
    end

    return table.concat(lines, "\n")
end

-- ============================================================================
-- ZIG MODULE SERIALIZER
-- ============================================================================
-- Emits a self-contained .zig file with:
--   - Type definitions (Param tagged union, Result enum, FuncKind, Event)
--   - Record types as extern struct with comptime offset assertions
--   - String table as const slice
--   - Flattened param stream per tree (same layout as binary emitter)
--   - Tree/module descriptors
--   - Comptime dispatch (three switches: main, oneshot, pred)
--
-- The flattened param stream matches the binary emitter's traversal order:
--   OPEN_CALL, func_ref, [params | children]..., CLOSE
-- with brace offsets for list/dict/array/tuple pairs and parent offsets
-- for O(1) upward navigation.
-- ============================================================================

local function serialize_module_zig(module_data, builtin_set)
    local lines = {}
    local mod = module_data
    builtin_set = builtin_set or {}

    -- ====================================================================
    -- Zig identifier helpers
    -- ====================================================================

    local function zig_str(s)
        local escaped = s:gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"):gsub("\r", "\\r")
        return '"' .. escaped .. '"'
    end

    local function zig_ident(name)
        -- Zig reserved words get @"..." quoting
        local reserved = {
            ["return"]=1, ["const"]=1, ["var"]=1, ["if"]=1, ["else"]=1,
            ["while"]=1, ["for"]=1, ["break"]=1, ["continue"]=1,
            ["fn"]=1, ["struct"]=1, ["enum"]=1, ["union"]=1,
            ["error"]=1, ["switch"]=1, ["pub"]=1, ["type"]=1,
            ["test"]=1, ["try"]=1, ["catch"]=1, ["null"]=1,
            ["undefined"]=1, ["true"]=1, ["false"]=1, ["void"]=1,
        }
        if reserved[name] then
            return '@"' .. name .. '"'
        end
        return name
    end

    local function zig_type_for_field(field)
        if field.is_char_array then
            return string.format("[%d]u8", field.array_len or field.size)
        elseif field.is_int32_array then
            return string.format("[%d]i32", field.array_len or (field.size / 4))
        elseif field.is_float32_array then
            return string.format("[%d]f32", field.array_len or (field.size / 4))
        elseif field.is_ptr64 or (field.is_pointer and field.type == "ptr64") then
            return "u64"
        elseif field.is_embedded then
            return field.embedded_record
        else
            local map = {
                int32  = "i32",  uint32 = "u32",
                int64  = "i64",  uint64 = "u64",
                float  = "f32",  double = "f64",
                int8   = "i8",   uint8  = "u8",
                int16  = "i16",  uint16 = "u16",
                bool   = "bool",
            }
            return map[field.type] or "u32"
        end
    end

    -- ====================================================================
    -- Lookup tables
    -- ====================================================================

    local fnv = _G.fnv1a_32

    local oneshot_hash_idx = {}
    for i, name in ipairs(mod.oneshot_funcs) do
        oneshot_hash_idx[fnv(name)] = i - 1
    end
    local main_hash_idx = {}
    for i, name in ipairs(mod.main_funcs) do
        main_hash_idx[fnv(name)] = i - 1
    end
    local pred_hash_idx = {}
    for i, name in ipairs(mod.pred_funcs) do
        pred_hash_idx[fnv(name)] = i - 1
    end
    local string_idx = {}
    for i, s in ipairs(mod.string_table) do
        string_idx[s] = i - 1
    end
    local const_idx = {}
    for i, name in ipairs(mod.const_order) do
        const_idx[name] = i - 1
    end

    -- ====================================================================
    -- Tree flattener — produces linear param descriptors
    -- ====================================================================

    local function count_node_params(node)
        local count = 2  -- OPEN_CALL + func_ref
        for _ in ipairs(node.params) do count = count + 1 end
        for _, child in ipairs(node.children) do
            count = count + count_node_params(child) + 1  -- +1 for child's CLOSE
        end
        return count
    end

    local function flatten_tree(tree)
        local flat = {}
        local node_idx = 0
        local parent_call_stack = {}
        local brace_stack = {}

        local function parent_offset()
            if #parent_call_stack == 0 then return 0 end
            return #flat - parent_call_stack[#parent_call_stack]
        end

        local function resolve_field(field_name)
            if not tree.record_name then return nil, nil end
            local rec = mod.records[tree.record_name]
            if not rec then return nil, nil end
            for _, f in ipairs(rec.fields) do
                if f.name == field_name then return f.offset, f.size end
            end
            return nil, nil
        end

        local function resolve_nested_field(path)
            if not tree.record_name then return nil, nil end
            local rec = mod.records[tree.record_name]
            if not rec then return nil, nil end
            local parts = {}
            for part in path:gmatch("[^%.]+") do table.insert(parts, part) end
            local offset = 0
            local current_rec = rec
            local field = nil
            for i, part in ipairs(parts) do
                field = nil
                for _, f in ipairs(current_rec.fields) do
                    if f.name == part then field = f; break end
                end
                if not field then return nil, nil end
                offset = offset + field.offset
                if i < #parts and field.is_embedded then
                    current_rec = mod.records[field.embedded_record]
                    if not current_rec then return nil, nil end
                end
            end
            return offset, field and field.size or 0
        end

        -- Emit one DSL param into flat[]
        local function emit_param(param)
            local t = param.type
            local v = param.value
            local po = parent_offset()

            if t == "int" then
                table.insert(flat, {tag="int", value=v})
            elseif t == "uint" then
                table.insert(flat, {tag="uint", value=v})
            elseif t == "float" then
                table.insert(flat, {tag="float", value=v})
            elseif t == "str_hash" then
                table.insert(flat, {tag="str_hash", value=v.hash, comment=v.str})
            elseif t == "str_idx" or t == "str_ptr" then
                table.insert(flat, {tag="str_idx", index=string_idx[v] or 0, len=#v})
            elseif t == "field_ref" then
                local off, sz = resolve_field(v)
                if off then
                    table.insert(flat, {tag="field_ref", offset=off, size=sz, comment=v})
                else
                    table.insert(flat, {tag="field_ref", offset=0, size=0,
                        comment=v .. " (hash fallback)"})
                end
            elseif t == "nested_field_ref" then
                local off, sz = resolve_nested_field(v)
                if off then
                    table.insert(flat, {tag="field_ref", offset=off, size=sz, comment=v})
                else
                    table.insert(flat, {tag="field_ref", offset=0, size=0,
                        comment=v .. " (hash fallback)"})
                end
            elseif t == "const_ref" then
                table.insert(flat, {tag="const_ref", index=const_idx[v] or 0, comment=v})
            elseif t == "result" then
                table.insert(flat, {tag="result", value=v})
            elseif t == "stack_tos" then
                table.insert(flat, {tag="stack_tos", value=v})
            elseif t == "stack_local" then
                table.insert(flat, {tag="stack_local", value=v})
            elseif t == "stack_push" then
                table.insert(flat, {tag="stack_push"})
            elseif t == "stack_pop" then
                table.insert(flat, {tag="stack_pop"})
            elseif t == "null_param" then
                table.insert(flat, {tag="null_param"})

            -- Brace pairs — open pushes onto brace_stack, close pops and patches
            elseif t == "list_start" then
                table.insert(flat, {tag="open_list", brace_offset=0, parent_offset=po})
                table.insert(brace_stack, {idx=#flat, btype="list"})
            elseif t == "list_end" then
                local info = table.remove(brace_stack)
                local bo = #flat + 1 - info.idx
                flat[info.idx].brace_offset = bo
                table.insert(flat, {tag="close_list", brace_offset=bo, parent_offset=po})

            elseif t == "dict_start" then
                table.insert(flat, {tag="open_dict", brace_offset=0, parent_offset=po})
                table.insert(brace_stack, {idx=#flat, btype="dict"})
            elseif t == "dict_end" then
                local info = table.remove(brace_stack)
                local bo = #flat + 1 - info.idx
                flat[info.idx].brace_offset = bo
                table.insert(flat, {tag="close_dict", brace_offset=bo, parent_offset=po})

            elseif t == "dict_key" then
                table.insert(flat, {tag="open_key", key_hash=fnv(v), comment=v})
                table.insert(brace_stack, {idx=#flat, btype="key"})
            elseif t == "dict_key_hash" then
                table.insert(flat, {tag="open_key", key_hash=v})
                table.insert(brace_stack, {idx=#flat, btype="key"})
            elseif t == "end_dict_key" then
                local info = table.remove(brace_stack)
                local bo = #flat + 1 - info.idx
                table.insert(flat, {tag="close_key", brace_offset=bo, parent_offset=po})

            elseif t == "array_start" then
                table.insert(flat, {tag="open_array", brace_offset=0, parent_offset=po})
                table.insert(brace_stack, {idx=#flat, btype="array"})
            elseif t == "array_end" then
                local info = table.remove(brace_stack)
                local bo = #flat + 1 - info.idx
                flat[info.idx].brace_offset = bo
                table.insert(flat, {tag="close_array", brace_offset=bo, parent_offset=po})

            elseif t == "tuple_start" then
                table.insert(flat, {tag="open_tuple", brace_offset=0, parent_offset=po})
                table.insert(brace_stack, {idx=#flat, btype="tuple"})
            elseif t == "tuple_end" then
                local info = table.remove(brace_stack)
                local bo = #flat + 1 - info.idx
                flat[info.idx].brace_offset = bo
                table.insert(flat, {tag="close_tuple", brace_offset=bo, parent_offset=po})

            else
                table.insert(flat, {tag="uint", value=v or 0, comment="fallback: " .. t})
            end
        end

        -- Emit one node recursively into flat[]
        local function emit_node(node)
            local content = count_node_params(node)
            local po = parent_offset()

            -- OPEN_CALL
            table.insert(flat, {tag="open_call", content_count=content, parent_offset=po})
            table.insert(parent_call_stack, #flat)

            -- func_ref
            local kind, func_index
            if node.call_type == "o_call" then
                kind = "oneshot"; func_index = oneshot_hash_idx[node.func_hash] or 0
            elseif node.call_type == "io_call" then
                kind = "oneshot_survives"; func_index = oneshot_hash_idx[node.func_hash] or 0
            elseif node.call_type == "m_call" then
                kind = "main_"; func_index = main_hash_idx[node.func_hash] or 0
            elseif node.call_type == "pt_m_call" then
                kind = "main_pointer"; func_index = main_hash_idx[node.func_hash] or 0
            elseif node.call_type == "p_call" then
                kind = "pred"; func_index = pred_hash_idx[node.func_hash] or 0
            elseif node.call_type == "p_call_composite" then
                kind = "pred_composite"; func_index = pred_hash_idx[node.func_hash] or 0
            else
                kind = "main_"; func_index = 0
            end

            local this_node_idx = node_idx
            node_idx = node_idx + 1

            table.insert(flat, {
                tag = "func_ref", kind = kind,
                node_index = this_node_idx, func_index = func_index,
                pointer_index = node.pointer_index or 0,
                comment = node.func_name,
            })

            -- Merge params and children, sort by order
            local items = {}
            for _, p in ipairs(node.params) do
                table.insert(items, {type="param", order=p.order or 0, data=p})
            end
            for _, c in ipairs(node.children) do
                table.insert(items, {type="child", order=c.order or 0, data=c})
            end
            table.sort(items, function(a, b) return a.order < b.order end)

            for _, item in ipairs(items) do
                if item.type == "param" then
                    emit_param(item.data)
                else
                    emit_node(item.data)
                end
            end

            -- CLOSE
            local my_open = table.remove(parent_call_stack)
            local bo = #flat + 1 - my_open
            table.insert(flat, {tag="close", brace_offset=bo, parent_offset=parent_offset()})
        end

        for _, node in ipairs(tree.nodes) do
            emit_node(node)
        end

        return flat
    end

    -- ====================================================================
    -- Convert one flat param descriptor to Zig initializer text
    -- ====================================================================

    local function param_to_zig(p)
        local tag = p.tag
        local cmt = p.comment and ("  // " .. p.comment) or ""

        if tag == "int" then
            return string.format("Param{ .int = %d },%s", p.value, cmt)
        elseif tag == "uint" then
            return string.format("Param{ .uint = %d },%s", p.value, cmt)
        elseif tag == "float" then
            -- emit as hex float for exact round-trip
            local buf = require("ffi").new("float[1]", p.value)
            local u = require("ffi").cast("uint32_t*", buf)[0]
            return string.format("Param{ .float = @bitCast(@as(u32, 0x%08X)) },%s", tonumber(u), cmt)
        elseif tag == "str_hash" then
            return string.format("Param{ .str_hash = 0x%08X },%s", p.value, cmt)
        elseif tag == "str_idx" then
            return string.format("Param{ .str_idx = .{ .index = %d, .len = %d } },%s",
                p.index, p.len, cmt)
        elseif tag == "field_ref" then
            return string.format("Param{ .field_ref = .{ .offset = %d, .size = %d } },%s",
                p.offset, p.size, cmt)
        elseif tag == "const_ref" then
            return string.format("Param{ .const_ref = .{ .index = %d } },%s", p.index, cmt)
        elseif tag == "result" then
            return string.format("Param{ .result = %d },%s", p.value, cmt)
        elseif tag == "open_call" then
            return string.format("Param{ .open_call = .{ .content_count = %d, .parent_offset = %d } },",
                p.content_count, p.parent_offset)
        elseif tag == "close" then
            return string.format("Param{ .close = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "func_ref" then
            return string.format(
                "Param{ .func_ref = .{ .kind = .%s, .node_index = %d, .func_index = %d } },%s",
                p.kind, p.node_index, p.func_index, cmt)
        elseif tag == "open_list" then
            return string.format("Param{ .open_list = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "close_list" then
            return string.format("Param{ .close_list = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "open_dict" then
            return string.format("Param{ .open_dict = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "close_dict" then
            return string.format("Param{ .close_dict = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "open_key" then
            return string.format("Param{ .open_key = .{ .key_hash = 0x%08X } },%s",
                p.key_hash, cmt)
        elseif tag == "close_key" then
            return string.format("Param{ .close_key = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "open_array" then
            return string.format("Param{ .open_array = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "close_array" then
            return string.format("Param{ .close_array = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "open_tuple" then
            return string.format("Param{ .open_tuple = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "close_tuple" then
            return string.format("Param{ .close_tuple = .{ .brace_offset = %d, .parent_offset = %d } },",
                p.brace_offset, p.parent_offset)
        elseif tag == "stack_tos" then
            return string.format("Param{ .stack_tos = %d },%s", p.value, cmt)
        elseif tag == "stack_local" then
            return string.format("Param{ .stack_local = %d },%s", p.value, cmt)
        elseif tag == "stack_push" then
            return "Param{ .stack_push = {} },"
        elseif tag == "stack_pop" then
            return "Param{ .stack_pop = {} },"
        elseif tag == "null_param" then
            return "Param{ .null_param = {} },"
        else
            return string.format("Param{ .uint = 0 },  // unknown: %s", tag)
        end
    end

    -- ====================================================================
    -- Emit the .zig file
    -- ====================================================================

    local function emit(s) table.insert(lines, s) end
    local function blank() table.insert(lines, "") end

    -- Header
    emit("// " .. string.rep("=", 74))
    emit("// " .. mod.name .. "_module.zig")
    emit("// Generated S-expression module — compiled Zig, no GC")
    emit("// DO NOT EDIT — Generated by s_compile.lua v5.3")
    emit("// " .. string.rep("=", 74))
    blank()

    -- ================================================================
    -- Result enum
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Result codes (main functions return these)")
    emit("// " .. string.rep("-", 74))
    emit("pub const Result = enum(u8) {")
    emit("    // Application results")
    emit("    continue_       = 0,")
    emit("    halt             = 1,")
    emit("    terminate        = 2,")
    emit("    reset            = 3,")
    emit("    disable          = 4,")
    emit("    skip_continue    = 5,")
    emit("    // Function results")
    emit("    fn_continue      = 6,")
    emit("    fn_halt          = 7,")
    emit("    fn_terminate     = 8,")
    emit("    fn_reset         = 9,")
    emit("    fn_disable       = 10,")
    emit("    fn_skip_continue = 11,")
    emit("    // Pipeline results")
    emit("    pl_continue      = 12,")
    emit("    pl_halt          = 13,")
    emit("    pl_terminate     = 14,")
    emit("    pl_reset         = 15,")
    emit("    pl_disable       = 16,")
    emit("    pl_skip_continue = 17,")
    emit("};")
    blank()

    -- ================================================================
    -- FuncKind enum
    -- ================================================================
    emit("pub const FuncKind = enum(u8) {")
    emit("    oneshot          = 0,")
    emit("    oneshot_survives = 1,")
    emit("    main_            = 2,")
    emit("    main_pointer     = 3,")
    emit("    pred             = 4,")
    emit("    pred_composite   = 5,")
    emit("};")
    blank()

    -- ================================================================
    -- Event type
    -- ================================================================
    emit("pub const Event = struct {")
    emit("    event_type: u16 = 0,")
    emit("    event_id: u16 = 0,")
    emit("    event_data: ?*anyopaque = null,")
    emit("};")
    blank()

    -- ================================================================
    -- Function signatures
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Function signatures")
    emit("// " .. string.rep("-", 74))
    emit("pub const MainFn    = *const fn ([]const Param, *anyopaque, Event) Result;")
    emit("pub const OneshotFn = *const fn ([]const Param, *anyopaque, Event) void;")
    emit("pub const PredFn    = *const fn ([]const Param, *anyopaque, Event) bool;")
    blank()

    -- ================================================================
    -- Param tagged union
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Param tagged union — each element in the flattened param stream")
    emit("// " .. string.rep("-", 74))
    emit("pub const Param = union(enum) {")
    emit("    // Value types")
    emit("    int: i32,")
    emit("    uint: u32,")
    emit("    float: f32,")
    emit("    str_hash: u32,")
    emit("    str_idx: struct { index: u16, len: u16 },")
    emit("    field_ref: struct { offset: u16, size: u16 },")
    emit("    const_ref: struct { index: u16 },")
    emit("    result: u32,")
    blank()
    emit("    // Call structure")
    emit("    open_call: struct { content_count: u16, parent_offset: u16 },")
    emit("    close: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    func_ref: struct { kind: FuncKind, node_index: u16, func_index: u16 },")
    blank()
    emit("    // Container braces")
    emit("    open_list: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    close_list: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    open_dict: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    close_dict: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    open_key: struct { key_hash: u32 },")
    emit("    close_key: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    open_array: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    close_array: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    open_tuple: struct { brace_offset: u16, parent_offset: u16 },")
    emit("    close_tuple: struct { brace_offset: u16, parent_offset: u16 },")
    blank()
    emit("    // Stack operations")
    emit("    stack_tos: u32,")
    emit("    stack_local: u32,")
    emit("    stack_push: void,")
    emit("    stack_pop: void,")
    emit("    null_param: void,")
    emit("};")
    blank()

    -- ================================================================
    -- TreeDef descriptor
    -- ================================================================
    emit("pub const TreeDef = struct {")
    emit("    name_hash: u32,")
    emit("    params: []const Param,")
    emit("    node_count: u16,")
    emit("    pointer_count: u16,")
    emit("    record_hash: u32,")
    emit("    defaults_index: u16,")
    emit("};")
    blank()

    -- ================================================================
    -- Records
    -- ================================================================
    if #mod.record_order > 0 then
        emit("// " .. string.rep("-", 74))
        emit("// Record types")
        emit("// " .. string.rep("-", 74))
        for _, rname in ipairs(mod.record_order) do
            local rec = mod.records[rname]
            emit(string.format("pub const %s = extern struct {", zig_ident(rname)))
            for _, f in ipairs(rec.fields) do
                emit(string.format("    %s: %s,  // offset=%d size=%d",
                    zig_ident(f.name), zig_type_for_field(f), f.offset, f.size))
            end
            emit("};")
            -- Comptime offset assertions
            emit("comptime {")
            for _, f in ipairs(rec.fields) do
                emit(string.format(
                    '    if (@offsetOf(%s, "%s") != %d) @compileError("offset mismatch: %s.%s");',
                    zig_ident(rname), f.name, f.offset, rname, f.name))
            end
            emit(string.format(
                '    if (@sizeOf(%s) != %d) @compileError("size mismatch: %s");',
                zig_ident(rname), rec.size, rname))
            emit("}")
            blank()
        end
    end

    -- ================================================================
    -- String table
    -- ================================================================
    if #mod.string_table > 0 then
        emit("// " .. string.rep("-", 74))
        emit("// String table")
        emit("// " .. string.rep("-", 74))
        emit(string.format("pub const string_table = [%d][]const u8{", #mod.string_table))
        for _, s in ipairs(mod.string_table) do
            emit("    " .. zig_str(s) .. ",")
        end
        emit("};")
        blank()
    end

    -- ================================================================
    -- Event definitions
    -- ================================================================
    if mod.events and #mod.events > 0 then
        emit("// " .. string.rep("-", 74))
        emit("// Events")
        emit("// " .. string.rep("-", 74))
        for _, evt in ipairs(mod.events) do
            emit(string.format("pub const event_%s: u16 = 0x%04X;",
                evt.name:lower(), evt.id))
        end
        blank()
    end

    -- ================================================================
    -- Per-tree param streams
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Tree param streams (flattened)")
    emit("// " .. string.rep("-", 74))

    local tree_var_names = {}

    for _, tname in ipairs(mod.tree_order) do
        local tree = mod.trees[tname]
        local var_name = tname:lower():gsub("[^%w_]", "_")
        tree_var_names[tname] = var_name

        local flat = flatten_tree(tree)

        emit(string.format("pub const %s_params = [%d]Param{", var_name, #flat))
        for _, p in ipairs(flat) do
            emit("    " .. param_to_zig(p))
        end
        emit("};")
        blank()

        -- Tree descriptor
        local rec_hash = 0
        if tree.record_name and mod.records[tree.record_name] then
            rec_hash = mod.records[tree.record_name].name_hash
        end
        emit(string.format("pub const %s = TreeDef{", var_name))
        emit(string.format("    .name_hash = 0x%08X,", tree.name_hash))
        emit(string.format("    .params = &%s_params,", var_name))
        emit(string.format("    .node_count = %d,", tree.node_count))
        emit(string.format("    .pointer_count = %d,", tree.pointer_count or 0))
        emit(string.format("    .record_hash = 0x%08X,", rec_hash))
        emit(string.format("    .defaults_index = 0x%04X,", tree.defaults_index or 0xFFFF))
        emit("};")
        blank()
    end

    -- ================================================================
    -- Module descriptor
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Module")
    emit("// " .. string.rep("-", 74))
    emit(string.format("pub const module_name_hash: u32 = 0x%08X;", mod.name_hash))
    if #mod.tree_order > 0 then
        emit(string.format("pub const module_trees = [%d]TreeDef{", #mod.tree_order))
        for _, tname in ipairs(mod.tree_order) do
            emit(string.format("    %s,", tree_var_names[tname]))
        end
        emit("};")
    else
        emit("pub const module_trees = [0]TreeDef{};")
    end
    blank()

    -- ================================================================
    -- Dispatch — comptime function that wires builtins + user fns
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// Comptime dispatch — three separate switches by func_index")
    emit("//   func_index is the 0-based position in the function list.")
    emit("//   The Zig compiler turns sequential index switches into jump tables.")
    emit("//")
    emit("// Usage:")
    emit("//   const builtins = @import(\"s_engine_builtins.zig\");")
    emit("//   const user     = @import(\"my_functions.zig\");")
    emit("//   const D        = Dispatch(builtins, user);")
    emit("//   const result   = D.main_(index, params, inst, event);")
    emit("// " .. string.rep("-", 74))

    emit("pub fn Dispatch(comptime builtins: type, comptime user: type) type {")
    emit("    return struct {")

    -- Main dispatch
    if #mod.main_funcs > 0 then
        emit("        pub fn main_(index: u16, params: []const Param, inst: *anyopaque, event: Event) Result {")
        emit("            return switch (index) {")
        for i, name in ipairs(mod.main_funcs) do
            local src = builtin_set[name] and "builtins" or "user"
            local zig_name = name:lower()
            emit(string.format(
                "                %d => %s.%s(params, inst, event),  // %s",
                i - 1, src, zig_name, name))
        end
        emit("                else => .terminate,")
        emit("            };")
        emit("        }")
    else
        emit("        pub fn main_(_: u16, _: []const Param, _: *anyopaque, _: Event) Result {")
        emit("            return .terminate;")
        emit("        }")
    end
    blank()

    -- Oneshot dispatch
    if #mod.oneshot_funcs > 0 then
        emit("        pub fn oneshot(index: u16, params: []const Param, inst: *anyopaque, event: Event) void {")
        emit("            switch (index) {")
        for i, name in ipairs(mod.oneshot_funcs) do
            local src = builtin_set[name] and "builtins" or "user"
            local zig_name = name:lower()
            emit(string.format(
                "                %d => %s.%s(params, inst, event),  // %s",
                i - 1, src, zig_name, name))
        end
        emit("                else => {},")
        emit("            }")
        emit("        }")
    else
        emit("        pub fn oneshot(_: u16, _: []const Param, _: *anyopaque, _: Event) void {}")
    end
    blank()

    -- Pred dispatch
    if #mod.pred_funcs > 0 then
        emit("        pub fn pred(index: u16, params: []const Param, inst: *anyopaque, event: Event) bool {")
        emit("            return switch (index) {")
        for i, name in ipairs(mod.pred_funcs) do
            local src = builtin_set[name] and "builtins" or "user"
            local zig_name = name:lower()
            emit(string.format(
                "                %d => %s.%s(params, inst, event),  // %s",
                i - 1, src, zig_name, name))
        end
        emit("                else => false,")
        emit("            };")
        emit("        }")
    else
        emit("        pub fn pred(_: u16, _: []const Param, _: *anyopaque, _: Event) bool {")
        emit("            return false;")
        emit("        }")
    end

    emit("    };")
    emit("}")
    blank()

    -- ================================================================
    -- evalCall — kind-aware dispatch bridge
    -- ================================================================
    emit("// " .. string.rep("-", 74))
    emit("// evalCall — dispatches by function kind, returns unified Result")
    emit("//   Predicates return bool → mapped to continue_/halt")
    emit("//   Oneshots return void  → mapped to continue_ (with done-bit guard)")
    emit("//   Main returns Result   → passed through")
    emit("// " .. string.rep("-", 74))
    emit("pub fn makeEvalCall(comptime D: type) type {")
    emit("    return struct {")
    emit("        pub fn evalCall(params: []const Param, inst: *anyopaque, event: Event) Result {")
    emit("            const ref = params[1].func_ref;")
    emit("            const body = params[2 .. params[0].open_call.content_count];")
    emit("            return switch (ref.kind) {")
    emit("                .main_, .main_pointer => D.main_(ref.func_index, body, inst, event),")
    emit("                .oneshot, .oneshot_survives => blk: {")
    emit("                    D.oneshot(ref.func_index, body, inst, event);")
    emit("                    break :blk .continue_;")
    emit("                },")
    emit("                .pred, .pred_composite => blk: {")
    emit("                    const ok = D.pred(ref.func_index, body, inst, event);")
    emit("                    break :blk if (ok) .continue_ else .halt;")
    emit("                },")
    emit("            };")
    emit("        }")
    emit("    };")
    emit("}")

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
        if not opts.micropython_module then opts.micropython_module = true end
        if not opts.zig_module then opts.zig_module = true end
        if not opts.lua53_module then opts.lua53_module = true end
    end

    -- Resolve --lua auto-name
    if opts.lua_module == true then
        opts.lua_module = base_name .. "_module.lua"
    end

    -- Resolve --python auto-name
    if opts.python_module == true then
        opts.python_module = base_name .. "_module.py"
    end

    -- Resolve --micropython auto-name
    if opts.micropython_module == true then
        opts.micropython_module = base_name .. "_module_mpy.py"
    end

    -- Resolve --zig auto-name
    if opts.zig_module == true then
        opts.zig_module = base_name .. "_module.zig"
    end

    -- Resolve --lua53 auto-name
    if opts.lua53_module == true then
        opts.lua53_module = base_name .. "_module_53.lua"
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

    if opts.micropython_module then
        local content = serialize_module_micropython(module_data)
        write_file(make_path(opts.outdir, opts.micropython_module), content)
    end

    if opts.zig_module then
        local content = serialize_module_zig(module_data, dsl.BUILTIN_SET)
        write_file(make_path(opts.outdir, opts.zig_module), content)
    end

    if opts.lua53_module then
        local content = serialize_module_lua53(module_data)
        write_file(make_path(opts.outdir, opts.lua53_module), content)
    end

    if opts.dump then
        print(gen:dump())
    end

    -- Default output if nothing specified
    if not opts.header and not opts.user_header and not opts.registration and
       not opts.records_header and not opts.binary_file and not opts.binary_header and
       not opts.debug_header and not opts.dump_header and not opts.lua_module and
       not opts.python_module and not opts.micropython_module and
       not opts.zig_module and not opts.lua53_module and not opts.dump then
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