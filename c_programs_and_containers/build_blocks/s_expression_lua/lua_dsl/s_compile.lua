#!/usr/bin/env luajit
-- ============================================================================
-- s_compile.lua
-- S-Expression Engine DSL Compiler - Version 5.4
-- 
-- Generates:
--   - C headers (.h) for records, module, user functions
--   - C registration code (.c) for C-native functions
--   - Lua trampoline registration code (.c) for Lua functions
--   - Lua trampoline registration header (.h) for Lua functions
--   - Lua hash manifest header (.h) for Lua runtime
--   - Binary module files (.bin) - direct s_expr_param_t, zero-copy
--   - Binary as C header (_bin.h)
--   - Debug parameter dump header (_dump.h)
--
-- VERSION 5.4 CHANGES:
--   - Added --lua-reg= option for Lua trampoline registration .c file
--   - Added --lua-reg-h= option for Lua trampoline registration .h file
--   - Added --lua-hashes= option for Lua hash manifest .h file
--   - --all and --all-bin now include Lua outputs when Lua functions present
--   - Summary output shows Lua function counts
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
        lua_reg = nil,
        lua_reg_h = nil,
        lua_hashes = nil,
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
        elseif arg:match("^%-%-lua%-reg%-h=") then
            opts.lua_reg_h = arg:match("^%-%-lua%-reg%-h=(.+)$")
        elseif arg:match("^%-%-lua%-reg=") then
            opts.lua_reg = arg:match("^%-%-lua%-reg=(.+)$")
        elseif arg:match("^%-%-lua%-hashes=") then
            opts.lua_hashes = arg:match("^%-%-lua%-hashes=(.+)$")
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
S-Expression Engine DSL Compiler v5.4

Usage: luajit s_compile.lua <input.lua> [options]

Options:
  --header=<file>      Generate main C header (default: <base>.h)
  --user=<file>        Generate user function header (C-native only)
  --reg=<file>         Generate user registration code (C-native only)
  --records=<file>     Generate records header (standalone structures)
  --debug=<file>       Generate debug header with hash->name mappings
  --binary=<file>      Generate binary module file (.bin)
  --binary-h=<file>    Generate binary header (const uint8_t array)
  --dump-h=<file>      Generate parameter dump header (human-readable params)
  --lua-reg=<file>     Generate Lua trampoline registration .c file
  --lua-reg-h=<file>   Generate Lua trampoline registration .h file
  --lua-hashes=<file>  Generate Lua hash manifest .h file
  --helpers=<file>     Load helper functions (can specify multiple times)
  --dump               Print debug dump of module to stdout
  --all                Generate all text outputs
  --all-bin            Generate all outputs including binary files
  --outdir=<dir>       Output directory (default: current)
  --32bit              Force 32-bit mode (default)
  --64bit              Force 64-bit mode
  --help, -h           Show this help

Generated files with --all:
  <base>_records.h              - Standalone record structures
  <base>.h                      - Module header (includes records)
  <base>_debug.h                - Debug hash reference
  <base>_user_functions.h       - C-native user function prototypes
  <base>_user_registration.c    - C-native function registration code
  <base>_lua_registration.c     - Lua trampoline registration (if lua funcs)
  <base>_lua_registration.h     - Lua trampoline registration header (if lua funcs)
  <base>_lua_hashes.h           - Lua hash manifest (if lua funcs)

Generated files with --all-bin (includes --all plus):
  <base>_32.bin or <base>_64.bin     - Binary module for runtime loading
  <base>_bin_32.h or <base>_bin_64.h - Binary as C array for ROM embedding
  <base>_dump_32.h or <base>_dump_64.h - Human-readable parameter dump

Version 5.4 Features:
  - lua_oneshot(), lua_main(), lua_pred() DSL declarations
  - LUA_FUNCTIONS{} batch declaration
  - Generated trampoline registration (.c and .h) for Lua runtime bridge
  - Binary format contains direct s_expr_param_t structs
  - Zero-copy loading: cast pointer directly from ROM
  - Two binary formats: 32-bit (8-byte params) and 64-bit (16-byte params)
  - Composable predicate API (p_call_composite)
  - Dict/hash support with OPEN_DICT, CLOSE_DICT, OPEN_KEY, CLOSE_KEY

Examples:
  luajit s_compile.lua my_module.lua --all --outdir=generated/
  luajit s_compile.lua my_module.lua --all-bin --64bit
  luajit s_compile.lua my_module.lua --binary=my_module_32.bin --32bit
  luajit s_compile.lua my_module.lua --helpers=s_engine_helpers.lua --all
  luajit s_compile.lua my_module.lua --lua-reg=my_module_lua_registration.c
  luajit s_compile.lua my_module.lua --lua-reg-h=my_module_lua_registration.h
  luajit s_compile.lua my_module.lua --lua-hashes=my_module_lua_hashes.h
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
-- HELPER: count lua functions in module
-- ============================================================================

local function has_lua_functions(module_data)
    local count = 0
    if module_data.lua_oneshot_funcs then count = count + #module_data.lua_oneshot_funcs end
    if module_data.lua_main_funcs then count = count + #module_data.lua_main_funcs end
    if module_data.lua_pred_funcs then count = count + #module_data.lua_pred_funcs end
    return count > 0
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
    
    -- Load the DSL library (v5.4)
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
    
    -- Load helper files
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
    
    -- Try to load s_engine_helpers.lua if it exists
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
    
    -- Check for lua functions
    local has_lua = has_lua_functions(module_data)
    
    -- Handle --all flags
    if opts.all or opts.all_bin then
        if not opts.header then opts.header = base_name .. ".h" end
        if not opts.user_header then opts.user_header = base_name .. "_user_functions.h" end
        if not opts.registration then opts.registration = base_name .. "_user_registration.c" end
        if not opts.debug_header then opts.debug_header = base_name .. "_debug.h" end
        if not opts.records_header and #module_data.record_order > 0 then
            opts.records_header = base_name .. "_records.h"
        end
        -- Lua outputs (only if lua functions are declared)
        if has_lua then
            if not opts.lua_reg then opts.lua_reg = base_name .. "_lua_registration.c" end
            if not opts.lua_reg_h then opts.lua_reg_h = base_name .. "_lua_registration.h" end
            if not opts.lua_hashes then opts.lua_hashes = base_name .. "_lua_hashes.h" end
        end
    end
    
    if opts.all_bin then
        if not opts.binary_file then opts.binary_file = base_name .. mode_suffix .. ".bin" end
        if not opts.binary_header then opts.binary_header = base_name .. "_bin" .. mode_suffix .. ".h" end
        if not opts.dump_header then opts.dump_header = base_name .. "_dump" .. mode_suffix .. ".h" end
    end
    
    -- ========================================================================
    -- Generate outputs
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
    
    if opts.lua_reg then
        local content = gen:to_c_lua_registration(base_name)
        if content then
            write_file(make_path(opts.outdir, opts.lua_reg), content)
        end
    end
    
    if opts.lua_reg_h then
        local content = gen:to_c_lua_registration_header(base_name)
        if content then
            write_file(make_path(opts.outdir, opts.lua_reg_h), content)
        end
    end
    
    if opts.lua_hashes then
        local content = gen:to_c_lua_hashes_header(base_name)
        if content then
            write_file(make_path(opts.outdir, opts.lua_hashes), content)
        end
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
    
    if opts.dump then
        print(gen:dump())
    end
    
    -- Default output if nothing specified
    if not opts.header and not opts.user_header and not opts.registration and
       not opts.records_header and not opts.binary_file and not opts.binary_header and 
       not opts.debug_header and not opts.dump_header and 
       not opts.lua_reg and not opts.lua_reg_h and not opts.lua_hashes and
       not opts.dump then
        if #module_data.record_order > 0 then
            local content = gen:to_c_records_header(base_name)
            write_file(make_path(opts.outdir, base_name .. "_records.h"), content)
        end
        
        local content = gen:to_c_header(base_name)
        write_file(make_path(opts.outdir, base_name .. ".h"), content)
    end
    
    -- ========================================================================
    -- Print summary
    -- ========================================================================
    
    local lua_oneshot_count = module_data.lua_oneshot_funcs and #module_data.lua_oneshot_funcs or 0
    local lua_main_count = module_data.lua_main_funcs and #module_data.lua_main_funcs or 0
    local lua_pred_count = module_data.lua_pred_funcs and #module_data.lua_pred_funcs or 0
    local lua_total = lua_oneshot_count + lua_main_count + lua_pred_count
    
    print("")
    print("Module: " .. module_data.name)
    print("  Trees: " .. #module_data.tree_order)
    print("  Records: " .. #module_data.record_order)
    print("  Constants: " .. #module_data.const_order)
    print("  Strings: " .. #module_data.string_table)
    print("  Oneshot functions: " .. #module_data.oneshot_funcs)
    print("  Main functions: " .. #module_data.main_funcs)
    print("  Pred functions: " .. #module_data.pred_funcs)
    if lua_total > 0 then
        print("  Lua functions: " .. lua_total .. 
              " (oneshot=" .. lua_oneshot_count .. 
              ", main=" .. lua_main_count .. 
              ", pred=" .. lua_pred_count .. ")")
    end
    print("  Mode: " .. (is_64bit and "64-bit" or "32-bit"))
    print("  Binary format: v5.4 (direct s_expr_param_t, zero-copy)")
end

-- Run
local ok, err = pcall(main)
if not ok then
    io.stderr:write("Error: " .. tostring(err) .. "\n")
    os.exit(1)
end