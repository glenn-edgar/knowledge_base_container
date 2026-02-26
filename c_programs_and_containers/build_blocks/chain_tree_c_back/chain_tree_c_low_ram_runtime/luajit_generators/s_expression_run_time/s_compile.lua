#!/usr/bin/env luajit
-- ============================================================================
-- s_compile.lua
-- ChainTree S-Expression DSL Compiler
-- Usage: luajit s_compile.lua <input.lua> [options]
--
-- Options:
--   --header=<file>    Generate main C header (default: <module_name>.h)
--   --user=<file>      Generate user function header
--   --reg=<file>       Generate user registration code
--   --records=<file>   Generate records header (standalone structures)
--   --dump             Print debug dump of module
--   --all              Generate all outputs (header, user, reg, records)
--   --outdir=<dir>     Output directory (default: current)
--   --32bit            Force 32-bit mode (default)
--   --64bit            Force 64-bit mode
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")

-- Parse command line arguments
local function parse_args(args)
    local opts = {
        input = nil,
        header = nil,
        user_header = nil,
        registration = nil,
        records_header = nil,
        dump = false,
        all = false,
        outdir = ".",
        pointer_size = 4,  -- Default 32-bit
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
        elseif arg:match("^%-%-outdir=") then
            opts.outdir = arg:match("^%-%-outdir=(.+)$")
        elseif arg == "--dump" then
            opts.dump = true
        elseif arg == "--all" then
            opts.all = true
        elseif arg == "--32bit" then
            opts.pointer_size = 4
        elseif arg == "--64bit" then
            opts.pointer_size = 8
        elseif arg == "--help" or arg == "-h" then
            return nil  -- Signal to print help
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
ChainTree S-Expression DSL Compiler v3.0

Usage: luajit s_compile.lua <input.lua> [options]

Options:
  --header=<file>    Generate main C header (default: <module_name>.h)
  --user=<file>      Generate user function header
  --reg=<file>       Generate user registration code
  --records=<file>   Generate records header (standalone structures)
  --dump             Print debug dump of module
  --all              Generate all outputs (header, user, reg, records)
  --outdir=<dir>     Output directory (default: current)
  --32bit            Force 32-bit mode (default)
  --64bit            Force 64-bit mode
  --help, -h         Show this help

Generated files with --all:
  <base>_records.h           - Standalone record structures (for user code)
  <base>.h                   - DSL internals (includes _records.h)
  <base>_user_functions.h    - User function prototypes
  <base>_user_registration.c - Function registration code

Examples:
  luajit s_compile.lua my_module.lua --header=my_module.h
  luajit s_compile.lua my_module.lua --all --outdir=generated/
  luajit s_compile.lua my_module.lua --all --64bit
]])
end

-- Main
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
    
    -- Determine script directory for relative requires
    local script_path = arg[0]
    local script_dir = script_path:match("(.*/)")
    if not script_dir then
        script_dir = "./"
    end
    
    -- Set pointer size as global before loading DSL
    _G._pointer_size = opts.pointer_size
    
    -- Load the DSL library - this sets up global functions
    local dsl_file = script_dir .. "s_expr_dsl.lua"
    
    -- Check if DSL file exists
    local f = io.open(dsl_file, "r")
    if not f then
        -- Try current directory
        dsl_file = "s_expr_dsl.lua"
        f = io.open(dsl_file, "r")
        if not f then
            io.stderr:write("Error: Cannot find s_expr_dsl.lua\n")
            io.stderr:write("Looked in: " .. script_dir .. " and ./\n")
            os.exit(1)
        end
    end
    f:close()
    
    local dsl = dofile(dsl_file)
    if not dsl or not dsl.ModuleGenerator then
        io.stderr:write("Error: Failed to load DSL library\n")
        os.exit(1)
    end
    
    -- Check if input file exists
    f = io.open(opts.input, "r")
    if not f then
        io.stderr:write("Error: Cannot open input file: " .. opts.input .. "\n")
        os.exit(1)
    end
    f:close()
    
    -- Run the input DSL file
    -- The DSL file should:
    -- 1. Call start_module(), defrecord(), start_tree(), etc.
    -- 2. Call end_module() which returns the module data
    -- 3. Return that module data
    local module_data = dofile(opts.input)
    
    if not module_data then
        io.stderr:write("Error: DSL file did not return module data\n")
        io.stderr:write("Make sure your DSL file ends with:\n")
        io.stderr:write("  return end_module(\"your_module_name\")\n")
        os.exit(1)
    end
    
    if type(module_data) ~= "table" or not module_data.name then
        io.stderr:write("Error: Invalid module data returned\n")
        io.stderr:write("Make sure your DSL file returns the result of end_module()\n")
        os.exit(1)
    end
    
    -- Create generator
    local gen = dsl.ModuleGenerator.new(module_data)
    
    -- Determine base name from module name
    local base_name = module_data.name:lower():gsub("[^%w_]", "_")
    
    -- Handle --all flag
    if opts.all then
        if not opts.header then
            opts.header = base_name .. ".h"
        end
        if not opts.user_header then
            opts.user_header = base_name .. "_user_functions.h"
        end
        if not opts.registration then
            opts.registration = base_name .. "_user_registration.c"
        end
        if not opts.records_header then
            opts.records_header = base_name .. "_records.h"
        end
    end
    
    -- Generate requested outputs
    local function write_file(filename, content)
        -- Handle outdir properly
        local path
        if opts.outdir == "." then
            path = filename
        else
            -- Create output directory if needed
            os.execute("mkdir -p " .. opts.outdir)
            path = opts.outdir .. "/" .. filename
        end
        
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
    
    -- Generate records header first (if records exist and requested)
    if opts.records_header and #module_data.record_order > 0 then
        local content = gen:to_c_records_header(base_name)
        write_file(opts.records_header, content)
    end
    
    if opts.header then
        local content = gen:to_c_header(base_name)
        write_file(opts.header, content)
    end
    
    if opts.user_header then
        local content = gen:to_c_user_header(base_name)
        write_file(opts.user_header, content)
    end
    
    if opts.registration then
        local content = gen:to_c_user_registration(base_name)
        write_file(opts.registration, content)
    end
    
    if opts.dump then
        print(gen:dump())
    end
    
    -- If no output specified, just generate the main header
    if not opts.header and not opts.user_header and not opts.registration and not opts.records_header and not opts.dump then
        -- Generate records header if records exist
        if #module_data.record_order > 0 then
            opts.records_header = base_name .. "_records.h"
            local content = gen:to_c_records_header(base_name)
            write_file(opts.records_header, content)
        end
        
        opts.header = base_name .. ".h"
        local content = gen:to_c_header(base_name)
        write_file(opts.header, content)
    end
    
    -- Print summary
    print("")
    print("Module: " .. module_data.name)
    print("  Trees: " .. #module_data.tree_order)
    print("  Records: " .. #module_data.record_order)
    print("  Oneshot functions: " .. #module_data.oneshot_funcs)
    print("  Main functions: " .. #module_data.main_funcs)
    print("  Pred functions: " .. #module_data.pred_funcs)
    if #module_data.string_table > 0 then
        print("  Strings: " .. #module_data.string_table)
    end
    print("  Mode: " .. (opts.pointer_size == 8 and "64-bit" or "32-bit"))
end

-- Run main
local ok, err = pcall(main)
if not ok then
    io.stderr:write("Error: " .. tostring(err) .. "\n")
    os.exit(1)
end