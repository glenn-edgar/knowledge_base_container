-- compile.lua
-- ChainTree S-Expression Compiler Driver
-- Usage: luajit compile.lua <input.lua> [options]

-- Load the DSL
dofile("s_expr_dsl.lua")

dofile("s_cfl_functions.lua")



-- Parse command line
local input_file = nil
local output_bin = nil
local output_header = nil
local output_user_header = nil
local output_user_reg = nil
local base_name = nil
local show_dump = false
local show_stats = false

local function print_usage()
    print("ChainTree S-Expression Compiler v3.0")
    print("")
    print("Usage: luajit compile.lua <input.lua> [options]")
    print("")
    print("Options:")
    print("  --bin=<file>         Generate binary file (.bin)")
    print("  --header=<file>      Generate C module header file (.h)")
    print("  --user-header=<file> Generate user functions header (.h)")
    print("  --user-reg=<file>    Generate user registration C file (.c)")
    print("  --name=<name>        Base name for generated symbols (default: from input)")
    print("  --dump               Show tree structure")
    print("  --stats              Show module statistics")
    print("  --help               Show this help")
    print("")
    print("Examples:")
    print("  luajit compile.lua motor.lua --header=motor_module.h")
    print("  luajit compile.lua motor.lua --header=motor_module.h --user-header=motor_user.h --user-reg=motor_user.c")
    print("  luajit compile.lua motor.lua --dump --stats")
    print("")
    print("If --header is specified without --user-header and --user-reg,")
    print("user files are auto-generated with _user_functions.h and _user_registration.c suffixes.")
    os.exit(0)
end

if not arg or #arg == 0 then
    print_usage()
end

for i, a in ipairs(arg) do
    if a == "--help" or a == "-h" then
        print_usage()
    elseif a:match("^--bin=") then
        output_bin = a:match("^--bin=(.+)$")
    elseif a:match("^--header=") then
        output_header = a:match("^--header=(.+)$")
    elseif a:match("^--user%-header=") then
        output_user_header = a:match("^--user%-header=(.+)$")
    elseif a:match("^--user%-reg=") then
        output_user_reg = a:match("^--user%-reg=(.+)$")
    elseif a:match("^--name=") then
        base_name = a:match("^--name=(.+)$")
    elseif a == "--dump" then
        show_dump = true
    elseif a == "--stats" then
        show_stats = true
    elseif not a:match("^%-") then
        input_file = a
    else
        print("Unknown option: " .. a)
        os.exit(1)
    end
end

if not input_file then
    print("Error: No input file specified")
    print_usage()
end

-- Derive base name from input file if not specified
if not base_name then
    base_name = input_file:match("([^/\\]+)%.lua$") or input_file:match("([^/\\]+)$") or "module"
end

-- Auto-generate user file names if header specified but user files not
if output_header and not output_user_header then
    output_user_header = base_name .. "_user_functions.h"
end
if output_header and not output_user_reg then
    output_user_reg = base_name .. "_user_registration.c"
end

-- Load and execute the input file
local chunk, err = loadfile(input_file)
if not chunk then
    print("Error loading " .. input_file .. ": " .. err)
    os.exit(1)
end

local ok, result = pcall(chunk)
if not ok then
    print("Error executing " .. input_file .. ":")
    print(result)
    os.exit(1)
end

-- The input file should return the generator
local gen = result
if not gen then
    print("Error: Input file must return a generator (return end_module(...))")
    os.exit(1)
end


local module_data = result
if not module_data then
    print("Error: Input file must return module data (return end_module(...))")
    os.exit(1)
end

local gen = ModuleGenerator.new(module_data)

if show_stats then
    print("")
    print("=== STATISTICS ===")
    print("  Oneshot functions: " .. #gen.module.oneshot_funcs)
    print("  Main functions:    " .. #gen.module.main_funcs)
    print("  Predicate functions: " .. #gen.module.pred_funcs)
    print("  Trees:             " .. #gen.module.tree_order)
    print("  Records:           " .. #gen.module.record_order)
    
    -- Count user vs system functions
    local user_oneshot, user_main, user_pred = 0, 0, 0
    for _, entry in ipairs(gen.module.oneshot_funcs) do
        if not entry.name:match("^CFL_") then user_oneshot = user_oneshot + 1 end
    end
    for _, entry in ipairs(gen.module.main_funcs) do
        if not entry.name:match("^CFL_") then user_main = user_main + 1 end
    end
    for _, entry in ipairs(gen.module.pred_funcs) do
        if not entry.name:match("^CFL_") then user_pred = user_pred + 1 end
    end
    print("")
    print("  User oneshot:      " .. user_oneshot)
    print("  User main:         " .. user_main)
    print("  User predicate:    " .. user_pred)
end

-- Generate module header file
if output_header then
    
    local header = gen:to_c_header(base_name)

    local f = io.open(output_header, "w")
    if f then
        f:write(header)
        f:write("\n")
        f:close()
        print("Generated: " .. output_header)
    else
        print("Error: Could not write " .. output_header)
        os.exit(1)
    end
end

-- Generate user functions header file
if output_user_header then
    local header = gen:to_c_user_header(base_name)
    local f = io.open(output_user_header, "w")
    if f then
        f:write(header)
        f:write("\n")
        f:close()
        print("Generated: " .. output_user_header)
    else
        print("Error: Could not write " .. output_user_header)
        os.exit(1)
    end
end

-- Generate user registration C file
if output_user_reg then
    local reg = gen:to_c_user_registration(base_name)
    local f = io.open(output_user_reg, "w")
    if f then
        f:write(reg)
        f:write("\n")
        f:close()
        print("Generated: " .. output_user_reg)
    else
        print("Error: Could not write " .. output_user_reg)
        os.exit(1)
    end
end

-- Generate binary file
if output_bin then
    local bin = gen:to_bin()
    local f = io.open(output_bin, "wb")
    if f then
        for _, b in ipairs(bin) do
            f:write(string.char(b))
        end
        f:close()
        print("Generated: " .. output_bin .. " (" .. #bin .. " bytes)")
    else
        print("Error: Could not write " .. output_bin)
        os.exit(1)
    end
end

-- If no output options specified, show header to stdout
if not output_bin and not output_header and not show_dump and not show_stats then
    print("")
    print(gen:to_c_header(base_name))
end

print("")
print("Done.")