-- compile.lua
-- ChainTree S-Expression Compiler Driver
-- Usage: luajit compile.lua <input.lua> [options]

-- Load the DSL
dofile("s_expr_dsl.lua")

-- Parse command line
local input_file = nil
local output_bin = nil
local output_header = nil
local base_name = nil
local show_dump = false
local show_stats = false

local function print_usage()
    print("ChainTree S-Expression Compiler v2.1")
    print("")
    print("Usage: luajit compile.lua <input.lua> [options]")
    print("")
    print("Options:")
    print("  --bin=<file>      Generate binary file (.bin)")
    print("  --header=<file>   Generate C header file (.h)")
    print("  --name=<name>     Base name for generated symbols (default: from input)")
    print("  --dump            Show tree structure")
    print("  --stats           Show module statistics")
    print("  --help            Show this help")
    print("")
    print("Examples:")
    print("  luajit compile.lua motor.lua --header=motor.h")
    print("  luajit compile.lua motor.lua --bin=motor.bin --header=motor.h")
    print("  luajit compile.lua motor.lua --dump --stats")
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

print("Input: " .. input_file)
print("Type: " .. (gen.tree_order and "Module" or "Single Tree"))
print("64-bit: " .. (gen.is_64bit and "yes" or "no"))

-- Show dump
if show_dump then
    print("")
    gen:dump()
end

-- Show stats
if show_stats then
    print("")
    print("=== STATISTICS ===")
    local t = gen.tables
    print("  Oneshot functions (@): " .. #t.oneshot_fns)
    print("  Boolean functions (?): " .. #t.boolean_fns)
    print("  Main functions (!):    " .. #t.main_fns)
    print("  Data strings:          " .. #t.strings)
    if gen.tree_order then
        print("  Trees:                 " .. #gen.tree_order)
        print("  Max node count:        " .. gen:get_max_node_count())
    end
end

-- Generate header file
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