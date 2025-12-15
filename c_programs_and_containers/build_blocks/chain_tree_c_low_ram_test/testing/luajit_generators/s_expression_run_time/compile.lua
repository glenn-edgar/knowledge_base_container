-- compile.lua
-- ChainTree S-Expression Compiler v2.0
-- Usage: luajit compile.lua <input.lua> [options]

-- Load the DSL
dofile("s_expr_dsl.lua")

-- Parse command line
local input_file = nil
local output_bin = nil
local output_header = nil
local base_name = nil
local show_dump = false
local show_nodes = false
local show_stats = false

local function print_usage()
    print("ChainTree S-Expression Compiler v2.0")
    print("")
    print("Usage: luajit compile.lua <input.lua> [options]")
    print("")
    print("Options:")
    print("  --bin=<file>      Generate binary file (.bin)")
    print("  --header=<file>   Generate C header file (.h)")
    print("  --name=<name>     Base name for generated symbols (default: from input)")
    print("  --dump            Show tree structure")
    print("  --nodes           Show flat node array")
    print("  --stats           Show module statistics")
    print("  --all             Enable --dump, --nodes, --stats")
    print("  --help            Show this help")
    print("")
    print("Examples:")
    print("  luajit compile.lua motor.lua --header=motor.h")
    print("  luajit compile.lua motor.lua --bin=motor.bin")
    print("  luajit compile.lua motor.lua --bin=motor.bin --header=motor.h")
    print("  luajit compile.lua motor.lua --dump --stats")
    os.exit(0)
end

if not arg or #arg == 0 then
    print_usage()
end

for i, a in ipairs(arg) do
    if a == "--help" then
        print_usage()
    elseif a:match("^--bin=") then
        output_bin = a:match("^--bin=(.+)$")
    elseif a:match("^--header=") then
        output_header = a:match("^--header=(.+)$")
    elseif a:match("^--name=") then
        base_name = a:match("^--name=(.+)$")
    elseif a == "--dump" then
        show_dump = true
    elseif a == "--nodes" then
        show_nodes = true
    elseif a == "--stats" then
        show_stats = true
    elseif a == "--all" then
        show_dump = true
        show_nodes = true
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

-- Execute the input file - it should return a generator
local ok, result = pcall(chunk)
if not ok then
    print("Error executing " .. input_file .. ":")
    print(result)
    os.exit(1)
end

-- The input file should return the generator
local gen = result or _G.gen

if not gen then
    print("Error: Input file must return a generator or set global 'gen'")
    os.exit(1)
end

-- Check if it's a TreeGenerator or ModuleGenerator
local is_module = gen.tree_order ~= nil

print("ChainTree S-Expression Compiler v2.0")
print("Input: " .. input_file)
print("Type: " .. (is_module and "Module" or "Single Tree"))
print("")

-- Show dump
if show_dump then
    print("=== STRUCTURE ===\n")
    gen:dump()
    print("")
end

-- Generate binary file
if output_bin then
    if not is_module then
        print("Error: Binary output only supported for modules")
        os.exit(1)
    end
    
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

-- Show nodes
if show_nodes then
    print("\n=== FLAT NODE ARRAYS ===\n")
    if is_module then
        gen:compile()
        for _, tree_name in ipairs(gen.tree_order) do
            print("Tree: " .. tree_name)
            local tree_gen = gen.tree_generators[tree_name]
            local nodes = tree_gen:get_nodes()
            local params = tree_gen:get_params()
            
            print("  Nodes (" .. #nodes .. "):")
            for i, n in ipairs(nodes) do
                local flags = ""
                if n.is_default then flags = " [default]" end
                print(string.format(
                    "    [%2d] type=0x%02X children=%d first=0x%04X next=0x%04X fn=%d param=%d+%d%s",
                    i - 1, n.type, n.child_count, n.first_child, n.next_sibling,
                    n.fn_index, n.param_offset, n.param_count, flags
                ))
            end
            
            if #params > 0 then
                print("  Params (" .. #params .. "):")
                local type_names = { [0]="i32", [1]="u32", [2]="f32", [3]="str" }
                for i, p in ipairs(params) do
                    print(string.format("    [%2d] %s = %s", 
                        i - 1, type_names[p.type] or "?", tostring(p.value)))
                end
            end
            print("")
        end
    else
        gen:compile()
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        
        print("Nodes (" .. #nodes .. "):")
        for i, n in ipairs(nodes) do
            local flags = ""
            if n.is_default then flags = " [default]" end
            print(string.format(
                "  [%2d] type=0x%02X children=%d first=0x%04X next=0x%04X fn=%d param=%d+%d%s",
                i - 1, n.type, n.child_count, n.first_child, n.next_sibling,
                n.fn_index, n.param_offset, n.param_count, flags
            ))
        end
        
        if #params > 0 then
            print("Params (" .. #params .. "):")
            local type_names = { [0]="i32", [1]="u32", [2]="f32", [3]="str" }
            for i, p in ipairs(params) do
                print(string.format("  [%2d] %s = %s", 
                    i - 1, type_names[p.type] or "?", tostring(p.value)))
            end
        end
    end
end

-- Show stats
if show_stats then
    print("\n=== STATISTICS ===\n")
    local t = gen.tables
    
    print("  Oneshot functions (@): " .. #t.oneshot_fns)
    for i, s in ipairs(t.oneshot_fns) do
        print("    [" .. (i-1) .. "] " .. s)
    end
    
    print("  Boolean functions (?): " .. #t.boolean_fns)
    for i, s in ipairs(t.boolean_fns) do
        print("    [" .. (i-1) .. "] " .. s)
    end
    
    print("  Main functions (!):    " .. #t.main_fns)
    for i, s in ipairs(t.main_fns) do
        print("    [" .. (i-1) .. "] " .. s)
    end
    
    print("  Data strings:          " .. #t.strings)
    for i, s in ipairs(t.strings) do
        print("    [" .. (i-1) .. "] \"" .. s .. "\"")
    end
    
    if is_module then
        gen:compile()
        print("  Trees:                 " .. #gen.tree_order)
        local total_nodes = 0
        local total_params = 0
        for _, name in ipairs(gen.tree_order) do
            local tree_gen = gen.tree_generators[name]
            local node_count = tree_gen:get_node_count()
            local param_count = #tree_gen:get_params()
            total_nodes = total_nodes + node_count
            total_params = total_params + param_count
            print(string.format("    - %s (%d nodes, %d params)", name, node_count, param_count))
        end
        print("  Max node count:        " .. gen:get_max_node_count())
        print("  Total nodes:           " .. total_nodes)
        print("  Total params:          " .. total_params)
        
        -- Binary size estimate
        local bin = gen:to_bin()
        print("  Binary size:           " .. #bin .. " bytes")
    else
        gen:compile()
        print("  Node count:            " .. gen:get_node_count())
        print("  Param count:           " .. #gen:get_params())
    end
end

-- If no output options, show header to stdout
if not output_bin and not output_header and not show_dump and not show_nodes and not show_stats then
    print(gen:to_c_header(base_name))
end

print("\nDone.")