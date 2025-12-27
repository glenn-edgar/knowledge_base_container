-- gen_registry.lua
-- Module Registry Generator
-- Usage: luajit gen_registry.lua <list_file> [options]
--
-- List file contains .h header files only (output of compile.lua)

local list_file = nil
local output_file = nil
local header_dir = ""

local function print_usage()
    print("Module Registry Generator")
    print("")
    print("Usage: luajit gen_registry.lua <list_file> [options]")
    print("")
    print("Options:")
    print("  --output=<file>      Output file (default: stdout)")
    print("  --header-dir=<dir>   Directory prefix for header includes")
    print("  --help               Show this help")
    print("")
    print("List file format (one .h file per line):")
    print("  chain_flow_dsl_tests.h   -> #include, base = chain_flow_dsl_tests")
    print("  path/to/motor.h          -> #include, base = motor")
    print("")
    print("Lines starting with # are comments")
    os.exit(0)
end

for i, a in ipairs(arg) do
    if a == "--help" or a == "-h" then
        print_usage()
    elseif a:match("^--output=") then
        output_file = a:match("^--output=(.+)$")
    elseif a:match("^--header%-dir=") then
        header_dir = a:match("^--header%-dir=(.+)$")
        if header_dir ~= "" and not header_dir:match("/$") then
            header_dir = header_dir .. "/"
        end
    elseif not a:match("^%-") then
        list_file = a
    else
        print("Unknown option: " .. a)
        os.exit(1)
    end
end

if not list_file then
    print("Error: No list file specified")
    print_usage()
end

-- Parse list file
local modules = {}

for line in io.lines(list_file) do
    line = line:match("^%s*(.-)%s*$")
    
    if line ~= "" and not line:match("^#") then
        local filename = line:match("([^/\\]+)$") or line
        
        if filename:match("%.h$") then
            local base_name = filename:match("(.+)%.h$")
            local header_path = header_dir .. filename
            
            table.insert(modules, { 
                base = base_name, 
                header = header_path,
                source = line 
            })
        else
            print("Error: Only .h files allowed in registry: " .. line)
            os.exit(1)
        end
    end
end

if #modules == 0 then
    print("Error: No modules found in " .. list_file)
    os.exit(1)
end

-- Generate output
local lines = {}

local function emit(s)
    table.insert(lines, s)
end

emit("// ============================================================================")
emit("// module_registry.h")
emit("// Generated from " .. list_file)
emit("// DO NOT EDIT")
emit("// ============================================================================")
emit("")
emit("#ifndef MODULE_REGISTRY_H")
emit("#define MODULE_REGISTRY_H")
emit("")
emit("#ifdef __cplusplus")
emit('extern "C" {')
emit("#endif")
emit("")
emit("#include <string.h>")
emit('#include "s_engine_types.h"')
emit("")

emit("// Module headers")
for _, m in ipairs(modules) do
    emit('#include "' .. m.header .. '"')
end
emit("")

emit("// Module registry table")
emit("static const s_expr_module_def_t* const module_registry[] = {")
for _, m in ipairs(modules) do
    emit("    &" .. m.base .. "_module,")
end
emit("};")
emit("")
emit("#define MODULE_REGISTRY_COUNT " .. #modules)
emit("")

emit("// Lookup module by name")
emit("static inline const s_expr_module_def_t* find_module(const char* name) {")
emit("    for (int i = 0; i < MODULE_REGISTRY_COUNT; i++) {")
emit("        if (strcmp(module_registry[i]->name, name) == 0) {")
emit("            return module_registry[i];")
emit("        }")
emit("    }")
emit("    return NULL;")
emit("}")
emit("")

emit("// Get module by index")
emit("static inline const s_expr_module_def_t* get_module(int index) {")
emit("    if (index >= 0 && index < MODULE_REGISTRY_COUNT) {")
emit("        return module_registry[index];")
emit("    }")
emit("    return NULL;")
emit("}")
emit("")

emit("#ifdef __cplusplus")
emit("}")
emit("#endif")
emit("")
emit("#endif // MODULE_REGISTRY_H")

-- Output
local content = table.concat(lines, "\n") .. "\n"

if output_file then
    local f = io.open(output_file, "w")
    if f then
        f:write(content)
        f:close()
        print("Generated: " .. output_file)
        print("  Modules: " .. #modules)
        for _, m in ipairs(modules) do
            print("    " .. m.base .. " <- " .. m.source)
        end
    else
        print("Error: Could not write " .. output_file)
        os.exit(1)
    end
else
    print(content)
end
