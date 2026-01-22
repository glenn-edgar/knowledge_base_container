-- ============================================================================
-- test_modular_dsl.lua
-- Test script to verify the modular s_expr_dsl works correctly
-- ============================================================================

-- Add the parent directory to package path
package.path = "/home/claude/?.lua;/home/claude/?/init.lua;" .. package.path

-- Load the modular DSL
local dsl = require("s_expr_dsl")

-- Install globals for backward compatibility
dsl.install_globals()

print("=== Testing Modular s_expr_dsl v5.2 ===\n")

-- Test 1: Hash function
print("Test 1: Hash function")
local h = fnv1a_32("test")
print(string.format("  fnv1a_32('test') = %s", dsl.fmt_hash(h)))
assert(type(h) == "number", "Hash should be a number")
print("  PASS\n")

-- Test 2: Create a simple module
print("Test 2: Create module with records")

local mod = start_module("test_module")

-- Define a record
RECORD("TestRecord")
    FIELD("x", "int32")
    FIELD("y", "int32")
    FIELD("value", "float")
END_RECORD()

-- Define a nested record
RECORD("NestedRecord")
    FIELD("id", "uint16")
    FIELD("data", "TestRecord")
END_RECORD()

print("  Records defined: TestRecord, NestedRecord")
print("  PASS\n")

-- Test 3: Define constants
print("Test 3: Define constants")

CONST("default_values", "TestRecord")
    VALUE("x", 10)
    VALUE("y", 20)
    VALUE("value", 3.14)
END_CONST()

print("  Constant 'default_values' defined")
print("  PASS\n")

-- Test 4: Define a tree with calls
print("Test 4: Define tree with calls")

start_tree("main_tree")
    use_record("TestRecord")
    use_defaults("default_values")
    
    local c1 = m_call("SE_PIPELINE")
        
        local c2 = m_call("SE_SEQUENCE")
            
            local c3 = o_call("SE_LOG")
                str("Starting sequence")
            end_call(c3)
            
            local c4 = m_call("SE_TICK_DELAY")
                uint(100)
            end_call(c4)
            
        end_call(c2)
        
    end_call(c1)
    
end_tree("main_tree")

print("  Tree 'main_tree' defined with 4 nodes")
print("  PASS\n")

-- Test 5: Define a tree with state machine
print("Test 5: Define tree with state machine and dict")

start_tree("state_tree")
    use_record("TestRecord")
    
    local sm = m_call("SE_STATE_MACHINE")
        field_ref("x")  -- dispatch field
        
        local d = dict_start("states")
            
            local k1 = key("IDLE")
                local idle = m_call("SE_NOP")
                end_call(idle)
            key_end(k1)
            
            local k2 = key("RUNNING")
                local run = m_call("SE_TICK_DELAY")
                    uint(50)
                end_call(run)
            key_end(k2)
            
        dict_end(d)
        
    end_call(sm)
    
end_tree("state_tree")

print("  Tree 'state_tree' defined with state machine")
print("  PASS\n")

-- Finalize module
mod = end_module(mod)

-- Test 6: Generate outputs
print("Test 6: Generate C headers")

local gen = dsl.ModuleGenerator.new(mod)

local records_h = gen:to_c_records_header("test_module")
assert(records_h:find("TestRecord_t"), "Should contain TestRecord_t")
assert(records_h:find("NestedRecord_t"), "Should contain NestedRecord_t")
print("  Records header generated")

local main_h = gen:to_c_header("test_module")
assert(main_h:find("TEST_MODULE_NAME_HASH"), "Should contain module hash")
assert(main_h:find("TEST_MODULE_TREE_COUNT"), "Should contain tree count")
print("  Main header generated")

local user_h = gen:to_c_user_header("test_module")
print("  User functions header generated")

print("  PASS\n")

-- Test 7: Generate binary
print("Test 7: Generate binary module")

local bin_gen = dsl.BinaryModuleGenerator.new(mod)
local bytes, size = bin_gen:generate()

assert(type(bytes) == "table", "Should return byte array")
assert(size > 0, "Size should be positive")
print(string.format("  Binary size: %d bytes", size))

-- Check magic number (SEXB = 0x42584553)
local magic = bytes[1] + bytes[2] * 256 + bytes[3] * 65536 + bytes[4] * 16777216
assert(magic == 0x42584553, "Magic number should be SEXB")
print("  Magic number verified: SEXB")

print("  PASS\n")

-- Test 8: Generate debug header
print("Test 8: Generate debug header")

local debug_h = dsl.generate_debug_header(mod)
assert(debug_h:find("test_module_tree_debug"), "Should contain tree debug info")
assert(debug_h:find("test_module_main_debug"), "Should contain main debug info")
print("  Debug header generated")
print("  PASS\n")

-- Summary
print("===========================================")
print("All tests passed!")
print("===========================================")
print("")
print("Module summary:")
print(gen:dump())
