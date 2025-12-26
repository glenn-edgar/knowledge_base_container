-- test_bool_ops.lua
-- Test XOR, NAND, NOR boolean operators

dofile("chaintree_dsl_v2_4.lua")

gensym_reset()

local mod = start_module("bool_ops_test")

-- ============================================================================
-- Tree 1: XOR test
-- Tests: bool_xor - true if exactly one is true (for 2 operands)
-- Expected: A XOR B = (A AND NOT B) OR (NOT A AND B)
-- ============================================================================
local tree1 = start_tree("xor_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local xr = bool_xor()
                    bool_fn("SWITCH_A")
                    bool_fn("SWITCH_B")
                end_bool_xor(xr)
            end_condition(c)
            local a = action()
                oneshot("LOG", str("exactly one switch is on"))
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline(p)
end_tree(tree1)

-- ============================================================================
-- Tree 2: NAND test
-- Tests: bool_nand - NOT (A AND B)
-- Expected: false only when all inputs are true
-- ============================================================================
local tree2 = start_tree("nand_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local nd = bool_nand()
                    bool_fn("SENSOR_A")
                    bool_fn("SENSOR_B")
                    bool_fn("SENSOR_C")
                end_bool_nand(nd)
            end_condition(c)
            local a = action()
                oneshot("LOG", str("not all sensors active"))
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline(p)
end_tree(tree2)

-- ============================================================================
-- Tree 3: NOR test
-- Tests: bool_nor - NOT (A OR B)
-- Expected: true only when all inputs are false
-- ============================================================================
local tree3 = start_tree("nor_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local nr = bool_nor()
                    bool_fn("HAS_FAULT")
                    bool_fn("HAS_WARNING")
                    bool_fn("HAS_ERROR")
                end_bool_nor(nr)
            end_condition(c)
            local a = action()
                oneshot("LOG", str("no faults, warnings, or errors"))
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline(p)
end_tree(tree3)

-- ============================================================================
-- Tree 4: Nested with new operators
-- Tests: Complex expression using all boolean operators
-- Expected: (A XOR B) NAND (C NOR D)
-- ============================================================================
local tree4 = start_tree("nested_new_ops")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local nd = bool_nand()
                    local xr = bool_xor()
                        bool_fn("INPUT_A")
                        bool_fn("INPUT_B")
                    end_bool_xor(xr)
                    local nr = bool_nor()
                        bool_fn("INPUT_C")
                        bool_fn("INPUT_D")
                    end_bool_nor(nr)
                end_bool_nand(nd)
            end_condition(c)
            local a = action()
                oneshot("LOG", str("complex condition met"))
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline(p)
end_tree(tree4)

-- ============================================================================
-- Tree 5: Mix old and new operators
-- Tests: Combining AND, OR, NOT with XOR, NAND, NOR
-- Expected: ((A AND B) XOR C) OR (NOT (D NAND E))
-- ============================================================================
local tree5 = start_tree("mixed_ops")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local top_or = bool_or()
                    local xr = bool_xor()
                        local inner_and = bool_and()
                            bool_fn("COND_A")
                            bool_fn("COND_B")
                        end_bool_and(inner_and)
                        bool_fn("COND_C")
                    end_bool_xor(xr)
                    local nt = bool_not()
                        local nd = bool_nand()
                            bool_fn("COND_D")
                            bool_fn("COND_E")
                        end_bool_nand(nd)
                    end_bool_not(nt)
                end_bool_or(top_or)
            end_condition(c)
            local a = action()
                oneshot("EXECUTE_ACTION")
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline(p)
end_tree(tree5)

local module = end_module(mod)

-- Dump module info
module:dump()

-- Generate C header
local header = module:to_c_header("bool_ops")
local f = io.open("bool_ops_module.h", "w")
f:write(header)
f:close()

print("\n=== Generated bool_ops_module.h ===")
print("XOR opcode: 0x0D")
print("NAND opcode: 0x0E")
print("NOR opcode: 0x0F")

