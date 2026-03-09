#!/usr/bin/env luajit
--[[
  test_bit_s_expression.lua — LuaJIT port of bit_s_expression.py __main__ block

  Runs the same tests as the Python original to verify S-expression evaluation.

  Usage:
    luajit test_bit_s_expression.lua
]]

local bse = require('bit_s_expression')
local KB_BIT_DATA          = bse.KB_BIT_DATA
local SExpressionProcessor = bse.SExpressionProcessor

-- ── Create test data (matches Python exactly) ───────────────────────────

local kb_data = {
    user1 = KB_BIT_DATA.new({
        user_name   = 'user1',
        flag_data   = { flag1 = 1, flag2 = 0, flag3 = 1 },
        flag_change = { flag1 = true, flag2 = false, flag3 = true },
    }),
    user2 = KB_BIT_DATA.new({
        user_name   = 'user2',
        flag_data   = { flag1 = 1, flag2 = 1, flag3 = 0 },
        flag_change = { flag1 = true, flag2 = true, flag3 = false },
    }),
    user3 = KB_BIT_DATA.new({
        user_name   = 'user3',
        flag_data   = { flag1 = 0, flag2 = 1 },
        flag_change = { flag1 = false, flag2 = true },
    }),
}

local processor = SExpressionProcessor.new()

-- ── Test bit_changed ────────────────────────────────────────────────────
print("Test bit_changed:")
local result = processor:execute("(bit_changed user1:flag1 user2:flag1)", kb_data)
print(string.format("  (bit_changed user1:flag1 user2:flag1) = %s", tostring(result)))
-- Expected: true

-- ── Test and ────────────────────────────────────────────────────────────
print("\nTest and:")
result = processor:execute("(and user1:flag1 user2:flag1)", kb_data)
print(string.format("  (and user1:flag1 user2:flag1) = %s", tostring(result)))
-- Expected: true

-- ── Test or ─────────────────────────────────────────────────────────────
print("\nTest or:")
result = processor:execute("(or user1:flag2 user3:flag2)", kb_data)
print(string.format("  (or user1:flag2 user3:flag2) = %s", tostring(result)))
-- Expected: true

-- ── Test if ─────────────────────────────────────────────────────────────
print("\nTest if:")
result = processor:execute("(if (bit_changed user1:flag1) user2:flag1 user2:flag3)", kb_data)
print(string.format("  (if (bit_changed user1:flag1) user2:flag1 user2:flag3) = %s",
      tostring(result)))
-- Expected: true

result = processor:execute("(if user1:flag2 user2:flag1 user2:flag2)", kb_data)
print(string.format("  (if user1:flag2 user2:flag1 user2:flag2) = %s", tostring(result)))
-- Expected: true (flag2=0 so else branch → user2:flag2=1)

-- ── Test cond ───────────────────────────────────────────────────────────
print("\nTest cond:")
result = processor:execute(
    "(cond ((bit_changed user1:flag1) user2:flag1) "
    .. "((and user1:flag2 user1:flag3) user2:flag2) "
    .. "(else user3:flag1))",
    kb_data)
print(string.format("  (cond ...) = %s", tostring(result)))
-- Expected: true (first condition matches)

result = processor:execute(
    "(cond ((and user1:flag2 user2:flag3) user1:flag1) "
    .. "((bit_changed user3:flag1) user2:flag2) "
    .. "(else user3:flag2))",
    kb_data)
print(string.format("  (cond with else) = %s", tostring(result)))
-- Expected: true (else clause)

-- ── Test nested if in pipeline ──────────────────────────────────────────
print("\nTest nested with if:")
result = processor:execute(
    "(and (if (bit_changed user1:flag1) user2:flag1 user2:flag3) user3:flag2)",
    kb_data)
print(string.format("  (and (if ...) user3:flag2) = %s", tostring(result)))
-- Expected: true

-- ── Test nested cond in pipeline ────────────────────────────────────────
print("\nTest nested with cond:")
result = processor:execute(
    "(or (cond ((bit_changed user1:flag1) user2:flag1) (else user2:flag3)) user3:flag1)",
    kb_data)
print(string.format("  (or (cond ...) user3:flag1) = %s", tostring(result)))
-- Expected: true

print("\nAll S-expression tests completed!")