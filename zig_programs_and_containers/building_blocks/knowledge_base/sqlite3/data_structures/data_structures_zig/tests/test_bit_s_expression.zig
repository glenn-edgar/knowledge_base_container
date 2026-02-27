//! test_bit_s_expression.zig
//! Mirrors test_bit_s_expression.c — S-expression evaluator unit tests.
//! Self-contained, no database required.

const std = @import("std");
const kb = @import("kb");
const c = kb.c;

const BitData = kb.BitData;
const sexprEval = kb.sexprEval;

// ── Basic Literals ──────────────────────────────────────────────────

test "literal 1" {
    const data = BitData{ .bit_mask = 0, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("1", data));
}

test "literal 0" {
    const data = BitData{ .bit_mask = 0, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("0", data));
}

test "literal true" {
    const data = BitData{ .bit_mask = 0, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("true", data));
}

test "literal false" {
    const data = BitData{ .bit_mask = 0, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("false", data));
}

// ── Bit Access ──────────────────────────────────────────────────────

test "bit 0 of 0x05" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(bit 0)", data));
}

test "bit 1 of 0x05" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("(bit 1)", data));
}

test "bit 2 of 0x05" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(bit 2)", data));
}

test "bit_changed 0 (change_mask=0x02)" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("(bit_changed 0)", data));
}

test "bit_changed 1 (change_mask=0x02)" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(bit_changed 1)", data));
}

// ── Boolean Ops ─────────────────────────────────────────────────────

test "and 1 1" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(and 1 1)", data));
}

test "and 1 0" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("(and 1 0)", data));
}

test "or 0 0" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("(or 0 0)", data));
}

test "or 0 1" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(or 0 1)", data));
}

test "not 1" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 0), try sexprEval("(not 1)", data));
}

test "not 0" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(not 0)", data));
}

// ── If and Cond ─────────────────────────────────────────────────────

test "if true branch" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 42), try sexprEval("(if 1 42 99)", data));
}

test "if false branch" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 99), try sexprEval("(if 0 42 99)", data));
}

test "cond first-match" {
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0 };
    try std.testing.expectEqual(@as(i32, 20), try sexprEval("(cond (0 10) (1 20) (1 30))", data));
}

// ── Nested Expressions ──────────────────────────────────────────────

test "nested and/not/bit" {
    // (and (bit 0) (not (bit 1))) => (and 1 (not 0)) => (and 1 1) => 1
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(and (bit 0) (not (bit 1)))", data));
}

test "nested or with bit_changed" {
    // (or (bit 1) (bit_changed 1)) => (or 0 1) => 1
    const data = BitData{ .bit_mask = 0x05, .change_mask = 0x02 };
    try std.testing.expectEqual(@as(i32, 1), try sexprEval("(or (bit 1) (bit_changed 1))", data));
}