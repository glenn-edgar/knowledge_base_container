/*
 * test_bit_s_expression.c
 * Knowledge Base C Port — S-expression evaluator unit tests
 *
 * Self-contained, no database required.
 */

#include <stdio.h>
#include "kb_common.h"
#include "bit_s_expression.h"
#include "test_common.h"

static void test_basic_literals(void)
{
    TEST_BEGIN("basic literals");

    kb_bit_data_t data = { .bit_mask = 0, .change_mask = 0 };
    int result;

    ASSERT_OK(kb_sexpr_eval("1", &data, &result), "literal 1");
    ASSERT_EQ_INT(result, 1, "literal 1 == 1");

    ASSERT_OK(kb_sexpr_eval("0", &data, &result), "literal 0");
    ASSERT_EQ_INT(result, 0, "literal 0 == 0");

    ASSERT_OK(kb_sexpr_eval("true", &data, &result), "true");
    ASSERT_EQ_INT(result, 1, "true == 1");

    ASSERT_OK(kb_sexpr_eval("false", &data, &result), "false");
    ASSERT_EQ_INT(result, 0, "false == 0");
}

static void test_bit_access(void)
{
    TEST_BEGIN("bit access");

    kb_bit_data_t data = { .bit_mask = 0x05, .change_mask = 0x02 };
    int result;

    ASSERT_OK(kb_sexpr_eval("(bit 0)", &data, &result), "bit 0");
    ASSERT_EQ_INT(result, 1, "bit 0 of 0x05 == 1");

    ASSERT_OK(kb_sexpr_eval("(bit 1)", &data, &result), "bit 1");
    ASSERT_EQ_INT(result, 0, "bit 1 of 0x05 == 0");

    ASSERT_OK(kb_sexpr_eval("(bit 2)", &data, &result), "bit 2");
    ASSERT_EQ_INT(result, 1, "bit 2 of 0x05 == 1");

    ASSERT_OK(kb_sexpr_eval("(bit_changed 0)", &data, &result), "bit_changed 0");
    ASSERT_EQ_INT(result, 0, "change_mask bit 0 == 0");

    ASSERT_OK(kb_sexpr_eval("(bit_changed 1)", &data, &result), "bit_changed 1");
    ASSERT_EQ_INT(result, 1, "change_mask bit 1 == 1");
}

static void test_boolean_ops(void)
{
    TEST_BEGIN("boolean ops");

    kb_bit_data_t data = { .bit_mask = 0x05, .change_mask = 0 };
    int result;

    ASSERT_OK(kb_sexpr_eval("(and 1 1)", &data, &result), "and 1 1");
    ASSERT_EQ_INT(result, 1, "and(1,1) == 1");

    ASSERT_OK(kb_sexpr_eval("(and 1 0)", &data, &result), "and 1 0");
    ASSERT_EQ_INT(result, 0, "and(1,0) == 0");

    ASSERT_OK(kb_sexpr_eval("(or 0 0)", &data, &result), "or 0 0");
    ASSERT_EQ_INT(result, 0, "or(0,0) == 0");

    ASSERT_OK(kb_sexpr_eval("(or 0 1)", &data, &result), "or 0 1");
    ASSERT_EQ_INT(result, 1, "or(0,1) == 1");

    ASSERT_OK(kb_sexpr_eval("(not 1)", &data, &result), "not 1");
    ASSERT_EQ_INT(result, 0, "not(1) == 0");

    ASSERT_OK(kb_sexpr_eval("(not 0)", &data, &result), "not 0");
    ASSERT_EQ_INT(result, 1, "not(0) == 1");
}

static void test_if_cond(void)
{
    TEST_BEGIN("if and cond");

    kb_bit_data_t data = { .bit_mask = 0x05, .change_mask = 0 };
    int result;

    ASSERT_OK(kb_sexpr_eval("(if 1 42 99)", &data, &result), "if true");
    ASSERT_EQ_INT(result, 42, "if(1) => 42");

    ASSERT_OK(kb_sexpr_eval("(if 0 42 99)", &data, &result), "if false");
    ASSERT_EQ_INT(result, 99, "if(0) => 99");

    ASSERT_OK(kb_sexpr_eval("(cond (0 10) (1 20) (1 30))", &data, &result),
              "cond first-match");
    ASSERT_EQ_INT(result, 20, "cond => 20 (first true clause)");
}

static void test_nested(void)
{
    TEST_BEGIN("nested expressions");

    kb_bit_data_t data = { .bit_mask = 0x05, .change_mask = 0x02 };
    int result;

    /* (and (bit 0) (not (bit 1))) => (and 1 (not 0)) => (and 1 1) => 1 */
    ASSERT_OK(kb_sexpr_eval("(and (bit 0) (not (bit 1)))", &data, &result),
              "nested and/not/bit");
    ASSERT_EQ_INT(result, 1, "and(bit0, not(bit1)) == 1");

    /* (or (bit 1) (bit_changed 1)) => (or 0 1) => 1 */
    ASSERT_OK(kb_sexpr_eval("(or (bit 1) (bit_changed 1))", &data, &result),
              "nested or with bit_changed");
    ASSERT_EQ_INT(result, 1, "or(bit1, changed1) == 1");
}

int main(void)
{
    printf("=== S-Expression Evaluator Unit Tests ===\n");

    test_basic_literals();
    test_bit_access();
    test_boolean_ops();
    test_if_cond();
    test_nested();

    TEST_END();
}
