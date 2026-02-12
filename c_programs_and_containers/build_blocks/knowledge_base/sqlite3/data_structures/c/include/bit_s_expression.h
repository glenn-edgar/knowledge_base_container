/*
 * bit_s_expression.h
 * Knowledge Base C Port — S-expression evaluator for bit masks
 *
 * Mirrors LuaJIT bit_s_expression.lua.
 * Evaluates S-expressions over bit mask data:
 *   (and expr1 expr2 ...)
 *   (or  expr1 expr2 ...)
 *   (not expr)
 *   (if cond then else)
 *   (cond (test1 result1) (test2 result2) ...)
 *   (bit_changed bit_position)
 *   (bit bit_position)         -- get bit value
 *   integer_literal            -- 0 or 1
 *
 * Operates against a KB_BIT_DATA context containing the current
 * bit_mask and change_mask values.
 */

#ifndef BIT_S_EXPRESSION_H
#define BIT_S_EXPRESSION_H

#include "kb_common.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Context for S-expression evaluation */
typedef struct {
    int64_t bit_mask;
    int64_t change_mask;
} kb_bit_data_t;

/*
 * Evaluate an S-expression string against bit data.
 *
 * @param expr      S-expression string to evaluate
 * @param bit_data  Current bit mask and change mask context
 * @param result    Output: evaluation result (0 or 1 for boolean ops)
 *
 * Returns KB_OK on success, KB_ERR_INVALID on parse/eval error.
 */
kb_error_t kb_sexpr_eval(const char *expr, const kb_bit_data_t *bit_data,
                          int *result);

#ifdef __cplusplus
}
#endif

#endif /* BIT_S_EXPRESSION_H */
