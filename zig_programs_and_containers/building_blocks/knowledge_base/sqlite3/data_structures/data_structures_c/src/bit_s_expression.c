/*
 * bit_s_expression.c
 * Knowledge Base C Port — S-expression evaluator for bit masks
 *
 * Mirrors LuaJIT bit_s_expression.lua.
 * Recursive descent parser/evaluator for S-expressions:
 *   (and expr ...)  (or expr ...)  (not expr)
 *   (if cond then else)  (cond (test result) ...)
 *   (bit N)  (bit_changed N)  integer_literal
 */

#include "bit_s_expression.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* ================================================================
 * Tokenizer
 * ================================================================ */

typedef enum {
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_SYMBOL,
    TOK_NUMBER,
    TOK_EOF,
    TOK_ERROR,
} tok_type_t;

typedef struct {
    tok_type_t type;
    char       text[64];
} token_t;

typedef struct {
    const char *src;
    int         pos;
    int         len;
} lexer_t;

static void lexer_init(lexer_t *lex, const char *src)
{
    lex->src = src;
    lex->pos = 0;
    lex->len = (int)strlen(src);
}

static void skip_whitespace(lexer_t *lex)
{
    while (lex->pos < lex->len && isspace((unsigned char)lex->src[lex->pos])) {
        lex->pos++;
    }
}

static token_t next_token(lexer_t *lex)
{
    token_t tok;
    memset(&tok, 0, sizeof(tok));

    skip_whitespace(lex);

    if (lex->pos >= lex->len) {
        tok.type = TOK_EOF;
        return tok;
    }

    char c = lex->src[lex->pos];

    if (c == '(') {
        tok.type = TOK_LPAREN;
        tok.text[0] = '(';
        lex->pos++;
        return tok;
    }

    if (c == ')') {
        tok.type = TOK_RPAREN;
        tok.text[0] = ')';
        lex->pos++;
        return tok;
    }

    /* Number: digits, optionally preceded by minus */
    if (isdigit((unsigned char)c) ||
        (c == '-' && lex->pos + 1 < lex->len &&
         isdigit((unsigned char)lex->src[lex->pos + 1]))) {
        int start = lex->pos;
        if (c == '-') lex->pos++;
        while (lex->pos < lex->len &&
               isdigit((unsigned char)lex->src[lex->pos])) {
            lex->pos++;
        }
        int len = lex->pos - start;
        if (len >= (int)sizeof(tok.text)) len = (int)sizeof(tok.text) - 1;
        memcpy(tok.text, lex->src + start, (size_t)len);
        tok.text[len] = '\0';
        tok.type = TOK_NUMBER;
        return tok;
    }

    /* Symbol: alphanumeric + underscore */
    if (isalpha((unsigned char)c) || c == '_') {
        int start = lex->pos;
        while (lex->pos < lex->len &&
               (isalnum((unsigned char)lex->src[lex->pos]) ||
                lex->src[lex->pos] == '_')) {
            lex->pos++;
        }
        int len = lex->pos - start;
        if (len >= (int)sizeof(tok.text)) len = (int)sizeof(tok.text) - 1;
        memcpy(tok.text, lex->src + start, (size_t)len);
        tok.text[len] = '\0';
        tok.type = TOK_SYMBOL;
        return tok;
    }

    tok.type = TOK_ERROR;
    return tok;
}

/* ================================================================
 * Parser / evaluator context
 * ================================================================ */

typedef struct {
    lexer_t            lex;
    token_t            current;
    const kb_bit_data_t *bit_data;
    kb_error_t         error;
} eval_ctx_t;

static void advance(eval_ctx_t *ctx)
{
    ctx->current = next_token(&ctx->lex);
}

static int eval_expr(eval_ctx_t *ctx);

/* ================================================================
 * Expression evaluator (recursive descent)
 * ================================================================ */

static int eval_expr(eval_ctx_t *ctx)
{
    if (ctx->error != KB_OK) return 0;

    /* Number literal */
    if (ctx->current.type == TOK_NUMBER) {
        int val = atoi(ctx->current.text);
        advance(ctx);
        return val;
    }

    /* Symbol: true=1, false=0 */
    if (ctx->current.type == TOK_SYMBOL) {
        if (strcmp(ctx->current.text, "true") == 0) {
            advance(ctx);
            return 1;
        }
        if (strcmp(ctx->current.text, "false") == 0) {
            advance(ctx);
            return 0;
        }
        ctx->error = KB_ERR_INVALID;
        return 0;
    }

    /* S-expression: ( operator args... ) */
    if (ctx->current.type == TOK_LPAREN) {
        advance(ctx); /* consume '(' */

        if (ctx->current.type != TOK_SYMBOL) {
            ctx->error = KB_ERR_INVALID;
            return 0;
        }

        char op[64];
        strncpy(op, ctx->current.text, sizeof(op) - 1);
        op[sizeof(op) - 1] = '\0';
        advance(ctx); /* consume operator symbol */

        int result = 0;

        /* (and expr expr ...) */
        if (strcmp(op, "and") == 0) {
            result = 1;
            while (ctx->current.type != TOK_RPAREN &&
                   ctx->current.type != TOK_EOF &&
                   ctx->error == KB_OK) {
                int val = eval_expr(ctx);
                if (!val) result = 0;
            }
        }
        /* (or expr expr ...) */
        else if (strcmp(op, "or") == 0) {
            result = 0;
            while (ctx->current.type != TOK_RPAREN &&
                   ctx->current.type != TOK_EOF &&
                   ctx->error == KB_OK) {
                int val = eval_expr(ctx);
                if (val) result = 1;
            }
        }
        /* (not expr) */
        else if (strcmp(op, "not") == 0) {
            int val = eval_expr(ctx);
            result = val ? 0 : 1;
        }
        /* (if cond then_expr else_expr) */
        else if (strcmp(op, "if") == 0) {
            int cond = eval_expr(ctx);
            int then_val = eval_expr(ctx);
            int else_val = 0;
            if (ctx->current.type != TOK_RPAREN &&
                ctx->current.type != TOK_EOF) {
                else_val = eval_expr(ctx);
            }
            result = cond ? then_val : else_val;
        }
        /* (cond (test1 result1) (test2 result2) ...) */
        else if (strcmp(op, "cond") == 0) {
            result = 0;
            bool matched = false;
            while (ctx->current.type == TOK_LPAREN &&
                   ctx->error == KB_OK && !matched) {
                advance(ctx); /* consume '(' of clause */
                int test = eval_expr(ctx);
                int clause_result = eval_expr(ctx);

                /* consume ')' of clause */
                if (ctx->current.type == TOK_RPAREN) {
                    advance(ctx);
                }

                if (test && !matched) {
                    result = clause_result;
                    matched = true;
                }
            }
            /* Skip remaining clauses */
            while (ctx->current.type == TOK_LPAREN &&
                   ctx->error == KB_OK) {
                advance(ctx);
                eval_expr(ctx);
                eval_expr(ctx);
                if (ctx->current.type == TOK_RPAREN) advance(ctx);
            }
        }
        /* (bit N) — get bit value at position N */
        else if (strcmp(op, "bit") == 0) {
            int pos = eval_expr(ctx);
            if (ctx->bit_data) {
                result = (int)((ctx->bit_data->bit_mask >> pos) & 1);
            }
        }
        /* (bit_changed N) — check if bit at position N changed */
        else if (strcmp(op, "bit_changed") == 0) {
            int pos = eval_expr(ctx);
            if (ctx->bit_data) {
                result = (int)((ctx->bit_data->change_mask >> pos) & 1);
            }
        }
        else {
            /* Unknown operator */
            ctx->error = KB_ERR_INVALID;
            return 0;
        }

        /* Consume closing ')' */
        if (ctx->current.type == TOK_RPAREN) {
            advance(ctx);
        } else if (ctx->error == KB_OK) {
            ctx->error = KB_ERR_INVALID;
        }

        return result;
    }

    /* Unexpected token */
    ctx->error = KB_ERR_INVALID;
    return 0;
}

/* ================================================================
 * Public API
 * ================================================================ */

kb_error_t kb_sexpr_eval(const char *expr, const kb_bit_data_t *bit_data,
                          int *result)
{
    if (!expr || !bit_data || !result) return KB_ERR_NULL_ARG;

    eval_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    lexer_init(&ctx.lex, expr);
    ctx.bit_data = bit_data;
    ctx.error = KB_OK;

    advance(&ctx); /* prime the first token */
    *result = eval_expr(&ctx);

    return ctx.error;
}
