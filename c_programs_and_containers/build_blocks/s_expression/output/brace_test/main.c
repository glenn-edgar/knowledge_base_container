/*
 * test_brace_verify.c - Verify brace_idx values in generated binary modules
 * 
 * This test loads a binary module and verifies that:
 * 1. Every OPEN has a matching CLOSE
 * 2. brace_idx in OPEN equals distance to matching CLOSE
 * 3. brace_idx in CLOSE equals distance back to OPEN
 * 
 * Compile: gcc -o test_brace_verify test_brace_verify.c
 * Run: ./test_brace_verify test_brace_lists.bin -v
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <stdint.h>
 #include <string.h>
 #include <stdbool.h>
 
 /* Parameter type enum - must match s_expr_param.h */
 typedef enum {
     S_EXPR_PARAM_INT         = 0x00,
     S_EXPR_PARAM_UINT        = 0x01,
     S_EXPR_PARAM_FLOAT       = 0x02,
     S_EXPR_PARAM_STR_HASH    = 0x03,
     S_EXPR_PARAM_SLOT        = 0x04,
     S_EXPR_PARAM_OPEN        = 0x05,
     S_EXPR_PARAM_CLOSE       = 0x06,
     S_EXPR_PARAM_OPEN_CALL   = 0x07,
     S_EXPR_PARAM_ONESHOT     = 0x08,
     S_EXPR_PARAM_MAIN        = 0x09,
     S_EXPR_PARAM_PRED        = 0x0A,
     S_EXPR_PARAM_FIELD       = 0x0B,
     S_EXPR_PARAM_RESULT      = 0x0C,
     S_EXPR_PARAM_STR_IDX     = 0x0D,
     S_EXPR_PARAM_CONST_REF   = 0x0E,
 } s_expr_param_type_t;
 
 static const char* param_type_name(uint8_t type) {
     static const char* names[] = {
         "INT", "UINT", "FLOAT", "STR_HASH", "SLOT", "OPEN", "CLOSE",
         "OPEN_CALL", "ONESHOT", "MAIN", "PRED", "FIELD", "RESULT", 
         "STR_IDX", "CONST_REF"
     };
     if (type < sizeof(names)/sizeof(names[0])) {
         return names[type];
     }
     return "UNKNOWN";
 }
 
 /* Parameter struct - 8 bytes for ARM alignment
  * Layout:
  *   offset 0: type (1 byte)
  *   offset 1: index_to_pointer (1 byte)
  *   offset 2-3: padding (2 bytes)
  *   offset 4-7: union (4 bytes)
  */
 #pragma pack(push, 1)
 typedef struct {
     uint8_t  type;              /* opcode */
     uint8_t  index_to_pointer;  /* pointer array index */
     uint16_t _padding;          /* alignment padding */
     union {
         struct { uint16_t a; uint16_t b; } ab;  /* generic access */
         struct { uint16_t node_index; uint16_t func_index; } func;
         struct { uint16_t field_offset; uint16_t field_size; } field;
         struct { uint16_t str_index; uint16_t str_len; } str;
         uint16_t brace_idx;     /* for OPEN, OPEN_CALL, CLOSE (only uses first 2 bytes) */
         int32_t  i32;
         uint32_t u32;
         float    f32;
     };
 } s_expr_param_t;
 #pragma pack(pop)
 
 /* Binary module header - matches actual generator output */
 #pragma pack(push, 1)
 typedef struct {
     uint32_t magic;          /* "SEXB" */
     uint16_t version;
     uint16_t flags;
     uint32_t module_hash;
     uint16_t tree_count;
     uint16_t record_count;
     uint16_t const_count;
     uint16_t string_count;
     uint16_t reserved1;
     uint16_t reserved2;
     uint32_t reserved3;
     uint32_t total_size;
     /* Header is 32 bytes, followed by offset table, then tree defs at 0x40 */
 } module_header_t;
 
 /* Tree definition - 20 bytes each, packed sequentially after header+offset_table */
 typedef struct {
     uint32_t name_hash;
     uint32_t record_hash;
     uint32_t func_node_count;  /* or flags - always 2 in test file */
     uint32_t param_offset;
     uint32_t param_count;
 } tree_def_t;
 #pragma pack(pop)
 
 #define TREE_DEFS_OFFSET 0x40  /* Trees start at offset 0x40 */
 
 /* Stack for tracking OPEN braces */
 #define MAX_STACK 64
 typedef struct {
     uint32_t index;
     uint16_t brace_idx;
     uint8_t  type;  /* S_EXPR_PARAM_OPEN or S_EXPR_PARAM_OPEN_CALL */
 } brace_entry_t;
 
 typedef struct {
     brace_entry_t entries[MAX_STACK];
     int top;
 } brace_stack_t;
 
 static void stack_init(brace_stack_t* s) {
     s->top = -1;
 }
 
 static bool stack_push(brace_stack_t* s, uint32_t index, uint16_t brace_idx, uint8_t type) {
     if (s->top >= MAX_STACK - 1) {
         fprintf(stderr, "ERROR: Brace stack overflow at index %u\n", index);
         return false;
     }
     s->top++;
     s->entries[s->top].index = index;
     s->entries[s->top].brace_idx = brace_idx;
     s->entries[s->top].type = type;
     return true;
 }
 
 static bool stack_pop(brace_stack_t* s, brace_entry_t* out) {
     if (s->top < 0) {
         return false;
     }
     *out = s->entries[s->top];
     s->top--;
     return true;
 }
 
 /* Verify brace pairs in a tree's parameter array
  * 
  * OPEN_CALL: brace_idx = content_count (params inside, excluding OPEN_CALL and CLOSE)
  * OPEN:      brace_idx = distance to matching CLOSE
  * CLOSE:    brace_idx = distance back to matching OPEN (0 for OPEN_CALL match)
  */
 static int verify_tree_braces(const char* tree_name, const s_expr_param_t* params, 
                                uint32_t param_count, bool verbose) {
     brace_stack_t stack;
     stack_init(&stack);
     
     int errors = 0;
     
     if (verbose) {
         printf("\n--- Tree: %s (%u params) ---\n", tree_name, param_count);
         printf("%-5s %-12s %-8s %-10s %-10s\n", "IDX", "TYPE", "PTR", "A/BRACE", "B");
         printf("----------------------------------------------\n");
     }
     
     for (uint32_t i = 0; i < param_count; i++) {
         const s_expr_param_t* p = &params[i];
         
         if (verbose) {
             const char* label = "";
             if (p->type == S_EXPR_PARAM_OPEN_CALL) label = " (content)";
             else if (p->type == S_EXPR_PARAM_OPEN) label = " (brace)";
             else if (p->type == S_EXPR_PARAM_CLOSE) label = " (brace)";
             
             printf("%-5u %-12s %-8u %-10u%-8s %-10u\n",
                    i, param_type_name(p->type), p->index_to_pointer,
                    p->ab.a, label, p->ab.b);
         }
         
         if (p->type == S_EXPR_PARAM_OPEN_CALL) {
             /* OPEN_CALL: brace_idx = content_count */
             if (!stack_push(&stack, i, p->brace_idx, p->type)) {
                 errors++;
             }
         }
         else if (p->type == S_EXPR_PARAM_OPEN) {
             /* OPEN: brace_idx = distance to matching CLOSE */
             if (!stack_push(&stack, i, p->brace_idx, p->type)) {
                 errors++;
             }
         }
         else if (p->type == S_EXPR_PARAM_CLOSE) {
             brace_entry_t open;
             if (!stack_pop(&stack, &open)) {
                 printf("  ERROR: CLOSE at %u has no matching OPEN/OPEN_CALL\n", i);
                 errors++;
                 continue;
             }
             
             uint32_t actual_distance = i - open.index;
             
             if (open.type == S_EXPR_PARAM_OPEN_CALL) {
                 /* OPEN_CALL stores content_count, not distance */
                 /* content_count = distance - 1 (excludes OPEN_CALL itself) */
                 uint32_t expected_content = actual_distance - 1;
                 if (open.brace_idx != expected_content) {
                     printf("  ERROR: OPEN_CALL at %u: content_count=%u, expected=%u\n",
                            open.index, open.brace_idx, expected_content);
                     errors++;
                 }
                 /* CLOSE matching OPEN_CALL should have brace_idx=0 */
                 if (p->brace_idx != 0) {
                     printf("  WARN: CLOSE at %u matching OPEN_CALL: brace_idx=%u (expected 0)\n",
                            i, p->brace_idx);
                 }
             }
             else {
                 /* OPEN stores brace_idx (distance) */
                 if (open.brace_idx != actual_distance) {
                     printf("  ERROR: OPEN at %u: brace_idx=%u, expected=%u\n",
                            open.index, open.brace_idx, actual_distance);
                     errors++;
                 }
                 /* CLOSE should also have matching brace_idx */
                 if (p->brace_idx != actual_distance) {
                     printf("  ERROR: CLOSE at %u: brace_idx=%u, expected=%u\n",
                            i, p->brace_idx, actual_distance);
                     errors++;
                 }
             }
         }
     }
     
     /* Check for unmatched OPENs */
     while (stack.top >= 0) {
         brace_entry_t unmatched;
         stack_pop(&stack, &unmatched);
         printf("  ERROR: %s at %u has no matching CLOSE\n",
                (unmatched.type == S_EXPR_PARAM_OPEN_CALL) ? "OPEN_CALL" : "OPEN",
                unmatched.index);
         errors++;
     }
     
     return errors;
 }
 
 /* Load and verify a binary module */
 static int verify_module(const char* filename, bool verbose) {
     FILE* f = fopen(filename, "rb");
     if (!f) {
         perror("Failed to open file");
         return -1;
     }
     
     /* Read entire file */
     fseek(f, 0, SEEK_END);
     long file_size = ftell(f);
     fseek(f, 0, SEEK_SET);
     
     uint8_t* data = malloc(file_size);
     if (!data) {
         fprintf(stderr, "Failed to allocate file buffer\n");
         fclose(f);
         return -1;
     }
     
     if (fread(data, 1, file_size, f) != (size_t)file_size) {
         fprintf(stderr, "Failed to read file data\n");
         free(data);
         fclose(f);
         return -1;
     }
     fclose(f);
     
     /* Parse header */
     module_header_t* header = (module_header_t*)data;
     
     if (header->magic != 0x42584553) { /* "SEXB" little-endian */
         fprintf(stderr, "Invalid magic: 0x%08X (expected 0x42584553 'SEXB')\n", header->magic);
         free(data);
         return -1;
     }
     
     printf("Module: %s\n", filename);
     printf("  Magic: %.4s\n", (char*)&header->magic);
     printf("  Version: 0x%04X, Flags: 0x%04X\n", header->version, header->flags);
     printf("  Module hash: 0x%08X\n", header->module_hash);
     printf("  Trees: %u, Records: %u, Constants: %u, Strings: %u\n",
            header->tree_count, header->record_count, 
            header->const_count, header->string_count);
     printf("  Total size: %u bytes (file: %ld bytes)\n", header->total_size, file_size);
     printf("  sizeof(s_expr_param_t) = %zu (expected 8)\n", sizeof(s_expr_param_t));
     
     /* Tree definitions start at fixed offset 0x40 */
     tree_def_t* trees = (tree_def_t*)(data + TREE_DEFS_OFFSET);
     
     if (verbose) {
         printf("\n  Tree definitions at 0x%X (%u trees, %zu bytes each):\n", 
                TREE_DEFS_OFFSET, header->tree_count, sizeof(tree_def_t));
     }
     
     int total_errors = 0;
     
     for (uint16_t t = 0; t < header->tree_count; t++) {
         tree_def_t* tree = &trees[t];
         uint32_t tree_offset = TREE_DEFS_OFFSET + t * sizeof(tree_def_t);
         
         if (verbose) {
             printf("\n  Tree %u @ 0x%X:\n", t, tree_offset);
             printf("    name_hash: 0x%08X\n", tree->name_hash);
             printf("    record_hash: 0x%08X\n", tree->record_hash);
             printf("    func_node_count: %u\n", tree->func_node_count);
             printf("    param_offset: 0x%08X (%u)\n", tree->param_offset, tree->param_offset);
             printf("    param_count: %u\n", tree->param_count);
         }
         
         /* Verify param_offset is within bounds */
         if (tree->param_offset + tree->param_count * sizeof(s_expr_param_t) > (uint32_t)file_size) {
             printf("  Tree %u: param_offset 0x%X + %u params (%zu bytes) out of bounds\n", 
                    t, tree->param_offset, tree->param_count,
                    tree->param_count * sizeof(s_expr_param_t));
             total_errors++;
             continue;
         }
         
         s_expr_param_t* params = (s_expr_param_t*)(data + tree->param_offset);
         
         char tree_name[48];
         snprintf(tree_name, sizeof(tree_name), "tree_%u (hash=0x%08X)", t, tree->name_hash);
         
         int errors = verify_tree_braces(tree_name, params, tree->param_count, verbose);
         total_errors += errors;
         
         if (errors == 0) {
             printf("  ✓ %s: OK (%u params)\n", tree_name, tree->param_count);
         } else {
             printf("  ✗ %s: %d errors\n", tree_name, errors);
         }
     }
     
     free(data);
     
     return total_errors;
 }
 
 /* Self-test with synthetic data */
 static int run_self_test(void) {
     printf("\n=== Self Test ===\n");
     printf("sizeof(s_expr_param_t) = %zu (expected 8)\n", sizeof(s_expr_param_t));
     
     if (sizeof(s_expr_param_t) != 8) {
         printf("FATAL: s_expr_param_t size mismatch!\n");
         return -1;
     }
     
     /* Test 1: Simple list [1, 2, 3]
      * Layout: OPEN_CALL(0), MAIN(1), OPEN(2), UINT(3), UINT(4), UINT(5), CLOSE(6), CLOSE(7)
      * OPEN_CALL content_count = 6 (indices 1-6)
      * OPEN brace_idx = 4 (distance from 2 to 6)
      */
     s_expr_param_t test1[] = {
         { S_EXPR_PARAM_OPEN_CALL, 0, 0, { .brace_idx = 6 } },
         { S_EXPR_PARAM_MAIN,      0, 0, { .ab = { 0, 0 } } },
         { S_EXPR_PARAM_OPEN,      0, 0, { .brace_idx = 4 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 1 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 2 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 3 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 4 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 0 } },
     };
     
     int err1 = verify_tree_braces("test_simple_list", test1, 
                                    sizeof(test1)/sizeof(test1[0]), true);
     printf("Test 1 (simple list): %s\n", err1 == 0 ? "PASS" : "FAIL");
     
     /* Test 2: Nested lists [[1, 2], 3]
      * Layout: OPEN_CALL(0), MAIN(1), OPEN(2), OPEN(3), UINT(4), UINT(5), CLOSE(6), UINT(7), CLOSE(8), CLOSE(9)
      */
     s_expr_param_t test2[] = {
         { S_EXPR_PARAM_OPEN_CALL, 0, 0, { .brace_idx = 8 } },
         { S_EXPR_PARAM_MAIN,      0, 0, { .ab = { 0, 0 } } },
         { S_EXPR_PARAM_OPEN,      0, 0, { .brace_idx = 6 } },
         { S_EXPR_PARAM_OPEN,      0, 0, { .brace_idx = 3 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 1 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 2 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 3 } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 3 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 6 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 0 } },
     };
     
     int err2 = verify_tree_braces("test_nested_list", test2,
                                    sizeof(test2)/sizeof(test2[0]), true);
     printf("Test 2 (nested list): %s\n", err2 == 0 ? "PASS" : "FAIL");
     
     /* Test 3: Dispatch with child - [pattern, CHILD_NODE] */
     s_expr_param_t test3[] = {
         { S_EXPR_PARAM_OPEN_CALL, 0, 0, { .brace_idx = 8 } },
         { S_EXPR_PARAM_MAIN,      0, 0, { .ab = { 0, 0 } } },
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 100 } },
         { S_EXPR_PARAM_OPEN,      0, 0, { .brace_idx = 5 } },
         { S_EXPR_PARAM_STR_IDX,   0, 0, { .ab = { 0, 0 } } },
         { S_EXPR_PARAM_OPEN_CALL, 0, 0, { .brace_idx = 1 } },
         { S_EXPR_PARAM_MAIN,      0, 0, { .ab = { 1, 1 } } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 0 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 5 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 0 } },
     };
     
     int err3 = verify_tree_braces("test_dispatch_child", test3,
                                    sizeof(test3)/sizeof(test3[0]), true);
     printf("Test 3 (dispatch with child): %s\n", err3 == 0 ? "PASS" : "FAIL");
     
     /* Test 4: Intentionally wrong - should fail */
     printf("\n--- Test 4: Intentionally wrong brace_idx (should FAIL) ---\n");
     s_expr_param_t test4[] = {
         { S_EXPR_PARAM_OPEN_CALL, 0, 0, { .brace_idx = 4 } },
         { S_EXPR_PARAM_MAIN,      0, 0, { .ab = { 0, 0 } } },
         { S_EXPR_PARAM_OPEN,      0, 0, { .brace_idx = 99 } },  /* WRONG! */
         { S_EXPR_PARAM_UINT,      0, 0, { .u32 = 1 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 2 } },
         { S_EXPR_PARAM_CLOSE,     0, 0, { .brace_idx = 0 } },
     };
     
     int err4 = verify_tree_braces("test_wrong_brace", test4,
                                    sizeof(test4)/sizeof(test4[0]), true);
     printf("Test 4 (wrong brace): %s (expected FAIL)\n", err4 > 0 ? "CORRECTLY FAILED" : "UNEXPECTED PASS");
     
     int total = err1 + err2 + err3;
     printf("\n=== Self Test Summary: %d errors in valid tests ===\n", total);
     
     return total;
 }
 
 int main(int argc, char* argv[]) {
     bool verbose = false;
     const char* filename = NULL;
     #if 0
     for (int i = 1; i < argc; i++) {
         if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
             verbose = true;
         } else if (strcmp(argv[i], "--self-test") == 0) {
             return run_self_test();
         } else {
             filename = argv[i];
         }
     }
     
     if (!filename) {
         printf("Usage: %s [options] <module.bin>\n", argv[0]);
         printf("Options:\n");
         printf("  -v, --verbose    Show detailed parameter dump\n");
         printf("  --self-test      Run built-in self tests\n");
         printf("\nOr run self-test:\n");
         printf("  %s --self-test\n", argv[0]);
         return 1;
     }
     #endif
     int errors = verify_module("s_expr_dsl_test_32.bin", true);
     
     if (errors < 0) {
         printf("Failed to load module\n");
         return 1;
     }
     
     printf("\n=== Total: %d brace errors ===\n", errors);
     return errors > 0 ? 1 : 0;
 }