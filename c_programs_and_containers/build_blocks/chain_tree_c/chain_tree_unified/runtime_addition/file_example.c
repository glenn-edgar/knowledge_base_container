/* ChainTree File Loader Example
 * 
 * Demonstrates loading a ChainTree binary file and accessing the runtime.
 * 
 * Build:
 *   gcc -o ct_example example_file_loader.c chaintree_file_loader.c \
 *       chaintree_binary_support.c -Wall -Wextra
 * 
 * Usage:
 *   ./ct_example <binary_file.bin>
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include "chaintree_file_loader.h"
 
 /* ===== Example Function Implementations ===== */
 
 /* CFL_NULL - does nothing, returns 0 */
 static unsigned cfl_null_main(void *handle, unsigned bool_idx,
     unsigned node_idx, unsigned event_type, unsigned event_id, void *event_data)
 {
     (void)handle; (void)bool_idx; (void)node_idx;
     (void)event_type; (void)event_id; (void)event_data;
     return 0;
 }
 
 static void cfl_null_one_shot(void *handle, unsigned node_idx) {
     (void)handle; (void)node_idx;
 }
 
 static bool cfl_null_boolean(void *handle, unsigned node_idx,
     unsigned event_type, unsigned event_id, void *event_data)
 {
     (void)handle; (void)node_idx;
     (void)event_type; (void)event_id; (void)event_data;
     return false;
 }
 
 /* Generic placeholder implementations */
 static unsigned generic_main(void *handle, unsigned bool_idx,
     unsigned node_idx, unsigned event_type, unsigned event_id, void *event_data)
 {
     (void)handle; (void)bool_idx; (void)event_data;
     printf("  [MAIN] node=%u event_type=%u event_id=%u\n", 
            node_idx, event_type, event_id);
     return 0;
 }
 
 static void generic_one_shot(void *handle, unsigned node_idx) {
     (void)handle;
     printf("  [ONE_SHOT] node=%u\n", node_idx);
 }
 
 static bool generic_boolean(void *handle, unsigned node_idx,
     unsigned event_type, unsigned event_id, void *event_data)
 {
     (void)handle; (void)event_data;
     printf("  [BOOLEAN] node=%u event_type=%u event_id=%u -> true\n",
            node_idx, event_type, event_id);
     return true;
 }
 
 /* ===== Function Resolvers ===== */
 
 static main_function_t resolve_main(uint32_t hash) {
     if (hash == 0x00000000) {  /* CFL_NULL */
         return cfl_null_main;
     }
     /* For this example, return generic handler for all other functions */
     printf("Resolving main function hash: 0x%08X\n", hash);
     return generic_main;
 }
 
 static one_shot_function_t resolve_one_shot(uint32_t hash) {
     if (hash == 0x00000000) {  /* CFL_NULL */
         return cfl_null_one_shot;
     }
     printf("Resolving one-shot function hash: 0x%08X\n", hash);
     return generic_one_shot;
 }
 
 static boolean_function_t resolve_boolean(uint32_t hash) {
     if (hash == 0x00000000) {  /* CFL_NULL */
         return cfl_null_boolean;
     }
     printf("Resolving boolean function hash: 0x%08X\n", hash);
     return generic_boolean;
 }
 
 /* ===== Main Program ===== */
 
 static void print_node_info(const chaintree_runtime_t *rt, uint16_t idx) {
     const chaintree_binary_node_t *node = CT_GET_NODE(rt, idx);
     
     printf("  Node[%u]:\n", idx);
     printf("    parent=%u, depth=%u\n", node->parent_index, node->depth);
     printf("    links: start=%u, count=%u, auto_start=%s\n",
            node->link_start, 
            CT_NODE_LINK_COUNT(node),
            CT_NODE_AUTO_START(node) ? "yes" : "no");
     printf("    functions: main=%u, init=%u, aux=%u, term=%u\n",
            node->main_function_index,
            node->init_function_index,
            node->aux_function_index,
            node->term_function_index);
     
     /* Print children */
     uint16_t link_count = CT_NODE_LINK_COUNT(node);
     if (link_count > 0) {
         printf("    children: ");
         for (uint16_t i = 0; i < link_count; i++) {
             uint16_t child_idx = CT_GET_LINK(rt, node->link_start + i);
             printf("%u ", child_idx);
         }
         printf("\n");
     }
 }
 
 static void print_kb_info(const chaintree_runtime_t *rt, uint16_t idx) {
     const chaintree_binary_kb_info_t *kb = CT_GET_KB(rt, idx);
     
     printf("  KB[%u]:\n", idx);
     printf("    name_hash=0x%08X\n", kb->kb_name_hash);
     printf("    root=%u, start=%u, count=%u\n",
            kb->root_node_index, kb->start_index, kb->node_count);
     printf("    max_depth=%u, memory_factor=%u\n",
            kb->max_depth, kb->memory_factor);
     printf("    aliases: count=%u, offset=%u\n",
            kb->alias_count, kb->aliases_offset);
 }
 
 int main(int argc, char *argv[]) {
     ct_file_handle_t handle;
     ct_file_result_t result;
     
     if (argc < 2) {
         fprintf(stderr, "Usage: %s <binary_file.bin>\n", argv[0]);
         return 1;
     }
     
     const char *filepath = argv[1];
     
     /* First, verify the file */
     printf("Verifying: %s\n", filepath);
     result = ct_file_verify(filepath);
     if (result != CT_FILE_OK) {
         fprintf(stderr, "Verification failed: %s\n", ct_file_result_str(result));
         return 1;
     }
     printf("  Verification OK\n\n");
     
     /* Get the unique ID */
     uint32_t id_hash;
     result = ct_file_get_id(filepath, &id_hash);
     if (result == CT_FILE_OK) {
         printf("Unique ID: 0x%08X\n\n", id_hash);
     }
     
     /* Set up resolvers */
     ct_resolver_t resolver = {
         .resolve_main = resolve_main,
         .resolve_one_shot = resolve_one_shot,
         .resolve_boolean = resolve_boolean
     };
     
     /* Load the file */
     printf("Loading: %s\n", filepath);
     result = ct_file_load(filepath, NULL, &resolver, &handle);
     
     if (result != CT_FILE_OK) {
         fprintf(stderr, "Load failed: %s\n", ct_file_result_str(result));
         return 1;
     }
     
     printf("  Loaded %u bytes\n\n", handle.binary_size);
     
     /* Access the runtime */
     const chaintree_runtime_t *rt = handle.runtime;
     const chaintree_binary_header_t *hdr = rt->header;
     
     /* Print header info */
     printf("=== Header Info ===\n");
     printf("  Version: 0x%04X\n", hdr->version);
     printf("  Total size: %u bytes\n", hdr->total_size);
     printf("  Nodes: %u\n", hdr->node_count);
     printf("  Knowledge bases: %u\n", hdr->kb_count);
     printf("  Main functions: %u\n", hdr->main_function_count);
     printf("  One-shot functions: %u\n", hdr->one_shot_function_count);
     printf("  Boolean functions: %u\n", hdr->boolean_function_count);
     printf("  Events: %u\n", hdr->event_count);
     printf("  Bitmasks: %u\n", hdr->bitmask_count);
     printf("\n");
     
     /* Print KB info */
     printf("=== Knowledge Bases ===\n");
     for (uint16_t i = 0; i < hdr->kb_count; i++) {
         print_kb_info(rt, i);
     }
     printf("\n");
     
     /* Print first few nodes */
     printf("=== Nodes (first 10) ===\n");
     uint16_t max_nodes = hdr->node_count < 10 ? hdr->node_count : 10;
     for (uint16_t i = 0; i < max_nodes; i++) {
         const chaintree_binary_node_t *node = CT_GET_NODE(rt, i);
         /* Skip invalid/placeholder nodes */
         if (node->parent_index != 0xFFFF || i == 0) {
             print_node_info(rt, i);
         }
     }
     printf("\n");
     
     /* Example: Call init functions on auto-start nodes */
     printf("=== Calling Init on Auto-Start Nodes ===\n");
     for (uint16_t i = 0; i < hdr->node_count; i++) {
         const chaintree_binary_node_t *node = CT_GET_NODE(rt, i);
         if (CT_NODE_AUTO_START(node)) {
             printf("Node %u has auto_start, calling init...\n", i);
             one_shot_function_t init_fn = rt->one_shot_functions[node->init_function_index];
             init_fn(NULL, i);
         }
     }
     printf("\n");
     
     /* Example: Simulate an event on the root node */
     printf("=== Simulating Event on Root ===\n");
     if (hdr->node_count > 0) {
         const chaintree_binary_node_t *root = CT_GET_NODE(rt, 0);
         main_function_t main_fn = rt->main_functions[root->main_function_index];
         
         printf("Calling main function on node 0...\n");
         unsigned result_code = main_fn(NULL, root->aux_function_index, 0, 0, 0, NULL);
         printf("  Result: %u\n", result_code);
     }
     printf("\n");
     
     /* Clean up */
     printf("Unloading...\n");
     ct_file_unload(&handle);
     printf("Done.\n");
     
     return 0;
 }