/**
 * example_main.c
 * 
 * Demonstrates full usage of the JSON record system:
 *   1. Include generated tree data
 *   2. Initialize reader
 *   3. Build ChainTree from records
 *   4. Navigate and print tree
 * 
 * Build:
 *   # First generate the header:
 *   python3 json_record_encoder.py example_tree.json -o example_tree_data.h -n tree
 *   
 *   # Then compile:
 *   gcc -o example example_main.c -I.
 *   ./example
 */

 #include <stdio.h>
 #include <string.h>
 
 #include "cfl_exception.h"
 #include "cfl_json_record_reader.h"
 #include "cfl_json_path.h"
 #include "cfl_chain_tree_nodes.h"
 #include "cfl_chain_tree_from_json.h"
 
 // Include generated tree data
 // Names derived from filename: example_tree.json -> example_tree_*
 #include "example_tree_data.h"
 
 //=============================================================================
 // Demo: Raw JSON path access
 //=============================================================================
 
 void demo_path_access(void) {
     printf("\n=== Demo: Path-based JSON access ===\n\n");
     
     // Simple: just pass the data descriptor
     json_reader_t reader;
     json_cursor_t c;
     json_cursor_init_from_data(&c, &reader, &example_tree_data);
     
     // Optional access (returns default if not found)
     const char* name = json_path_string(&c, "name", "(unnamed)");
     const char* type = json_path_string(&c, "type", "(unknown)");
     int32_t handler = json_path_int(&c, "handler_id", -1);
     
     printf("Root node:\n");
     printf("  name: %s\n", name);
     printf("  type: %s\n", type);
     printf("  handler_id: %d\n", handler);
     
     // Nested path access
     const char* first_child = json_path_string(&c, "children[0].name", "(none)");
     const char* deep_path = json_path_string(&c, "children[1].children[0].name", "(none)");
     
     printf("\nPath access:\n");
     printf("  children[0].name: %s\n", first_child);
     printf("  children[1].children[0].name: %s\n", deep_path);
     
     // Check existence
     printf("\nPath existence:\n");
     printf("  'children' exists: %s\n", json_path_exists(&c, "children") ? "yes" : "no");
     printf("  'foobar' exists: %s\n", json_path_exists(&c, "foobar") ? "yes" : "no");
     
     // Array iteration at path
     printf("\nChildren of root:\n");
     json_path_array_iter_t it;
     if (json_path_array_iter_init(&it, &c, "children")) {
         json_cursor_t child;
         int i = 0;
         while (json_path_array_iter_next(&it, &child)) {
             const char* child_name = json_path_string(&child, "name", "(unnamed)");
             const char* child_type = json_path_string(&child, "type", "(unknown)");
             printf("  [%d] %s (%s)\n", i++, child_name, child_type);
         }
     }
 }
 
 //=============================================================================
 // Demo: ChainTree building
 //=============================================================================
 
 void demo_chaintree(void) {
     printf("\n=== Demo: ChainTree from JSON ===\n\n");
     
     // Use node_count from data descriptor to size buffer
     printf("Node count from data: %u\n", example_tree_data.node_count);
     
     // Static buffer for nodes - sized from data
     static ct_node_t node_buffer[32];  // Or dynamically: malloc(data.node_count * sizeof(ct_node_t))
     ct_tree_t ct;
     
     // Simple: just pass the data descriptor
     bool ok = ct_build_from_data(&ct, node_buffer, example_tree_data.node_count, &example_tree_data);
     
     if (!ok) {
         printf("Failed to build tree!\n");
         return;
     }
     
     printf("Built tree with %u nodes\n\n", ct.node_count);
     
     // Print flat table
     ct_dump_tree(&ct);
     
     // Print hierarchical
     printf("\nHierarchical view:\n");
     ct_print_tree(&ct);
     
     // Navigate programmatically
     printf("\nProgrammatic navigation:\n");
     const ct_node_t* root = ct_get_root(&ct);
     if (root) {
         printf("Root: %s (%s)\n", root->name ? root->name : "(null)", ct_type_name(root->type));
         
         ct_child_iter_t it;
         ct_child_iter_init(&it, &ct, root);
         
         const ct_node_t* child;
         while ((child = ct_child_iter_next(&it)) != NULL) {
             printf("  Child: %s (%s) handler=%u\n", 
                    child->name ? child->name : "(null)",
                    ct_type_name(child->type),
                    child->handler_id);
             
             // Show grandchildren count
             uint16_t grandchildren = ct_count_children(&ct, child);
             if (grandchildren > 0) {
                 printf("    -> has %u children\n", grandchildren);
             }
         }
     }
 }
 
 //=============================================================================
 // Demo: Exception handling
 //=============================================================================
 
 void demo_exceptions(void) {
     printf("\n=== Demo: Exception handling ===\n\n");
     
     json_reader_t reader;
     json_cursor_t c;
     json_cursor_init_from_data(&c, &reader, &example_tree_data);
     
     // This will trigger an exception (path not found)
     printf("Attempting to access non-existent path...\n");
     const char* bad = json_path_string_ex(&c, "this.path.does.not.exist");
     printf("Got: %s\n", bad ? bad : "(null)");  // Won't reach here if exception halts
     
     // This will trigger type mismatch
     printf("\nAttempting type mismatch (reading 'name' as int)...\n");
     int32_t bad_int = json_path_int_ex(&c, "name");
     printf("Got: %d\n", bad_int);
 }
 
 //=============================================================================
 // Main
 //=============================================================================
 
 int main(int argc, char* argv[]) {
     printf("JSON Record System Demo\n");
     printf("========================\n");
     
     demo_path_access();
     demo_chaintree();
     
     // Uncomment to see exception behavior
     // demo_exceptions();
     
     printf("\nDone.\n");
     return 0;
 }