/**
 * example_file_load.c
 * 
 * Demonstrates loading JSON records from binary file at runtime.
 * 
 * Build:
 *   # Generate binary file:
 *   python3 json_record_encoder.py example_tree.json -b tree.bin
 *   
 *   # Compile:
 *   gcc -Wall -o example_file example_file_load.c
 *   
 *   # Run:
 *   ./example_file tree.bin
 */

 #include <stdio.h>
 #include <string.h>
 
 #include "cfl_exception.h"
 #include "cfl_json_record_reader.h"
 #include "cfl_json_path.h"
 #include "cfl_chain_tree_nodes.h"
 #include "cfl_chain_tree_from_json.h"
 #include "cfl_json_record_file.h"
 
 //=============================================================================
 // Example 1: Static buffers (embedded-friendly, no malloc)
 //=============================================================================
 
 void demo_static_load(const char* path) {
     printf("\n=== Loading with static buffers ===\n\n");
     
     // Pre-allocated buffers - size these for your max expected data
     static char string_buf[2048];
     static json_record_t record_buf[256];
     static record_control_t control_buf[16];
     
     json_file_buffers_t bufs = {
         .strings = string_buf,
         .strings_size = sizeof(string_buf),
         .records = record_buf,
         .records_count = 256,
         .controls = control_buf,
         .controls_count = 16
     };
     
     json_data_t data;
     record_control_t* controls;
     uint32_t num_controls;
     
     if (!json_file_load(path, &bufs, &data, &controls, &num_controls)) {
         printf("Failed to load file!\n");
         return;
     }
     
     printf("Loaded: %u records, %u controls, %u nodes\n", 
            data.record_count, num_controls, data.node_count);
     
     // Use json_cursor_init_from_data - simple!
     json_reader_t reader;
     json_cursor_t c;
     json_cursor_init_from_data(&c, &reader, &data);
     
     const char* name = json_path_string(&c, "name", "(unknown)");
     const char* type = json_path_string(&c, "type", "(unknown)");
     printf("Root: %s (%s)\n", name, type);
     
     // Build ChainTree - use node_count from data!
     static ct_node_t node_buf[64];
     ct_tree_t tree;
     
     if (ct_build_from_data(&tree, node_buf, data.node_count, &data)) {
         printf("\nChainTree built: %u nodes\n", tree.node_count);
         ct_print_tree(&tree);
     }
 }
 
 //=============================================================================
 // Example 2: Dynamic allocation (Linux/RTOS)
 //=============================================================================
 
 void demo_dynamic_load(const char* path) {
     printf("\n=== Loading with dynamic allocation ===\n\n");
     
     // Query file info first (optional)
     json_file_info_t info = json_file_get_info(path);
     if (!info.valid) {
         printf("Invalid file!\n");
         return;
     }
     printf("File info: %u records, %u string bytes, %u controls, %u nodes\n",
            info.record_count, info.string_size, info.control_count, info.node_count);
     
     // Load with allocation
     json_file_data_t* file_data = json_file_load_alloc(path);
     if (!file_data) {
         printf("Failed to load file!\n");
         return;
     }
     
     printf("Loaded successfully\n");
     
     // Use json_cursor_init_from_data - simple!
     json_reader_t reader;
     json_cursor_t c;
     json_cursor_init_from_data(&c, &reader, &file_data->data);
     
     const char* name = json_path_string(&c, "name", "(unknown)");
     printf("Root name: %s\n", name);
     
     // Iterate all controls (if multiple JSON objects were encoded)
     printf("\nAll controls:\n");
     for (uint32_t i = 0; i < file_data->control_count; i++) {
         json_cursor_t cursor;
         json_cursor_init(&cursor, &reader, &file_data->controls[i]);
         
         const char* obj_name = json_path_string(&cursor, "name", "(unnamed)");
         const char* obj_type = json_path_string(&cursor, "type", "(unknown)");
         printf("  [%u] %s (%s)\n", i, obj_name, obj_type);
     }
     
     // Build ChainTree - use node_count from data!
     static ct_node_t node_buf[64];
     ct_tree_t tree;
     
     if (ct_build_from_data(&tree, node_buf, file_data->data.node_count, &file_data->data)) {
         printf("\nChainTree:\n");
         ct_print_tree(&tree);
     }
     
     // Cleanup
     json_file_free(file_data);
     printf("\nFreed resources\n");
 }
 
 //=============================================================================
 // Main
 //=============================================================================
 
 int main(int argc, char* argv[]) {
     if (argc < 2) {
         printf("Usage: %s <file.bin>\n", argv[0]);
         printf("\nGenerate binary file with:\n");
         printf("  python3 json_record_encoder.py tree.json -b tree.bin\n");
         return 1;
     }
     
     const char* path = argv[1];
     printf("Loading: %s\n", path);
     
     demo_static_load(path);
     demo_dynamic_load(path);
     
     printf("\nDone.\n");
     return 0;
 }