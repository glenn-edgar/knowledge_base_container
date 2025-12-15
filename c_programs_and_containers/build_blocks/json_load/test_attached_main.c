#include <stdio.h>
#include "cfl_exception.h"
#include "cfl_json_record_reader.h"
#include "cfl_json_path.h"
#include "cfl_chain_tree_nodes.h"
#include "cfl_chain_tree_from_json.h"
#include "test_attached_data.h"

void print_node_data(const ct_tree_t* tree, const ct_node_t* node) {
    json_reader_t reader;
    json_cursor_t c;
    
    if (!ct_node_data(tree, node, &reader, &c)) {
        printf("    (no attached data)\n");
        return;
    }
    
    printf("    Attached data:\n");
    
    // Try various fields that might exist
    int32_t timeout = json_path_int(&c, "timeout_ms", -1);
    if (timeout >= 0) printf("      timeout_ms: %d\n", timeout);
    
    const char* sensor = json_path_string(&c, "sensor_id", NULL);
    if (sensor) printf("      sensor_id: %s\n", sensor);
    
    int32_t sample_rate = json_path_int(&c, "sample_rate_hz", -1);
    if (sample_rate >= 0) printf("      sample_rate_hz: %d\n", sample_rate);
    
    int32_t min_val = json_path_int(&c, "thresholds.min", -9999);
    int32_t max_val = json_path_int(&c, "thresholds.max", -9999);
    if (min_val != -9999) printf("      thresholds: min=%d, max=%d\n", min_val, max_val);
    
    int32_t retry_count = json_path_int(&c, "retry_count", -1);
    if (retry_count >= 0) printf("      retry_count: %d\n", retry_count);
    
    int32_t retry_delay = json_path_int(&c, "retry_delay_ms", -1);
    if (retry_delay >= 0) printf("      retry_delay_ms: %d\n", retry_delay);
    
    const char* mode = json_path_string(&c, "params.mode", NULL);
    if (mode) printf("      params.mode: %s\n", mode);
    
    int32_t max_flow = json_path_int(&c, "params.max_flow", -1);
    if (max_flow >= 0) printf("      params.max_flow: %d\n", max_flow);
    
    const char* log_level = json_path_string(&c, "log_level", NULL);
    if (log_level) printf("      log_level: %s\n", log_level);
    
    // Check for array field
    json_cursor_t arr_c;
    if (json_path_cursor(&c, "alert_channels", &arr_c) == JSON_PATH_OK) {
        printf("      alert_channels: ");
        json_array_iter_t it;
        if (json_array_iter_init(&it, &arr_c)) {
            json_cursor_t elem;
            bool first = true;
            while (json_array_iter_next(&it, &elem)) {
                const char* ch = json_get_string(&elem);
                if (ch) {
                    if (!first) printf(", ");
                    printf("%s", ch);
                    first = false;
                }
            }
        }
        printf("\n");
    }
}

void walk_tree(const ct_tree_t* tree, const ct_node_t* node, int depth) {
    if (!node) return;
    
    // Indent
    for (int i = 0; i < depth; i++) printf("  ");
    
    printf("%s \"%s\" (handler=%u)\n", 
           ct_type_name(node->type),
           node->name ? node->name : "(unnamed)",
           node->handler_id);
    
    print_node_data(tree, node);
    
    // Walk children
    ct_child_iter_t it;
    ct_child_iter_init(&it, tree, node);
    const ct_node_t* child;
    while ((child = ct_child_iter_next(&it)) != NULL) {
        walk_tree(tree, child, depth + 1);
    }
}

int main(void) {
    printf("ChainTree Attached Data Demo\n");
    printf("============================\n\n");
    
    static ct_node_t nodes[32];
    ct_tree_t tree;
    
    printf("Building tree from JSON (node_count=%u)...\n\n", test_attached_data_data.node_count);
    
    if (!ct_build_from_data(&tree, nodes, test_attached_data_data.node_count, &test_attached_data_data)) {
        printf("Failed to build tree!\n");
        return 1;
    }
    
    printf("Tree structure with attached data:\n\n");
    walk_tree(&tree, ct_get_root(&tree), 0);
    
    return 0;
}

