#include "cfl_hbit.h"
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "cfl_exception.h"

static bool print_node(cfl_hbit2_tree_t* tree, int32_t node, int depth, void* user) {
    uint16_t bitspace_id = (uint16_t)(size_t)user;
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Node %d\n", node);
    
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Depth: %d\n", depth);
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Bits: %d\n", cfl_hbit2_info_bits(tree, node, bitspace_id));
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Bytes: %d\n", cfl_hbit2_info_bytes(tree, node, bitspace_id));
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Is leaf: %d\n", cfl_hbit2_info_is_leaf(tree, node));
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Children: %d\n", cfl_hbit2_nav_child_count(tree, node));
    for (int i = 0; i < depth*2; i++) printf("----");
    printf("--> Parent: %d\n", cfl_hbit2_nav_parent(tree, node));
    printf("\n\n");
    return true;  // continue
}

void cfl_hbit_print_tree(cfl_hbit2_tree_t* tree, const char* path, uint16_t bitspace_id) {
    int32_t root = cfl_hbit2_node(tree, path);
    if (root < 0) {
        printf("Node not found: %s\n", path);
        EXCEPTION("Node not found");
        return;
    }
    printf("\n\n");
    printf("Printing tree for path: %s\n", path);
    printf("\n");
    cfl_hbit2_walk_preorder(tree, root, print_node, (void*)(size_t)bitspace_id);
    printf("\n\n");
    printf("Done printing tree for path: %s\n", path);
    printf("\n\n");
}

cfl_hbit2_controller_t* cfl_hbit_create_controller(cfl_hbit2_tree_t* tree, const char* path, uint16_t bitspace_id) {
    int32_t root = cfl_hbit2_node(tree, path);
    if (root < 0) {
        printf("Node not found: %s\n", path);
        EXCEPTION("Node not found");
        return NULL;
    }
    return cfl_hbit2_controller_create(tree, root, bitspace_id);
}


static bool fill_node_leaves(cfl_hbit2_tree_t* tree, int32_t node, int depth, void* user) {
    (void)depth;
    uint16_t bitspace_id = (uint16_t)(size_t)user;
    if (cfl_hbit2_info_is_leaf(tree, node)) {
    
        cfl_hbit2_bank_fill(tree, node, bitspace_id, 0xFF);
    }
    return true;
}

static bool clear_node_leaves(cfl_hbit2_tree_t* tree, int32_t node, int depth, void* user) {
    (void)depth;
    uint16_t bitspace_id = (uint16_t)(size_t)user;
    if (cfl_hbit2_info_is_leaf(tree, node)) {
        cfl_hbit2_bank_clear(tree, node, bitspace_id);
    }
    return true;
}

void cfl_hbit_fill_all_leaf_nodes(cfl_hbit2_tree_t* tree,uint32_t top_node_id, uint16_t bitspace_id) {
    cfl_hbit2_walk_preorder(tree, top_node_id, fill_node_leaves, (void*)(size_t)bitspace_id);
}

void cfl_hbit_clear_all_leaf_nodes(cfl_hbit2_tree_t* tree, int32_t top_node_id, uint16_t bitspace_id) {
    cfl_hbit2_walk_preorder(tree, top_node_id, clear_node_leaves, (void*)(size_t)bitspace_id);
}