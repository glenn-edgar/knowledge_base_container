/**
 * chaintree_nodes.h
 * 
 * ChainTree node definitions - flat array representation with linked structure.
 * 
 * Memory layout:
 *   - Nodes stored in flat array (ROM or RAM)
 *   - Children/siblings linked by index
 *   - Names point into string table (zero copy)
 * 
 * Node types are defined in chaintree_node_types.h (generated from YAML config).
 * To add/modify node types, edit chaintree_types.yaml and regenerate:
 *   python3 generate_node_types.py chaintree_types.yaml -o chaintree_node_types.h
 */

 #ifndef CFL_CHAIN_TREE_NODES_H
 #define CFL_CHAIN_TREE_NODES_H
 #ifdef __cplusplus
 extern "C" {
 #endif
 #include <stdint.h>
 #include <stdbool.h>
 
 // Include generated node types (enum, ct_type_from_string, ct_type_name)
 #include "cfl_chain_tree_node_types.h"
 
 //=============================================================================
 // Node structure
 //=============================================================================
 
 typedef struct {
     uint16_t type;           // ct_node_type_t
     uint16_t first_child;    // index into node array, CT_NO_LINK if none
     uint16_t next_sibling;   // index into node array, CT_NO_LINK if none
     uint16_t parent;         // index into node array, CT_NO_LINK if root
     uint16_t handler_id;     // maps to function pointer table
     uint16_t flags;          // user-defined flags
     const char* name;        // points into string table (may be NULL)
     uint32_t data_pos;       // position in JSON records for attached data access
 } ct_node_t;
 
 typedef struct {
     const ct_node_t* nodes;
     uint16_t node_count;
     uint16_t root_index;
     const void* data;  // json_data_t* for attached data access (may be NULL)
 } ct_tree_t;
 
 //=============================================================================
 // Tree navigation helpers
 //=============================================================================
 
 static inline const ct_node_t* ct_get_node(const ct_tree_t* tree, uint16_t index) {
     if (!tree || index >= tree->node_count) return NULL;
     return &tree->nodes[index];
 }
 
 static inline const ct_node_t* ct_get_root(const ct_tree_t* tree) {
     if (!tree) return NULL;
     return ct_get_node(tree, tree->root_index);
 }
 
 static inline const ct_node_t* ct_get_first_child(const ct_tree_t* tree, const ct_node_t* node) {
     if (!tree || !node || node->first_child == CT_NO_LINK) return NULL;
     return ct_get_node(tree, node->first_child);
 }
 
 static inline const ct_node_t* ct_get_next_sibling(const ct_tree_t* tree, const ct_node_t* node) {
     if (!tree || !node || node->next_sibling == CT_NO_LINK) return NULL;
     return ct_get_node(tree, node->next_sibling);
 }
 
 static inline const ct_node_t* ct_get_parent(const ct_tree_t* tree, const ct_node_t* node) {
     if (!tree || !node || node->parent == CT_NO_LINK) return NULL;
     return ct_get_node(tree, node->parent);
 }
 
 static inline uint16_t ct_get_index(const ct_tree_t* tree, const ct_node_t* node) {
     if (!tree || !node) return CT_NO_LINK;
     return (uint16_t)(node - tree->nodes);
 }
 
 static inline bool ct_is_leaf(const ct_node_t* node) {
     return node && node->first_child == CT_NO_LINK;
 }
 
 static inline bool ct_has_children(const ct_node_t* node) {
     return node && node->first_child != CT_NO_LINK;
 }
 
 //=============================================================================
 // Child iteration
 //=============================================================================
 
 typedef struct {
     const ct_tree_t* tree;
     uint16_t current;
 } ct_child_iter_t;
 
 static inline void ct_child_iter_init(ct_child_iter_t* it, const ct_tree_t* tree, const ct_node_t* parent) {
     if (!it) return;
     it->tree = tree;
     it->current = (tree && parent) ? parent->first_child : CT_NO_LINK;
 }
 
 static inline const ct_node_t* ct_child_iter_next(ct_child_iter_t* it) {
     if (!it || !it->tree || it->current == CT_NO_LINK) return NULL;
     
     const ct_node_t* node = ct_get_node(it->tree, it->current);
     if (node) {
         it->current = node->next_sibling;
     }
     return node;
 }
 
 // Count children of a node
 static inline uint16_t ct_count_children(const ct_tree_t* tree, const ct_node_t* node) {
     if (!tree || !node) return 0;
     
     uint16_t count = 0;
     ct_child_iter_t it;
     ct_child_iter_init(&it, tree, node);
     while (ct_child_iter_next(&it)) {
         count++;
     }
     return count;
 }
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif // CFL_CHAIN_TREE_NODES_H