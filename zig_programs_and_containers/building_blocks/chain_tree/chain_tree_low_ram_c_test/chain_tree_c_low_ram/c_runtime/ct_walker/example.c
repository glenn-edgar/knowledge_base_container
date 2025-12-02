/**
 * example.c
 * 
 * Demonstration of CT_Tree_Walker usage with various examples
 */

 #include "CT_Tree_Walker.h"
 #include <stdio.h>
 #include <string.h>
 
 /* ============================================================================
  * EXAMPLE GRAPH DATA STRUCTURE
  * ============================================================================ */
 
 #define NUM_NODES 10
 #define MAX_CHILDREN 4
 
 /* Simple graph representation */
 typedef struct {
     unsigned int children[NUM_NODES][MAX_CHILDREN];
     unsigned int num_children[NUM_NODES];
 } Graph;
 
 /* Global graph for examples */
 static Graph g_graph;
 static uint8_t g_flags[NUM_NODES];
 static CT_StackEntry g_stack[100];  /* Stack for iterative/BFS methods */
 #define STACK_CAPACITY 100
 
 /* ============================================================================
  * GRAPH INITIALIZATION
  * ============================================================================ */
 
 /**
  * Initialize example graph:
  *       0
  *      /|\
  *     1 2 3
  *    /|   |\
  *   4 5   6 7
  *  /
  * 8 9
  */
 static void init_graph(Graph* graph) {
     memset(graph, 0, sizeof(Graph));
     
     /* Node 0: children = {1, 2, 3} */
     graph->children[0][0] = 1;
     graph->children[0][1] = 2;
     graph->children[0][2] = 3;
     graph->num_children[0] = 3;
     
     /* Node 1: children = {4, 5} */
     graph->children[1][0] = 4;
     graph->children[1][1] = 5;
     graph->num_children[1] = 2;
     
     /* Node 2: no children */
     graph->num_children[2] = 0;
     
     /* Node 3: children = {6, 7} */
     graph->children[3][0] = 6;
     graph->children[3][1] = 7;
     graph->num_children[3] = 2;
     
     /* Node 4: children = {8, 9} */
     graph->children[4][0] = 8;
     graph->children[4][1] = 9;
     graph->num_children[4] = 2;
     
     /* Nodes 5-9: no children */
     for (int i = 5; i < NUM_NODES; i++) {
         graph->num_children[i] = 0;
     }
 }
 
 /* ============================================================================
  * CALLBACK FUNCTIONS
  * ============================================================================ */
 
 /**
  * Get children callback
  */
 static unsigned int get_children(
     void* user_handle,
     unsigned int node_id,
     unsigned int* children_out,
     unsigned int max_children
 ) {
     Graph* graph = (Graph*)user_handle;
     
     if (node_id >= NUM_NODES) {
         return 0;
     }
     
     unsigned int count = graph->num_children[node_id];
     if (count > max_children) {
         count = max_children;
     }
     
     for (unsigned int i = 0; i < count; i++) {
         children_out[i] = graph->children[node_id][i];
     }
     
     return count;
 }
 
 /**
  * Simple print callback
  */
 static CT_ReturnCode print_node(
     void* user_handle,
     unsigned int node_id,
     unsigned int level,
     uint8_t* flags
 ) {
     (void)user_handle;
     (void)flags;
     
     /* Print indentation */
     for (unsigned int i = 0; i < level; i++) {
         printf("  ");
     }
     
     printf("Node %u (level %u)\n", node_id, level);
     
     return CT_CONTINUE;
 }
 
 /* ============================================================================
  * EXAMPLE FUNCTIONS
  * ============================================================================ */
 
 static void example_basic_dfs_recursive(void) {
     printf("\n=== Example: Basic DFS (Recursive) ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 0xFFFF);
 }
 
 static void example_basic_dfs_iterative(void) {
     printf("\n=== Example: Basic DFS (Iterative) ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_ITERATIVE, g_stack, STACK_CAPACITY, 0xFFFF);
 }
 
 static void example_basic_bfs(void) {
     printf("\n=== Example: Basic BFS ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_BFS, g_stack, STACK_CAPACITY, 0xFFFF);
 }
 
 /**
  * Example: Stop at a specific level
  */
 static CT_ReturnCode print_until_level_2(
     void* user_handle,
     unsigned int node_id,
     unsigned int level,
     uint8_t* flags
 ) {
     (void)user_handle;
     (void)flags;
     
     for (unsigned int i = 0; i < level; i++) {
         printf("  ");
     }
     printf("Node %u (level %u)\n", node_id, level);
     
     if (level >= 2) {
         return CT_STOP_LEVEL;
     }
     
     return CT_CONTINUE;
 }
 
 static void example_stop_level(void) {
     printf("\n=== Example: Stop at Level 2 ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_until_level_2);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 0xFFFF);
 }
 
 /**
  * Example: Skip children of node 1
  */
 static CT_ReturnCode skip_node_1_children(
     void* user_handle,
     unsigned int node_id,
     unsigned int level,
     uint8_t* flags
 ) {
     (void)user_handle;
     (void)flags;
     
     for (unsigned int i = 0; i < level; i++) {
         printf("  ");
     }
     printf("Node %u (level %u)", node_id, level);
     
     if (node_id == 1) {
         printf(" [skipping children]\n");
         return CT_SKIP_CHILDREN;
     }
     
     printf("\n");
     return CT_CONTINUE;
 }
 
 static void example_skip_children(void) {
     printf("\n=== Example: Skip Children of Node 1 ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, skip_node_1_children);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 0xFFFF);
 }
 
 /**
  * Example: Stop siblings after node 5
  */
 static CT_ReturnCode stop_after_node_5(
     void* user_handle,
     unsigned int node_id,
     unsigned int level,
     uint8_t* flags
 ) {
     (void)user_handle;
     (void)flags;
     
     for (unsigned int i = 0; i < level; i++) {
         printf("  ");
     }
     printf("Node %u (level %u)", node_id, level);
     
     if (node_id == 5) {
         printf(" [stopping siblings]\n");
         return CT_STOP_SIBLINGS;
     }
     
     printf("\n");
     return CT_CONTINUE;
 }
 
 static void example_stop_siblings(void) {
     printf("\n=== Example: Stop Siblings After Node 5 ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, stop_after_node_5);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 0xFFFF);
 }
 
 /**
  * Example: Using max_level parameter
  */
 static void example_max_level(void) {
     printf("\n=== Example: Max Level = 2 ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 2);
 }
 
 /**
  * Example: Small stack size (will stop early)
  */
 static void example_small_stack(void) {
     printf("\n=== Example: Small Stack (size=3) ===\n");
     
     CT_StackEntry small_stack[3];
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     printf("Note: May not complete due to stack overflow\n");
     ct_walker_walk(&walker, &g_graph, 0, CT_ITERATIVE, small_stack, 3, 0xFFFF);
 }
 
 /**
  * Example: Compare all three methods
  */
 static void example_compare_methods(void) {
     printf("\n=== Example: Compare All Methods ===\n");
     
     CT_TreeWalker walker;
     ct_walker_init(&walker, NUM_NODES, g_flags, get_children, print_node);
     
     printf("\n--- Recursive DFS ---\n");
     ct_walker_walk(&walker, &g_graph, 0, CT_RECURSIVE, NULL, 0, 0xFFFF);
     
     printf("\n--- Iterative DFS ---\n");
     ct_walker_walk(&walker, &g_graph, 0, CT_ITERATIVE, g_stack, STACK_CAPACITY, 0xFFFF);
     
     printf("\n--- BFS ---\n");
     ct_walker_walk(&walker, &g_graph, 0, CT_BFS, g_stack, STACK_CAPACITY, 0xFFFF);
 }
 
 /* ============================================================================
  * MAIN
  * ============================================================================ */
 
 int main(void) {
     printf("CT_Tree_Walker Examples\n");
     printf("=======================\n");
     
     /* Initialize graph */
     init_graph(&g_graph);
     
     /* Run examples */
     example_basic_dfs_recursive();
     example_basic_dfs_iterative();
     example_basic_bfs();
     example_stop_level();
     example_skip_children();
     example_stop_siblings();
     example_max_level();
     example_small_stack();
     example_compare_methods();
     
     printf("\n=== All Examples Complete ===\n");
     
     return 0;
 }