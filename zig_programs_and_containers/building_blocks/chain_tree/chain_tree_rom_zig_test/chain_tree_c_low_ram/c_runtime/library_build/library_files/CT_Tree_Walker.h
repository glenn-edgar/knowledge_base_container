/**
 * CT_Tree_Walker.h
 * 
 * A reentrant C implementation for traversing tree/graph structures.
 * Supports DFS (recursive and iterative) and BFS traversal methods.
 */

 #ifndef CT_TREE_WALKER_H
 #define CT_TREE_WALKER_H
 
 #include <stdint.h>
 #include <stdbool.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ============================================================================
  * CONSTANTS AND ENUMS
  * ============================================================================ */
 
 /**
  * Return codes for apply function
  */
 typedef enum {
     CT_CONTINUE = 0,          /* Continue traversing normally */
     CT_STOP_BRANCH = 1,       /* Stop this branch (like False in Python) */
     CT_SKIP_CHILDREN = 2,     /* Skip children but continue with siblings */
     CT_STOP_LEVEL = 3,        /* Stop processing at current level */
     CT_STOP_SIBLINGS = 4,     /* Stop processing siblings, return to parent */
     CT_STOP_ALL = 5           /* Stop entire traversal immediately */
 } CT_ReturnCode;
 
 /**
  * Traversal methods
  */
 typedef enum {
     CT_ITERATIVE = 0,
     CT_BFS = 1
 } CT_TraversalMethod;
 
 /**
  * Flag bits (lower 4 bits reserved for engine, upper 4 bits for user)
  */
 #define CT_FLAG_VISITED     0x01  /* Bit 0: Node has been visited */
 #define CT_FLAG_IN_STACK    0x02  /* Bit 1: Node is in processing stack */
 #define CT_FLAG_STOP_SIBS   0x04  /* Bit 2: Stop processing siblings */
 #define CT_FLAG_RESERVED    0x08  /* Bit 3: Reserved for future use */
 #define CT_FLAG_USER_MASK   0xF0  /* Bits 4-7: Available for user application */
 
 /* User flag helpers (upper 4 bits) */
 #define CT_FLAG_USER_BIT0   0x10  /* User bit 0 */
 #define CT_FLAG_USER_BIT1   0x20  /* User bit 1 */
 #define CT_FLAG_USER_BIT2   0x40  /* User bit 2 */
 #define CT_FLAG_USER_BIT3   0x80  /* User bit 3 */
 
 /* ============================================================================
  * TYPE DEFINITIONS
  * ============================================================================ */
 
 /* Forward declaration */
 typedef struct CT_TreeWalker CT_TreeWalker;
 
 /**
  * Function pointer type for getting children of a node
  * 
  * @param user_handle User-defined handle passed through
  * @param node_id The node to get children for
  * @param children_out Output array to store child node IDs
  * @param max_children Maximum number of children that can be stored
  * @return Number of children returned (0 if no children)
  */
 typedef unsigned int (*CT_GetChildrenFunc)(
     void* user_handle,
     unsigned int node_id,
     unsigned int* children_out,
     unsigned int max_children
 );
 
 /**
  * Function pointer type for applying operation to each node
  * 
  * @param user_handle User-defined handle passed through
  * @param node_id The node being visited
  * @param level Current tree level (0 for root)
  * @param flags Pointer to flags array for this walker
  * @return CT_ReturnCode indicating how to proceed
  */
 typedef CT_ReturnCode (*CT_ApplyFunc)(
     void* user_handle,
     unsigned int node_id,
     unsigned int level,
     uint8_t* flags
 );
 
 /**
  * Stack entry for iterative/BFS traversal
  */
 typedef struct {
     unsigned int node_id;
     unsigned int level;
     unsigned int child_index;  /* For resumable iteration */
 } CT_StackEntry;
 
 /**
  * Tree walker instance
  */
 struct CT_TreeWalker {
     /* User-provided data */
     void* user_handle;
     unsigned int max_nodes;
     uint8_t* flags;
     
     /* Function pointers */
     CT_GetChildrenFunc get_children;
     CT_ApplyFunc apply_func;
     
     /* Internal state */
     unsigned int max_level;
     bool stop_all;
 };
 
 /* ============================================================================
  * PUBLIC API
  * ============================================================================ */
 
 /**
  * Initialize a tree walker instance
  * 
  * @param walker Pointer to walker structure to initialize
  * @param max_nodes Maximum number of nodes in the tree
  * @param flags Pointer to flags array (must be max_nodes bytes)
  * @param get_children Function to retrieve child nodes
  * @param apply_func Function to apply to each node
  * @return true on success, false on error
  */
 bool ct_walker_init(
     CT_TreeWalker* walker,
     unsigned int max_nodes,
     uint8_t* flags,
     CT_GetChildrenFunc get_children,
     CT_ApplyFunc apply_func
 );
 
 /**
  * Walk the tree starting from root_id
  * 
  * @param walker Pointer to initialized walker
  * @param user_handle User handle to pass to callbacks
  * @param root_id Starting node ID
  * @param method Traversal method (RECURSIVE, ITERATIVE, or BFS)
  * @param stack Stack array for iterative/BFS methods (NULL for recursive)
  * @param stack_capacity Size of stack array (0 for recursive)
  * @param max_level Maximum depth to traverse (0xFFFF for unlimited)
  * @return CT_ReturnCode from traversal
  */
 CT_ReturnCode ct_walker_walk(
     CT_TreeWalker* walker,
     void* user_handle,
     unsigned int root_id,
     CT_TraversalMethod method,
     CT_StackEntry* stack,
     unsigned int stack_capacity,
     unsigned int max_level
 );
 
 /**
  * Reset walker flags for a new traversal
  * 
  * @param walker Pointer to walker
  */
 void ct_walker_reset(CT_TreeWalker* walker);
 
 /**
  * Check if a node has been visited
  * 
  * @param walker Pointer to walker
  * @param node_id Node to check
  * @return true if visited
  */
 bool ct_walker_is_visited(const CT_TreeWalker* walker, unsigned int node_id);
 
 /**
  * Set/clear user flags for a node
  * 
  * @param walker Pointer to walker
  * @param node_id Node to modify
  * @param flags User flags to set (upper 4 bits only)
  */
 void ct_walker_set_user_flags(CT_TreeWalker* walker, unsigned int node_id, uint8_t flags);
 
 /**
  * Get user flags for a node
  * 
  * @param walker Pointer to walker
  * @param node_id Node to query
  * @return User flags (upper 4 bits only)
  */
 uint8_t ct_walker_get_user_flags(const CT_TreeWalker* walker, unsigned int node_id);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CT_TREE_WALKER_H */