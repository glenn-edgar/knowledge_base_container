/**
 * CT_Tree_Walker.c
 * 
 * Implementation of the C tree walker with exception handling
 * Uses iterative DFS for memory-efficient traversal in embedded environments
 */

 #include "CT_Tree_Walker.h"
 #include "cfl_exception.h"
 #include <string.h>
 #include <stdio.h>
 
 /* Maximum children buffer size */
 #define MAX_CHILDREN_BUFFER 256
 
 /* ============================================================================
  * PRIVATE HELPER FUNCTIONS
  * ============================================================================ */
 
 /**
  * Clear all engine flags for all nodes
  */
 static void clear_engine_flags(CT_TreeWalker* walker) {
     for (unsigned int i = 0; i < walker->max_nodes; i++) {
         walker->flags[i] &= CT_FLAG_USER_MASK;  /* Keep user flags, clear engine flags */
     }
 }
 
 /**
  * Iterative DFS implementation using explicit stack
  * Memory usage: O(max_tree_depth) - optimal for embedded systems
  * Iteration count bounded by max_node_id for runaway protection
  */
 static CT_ReturnCode walk_iterative(
     CT_TreeWalker* walker,
     unsigned int root_id,
     CT_StackEntry* stack,
     unsigned int stack_capacity
 ) {
     if (stack_capacity == 0) {
         EXCEPTION("walk_iterative: stack_capacity is 0");
     }
     
     /* Check root bounds against walker limits */
     if (root_id >= walker->max_nodes) {
         EXCEPTION("walk_iterative: root_id exceeds max_nodes");
     }
     
     /* Check root bounds against walk-specific limits */
     if (root_id > walker->max_node_id) {
         EXCEPTION("walk_iterative: root_id exceeds max_node_id");
     }
     
     /* Initialize stack with root */
     int stack_top = 0;
     stack[0].node_id = root_id;
     stack[0].level = 0;
     stack[0].child_index = 0;
     
     /* Iteration counter for runaway protection */
     unsigned int iteration_count = 0;
     
     while (stack_top >= 0) {
         /* Check iteration count against max_node_id */
         /* Cannot possibly visit more nodes than max_node_id + 1 */
         iteration_count++;
         if (iteration_count > walker->max_node_id + 1) {
             EXCEPTION("walk_iterative: iteration count exceeded max_node_id - possible infinite loop");
         }
         
         if (walker->stop_all) {
             return CT_STOP_ALL;
         }
         
         /* Pop current entry */
         CT_StackEntry current = stack[stack_top];
         
         /* Check bounds against walker limits */
         if (current.node_id >= walker->max_nodes) {
             EXCEPTION("walk_iterative: node_id exceeds max_nodes during traversal");
         }
         
         /* Check bounds against walk-specific limits */
         if (current.node_id > walker->max_node_id) {
             EXCEPTION("walk_iterative: node_id exceeds max_node_id during traversal");
         }
         
         /* Check if already visited */
         if (walker->flags[current.node_id] & CT_FLAG_VISITED) {
             if (current.child_index == 0) {
                 /* Already fully processed */
                 stack_top--;
                 continue;
             }
         } else {
             /* First visit - mark as visited and apply function */
             walker->flags[current.node_id] |= CT_FLAG_VISITED;
             
             /* Check max level */
             if (current.level > walker->max_level) {
                 stack_top--;
                 continue;
             }
             
             /* Apply function */
             CT_ReturnCode ret = walker->apply_func(
                 walker->user_handle,
                 current.node_id,
                 current.level,
                 walker->flags
             );
 
             if (ret == CT_STOP_ALL) {
                 walker->stop_all = true;
                 return ret;
             }
 
             if (ret == CT_STOP_BRANCH || ret == CT_STOP_SIBLINGS) {
                 stack_top--;
                 continue;
             }
 
             if (ret == CT_STOP_LEVEL) {
                 walker->max_level = current.level;
                 stack_top--;
                 continue;
             }
 
             if (ret == CT_SKIP_CHILDREN) {
                 stack_top--;
                 continue;
             }
         }
         
         /* Get children */
         unsigned int children[MAX_CHILDREN_BUFFER];
         unsigned int num_children = walker->get_children(
             walker->user_handle,
             current.node_id,
             children,
             MAX_CHILDREN_BUFFER
         );
         
         /* Check for buffer overflow */
         if (num_children > MAX_CHILDREN_BUFFER) {
             EXCEPTION("walk_iterative: get_children returned too many children");
         }
         
         /* Check if we have more children to process */
         if (current.child_index >= num_children) {
             stack_top--;
             continue;
         }
         
         /* Get the child to process */
         unsigned int child_id = children[current.child_index];
         
         /* Update current entry to process next child later */
         stack[stack_top].child_index++;
         
         /* Validate child ID against walker limits */
         if (child_id >= walker->max_nodes) {
             EXCEPTION("walk_iterative: child_id exceeds max_nodes from get_children");
         }
         
         /* Validate child ID against walk-specific limits */
         if (child_id > walker->max_node_id) {
             EXCEPTION("walk_iterative: child_id exceeds max_node_id from get_children");
         }
         
         /* Skip if already visited */
         if (walker->flags[child_id] & CT_FLAG_VISITED) {
             continue;  /* Stay at same stack level, process next child */
         }
         
         /* Push next child onto stack */
         if (stack_top + 1 >= (int)stack_capacity) {
             /* Stack overflow - fail hard */
             EXCEPTION("walk_iterative: stack overflow - increase stack_capacity");
         }
         
         stack_top++;
         stack[stack_top].node_id = child_id;
         stack[stack_top].level = current.level + 1;
         stack[stack_top].child_index = 0;
     }
     
     return CT_CONTINUE;
 }
 
 /* ============================================================================
  * PUBLIC API IMPLEMENTATION
  * ============================================================================ */
 
 bool ct_walker_init(
     CT_TreeWalker* walker,
     unsigned int max_nodes,
     uint8_t* flags,
     CT_GetChildrenFunc get_children,
     CT_ApplyFunc apply_func
 ) {
     if (!walker) {
         EXCEPTION("ct_walker_init: walker is NULL");
     }
     
     if (!flags) {
         EXCEPTION("ct_walker_init: flags is NULL");
     }
     
     if (!get_children) {
         EXCEPTION("ct_walker_init: get_children is NULL");
     }
     
     if (!apply_func) {
         EXCEPTION("ct_walker_init: apply_func is NULL");
     }
     
     if (max_nodes == 0) {
         EXCEPTION("ct_walker_init: max_nodes is 0");
     }
     
     walker->user_handle = NULL;
     walker->max_nodes = max_nodes;
     walker->flags = flags;
     walker->get_children = get_children;
     walker->apply_func = apply_func;
     walker->max_level = 0xFFFF;
     walker->max_node_id = max_nodes - 1;  /* Initialize to max possible */
     walker->stop_all = false;
     
     /* Clear engine flags */
     clear_engine_flags(walker);
     
     return true;
 }
 
 CT_ReturnCode ct_walker_walk(
     CT_TreeWalker* walker,
     void* user_handle,
     unsigned int root_id,
     CT_StackEntry* stack,
     unsigned int stack_capacity,
     unsigned int max_level,
     unsigned int max_node_id
 ) {
     if (!walker) {
         EXCEPTION("ct_walker_walk: walker is NULL");
     }
     
     if (!stack) {
         EXCEPTION("ct_walker_walk: stack is NULL");
     }
     
     if (stack_capacity == 0) {
         EXCEPTION("ct_walker_walk: stack_capacity is 0");
     }
     
     if (root_id >= walker->max_nodes) {
         EXCEPTION("ct_walker_walk: root_id exceeds max_nodes");
     }
     
     /* Validate max_node_id parameter */
     if (max_node_id > walker->max_nodes) {
         EXCEPTION("ct_walker_walk: max_node_id exceeds walker max_nodes");
     }
     
     /* Set user handle, max level, and max node ID */
     walker->user_handle = user_handle;
     walker->max_level = max_level;
     walker->max_node_id = max_node_id;
     walker->stop_all = false;
     
     /* Clear engine flags before walk */
     clear_engine_flags(walker);
     
     /* Execute iterative DFS traversal */
     return walk_iterative(walker, root_id, stack, stack_capacity);
 }
 
 void ct_walker_reset(CT_TreeWalker* walker) {
     if (!walker) {
         EXCEPTION("ct_walker_reset: walker is NULL");
     }
     
     clear_engine_flags(walker);
     walker->stop_all = false;
 }
 
 bool ct_walker_is_visited(const CT_TreeWalker* walker, unsigned int node_id) {
     if (!walker) {
         EXCEPTION("ct_walker_is_visited: walker is NULL");
     }
     
     if (node_id >= walker->max_nodes) {
         EXCEPTION("ct_walker_is_visited: node_id out of bounds");
     }
     
     return (walker->flags[node_id] & CT_FLAG_VISITED) != 0;
 }
 
 void ct_walker_set_user_flags(CT_TreeWalker* walker, unsigned int node_id, uint8_t flags) {
     if (!walker) {
         EXCEPTION("ct_walker_set_user_flags: walker is NULL");
     }
     
     if (node_id >= walker->max_nodes) {
         EXCEPTION("ct_walker_set_user_flags: node_id out of bounds");
     }
     
     walker->flags[node_id] &= ~CT_FLAG_USER_MASK;  /* Clear old user flags */
     walker->flags[node_id] |= (flags & CT_FLAG_USER_MASK);  /* Set new user flags */
 }
 
 uint8_t ct_walker_get_user_flags(const CT_TreeWalker* walker, unsigned int node_id) {
     if (!walker) {
         EXCEPTION("ct_walker_get_user_flags: walker is NULL");
     }
     
     if (node_id >= walker->max_nodes) {
         EXCEPTION("ct_walker_get_user_flags: node_id out of bounds");
     }
     
     return walker->flags[node_id] & CT_FLAG_USER_MASK;
 }