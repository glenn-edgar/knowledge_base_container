/**
 * CT_Tree_Walker.c
 * 
 * Implementation of the C tree walker
 */

 #include "CT_Tree_Walker.h"
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
  */
 static CT_ReturnCode walk_iterative(
     CT_TreeWalker* walker,
     unsigned int root_id,
     CT_StackEntry* stack,
     unsigned int stack_capacity
 ) {
     if (stack_capacity == 0) {
         return CT_STOP_ALL;  /* Need stack for iterative */
     }
     
     /* Check root bounds */
     if (root_id >= walker->max_nodes) {
         return CT_CONTINUE;
     }
     
     /* Initialize stack with root */
     int stack_top = 0;
     stack[0].node_id = root_id;
     stack[0].level = 0;
     stack[0].child_index = 0;
     
     while (stack_top >= 0) {
         if (walker->stop_all) {
             return CT_STOP_ALL;
         }
         
         /* Pop current entry */
         CT_StackEntry current = stack[stack_top];
         
         /* Check bounds */
         if (current.node_id >= walker->max_nodes) {
             stack_top--;
             continue;
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
                walker->max_level = current.level;  // ← ADD THIS LINE
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
         
         /* Defensive: cap at buffer size */
         if (num_children > MAX_CHILDREN_BUFFER) {
             num_children = MAX_CHILDREN_BUFFER;
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
         
         /* Skip if child is out of bounds or already visited */
         if (child_id >= walker->max_nodes || 
             (walker->flags[child_id] & CT_FLAG_VISITED)) {
             continue;  /* Stay at same stack level, process next child */
         }
         
         /* Push next child onto stack */
         if (stack_top + 1 >= (int)stack_capacity) {
             /* Stack overflow */
             return CT_STOP_ALL;
         }
         
         stack_top++;
         stack[stack_top].node_id = child_id;
         stack[stack_top].level = current.level + 1;
         stack[stack_top].child_index = 0;
     }
     
     return CT_CONTINUE;
 }
 
 /**
  * BFS implementation using queue (simulated with stack array)
  */
 static CT_ReturnCode walk_bfs(
     CT_TreeWalker* walker,
     unsigned int root_id,
     CT_StackEntry* queue,
     unsigned int queue_capacity
 ) {
     if (queue_capacity == 0) {
         return CT_STOP_ALL;
     }
     
     /* Check root bounds */
     if (root_id >= walker->max_nodes) {
         return CT_CONTINUE;
     }
     
     /* Initialize queue with root */
     unsigned int head = 0;
     unsigned int tail = 0;
     
     queue[tail].node_id = root_id;
     queue[tail].level = 0;
     tail = (tail + 1) % queue_capacity;
     
     while (head != tail) {
         if (walker->stop_all) {
             return CT_STOP_ALL;
         }
         
         /* Dequeue */
         CT_StackEntry current = queue[head];
         head = (head + 1) % queue_capacity;
         
         /* Check bounds */
         if (current.node_id >= walker->max_nodes) {
             continue;
         }
         
         /* Check if already visited */
         if (walker->flags[current.node_id] & CT_FLAG_VISITED) {
             continue;
         }
         
         /* Mark as visited */
         walker->flags[current.node_id] |= CT_FLAG_VISITED;
         
         /* Check max level */
         if (current.level > walker->max_level) {
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
            continue;
        }

        if (ret == CT_STOP_LEVEL) {
            walker->max_level = current.level;  // ← ADD THIS LINE
            continue;
        }

        if (ret == CT_SKIP_CHILDREN) {
            continue;
        }
         
         /* Get children */
         unsigned int children[MAX_CHILDREN_BUFFER];
         unsigned int num_children = walker->get_children(
             walker->user_handle,
             current.node_id,
             children,
             MAX_CHILDREN_BUFFER
         );
         
         /* Defensive: cap at buffer size */
         if (num_children > MAX_CHILDREN_BUFFER) {
             num_children = MAX_CHILDREN_BUFFER;
         }
         
         /* Enqueue children */
         for (unsigned int i = 0; i < num_children; i++) {
             unsigned int child_id = children[i];
             
             /* Skip if child is out of bounds or already visited */
             if (child_id >= walker->max_nodes || 
                 (walker->flags[child_id] & CT_FLAG_VISITED)) {
                 continue;
             }
             
             unsigned int next_tail = (tail + 1) % queue_capacity;
             
             if (next_tail == head) {
                 /* Queue full */
                 return CT_STOP_ALL;
             }
             
             queue[tail].node_id = child_id;
             queue[tail].level = current.level + 1;
             tail = next_tail;
         }
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
     if (!walker || !flags || !get_children || !apply_func) {
         return false;
     }
     
     if (max_nodes == 0) {
         return false;
     }
     
     walker->user_handle = NULL;
     walker->max_nodes = max_nodes;
     walker->flags = flags;
     walker->get_children = get_children;
     walker->apply_func = apply_func;
     walker->max_level = 0xFFFF;
     walker->stop_all = false;
     
     /* Clear engine flags */
     clear_engine_flags(walker);
     
     return true;
 }
 
 CT_ReturnCode ct_walker_walk(
     CT_TreeWalker* walker,
     void* user_handle,
     unsigned int root_id,
     CT_TraversalMethod method,
     CT_StackEntry* stack,
     unsigned int stack_capacity,
     unsigned int max_level
 ) {
     if (!walker || !stack || stack_capacity == 0) {
         return CT_STOP_ALL;
     }
     
     if (root_id >= walker->max_nodes) {
         return CT_STOP_ALL;
     }
     
     /* Set user handle and max level */
     walker->user_handle = user_handle;
     walker->max_level = max_level;
     walker->stop_all = false;
     
     /* Clear engine flags before walk */
     clear_engine_flags(walker);
     
     /* Execute appropriate traversal method */
     CT_ReturnCode ret;
     
     switch (method) {
         case CT_ITERATIVE:
             ret = walk_iterative(walker, root_id, stack, stack_capacity);
             break;
             
         case CT_BFS:
             ret = walk_bfs(walker, root_id, stack, stack_capacity);
             break;
             
         default:
             ret = CT_STOP_ALL;
             break;
     }
     
     return ret;
 }
 
 void ct_walker_reset(CT_TreeWalker* walker) {
     if (walker) {
         clear_engine_flags(walker);
         walker->stop_all = false;
     }
 }
 
 bool ct_walker_is_visited(const CT_TreeWalker* walker, unsigned int node_id) {
     if (!walker || node_id >= walker->max_nodes) {
         return false;
     }
     return (walker->flags[node_id] & CT_FLAG_VISITED) != 0;
 }
 
 void ct_walker_set_user_flags(CT_TreeWalker* walker, unsigned int node_id, uint8_t flags) {
     if (walker && node_id < walker->max_nodes) {
         walker->flags[node_id] &= ~CT_FLAG_USER_MASK;  /* Clear old user flags */
         walker->flags[node_id] |= (flags & CT_FLAG_USER_MASK);  /* Set new user flags */
     }
 }
 
 uint8_t ct_walker_get_user_flags(const CT_TreeWalker* walker, unsigned int node_id) {
     if (!walker || node_id >= walker->max_nodes) {
         return 0;
     }
     return walker->flags[node_id] & CT_FLAG_USER_MASK;
 }