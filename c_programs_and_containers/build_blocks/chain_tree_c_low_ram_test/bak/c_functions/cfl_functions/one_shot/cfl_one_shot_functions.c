




/* === BEGIN: void cfl_log_message(void *handle, void *node) === */

#include "cfl_common_functions.h"

void cfl_log_message(void *handle, int node_index)
{
    char *message = NULL;
    char buffer[16];
    int_to_str(node_index, buffer);
    puts("cfl_log_message --- node index: ");
    puts(buffer);
    puts(" --- message: ");
    puts(message);
}

/* === END: void cfl_log_message(void *handle, void *node) === */

/* === BEGIN: void cfl_column_init(void *handle, int node_index) === */
void cfl_column_init(void *handle, int node_index)
{
    cfl_enable_links(handle,node_index)
}
/* === END: void cfl_column_init(void *handle, int node_index) === */
/* === BEGIN: void cfl_column_termination(void *handle, int node_index) === */
void cfl_column_termination(void *handle, int node_index)
{
    ;
}
/* === END: void cfl_column_termination(void *handle, int node_index) === */

/* === BEGIN: void cfl_enable_links(void *handle, int node_index) === */
void cfl_enable_links(void *handle, int node_index)
{   
    (HANDLE_ARRAY *) handle_array = (HANDLE_ARRAY *) handle;

    (CHAIN_TREE *)handle; = handle_array[0];
    (CHAIN_TREE_ENGINE *)chain_tree_engine = handle_array[1];
    label_dict = node["label_dict"]
    links = label_dict["links"]
    for link in links:
        
        ct_engine.reset_node_id(link)
      
/* === END: void cfl_enable_links(void *handle, int node_index) === */

