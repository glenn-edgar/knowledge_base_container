#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>


bool cfl_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return false;
}


bool cfl_bool_false_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    return false;
    
}

bool cfl_column_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return false;
    
}

bool cfl_gate_node_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    
    return false;
    
}

bool cfl_verify_time_out_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    printf("cfl_verify_time_out_boolean_fn\n");
    exit(0);
    return false;
    
}

bool cfl_wait_for_event_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("cfl_wait_for_event_boolean_fn\n");
    exit(0);
    return false;
    
}
