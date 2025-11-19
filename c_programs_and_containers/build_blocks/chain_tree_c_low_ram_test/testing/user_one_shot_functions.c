#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "json_node_decoder.h"

void activate_valve_one_shot_fn(void *handle, unsigned node_index){
    
    
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    const char *state;
   
    
    
    
    json_decoder_init_from_runtime(runtime, node_index);
    
    // Step 2: Extract the value using path notation
    json_extract_string_runtime(runtime, "node_dict.state", &state);
    
    // Step 3: Use the value (state now points to "open")
    // No need to free - string points into flash memory
    
    if (strcmp(state, "open") == 0) {
        printf("Valve is open\n");
    }

}



void wait_for_event_error_one_shot_fn(void *handle, unsigned node_index){
    (void)node_index;
    (void)handle;
    printf("wait_for_event_error_one_shot_fn node index: %d\n", node_index);
    exit(0);
}

void verify_error_one_shot_fn(void *handle, unsigned node_index){
    (void)node_index;
    (void)handle;
    printf("verify_error_one_shot_fn node index: %d\n", node_index);
    exit(0);
}