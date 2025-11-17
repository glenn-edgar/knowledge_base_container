#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>


void activate_valve_one_shot_fn(void *handle, unsigned node_index){
    (void)node_index;
    (void)handle;
    printf("activate_valve_one_shot_fn node index: %d\n", node_index);
    exit(0);
}