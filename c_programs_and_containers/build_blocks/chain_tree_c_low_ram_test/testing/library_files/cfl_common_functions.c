#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "cfl_engine.h"

#include "cfl_heap_arena_allocate.h"

void cfl_uint16_to_str(uint16_t value, char* buffer) {
    char temp[6];  // Max 5 digits + null
    int i = 0;
    
    if (value == 0) {
        buffer[0] = '0';
        buffer[1] = '\0';
        return;
    }
    
    while (value > 0) {
        temp[i++] = '0' + (value % 10);
        value /= 10;
    }
    
    // Reverse into output buffer
    int j;
    for (j = 0; j < i; j++) {
        buffer[j] = temp[i - 1 - j];
    }
    buffer[j] = '\0';
}

void *cfl_smart_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size){
    
    uint16_t memory_index = cfl_heap_arena_get_node_memory_index((volatile cfl_heap_arena_system_t *)handle->arena_system, node_index);
    if (memory_index == 0xFFFF){
        return  cfl_arena_system_alloc(handle->arena_system, node_index, size);
    }
    return cfl_heap_arena_get_node_ptr(handle->arena_system, node_index);
    
}