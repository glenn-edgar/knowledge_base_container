#include <stdio.h>
#include "arena_alloc.h"
#include "msgpack_arena.h"

// Forward declarations from generated files
extern bool msgpack_data_init(void);
extern const MsgPackNode* msgpack_data_root(void);
extern MsgPackArena msgpack_data_arena;
extern const char* msgpack_hash_to_string(uint64_t hash);

void example_basic_access(void) {
    printf("=== Basic Access ===\n\n");
    
    // Initialize
    if (!msgpack_data_init()) {
        printf("Failed to initialize\n");
        return;
    }
    
    // Get root
    const MsgPackNode* root = msgpack_data_root();
    
    // Access simple values
    const MsgPackNode* name = msgpack_map_get_str(&msgpack_data_arena, root, "device_name");
    if (name) {
        size_t len;
        const char* str = msgpack_get_string(&msgpack_data_arena, name, &len);
        printf("Device name: %.*s\n", (int)len, str);
    }
    
    const MsgPackNode* version = msgpack_map_get_str(&msgpack_data_arena, root, "firmware_version");
    if (version) {
        uint64_t val;
        if (msgpack_get_uint(&msgpack_data_arena, version, &val)) {
            printf("Firmware version: %llu\n", (unsigned long long)val);
        }
    }
    
    printf("\n");
}

void example_nested_access(void) {
    printf("=== Nested Access ===\n\n");
    
    const MsgPackNode* root = msgpack_data_root();
    
    // Navigate to network.ssid
    const MsgPackNode* network = msgpack_map_get_str(&msgpack_data_arena, root, "network");
    if (network) {
        const MsgPackNode* ssid = msgpack_map_get_str(&msgpack_data_arena, network, "ssid");
        if (ssid) {
            size_t len;
            const char* str = msgpack_get_string(&msgpack_data_arena, ssid, &len);
            printf("Network SSID: %.*s\n", (int)len, str);
        }
        
        const MsgPackNode* timeout = msgpack_map_get_str(&msgpack_data_arena, network, "timeout");
        if (timeout) {
            uint64_t val;
            if (msgpack_get_uint(&msgpack_data_arena, timeout, &val)) {
                printf("Network timeout: %llu ms\n", (unsigned long long)val);
            }
        }
    }
    
    printf("\n");
}

void example_array_access(void) {
    printf("=== Array Access ===\n\n");
    
    const MsgPackNode* root = msgpack_data_root();
    
    // Navigate to sensors.temperature.calibration
    const MsgPackNode* sensors = msgpack_map_get_str(&msgpack_data_arena, root, "sensors");
    if (sensors) {
        const MsgPackNode* temp = msgpack_map_get_str(&msgpack_data_arena, sensors, "temperature");
        if (temp) {
            const MsgPackNode* cal = msgpack_map_get_str(&msgpack_data_arena, temp, "calibration");
            if (cal && cal->type == MSGPACK_TYPE_ARRAY) {
                printf("Calibration values: [");
                for (uint16_t i = 0; i < cal->element_count; i++) {
                    const MsgPackNode* elem = msgpack_array_get(&msgpack_data_arena, cal, i);
                    if (elem) {
                        double val;
                        if (msgpack_get_double(&msgpack_data_arena, elem, &val)) {
                            printf("%.2f", val);
                            if (i < cal->element_count - 1) printf(", ");
                        }
                    }
                }
                printf("]\n");
            }
        }
    }
    
    printf("\n");
}

void example_subtree_extraction(void) {
    printf("=== Subtree Extraction ===\n\n");
    
    const MsgPackNode* root = msgpack_data_root();
    
    // Extract sensors subtree to RAM
    const MsgPackNode* sensors = msgpack_map_get_str(&msgpack_data_arena, root, "sensors");
    if (sensors) {
        size_t subtree_size = msgpack_subtree_size(&msgpack_data_arena, sensors);
        printf("Sensors subtree size: %zu bytes\n", subtree_size);
        
        // Create new arena for extracted data
        ArenaAllocator* ram_arena = arena_create(subtree_size + 1024);
        MsgPackArena ram_msgpack;
        
        if (msgpack_subtree_extract(&msgpack_data_arena, sensors, &ram_msgpack, ram_arena)) {
            printf("✓ Extracted to RAM\n");
            
            // Access from RAM copy
            const MsgPackNode* ram_root = msgpack_arena_root(&ram_msgpack);
            const MsgPackNode* temp = msgpack_map_get_str(&ram_msgpack, ram_root, "temperature");
            if (temp) {
                const MsgPackNode* enabled = msgpack_map_get_str(&ram_msgpack, temp, "enabled");
                if (enabled) {
                    bool val;
                    if (msgpack_get_bool(&ram_msgpack, enabled, &val)) {
                        printf("Temperature enabled (from RAM): %s\n", val ? "true" : "false");
                    }
                }
            }
        }
        
        arena_destroy(ram_arena);
    }
    
    printf("\n");
}

void example_serialization(void) {
    printf("=== Serialization ===\n\n");
    
    const MsgPackNode* root = msgpack_data_root();
    
    // Serialize entire structure
    char buffer[2048];
    size_t len;
    if (msgpack_to_string(&msgpack_data_arena, root, buffer, sizeof(buffer), &len)) {
        printf("Serialized (%zu bytes):\n%s\n", len, buffer);
    }
    
    printf("\n");
}

void example_hash_lookup(void) {
    printf("=== Hash Lookup ===\n\n");
    
    // Test hash function
    uint64_t hash = msgpack_hash64("device_name");
    printf("Hash of 'device_name': 0x%016llX\n", (unsigned long long)hash);
    
    // Reverse lookup
    const char* str = msgpack_hash_to_string(hash);
    if (str) {
        printf("Reverse lookup: %s\n", str);
    }
    
    printf("\n");
}

int main(void) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║        MessagePack Arena Example                      ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n");
    printf("\n");
    
    example_basic_access();
    example_nested_access();
    example_array_access();
    example_subtree_extraction();
    example_serialization();
    example_hash_lookup();
    
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║              All Examples Completed!                  ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n");
    printf("\n");
    
    return 0;
}


