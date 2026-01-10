#include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 
 #include "cfl_hbit.h"
 #include "./irrigation_output/generated_Irrigation_Example.bin.h"
 #include "./irrigation_output/generated_Irrigation_Example_hashes.h"

 void cfl_exception_handler(const char* file, const char* func, uint16_t line, const char* msg) {
     fprintf(stderr, "EXCEPTION at %s:%s:%u: %s\n", file, func, line, msg);
     abort();
 }



 typedef struct {
    int32_t* station_nodes;   // station aggregate nodes
    int32_t* bank_nodes;      // flat array [station][bank]
    int num_stations;
    int* banks_per_station;
    int bank_width;
} station_lookup_t;

static void station_lookup_init(cfl_hbit2_tree_t* tree, station_lookup_t* lookup, int bank_width) {
    memset(lookup, 0, sizeof(*lookup));
    
    int32_t root = cfl_hbit2_node(tree, "Overall_Valve_Status");
    if (root < 0) return;
    
    lookup->num_stations = cfl_hbit2_nav_child_count(tree, root);
    lookup->station_nodes = malloc(lookup->num_stations * sizeof(int32_t));
    lookup->banks_per_station = malloc(lookup->num_stations * sizeof(int));
    lookup->bank_width = bank_width;
    
    cfl_hbit2_nav_children(tree, root, lookup->station_nodes, lookup->num_stations);
    
    // Count total banks
    int total_banks = 0;
    for (int s = 0; s < lookup->num_stations; s++) {
        lookup->banks_per_station[s] = cfl_hbit2_nav_child_count(tree, lookup->station_nodes[s]);
        total_banks += lookup->banks_per_station[s];
    }
    
    // Build flat bank array
    lookup->bank_nodes = malloc(total_banks * sizeof(int32_t));
    int idx = 0;
    for (int s = 0; s < lookup->num_stations; s++) {
        int32_t banks[16];
        int n = cfl_hbit2_nav_children(tree, lookup->station_nodes[s], banks, 16);
        for (int b = 0; b < n; b++) {
            lookup->bank_nodes[idx++] = banks[b];
        }
    }
}

static void station_lookup_destroy(station_lookup_t* lookup) {
    free(lookup->station_nodes);
    free(lookup->bank_nodes);
    free(lookup->banks_per_station);
    memset(lookup, 0, sizeof(*lookup));
}

static int32_t station_lookup_get_station(station_lookup_t* lookup, int station_id) {
    if (station_id < 0 || station_id >= lookup->num_stations) return -1;
    return lookup->station_nodes[station_id];
}

static int32_t station_lookup_get_bank(station_lookup_t* lookup, int station_id, int station_bit, int* bit_out) {
    if (station_id < 0 || station_id >= lookup->num_stations) return -1;
    
    int bank_id = station_bit / lookup->bank_width;
    *bit_out = station_bit % lookup->bank_width;
    
    int offset = 0;
    for (int s = 0; s < station_id; s++) {
        offset += lookup->banks_per_station[s];
    }
    
    if (bank_id >= lookup->banks_per_station[station_id]) return -1;
    return lookup->bank_nodes[offset + bank_id];
}

static void print_tree_state(cfl_hbit2_tree_t* tree, station_lookup_t* lookup, char* root_name, int16_t bs_id) {
    int32_t root = cfl_hbit2_node(tree, root_name);
    int root_bytes = cfl_hbit2_info_bytes(tree, root, bs_id);
    
    printf("========================================\n");
    printf("TREE STATE (bitspace %d)\n", bs_id);
    printf("========================================\n\n");
    
    // Print root
    printf("ROOT (node %d):\n", root);
    const uint8_t* bits = cfl_hbit2_bank_get(tree, root, bs_id);
    printf("  bits: ");
    for (int i = 0; i < root_bytes; i++) printf("%02X ", bits[i]);
    printf("\n\n");
    
    // Print each station and its banks
    for (int s = 0; s < lookup->num_stations; s++) {
        int32_t station = station_lookup_get_station(lookup, s);
        int station_bytes = cfl_hbit2_info_bytes(tree, station, bs_id);
        
        printf("STATION %d (node %d):\n", s, station);
        bits = cfl_hbit2_bank_get(tree, station, bs_id);
        printf("  bits: ");
        for (int i = 0; i < station_bytes; i++) printf("%02X ", bits[i]);
        printf("\n");
        
        // Get bank offset
        int offset = 0;
        for (int i = 0; i < s; i++) offset += lookup->banks_per_station[i];
        
        // Print each bank
        for (int b = 0; b < lookup->banks_per_station[s]; b++) {
            int32_t bank = lookup->bank_nodes[offset + b];
            int bank_bytes = cfl_hbit2_info_bytes(tree, bank, bs_id);
            
            printf("  BANK %d (node %d):\n", b, bank);
            
            // Bits
            bits = cfl_hbit2_bank_get(tree, bank, bs_id);
            printf("    bits:  ");
            for (int i = 0; i < bank_bytes; i++) printf("%02X ", bits[i]);
            printf("\n");
            
            // Latch
            const uint8_t* latch = cfl_hbit2_latch_get(tree, bank, bs_id);
            if (latch) {
                printf("    latch: ");
                for (int i = 0; i < bank_bytes; i++) printf("%02X ", latch[i]);
                printf("\n");
            }
            
            // Mask
            const uint8_t* mask = cfl_hbit2_mask_get(tree, bank, bs_id);
            if (mask) {
                printf("    mask:  ");
                for (int i = 0; i < bank_bytes; i++) printf("%02X ", mask[i]);
                printf("\n");
            }
        }
        printf("\n");
    }
    printf("========================================\n");
}

static cfl_hbit2_tree_t g_tree;
static station_lookup_t g_station_lookup;
 int main(void) {
    printf("Irrigation Example\n");
    printf("==================\n\n");
    cfl_hbit2_status_t s = cfl_hbit2_init(&g_tree, 
        Irrigation_Example_descriptor, sizeof(Irrigation_Example_descriptor));
    printf("Tree initialized %s\n", s == CFL_HBIT2_OK ? "OK" : "FAILED");
    station_lookup_init(&g_tree, &g_station_lookup, 8);
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);
    int32_t bit_out;
    int32_t bank = station_lookup_get_bank(&g_station_lookup, 0, 0, &bit_out);
    printf("Bank: %d\n", bank);
    
    printf("Bit: %d\n", bit_out);
    cfl_hbit2_bit_set(&g_tree, bank, 0, bit_out, true);
    cfl_hbit2_sync(&g_tree);
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);
    cfl_hbit2_bit_set(&g_tree, bank, 0, bit_out, false);
    printf("Latch cleared: %d\n", cfl_hbit2_latch_clear(&g_tree, bank, 0));
    
    cfl_hbit2_sync(&g_tree);
    
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);
    uint8_t mask = 0xfe;
    cfl_hbit2_mask_set(&g_tree, bank, 0, &mask, 1);
    cfl_hbit2_bit_set(&g_tree, bank, 0 , bit_out, 1);
    cfl_hbit2_sync(&g_tree);
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);
    mask = 0xff;
    cfl_hbit2_mask_set(&g_tree, bank, 0, &mask, 1);
    cfl_hbit2_bit_set(&g_tree, bank, 0 , bit_out, 1);
    cfl_hbit2_sync(&g_tree);
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);
    mask = 0xfe;
    cfl_hbit2_mask_set(&g_tree, bank, 0, &mask, 1);
    printf("Mask set: %d %d\n", mask, cfl_hbit2_mask_set(&g_tree, bank, 0, &mask, 1));
    
    cfl_hbit2_sync(&g_tree);
    print_tree_state(&g_tree, &g_station_lookup,"Overall_Valve_Status",0);

    station_lookup_destroy(&g_station_lookup);
    cfl_hbit2_destroy(&g_tree);
    exit(0);
    return 0;
 }