/**
 * @file example_usage.c
 * @brief Comprehensive test for Hierarchical Bit Map Runtime
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 /* Runtime first */
 #include "cfl_hbit.h"
 
 /* Generated headers */
 #define IRRIGATION_VALVES_INCLUDE_PATH_STRINGS
 #include "../test_out/generated_irrigation_valves.h"
 #include "../test_out/generated_irrigation_valves_data.h"
 
 /* ============================================ */
 /* Test Counters                                */
 /* ============================================ */
 
 static int g_tests_passed = 0;
 static int g_tests_failed = 0;
 
 #define TEST_ASSERT(cond, msg) do { \
     if (cond) { g_tests_passed++; } \
     else { g_tests_failed++; printf("  FAIL: %s\n", msg); } \
 } while(0)
 
 #define TEST_ASSERT_EQ(a, b, msg) do { \
     if ((a) == (b)) { g_tests_passed++; } \
     else { g_tests_failed++; printf("  FAIL: %s (got %d, expected %d)\n", msg, (int)(a), (int)(b)); } \
 } while(0)
 
 /* ============================================ */
 /* Allocator                                    */
 /* ============================================ */
 
 static int g_alloc_count = 0;
 static int g_free_count = 0;
 
 static void* my_alloc(size_t size, void* ctx) {
     (void)ctx;
     g_alloc_count++;
     return malloc(size);
 }
 
 static void my_free(void* ptr, void* ctx) {
     (void)ctx;
     if (ptr) g_free_count++;
     free(ptr);
 }
 
 static const cfl_hbit_allocator_t g_alloc = { my_alloc, my_free, NULL };
 
 /* ============================================ */
 /* Helpers                                      */
 /* ============================================ */
 
 static const char* node_path(cfl_hbit_instance_t* inst, uint16_t idx) {
     (void)inst;
     return irrigation_valves_node_paths[idx];
 }
 
 static void print_tree(cfl_hbit_instance_t* inst, uint16_t root, uint16_t buf) {
     const cfl_hbit_node_t* r = &inst->config->nodes[root];
     uint16_t end = root + 1;
     while (end < inst->config->node_count && inst->config->nodes[end].depth > r->depth) end++;
     
     for (uint16_t i = root; i < end; i++) {
         uint8_t data[4] = {0};
         cfl_hbit_read_node(inst, buf, i, data, sizeof(data));
         printf("    [%2d] %-50s = 0x%02X\n", i, node_path(inst, i), data[0]);
     }
 }
 
 /* ============================================ */
 /* Test: Create/Destroy                         */
 /* ============================================ */
 
 static void test_create_destroy(void) {
     printf("\n=== Test: Create/Destroy ===\n");
     
     g_alloc_count = 0;
     g_free_count = 0;
     
     cfl_hbit_instance_t* inst = cfl_hbit_create(
         &g_alloc,
         (const cfl_hbit_config_t*)&irrigation_valves_config);
     
     TEST_ASSERT(inst != NULL, "create returns non-NULL");
     TEST_ASSERT_EQ(g_alloc_count, 2, "create calls alloc twice (instance + RAM)");
     TEST_ASSERT(inst->config == (const cfl_hbit_config_t*)&irrigation_valves_config, "config pointer set");
     TEST_ASSERT(inst->ram != NULL, "RAM allocated");
     
     cfl_hbit_destroy(inst);
     TEST_ASSERT_EQ(g_free_count, 2, "destroy calls free twice");
     
     /* Test NULL handling */
     cfl_hbit_destroy(NULL);  /* Should not crash */
     TEST_ASSERT(1, "destroy(NULL) doesn't crash");
     
     inst = cfl_hbit_create(NULL, (const cfl_hbit_config_t*)&irrigation_valves_config);
     TEST_ASSERT(inst == NULL, "create with NULL allocator returns NULL");
 }
 
 /* ============================================ */
 /* Test: Reset                                  */
 /* ============================================ */
 
 static void test_reset(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Reset ===\n");
     
     /* Set some bits */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 0);
     cfl_hbit_propagate(inst);
     
     bool before = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(before == true, "bit set before reset");
     
     cfl_hbit_reset(inst);
     
     bool after = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(after == false, "bit cleared after reset");
     
     /* Verify leaf nodes in ALARM_LATCHED buffer are zero */
     bool all_zero = true;
     for (uint16_t i = 0; i < inst->config->node_count; i++) {
         if (inst->config->nodes[i].is_leaf) {
             uint8_t data[4] = {0xFF};
             uint8_t len = cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, i, data, sizeof(data));
             if (len > 0 && data[0] != 0) all_zero = false;
         }
     }
     TEST_ASSERT(all_zero, "all leaf nodes zero after reset");
 }
 
 /* ============================================ */
 /* Test: Leaf Set/Clear/Read                    */
 /* ============================================ */
 
 static void test_leaf_operations(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Leaf Operations ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Test set bit */
     cfl_hbit_error_t err = cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 
                                                   IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "set_bit returns OK");
     
     bool val = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 
                                   IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK);
     TEST_ASSERT(val == true, "read_bit after set returns true");
     
     /* Test clear bit */
     err = cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2,
                                    IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "clear_bit returns OK");
     
     val = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2,
                              IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK);
     TEST_ASSERT(val == false, "read_bit after clear returns false");
     
     /* Test multiple bits */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 3);
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 7);
     
     uint8_t data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x89, "multiple bits set correctly (0x89 = bits 0,3,7)");
     
     /* Test leaf_write */
     cfl_hbit_reset(inst);
     uint8_t write_data[1] = {0x55};
     err = cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, write_data, 1);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "leaf_write returns OK");
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x55, "leaf_write sets correct value");
     
     /* Test error cases */
     err = cfl_hbit_leaf_set_bit(inst, 99, 2, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_ERR_INVALID_BUFFER, "invalid buffer returns error");
     
     err = cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 999, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_ERR_INVALID_NODE, "invalid node returns error");
     
     err = cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_ERR_NOT_LEAF, "non-leaf node returns error");
     
     err = cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 99);
     TEST_ASSERT_EQ(err, CFL_HBIT_ERR_OUT_OF_RANGE, "out of range bit returns error");
 }
 
 /* ============================================ */
 /* Test: OR Propagation                         */
 /* ============================================ */
 
 static void test_or_propagation(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: OR Propagation ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Set bits in different leaves under same parent */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);  /* BANK_1: bit 0 */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 3, 1);  /* BANK_2: bit 1 */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 4, 2);  /* BANK_3: bit 2 */
     
     cfl_hbit_propagate(inst);
     
     /* Check parent (STATION_1) has OR of children */
     uint8_t data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x07, "parent ORs children (0x07 = bits 0,1,2)");
     
     /* Check root (VALVE_STATUS) also propagates */
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x07, "root ORs all descendants");
     
     /* Set bit in different station */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 7);  /* STATION_2.BANK_1: bit 7 */
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x87, "root ORs across stations (0x87 = bits 0,1,2,7)");
     
     printf("  Tree after OR propagation:\n");
     print_tree(inst, 0, IRRIGATION_VALVES_BUF_ALARM_LATCHED);
 }
 
 /* ============================================ */
 /* Test: AND Propagation                        */
 /* ============================================ */
 
 static void test_and_propagation(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: AND Propagation ===\n");
     
     cfl_hbit_reset(inst);
     
     /* AND buffer - all bits must be set for parent to be set */
     /* Set all bits in all leaves under STATION_1_VALVE_STATE */
     uint8_t all_bits[1] = {0xFF};
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 20, all_bits, 1);  /* BANK_1 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 21, all_bits, 1);  /* BANK_2 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 22, all_bits, 1);  /* BANK_3 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 23, all_bits, 1);  /* BANK_4 */
     
     cfl_hbit_propagate(inst);
     
     /* Parent should have 0xFF (AND of all 0xFF) */
     uint8_t data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 19, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0xFF, "AND parent is 0xFF when all children 0xFF");
     
     /* Clear one bit in one child */
     cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 20, 0);
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_AND_LATCHED, 19, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0xFE, "AND parent clears bit when any child clears (0xFE)");
     
     printf("  AND tree after propagation:\n");
     print_tree(inst, 18, IRRIGATION_VALVES_BUF_AND_LATCHED);
 }
 
 /* ============================================ */
 /* Test: Latched Buffer                         */
 /* ============================================ */
 
 static void test_latched_buffer(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Latched Buffer ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Set a bit - should latch */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_propagate(inst);
     
     bool current = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     bool latched = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(current == true, "current bit is set");
     TEST_ASSERT(latched == true, "latched bit is set");
     
     /* Clear current - latched should remain */
     cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_propagate(inst);
     
     current = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     latched = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(current == false, "current bit cleared");
     TEST_ASSERT(latched == true, "latched bit still set");
     
     /* Clear latch */
     cfl_hbit_error_t err = cfl_hbit_clear_latch_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "clear_latch_bit returns OK");
     
     latched = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(latched == false, "latched bit cleared");
     
     /* Test clear_latch_all */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 1);
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 2);
     cfl_hbit_propagate(inst);
     
     err = cfl_hbit_clear_latch_all(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "clear_latch_all returns OK");
     
     latched = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(latched == false, "all latched bits cleared");
 }
 
 /* ============================================ */
 /* Test: Latch Propagation                      */
 /* ============================================ */
 
 static void test_latch_propagation(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Latch Propagation ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Set bits in multiple leaves */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);  /* BANK_1: bit 0 */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 3, 1);  /* BANK_2: bit 1 */
     cfl_hbit_propagate(inst);
     
     /* Check parent has latched bits from children */
     bool parent_latched_0 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 0);
     bool parent_latched_1 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 1);
     TEST_ASSERT(parent_latched_0 == true, "parent latched bit 0 from child BANK_1");
     TEST_ASSERT(parent_latched_1 == true, "parent latched bit 1 from child BANK_2");
     
     /* Check root has latched bits */
     bool root_latched_0 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 0);
     bool root_latched_1 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 1);
     TEST_ASSERT(root_latched_0 == true, "root latched bit 0 propagated up");
     TEST_ASSERT(root_latched_1 == true, "root latched bit 1 propagated up");
     
     /* Clear current bits in leaves */
     cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 3, 1);
     cfl_hbit_propagate(inst);
     
     /* Current should be cleared, but latched should remain */
     bool parent_current_0 = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 0);
     bool parent_current_1 = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 1);
     TEST_ASSERT(parent_current_0 == false, "parent current bit 0 cleared");
     TEST_ASSERT(parent_current_1 == false, "parent current bit 1 cleared");
     
     parent_latched_0 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 0);
     parent_latched_1 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 1);
     TEST_ASSERT(parent_latched_0 == true, "parent latched bit 0 still set after current cleared");
     TEST_ASSERT(parent_latched_1 == true, "parent latched bit 1 still set after current cleared");
     
     /* Clear latch at leaf level */
     cfl_hbit_clear_latch_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     cfl_hbit_propagate(inst);
     
     /* Leaf latch cleared, but parent latch should still be set (no auto-clear) */
     bool leaf_latched = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 2, 0);
     TEST_ASSERT(leaf_latched == false, "leaf latched bit cleared");
     
     /* Parent latch propagates from children - check if it recalculates */
     parent_latched_0 = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 1, 0);
     /* Note: Latch propagation re-ORs children, so parent should reflect cleared child */
     
     printf("  After clearing leaf latch, parent latched bit 0 = %d\n", parent_latched_0);
     
     /* Test transient alarm: set, propagate, clear, verify latch remains at all levels */
     cfl_hbit_reset(inst);
     
     /* Simulate transient alarm */
     cfl_hbit_leaf_set_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 3);  /* STATION_2.BANK_1: LEAK */
     cfl_hbit_propagate(inst);
     
     /* Verify latch at all levels */
     bool leaf_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 3);
     bool station_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 6, 3);
     bool root_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 3);
     TEST_ASSERT(leaf_l == true, "transient: leaf latched");
     TEST_ASSERT(station_l == true, "transient: station latched");
     TEST_ASSERT(root_l == true, "transient: root latched");
     
     /* Clear current (alarm goes away) */
     cfl_hbit_leaf_clear_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 3);
     cfl_hbit_propagate(inst);
     
     /* Current should be gone */
     bool leaf_c = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 3);
     bool station_c = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 6, 3);
     bool root_c = cfl_hbit_read_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 3);
     TEST_ASSERT(leaf_c == false, "transient: leaf current cleared");
     TEST_ASSERT(station_c == false, "transient: station current cleared");
     TEST_ASSERT(root_c == false, "transient: root current cleared");
     
     /* Latch should remain */
     leaf_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 7, 3);
     station_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 6, 3);
     root_l = cfl_hbit_read_latched_bit(inst, IRRIGATION_VALVES_BUF_ALARM_LATCHED, 0, 3);
     TEST_ASSERT(leaf_l == true, "transient: leaf latch remains");
     TEST_ASSERT(station_l == true, "transient: station latch remains");
     TEST_ASSERT(root_l == true, "transient: root latch remains");
     
     printf("  Transient alarm test: current cleared, latch remains at all levels\n");
 }
 
 /* ============================================ */
 /* Test: Mask Buffer                            */
 /* ============================================ */
 
 static void test_mask_buffer(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Mask Buffer ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Set bits in leaf */
     uint8_t data[1] = {0xFF};
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, data, 1);
     cfl_hbit_propagate(inst);
     
     /* By default mask is 0xFF, so all bits propagate */
     uint8_t parent_data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, parent_data, sizeof(parent_data));
     TEST_ASSERT_EQ(parent_data[0], 0xFF, "all bits propagate with default mask");
     
     /* Set mask to block some bits */
     uint8_t mask[1] = {0x0F};  /* Only lower 4 bits */
     cfl_hbit_error_t err = cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, mask, 1);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "set_mask returns OK");
     
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, parent_data, sizeof(parent_data));
     TEST_ASSERT_EQ(parent_data[0], 0x0F, "mask blocks upper bits");
     
     /* Test set_mask_bit - enable bit 7 */
     err = cfl_hbit_set_mask_bit(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, 7, true);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "set_mask_bit returns OK");
     
     /* Mark node dirty so propagate recalculates */
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, parent_data, sizeof(parent_data));
     TEST_ASSERT_EQ(parent_data[0], 0x8F, "mask bit 7 enabled (0x8F)");
 }
 
 /* ============================================ */
 /* Test: Mask Propagation                       */
 /* ============================================ */
 
 static void test_mask_propagation(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Mask Propagation ===\n");
     
     cfl_hbit_reset(inst);
     
     /* Set all bits in multiple leaves under STATION_1 */
     uint8_t all_bits[1] = {0xFF};
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, all_bits, 1);  /* BANK_1 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 3, all_bits, 1);  /* BANK_2 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 4, all_bits, 1);  /* BANK_3 */
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 5, all_bits, 1);  /* BANK_4 */
     cfl_hbit_propagate(inst);
     
     /* All should propagate with default mask */
     uint8_t station_data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0xFF, "station has all bits with default mask");
     
     uint8_t root_data[4] = {0};
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 0, root_data, sizeof(root_data));
     TEST_ASSERT_EQ(root_data[0], 0xFF, "root has all bits with default mask");
     
     /* Now mask out bit 0 on BANK_1 only */
     uint8_t mask_no_bit0[1] = {0xFE};
     cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, mask_no_bit0, 1);
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     /* Station should still have bit 0 from other banks */
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0xFF, "station still has bit 0 from other banks");
     
     /* Mask out bit 0 on ALL banks in STATION_1 */
     cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 3, mask_no_bit0, 1);
     cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 4, mask_no_bit0, 1);
     cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 5, mask_no_bit0, 1);
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     /* Now station should NOT have bit 0 */
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0xFE, "station bit 0 masked when all children mask it");
     
     printf("  Station after masking bit 0 on all banks: 0x%02X\n", station_data[0]);
     
     /* Test selective masking: different masks per leaf */
     cfl_hbit_reset(inst);
     
     /* Each bank has different alarm bits active */
     uint8_t bank1_bits[1] = {0x01};  /* Only OVERCURRENT */
     uint8_t bank2_bits[1] = {0x02};  /* Only STUCK_OPEN */
     uint8_t bank3_bits[1] = {0x04};  /* Only STUCK_CLOSED */
     uint8_t bank4_bits[1] = {0x08};  /* Only LEAK */
     
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, bank1_bits, 1);
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 3, bank2_bits, 1);
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 4, bank3_bits, 1);
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 5, bank4_bits, 1);
     cfl_hbit_propagate(inst);
     
     /* Station should OR all: 0x0F */
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0x0F, "station ORs different bits from each bank");
     
     /* Now mask BANK_1 completely */
     uint8_t mask_none[1] = {0x00};
     cfl_hbit_set_mask(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, mask_none, 1);
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     /* Station should now be 0x0E (missing bit 0 from BANK_1) */
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0x0E, "masking BANK_1 removes its contribution");
     
     printf("  Selective masking: station = 0x%02X after masking BANK_1\n", station_data[0]);
     
     /* Test mask disable/enable toggle */
     cfl_hbit_reset(inst);
     
     cfl_hbit_leaf_write(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, all_bits, 1);
     cfl_hbit_propagate(inst);
     
     /* Disable bit 7 via mask */
     cfl_hbit_set_mask_bit(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, 7, false);
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0x7F, "disable bit 7 via set_mask_bit");
     
     /* Re-enable bit 7 */
     cfl_hbit_set_mask_bit(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 2, 7, true);
     cfl_hbit_mark_dirty(inst, 1);
     cfl_hbit_propagate(inst);
     
     cfl_hbit_read_node(inst, IRRIGATION_VALVES_BUF_ALARM_MASK, 1, station_data, sizeof(station_data));
     TEST_ASSERT_EQ(station_data[0], 0xFF, "re-enable bit 7 via set_mask_bit");
     
     printf("  Toggle test passed: disable/enable bit 7\n");
 }
 
 /* ============================================ */
 /* Test: Node Lookup                            */
 /* ============================================ */
 
 static void test_node_lookup(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Node Lookup ===\n");
     
     /* Get hash from node table and search for it */
     uint32_t hash0 = inst->config->nodes[0].path_hash;  /* VALVE_STATUS */
     int16_t idx = cfl_hbit_find_node(inst, hash0);
     TEST_ASSERT_EQ(idx, 0, "find VALVE_STATUS returns index 0");
     
     uint32_t hash1 = inst->config->nodes[1].path_hash;  /* STATION_1_VALVE_STATUS */
     idx = cfl_hbit_find_node(inst, hash1);
     TEST_ASSERT_EQ(idx, 1, "find STATION_1_VALVE_STATUS returns index 1");
     
     uint32_t hash2 = inst->config->nodes[2].path_hash;  /* BANK_1_VALVE_STATUS */
     idx = cfl_hbit_find_node(inst, hash2);
     TEST_ASSERT_EQ(idx, 2, "find BANK_1_VALVE_STATUS returns index 2");
     
     /* Test not found */
     idx = cfl_hbit_find_node(inst, 0xDEADBEEF);
     TEST_ASSERT_EQ(idx, -1, "invalid hash returns -1");
     
     /* Test path string lookup */
     idx = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
     TEST_ASSERT_EQ(idx, 0, "find_node_path VALVE_STATUS returns 0");
     
     idx = cfl_hbit_find_node_path(inst, "VALVE_STATUS.STATION_1_VALVE_STATUS");
     TEST_ASSERT_EQ(idx, 1, "find_node_path STATION_1 returns 1");
     
     idx = cfl_hbit_find_node_path(inst, "VALVE_STATUS.STATION_1_VALVE_STATUS.BANK_1_VALVE_STATUS");
     TEST_ASSERT_EQ(idx, 2, "find_node_path BANK_1 returns 2");
     
     /* Test printf-style formatting */
     idx = cfl_hbit_find_node_path(inst, "VALVE_STATUS.STATION_%d_VALVE_STATUS", 2);
     TEST_ASSERT_EQ(idx, 6, "find_node_path STATION_2 via printf returns 6");
     
     idx = cfl_hbit_find_node_path(inst, "VALVE_STATUS.STATION_%d_VALVE_STATUS.BANK_%d_VALVE_STATUS", 1, 3);
     TEST_ASSERT_EQ(idx, 4, "find_node_path STATION_1.BANK_3 via printf returns 4");
     
     /* Test not found with path */
     idx = cfl_hbit_find_node_path(inst, "NONEXISTENT.PATH");
     TEST_ASSERT_EQ(idx, -1, "find_node_path invalid path returns -1");
     
     printf("  Path lookup with printf formatting works\n");
     
     /* Test bit lookup by name */
     int8_t bit_idx = irrigation_valves_find_node_bit(inst, 2, IRRIGATION_VALVES_BUF_ALARM_LATCHED, "LEAK");
     TEST_ASSERT_EQ(bit_idx, IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK, "find_node_bit LEAK returns correct index");
     
     bit_idx = irrigation_valves_find_node_bit(inst, 2, IRRIGATION_VALVES_BUF_ALARM_LATCHED, "OVERCURRENT");
     TEST_ASSERT_EQ(bit_idx, IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_OVERCURRENT, "find_node_bit OVERCURRENT returns correct index");
     
     bit_idx = irrigation_valves_find_node_bit(inst, 2, IRRIGATION_VALVES_BUF_ALARM_LATCHED, "HIGH_PRESSURE");
     TEST_ASSERT_EQ(bit_idx, IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_HIGH_PRESSURE, "find_node_bit HIGH_PRESSURE returns correct index");
     
     bit_idx = irrigation_valves_find_node_bit(inst, 2, IRRIGATION_VALVES_BUF_ALARM_LATCHED, "NONEXISTENT");
     TEST_ASSERT_EQ(bit_idx, -1, "find_node_bit invalid name returns -1");
     
     /* Test AND buffer bits */
     bit_idx = irrigation_valves_find_node_bit(inst, 20, IRRIGATION_VALVES_BUF_AND_LATCHED, "POWERED");
     TEST_ASSERT_EQ(bit_idx, IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_POWERED, "find_node_bit POWERED in AND buffer");
     
     printf("  Bit lookup by name works\n");
     
     /* Test buffer lookup by name */
     int16_t buf_idx = irrigation_valves_find_buffer("ALARM_LATCHED");
     TEST_ASSERT_EQ(buf_idx, IRRIGATION_VALVES_BUF_ALARM_LATCHED, "find_buffer ALARM_LATCHED");
     
     buf_idx = irrigation_valves_find_buffer("ALARM_MASK");
     TEST_ASSERT_EQ(buf_idx, IRRIGATION_VALVES_BUF_ALARM_MASK, "find_buffer ALARM_MASK");
     
     buf_idx = irrigation_valves_find_buffer("AND_LATCHED");
     TEST_ASSERT_EQ(buf_idx, IRRIGATION_VALVES_BUF_AND_LATCHED, "find_buffer AND_LATCHED");
     
     buf_idx = irrigation_valves_find_buffer("NONEXISTENT");
     TEST_ASSERT_EQ(buf_idx, -1, "find_buffer invalid returns -1");
     
     printf("  Buffer lookup by name works\n");
 }
 
 /* ============================================ */
 /* Test: Tree Walking                           */
 /* ============================================ */
 
 static int g_walk_count = 0;
 static uint16_t g_walk_nodes[64];
 
 static bool walk_callback(cfl_hbit_instance_t* inst, uint16_t node_idx, void* ctx) {
     (void)inst;
     (void)ctx;
     if (g_walk_count < 64) {
         g_walk_nodes[g_walk_count] = node_idx;
     }
     g_walk_count++;
     return true;
 }
 
 static bool walk_callback_stop(cfl_hbit_instance_t* inst, uint16_t node_idx, void* ctx) {
     (void)inst;
     (void)node_idx;
     (void)ctx;
     g_walk_count++;
     return (g_walk_count < 3);  /* Stop after 3 nodes */
 }
 
 static void test_tree_walking(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Tree Walking ===\n");
     
     /* Preorder walk */
     g_walk_count = 0;
     cfl_hbit_walk_preorder(inst, 0, walk_callback, NULL);
     TEST_ASSERT_EQ(g_walk_count, 18, "preorder visits 18 nodes in VALVE_STATUS tree");
     TEST_ASSERT_EQ(g_walk_nodes[0], 0, "preorder starts at root");
     TEST_ASSERT_EQ(g_walk_nodes[1], 1, "preorder visits first child second");
     
     /* Postorder walk */
     g_walk_count = 0;
     cfl_hbit_walk_postorder(inst, 0, walk_callback, NULL);
     TEST_ASSERT_EQ(g_walk_count, 18, "postorder visits 18 nodes");
     TEST_ASSERT_EQ(g_walk_nodes[17], 0, "postorder visits root last");
     
     /* Foreach child */
     g_walk_count = 0;
     cfl_hbit_foreach_child(inst, 0, walk_callback, NULL);
     TEST_ASSERT_EQ(g_walk_count, 4, "foreach_child visits 4 children of root");
     
     /* Early termination */
     g_walk_count = 0;
     cfl_hbit_walk_preorder(inst, 0, walk_callback_stop, NULL);
     TEST_ASSERT_EQ(g_walk_count, 3, "walk stops after callback returns false");
 }
 
 /* ============================================ */
 /* Test: Controller                             */
 /* ============================================ */
 
 static void test_controller(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Controller ===\n");
     
     cfl_hbit_reset(inst);
     
     cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, 0, IRRIGATION_VALVES_BUF_ALARM_LATCHED);
     TEST_ASSERT(ctrl != NULL, "controller create returns non-NULL");
     
     if (!ctrl) return;
     
     TEST_ASSERT_EQ(ctrl->child_count, 4, "controller has 4 children (stations)");
     TEST_ASSERT_EQ(ctrl->leaf_count, 13, "controller has 13 leaves (banks)");
     TEST_ASSERT_EQ(ctrl->bits_per_leaf, 8, "8 bits per leaf");
     TEST_ASSERT_EQ(ctrl->total_bits, 104, "104 total bits (13 * 8)");
     
     /* Test flat bitmap access */
     cfl_hbit_error_t err = cfl_hbit_controller_set_bit(ctrl, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "controller_set_bit returns OK");
     
     cfl_hbit_propagate(inst);
     
     bool val = cfl_hbit_controller_read_bit(ctrl, 0);
     TEST_ASSERT(val == true, "controller_read_bit returns true");
     
     err = cfl_hbit_controller_clear_bit(ctrl, 0);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "controller_clear_bit returns OK");
     
     val = cfl_hbit_controller_read_bit(ctrl, 0);
     TEST_ASSERT(val == false, "controller_read_bit returns false after clear");
     
     /* Test child-indexed access */
     err = cfl_hbit_controller_set_child_bit(ctrl, 1, 0);  /* Station 2, bit 0 */
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "controller_set_child_bit returns OK");
     
     cfl_hbit_propagate(inst);
     
     val = cfl_hbit_controller_read_child_bit(ctrl, 1, 0);
     TEST_ASSERT(val == true, "controller_read_child_bit returns true");
     
     /* Test get_node_bit mapping */
     uint8_t bit_idx;
     int16_t node = cfl_hbit_controller_get_node_bit(ctrl, 0, 0, &bit_idx);
     TEST_ASSERT_EQ(node, 2, "child 0 bit 0 maps to node 2");
     TEST_ASSERT_EQ(bit_idx, 0, "child 0 bit 0 maps to bit 0");
     
     node = cfl_hbit_controller_get_node_bit(ctrl, 1, 0, &bit_idx);
     TEST_ASSERT_EQ(node, 7, "child 1 bit 0 maps to node 7");
     
     /* Test get_bitmap_node mapping */
     node = cfl_hbit_controller_get_bitmap_node(ctrl, 0, &bit_idx);
     TEST_ASSERT_EQ(node, 2, "flat 0 maps to node 2");
     
     node = cfl_hbit_controller_get_bitmap_node(ctrl, 32, &bit_idx);
     TEST_ASSERT_EQ(node, 7, "flat 32 maps to node 7 (first leaf of station 2)");
     TEST_ASSERT_EQ(bit_idx, 0, "flat 32 maps to bit 0");
     
     /* Test setting all leaves via flat index */
     cfl_hbit_reset(inst);
     for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
         cfl_hbit_controller_set_bit(ctrl, i * ctrl->bits_per_leaf);
     }
     cfl_hbit_propagate(inst);
     
     /* Verify all leaves have bit 0 set */
     bool all_set = true;
     for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
         if (!cfl_hbit_controller_read_bit(ctrl, i * ctrl->bits_per_leaf)) {
             all_set = false;
         }
     }
     TEST_ASSERT(all_set, "all leaves have bit 0 set via flat index");
     
     /* Test boundary: last bit */
     err = cfl_hbit_controller_set_bit(ctrl, ctrl->total_bits - 1);
     TEST_ASSERT_EQ(err, CFL_HBIT_OK, "set last bit OK");
     
     err = cfl_hbit_controller_set_bit(ctrl, ctrl->total_bits);
     TEST_ASSERT_EQ(err, CFL_HBIT_ERR_OUT_OF_RANGE, "set beyond total_bits returns error");
     
     cfl_hbit_controller_destroy(ctrl);
     TEST_ASSERT(1, "controller_destroy completes");
 }
 
 /* ============================================ */
 /* Test: Controller Per-Bank                    */
 /* ============================================ */
 
 static void test_controller_per_bank(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Controller Per-Bank ===\n");
     
     cfl_hbit_reset(inst);
     
     cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, 0, IRRIGATION_VALVES_BUF_ALARM_LATCHED);
     if (!ctrl) { printf("  SKIP: controller create failed\n"); return; }
     
     const char* alarm_names[] = {
         "OVERCURRENT", "STUCK_OPEN", "STUCK_CLOSED", "LEAK",
         "OVERTEMP", "COMM_FAIL", "LOW_PRESSURE", "HIGH_PRESSURE"
     };
     
     /* Set different alarm in each bank */
     printf("  Setting rotating alarms in each bank:\n");
     for (uint16_t leaf = 0; leaf < ctrl->leaf_count; leaf++) {
         uint8_t alarm = leaf % 8;
         uint16_t flat_idx = leaf * ctrl->bits_per_leaf + alarm;
         cfl_hbit_controller_set_bit(ctrl, flat_idx);
         printf("    Leaf %2d (node %2d): %s\n", 
                leaf, ctrl->leaf_nodes[leaf], alarm_names[alarm]);
     }
     
     cfl_hbit_propagate(inst);
     
     /* Verify each bank */
     bool all_correct = true;
     for (uint16_t leaf = 0; leaf < ctrl->leaf_count; leaf++) {
         uint8_t expected_alarm = leaf % 8;
         for (uint8_t bit = 0; bit < 8; bit++) {
             uint16_t flat_idx = leaf * ctrl->bits_per_leaf + bit;
             bool val = cfl_hbit_controller_read_bit(ctrl, flat_idx);
             bool expected = (bit == expected_alarm);
             if (val != expected) all_correct = false;
         }
     }
     TEST_ASSERT(all_correct, "all banks have correct rotating alarm set");
     
     /* Test per-child access */
     cfl_hbit_reset(inst);
     
     printf("  Setting bit 0 in each child's first and last bank:\n");
     for (uint16_t child = 0; child < ctrl->child_count; child++) {
         cfl_hbit_child_t* c = &ctrl->children[child];
         
         /* First bit of child */
         cfl_hbit_controller_set_child_bit(ctrl, child, 0);
         /* Last bit of child */
         cfl_hbit_controller_set_child_bit(ctrl, child, c->bit_count - 1);
         
         printf("    Child %d: bits 0 and %d\n", child, c->bit_count - 1);
     }
     
     cfl_hbit_propagate(inst);
     
     /* Verify */
     all_correct = true;
     for (uint16_t child = 0; child < ctrl->child_count; child++) {
         cfl_hbit_child_t* c = &ctrl->children[child];
         if (!cfl_hbit_controller_read_child_bit(ctrl, child, 0)) all_correct = false;
         if (!cfl_hbit_controller_read_child_bit(ctrl, child, c->bit_count - 1)) all_correct = false;
     }
     TEST_ASSERT(all_correct, "all children have first and last bit set");
     
     cfl_hbit_controller_destroy(ctrl);
 }
 
 /* ============================================ */
 /* Test: Shadow Buffer and Sync                 */
 /* ============================================ */
 
 static void test_shadow_sync(cfl_hbit_instance_t* inst) {
     printf("\n=== Test: Shadow Buffer and Sync ===\n");
     
     cfl_hbit_reset(inst);
     
     uint16_t buf = IRRIGATION_VALVES_BUF_ALARM_LATCHED;
     
     /* Write to shadow - should NOT affect current yet */
     cfl_hbit_shadow_set_bit(inst, buf, 2, 0);  /* BANK_1: bit 0 */
     cfl_hbit_shadow_set_bit(inst, buf, 3, 1);  /* BANK_2: bit 1 */
     cfl_hbit_shadow_set_bit(inst, buf, 7, 2);  /* STATION_2.BANK_1: bit 2 */
     
     /* Current should still be zero */
     bool cur_before = cfl_hbit_read_bit(inst, buf, 2, 0);
     TEST_ASSERT(cur_before == false, "current unchanged before sync");
     
     /* Sync shadow to current */
     cfl_hbit_sync(inst);
     
     /* Now current should have the bits */
     bool cur_after = cfl_hbit_read_bit(inst, buf, 2, 0);
     TEST_ASSERT(cur_after == true, "current updated after sync");
     
     /* Propagate to update parents */
     cfl_hbit_propagate(inst);
     
     /* Check parent has OR of all children */
     uint8_t data[4] = {0};
     cfl_hbit_read_node(inst, buf, 1, data, sizeof(data));  /* STATION_1 */
     TEST_ASSERT_EQ(data[0], 0x03, "STATION_1 has bits 0,1 after sync+propagate");
     
     cfl_hbit_read_node(inst, buf, 0, data, sizeof(data));  /* Root */
     TEST_ASSERT_EQ(data[0], 0x07, "Root has bits 0,1,2 after sync+propagate");
     
     printf("  Verified: shadow -> sync -> propagate workflow\n");
     
     /* Test atomic update: multiple leaves, single sync */
     cfl_hbit_reset(inst);
     
     /* Stage multiple updates in shadow */
     for (uint16_t leaf = 2; leaf <= 5; leaf++) {
         cfl_hbit_shadow_set_bit(inst, buf, leaf, 7);  /* All STATION_1 banks: bit 7 */
     }
     
     /* Verify current still zero */
     bool any_set = false;
     for (uint16_t leaf = 2; leaf <= 5; leaf++) {
         if (cfl_hbit_read_bit(inst, buf, leaf, 7)) any_set = true;
     }
     TEST_ASSERT(any_set == false, "all leaves still zero before sync");
     
     /* Single sync updates all atomically */
     cfl_hbit_sync(inst);
     cfl_hbit_propagate(inst);
     
     /* Now all should be set */
     bool all_set = true;
     for (uint16_t leaf = 2; leaf <= 5; leaf++) {
         if (!cfl_hbit_read_bit(inst, buf, leaf, 7)) all_set = false;
     }
     TEST_ASSERT(all_set == true, "all leaves set after single sync");
     
     cfl_hbit_read_node(inst, buf, 1, data, sizeof(data));  /* STATION_1 */
     TEST_ASSERT_EQ(data[0], 0x80, "STATION_1 has bit 7 from all banks");
     
     printf("  Verified: atomic multi-leaf update via shadow\n");
     
     /* Test shadow_write */
     cfl_hbit_reset(inst);
     
     uint8_t pattern[1] = {0xAA};
     cfl_hbit_shadow_write(inst, buf, 2, pattern, 1);
     
     /* Current should be zero */
     cfl_hbit_read_node(inst, buf, 2, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0x00, "current zero before sync (shadow_write)");
     
     cfl_hbit_sync(inst);
     
     cfl_hbit_read_node(inst, buf, 2, data, sizeof(data));
     TEST_ASSERT_EQ(data[0], 0xAA, "current has pattern after sync");
     
     /* Verify latch was updated during sync */
     bool lat_5 = cfl_hbit_read_latched_bit(inst, buf, 2, 5);
     bool lat_6 = cfl_hbit_read_latched_bit(inst, buf, 2, 6);
     TEST_ASSERT(lat_5 == true, "latch bit 5 set (0xAA has bit 5)");
     TEST_ASSERT(lat_6 == false, "latch bit 6 clear (0xAA lacks bit 6)");
     
     printf("  Verified: shadow_write with latch update\n");
 }
 
 /* ============================================ */
 /* Main                                         */
 /* ============================================ */
 
 int main(void) {
     printf("========================================\n");
     printf("Hierarchical Bit Map Comprehensive Test\n");
     printf("========================================\n");
     printf("Schema: irrigation_valves v%s\n", IRRIGATION_VALVES_VERSION);
     printf("Nodes: %d, Buffers: %d, Roots: %d\n",
            IRRIGATION_VALVES_NODE_COUNT,
            IRRIGATION_VALVES_BUFFER_COUNT,
            IRRIGATION_VALVES_ROOT_COUNT);
     printf("RAM: %d bytes\n", IRRIGATION_VALVES_RAM_SIZE);
     
     /* Test create/destroy standalone */
     test_create_destroy();
     
     /* Create instance for remaining tests */
     g_alloc_count = 0;
     g_free_count = 0;
     
     cfl_hbit_instance_t* inst = cfl_hbit_create(
         &g_alloc,
         (const cfl_hbit_config_t*)&irrigation_valves_config);
     
     if (!inst) {
         printf("\nFATAL: Failed to create instance\n");
         return 1;
     }
     
     /* Run tests */
     test_reset(inst);
     test_leaf_operations(inst);
     test_or_propagation(inst);
     test_and_propagation(inst);
     test_latched_buffer(inst);
     test_latch_propagation(inst);
     test_mask_buffer(inst);
     test_mask_propagation(inst);
     test_node_lookup(inst);
     test_tree_walking(inst);
     test_controller(inst);
     test_controller_per_bank(inst);
     test_shadow_sync(inst);
     
     /* Cleanup */
     cfl_hbit_destroy(inst);
     
     /* Summary */
     printf("\n========================================\n");
     printf("Test Summary\n");
     printf("========================================\n");
     printf("Passed: %d\n", g_tests_passed);
     printf("Failed: %d\n", g_tests_failed);
     printf("Allocs: %d, Frees: %d\n", g_alloc_count, g_free_count);
     
     if (g_alloc_count != g_free_count) {
         printf("WARNING: Memory leak detected!\n");
     }
     
     if (g_tests_failed > 0) {
         printf("\n*** TESTS FAILED ***\n");
         return 1;
     }
     
     printf("\n*** ALL TESTS PASSED ***\n");
     return 0;
 }