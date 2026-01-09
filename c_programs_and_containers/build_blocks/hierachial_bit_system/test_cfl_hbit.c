/**
 * @file test_cfl_hbit.c
 * @brief Test program for ChainTree Hierarchical Bit Map Runtime
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <stdint.h>
 #include <string.h>
 #include <assert.h>
 
 /* Exception handler implementation */
 void cfl_exception_handler(const char* file, const char* func, uint16_t line, const char* msg) {
     fprintf(stderr, "EXCEPTION in %s:%s:%d: %s\n", file, func, line, msg);
     exit(1);
 }
 
 #include "cfl_hbit.h"
 #include "output/generated_ChainBitTreeDemo.bin.h"
 
 /* ============================================ */
 /* Test Helpers                                 */
 /* ============================================ */
 
 static int tests_passed = 0;
 static int tests_failed = 0;
 
 #define TEST(name) \
     printf("  Testing %s... ", name); \
     fflush(stdout);
 
 #define PASS() \
     printf("PASS\n"); \
     tests_passed++;
 
 #define FAIL(msg) \
     printf("FAIL: %s\n", msg); \
     tests_failed++;
 
 #define ASSERT_EQ(a, b, msg) \
     if ((a) != (b)) { FAIL(msg); return; } 
 
 #define ASSERT_OK(status, msg) \
     if ((status) != CFL_HBIT_OK) { FAIL(msg); return; }
 
 #define ASSERT_TRUE(cond, msg) \
     if (!(cond)) { FAIL(msg); return; }
 
 /* Change callback for testing */
 static int change_callback_count = 0;
 static uint32_t last_changed_hash = 0;
 static uint16_t last_changed_bitspace = 0;
 
 static void test_change_cb(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t node_hash,
     const uint8_t* old_bits,
     const uint8_t* new_bits,
     uint16_t byte_count,
     void* user_data
 ) {
     (void)tree;
     (void)old_bits;
     (void)new_bits;
     (void)byte_count;
     (void)user_data;
     
     change_callback_count++;
     last_changed_hash = node_hash;
     last_changed_bitspace = bitspace_id;
 }
 
 /* ============================================ */
 /* Tests                                        */
 /* ============================================ */
 
 static void test_hash_function(void) {
     TEST("hash_function");
     
     uint32_t h1 = cfl_hbit_hash_string("Plant");
     uint32_t h2 = cfl_hbit_hash_string("Plant");
     ASSERT_EQ(h1, h2, "Same string should produce same hash");
     
     uint32_t h3 = cfl_hbit_hash_string("Plant.Line1");
     ASSERT_TRUE(h1 != h3, "Different strings should produce different hashes");
     
     uint32_t h4 = cfl_hbit_hash_printf("Plant.Line%d", 1);
     ASSERT_EQ(h3, h4, "Printf hash should match string hash");
     
     uint32_t h5 = cfl_hbit_hash_printf("Plant.Line%d.Cell%d", 1, 1);
     uint32_t h6 = cfl_hbit_hash_string("Plant.Line1.Cell1");
     ASSERT_EQ(h5, h6, "Printf hash with multiple args should match");
     
     PASS();
 }
 
 static void test_init(cfl_hbit_t* tree) {
     TEST("initialization");
     
     cfl_hbit_status_t status = cfl_hbit_init(
         tree,
         ChainBitTreeDemo_descriptor,
         ChainBitTreeDemo_descriptor_size,
         NULL  /* Use stdlib malloc/free */
     );
     ASSERT_OK(status, "init failed");
     
     ASSERT_EQ(cfl_hbit_node_count(tree), CHAINBITTREEDEMO_NODE_COUNT, "node count mismatch");
     ASSERT_EQ(cfl_hbit_bitspace_count(tree), CHAINBITTREEDEMO_BITSPACE_COUNT, "bitspace count mismatch");
     
     PASS();
 }
 
 static void test_path_exists(cfl_hbit_t* tree) {
     TEST("path_exists");
     
     ASSERT_TRUE(cfl_hbit_path_exists(tree, "Plant"), "Plant should exist");
     ASSERT_TRUE(cfl_hbit_path_exists(tree, "Plant.Line1"), "Plant.Line1 should exist");
     ASSERT_TRUE(cfl_hbit_path_exists(tree, "Plant.Line1.Cell1.Robot1"), "Robot1 should exist");
     ASSERT_TRUE(!cfl_hbit_path_exists(tree, "Plant.Line99"), "Line99 should not exist");
     
     ASSERT_TRUE(cfl_hbit_path_exists(tree, "Plant.Line%d", 1), "Printf path should work");
     ASSERT_TRUE(cfl_hbit_path_exists(tree, "Plant.Line%d.Cell%d", 1, 1), "Printf path multi args");
     
     PASS();
 }
 
 static void test_set_get_bit(cfl_hbit_t* tree) {
     TEST("set_get_bit");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     cfl_hbit_status_t status = cfl_hbit_set_bit(
         tree,
         CHAINBITTREEDEMO_BS_ALARM,
         CHAINBITTREEDEMO_ROBOTARM_ALARM_OVERTORQUE,
         true,
         "Plant.Line1.Cell1.Robot1"
     );
     ASSERT_OK(status, "set_bit failed");
     
     cfl_hbit_sync(tree);
     
     int val = cfl_hbit_get_bit(
         tree,
         CHAINBITTREEDEMO_BS_ALARM,
         CHAINBITTREEDEMO_ROBOTARM_ALARM_OVERTORQUE,
         "Plant.Line1.Cell1.Robot1"
     );
     ASSERT_EQ(val, 1, "bit should be set");
     
     status = cfl_hbit_set_bit(
         tree,
         CHAINBITTREEDEMO_BS_ALARM,
         CHAINBITTREEDEMO_ROBOTARM_ALARM_OVERTORQUE,
         false,
         "Plant.Line1.Cell1.Robot1"
     );
     ASSERT_OK(status, "clear_bit failed");
     
     cfl_hbit_sync(tree);
     
     val = cfl_hbit_get_bit(
         tree,
         CHAINBITTREEDEMO_BS_ALARM,
         CHAINBITTREEDEMO_ROBOTARM_ALARM_OVERTORQUE,
         "Plant.Line1.Cell1.Robot1"
     );
     ASSERT_EQ(val, 0, "bit should be cleared");
     
     PASS();
 }
 
 static void test_or_propagation(cfl_hbit_t* tree) {
     TEST("or_propagation");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM, 0, true, 
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     const uint8_t* cell_alarm = cfl_hbit_get_bits(tree, CHAINBITTREEDEMO_BS_ALARM,
                                                    "Plant.Line1.Cell1");
     ASSERT_TRUE(cell_alarm != NULL, "Cell1 bits should exist");
     ASSERT_TRUE((*cell_alarm & 0x01) != 0, "Alarm should propagate to Cell1");
     
     const uint8_t* line_alarm = cfl_hbit_get_bits(tree, CHAINBITTREEDEMO_BS_ALARM,
                                                    "Plant.Line1");
     ASSERT_TRUE(line_alarm != NULL, "Line1 bits should exist");
     ASSERT_TRUE((*line_alarm & 0x01) != 0, "Alarm should propagate to Line1");
     
     const uint8_t* plant_alarm = cfl_hbit_get_bits(tree, CHAINBITTREEDEMO_BS_ALARM,
                                                     "Plant");
     ASSERT_TRUE(plant_alarm != NULL, "Plant bits should exist");
     ASSERT_TRUE((*plant_alarm & 0x01) != 0, "Alarm should propagate to Plant");
     
     PASS();
 }
 
 static void test_latch(cfl_hbit_t* tree) {
     TEST("latch");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED, 0, true,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     int val = cfl_hbit_get_bit(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED, 0,
                                "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(val, 1, "latched alarm should be set");
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED, 0, false,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     val = cfl_hbit_get_bit(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED, 0,
                            "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(val, 1, "alarm should still be latched");
     
     cfl_hbit_status_t status = cfl_hbit_clear_latch(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED,
                                                     "Plant.Line1.Cell1.Robot1");
     ASSERT_OK(status, "clear_latch failed");
     cfl_hbit_sync(tree);
     
     val = cfl_hbit_get_bit(tree, CHAINBITTREEDEMO_BS_ALARM_LATCHED, 0,
                            "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(val, 0, "alarm should be cleared after latch clear");
     
     PASS();
 }
 
 static void test_mask(cfl_hbit_t* tree) {
     TEST("mask");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     /* Set mask on Robot1 to block bit 0 (now done at runtime, not schema) */
     uint8_t mask[2] = {0xFE, 0xFF};  /* 16-bit ALARM_ACK bank, bit 0 blocked */
     cfl_hbit_set_mask(tree, CHAINBITTREEDEMO_BS_ALARM_ACK, mask, 2,
                       "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);  /* Swap mask to current */
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM_ACK, 0, true,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM_ACK, 1, true,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     const uint8_t* cell_bits = cfl_hbit_get_bits(tree, CHAINBITTREEDEMO_BS_ALARM_ACK,
                                                   "Plant.Line1.Cell1");
     ASSERT_TRUE(cell_bits != NULL, "Cell1 bits should exist");
     ASSERT_TRUE((*cell_bits & 0x01) == 0, "Bit 0 should be blocked by mask");
     ASSERT_TRUE((*cell_bits & 0x02) != 0, "Bit 1 should pass through mask");
     
     PASS();
 }
 
 static void test_runtime_mask(cfl_hbit_t* tree) {
     TEST("runtime_mask");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     /* Set mask - block lower 4 bits */
     uint8_t mask[2] = {0xF0, 0xFF};  /* 16-bit ALARM_ACK bank */
     cfl_hbit_set_mask(tree, CHAINBITTREEDEMO_BS_ALARM_ACK, mask, 2,
                       "Plant.Line1.Cell1.Robot2");
     cfl_hbit_sync(tree);  /* Swap to current */
     
     uint16_t mask_bytes = 0;
     const uint8_t* got_mask = cfl_hbit_get_mask(tree, CHAINBITTREEDEMO_BS_ALARM_ACK,
                                                  &mask_bytes, "Plant.Line1.Cell1.Robot2");
     ASSERT_TRUE(got_mask != NULL, "should get mask for leaf node");
     ASSERT_EQ(mask_bytes, 2, "mask should be 2 bytes");
     ASSERT_EQ(got_mask[0], 0xF0, "mask byte 0 should be 0xF0");
     
     cfl_hbit_clear_mask(tree, CHAINBITTREEDEMO_BS_ALARM_ACK,
                         "Plant.Line1.Cell1.Robot2");
     cfl_hbit_sync(tree);  /* Swap to current */
     
     got_mask = cfl_hbit_get_mask(tree, CHAINBITTREEDEMO_BS_ALARM_ACK,
                                   &mask_bytes, "Plant.Line1.Cell1.Robot2");
     ASSERT_TRUE(got_mask != NULL, "should get mask");
     ASSERT_EQ(got_mask[0], 0xFF, "mask should be cleared to 0xFF");
     
     PASS();
 }
 
 static void test_is_leaf(cfl_hbit_t* tree) {
     TEST("is_leaf");
     
     ASSERT_TRUE(cfl_hbit_is_leaf(tree, "Plant.Line1.Cell1.Robot1"), "Robot1 should be leaf");
     ASSERT_TRUE(cfl_hbit_is_leaf(tree, "Plant.Line1.Cell1.Sensor1"), "Sensor1 should be leaf");
     ASSERT_TRUE(!cfl_hbit_is_leaf(tree, "Plant"), "Plant should not be leaf");
     ASSERT_TRUE(!cfl_hbit_is_leaf(tree, "Plant.Line1"), "Line1 should not be leaf");
     ASSERT_TRUE(!cfl_hbit_is_leaf(tree, "Plant.Line1.Cell1"), "Cell1 should not be leaf");
     
     PASS();
 }
 
 static void test_change_callback(cfl_hbit_t* tree) {
     TEST("change_callback");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     change_callback_count = 0;
     last_changed_hash = 0;
     
     cfl_hbit_status_t status = cfl_hbit_register_callback(
         tree, -1, test_change_cb, NULL);
     ASSERT_OK(status, "register_callback failed");
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM, 0, true,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     cfl_hbit_notify_changes(tree);
     
     ASSERT_TRUE(change_callback_count > 0, "callback should have been called");
     
     PASS();
 }
 
 static void test_config_access(cfl_hbit_t* tree) {
     TEST("config_access");
     
     int32_t speed = cfl_hbit_config_get_int(tree, 0,
         "Plant.Line1.Cell1.Robot1.Config.Motion.MaxSpeed");
     ASSERT_EQ(speed, 1500, "MaxSpeed should be 1500");
     
     /* Note: Lua normalizes 4.0 to 4, so this may be stored as int */
     /* Try float first, fall back to int */
     float accel = cfl_hbit_config_get_float(tree, -1.0f,
         "Plant.Line1.Cell1.Robot1.Config.Motion.MaxAccel");
     if (accel < 0) {
         /* Stored as int */
         int32_t accel_int = cfl_hbit_config_get_int(tree, 0,
             "Plant.Line1.Cell1.Robot1.Config.Motion.MaxAccel");
         ASSERT_EQ(accel_int, 4, "MaxAccel should be 4");
     } else {
         ASSERT_TRUE(accel > 3.9f && accel < 4.1f, "MaxAccel should be ~4.0");
     }
     
     bool enabled = cfl_hbit_config_get_bool(tree, false,
         "Plant.Line1.Cell1.Robot1.Config.Comm.Enabled");
     ASSERT_TRUE(enabled, "Comm.Enabled should be true");
     
     const char* name = cfl_hbit_config_get_string(tree, "",
         "Plant.Line1.Cell1.Robot1.Config.Name");
     ASSERT_TRUE(name != NULL, "Name should not be NULL");
     ASSERT_TRUE(strcmp(name, "Welder_01") == 0, "Name should be Welder_01");
     
     int32_t missing = cfl_hbit_config_get_int(tree, -999,
         "Plant.NonExistent.Config.Value");
     ASSERT_EQ(missing, -999, "Missing config should return default");
     
     PASS();
 }
 
 static void test_mem_info(cfl_hbit_t* tree) {
     TEST("mem_info");
     
     cfl_hbit_mem_info_t info;
     cfl_hbit_get_mem_info(tree, &info);
     
     ASSERT_EQ(info.node_count, CHAINBITTREEDEMO_NODE_COUNT, "node count mismatch");
     ASSERT_EQ(info.bitspace_count, CHAINBITTREEDEMO_BITSPACE_COUNT, "bitspace count mismatch");
     ASSERT_TRUE(info.total_ram > 0, "total_ram should be > 0");
     ASSERT_TRUE(info.arena_size > 0, "arena_size should be > 0");
     ASSERT_TRUE(info.leaf_count > 0, "leaf_count should be > 0");
     
     printf("\n    Memory: desc=%u, arenas=%u, leaf_masks=%u, total=%u bytes\n",
            info.descriptor_size, info.arena_size, info.leaf_masks_size, info.total_ram);
     printf("    Nodes: %u total, %u leaves\n    ",
            info.node_count, info.leaf_count);
     
     PASS();
 }
 
 static void test_printf_paths(cfl_hbit_t* tree) {
     TEST("printf_paths");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     for (int cell = 1; cell <= 3; cell++) {
         bool exists = cfl_hbit_path_exists(tree, "Plant.Line1.Cell%d", cell);
         ASSERT_TRUE(exists, "Cell should exist");
     }
     
     cfl_hbit_status_t status = cfl_hbit_set_bit(
         tree, CHAINBITTREEDEMO_BS_ALARM, 0, true,
         "Plant.Line%d.Cell%d.Robot%d", 1, 1, 1);
     ASSERT_OK(status, "set_bit with printf path failed");
     
     cfl_hbit_sync(tree);
     
     int val = cfl_hbit_get_bit(
         tree, CHAINBITTREEDEMO_BS_ALARM, 0,
         "Plant.Line%d.Cell%d.Robot%d", 1, 1, 1);
     ASSERT_EQ(val, 1, "bit should be set");
     
     PASS();
 }
 
 static void test_node_indexed(cfl_hbit_t* tree) {
     TEST("node_indexed");
     
     cfl_hbit_reset(tree);
     
     /* Lookup node once */
     int32_t robot1 = cfl_hbit_find_node_path(tree, "Plant.Line1.Cell1.Robot1");
     ASSERT_TRUE(robot1 >= 0, "should find Robot1 node");
     
     int32_t robot2 = cfl_hbit_find_node_path(tree, "Plant.Line1.Cell1.Robot2");
     ASSERT_TRUE(robot2 >= 0, "should find Robot2 node");
     
     int32_t not_found = cfl_hbit_find_node_path(tree, "Plant.NonExistent");
     ASSERT_EQ(not_found, -1, "should return -1 for missing node");
     
     /* Use _n functions with cached node index */
     cfl_hbit_set_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 0, true, robot1);
     cfl_hbit_set_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 1, true, robot1);
     cfl_hbit_set_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 2, true, robot2);
     
     cfl_hbit_sync(tree);
     
     /* Read back with _n functions */
     int val = cfl_hbit_get_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 0, robot1);
     ASSERT_EQ(val, 1, "Robot1 bit 0 should be set");
     
     val = cfl_hbit_get_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 1, robot1);
     ASSERT_EQ(val, 1, "Robot1 bit 1 should be set");
     
     val = cfl_hbit_get_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 2, robot2);
     ASSERT_EQ(val, 1, "Robot2 bit 2 should be set");
     
     val = cfl_hbit_get_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 0, robot2);
     ASSERT_EQ(val, 0, "Robot2 bit 0 should be clear");
     
     /* Test get_bits_n */
     const uint8_t* bits = cfl_hbit_get_bits_n(tree, CHAINBITTREEDEMO_BS_ALARM, robot1);
     ASSERT_TRUE(bits != NULL, "get_bits_n should return pointer");
     ASSERT_TRUE((bits[0] & 0x03) == 0x03, "bits 0 and 1 should be set");
     
     /* Test edge detection with _n */
     cfl_hbit_set_bit_n(tree, CHAINBITTREEDEMO_BS_ALARM, 3, true, robot1);
     cfl_hbit_sync(tree);
     
     int edge = cfl_hbit_get_bit_edge_n(tree, CHAINBITTREEDEMO_BS_ALARM, 3, robot1);
     ASSERT_EQ(edge, 1, "should have rising edge on bit 3");
     
     edge = cfl_hbit_get_bit_edge_n(tree, CHAINBITTREEDEMO_BS_ALARM, 0, robot1);
     ASSERT_EQ(edge, 0, "bit 0 should have no edge (was already set)");
     
     PASS();
 }
 
 static void test_edge_detection(cfl_hbit_t* tree) {
     TEST("edge_detection");
     
     cfl_hbit_reset(tree);
     cfl_hbit_sync(tree);
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM, 0, true,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     int edge = cfl_hbit_get_bit_edge(tree, CHAINBITTREEDEMO_BS_ALARM, 0,
                                       "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(edge, 1, "should have rising edge on bit 0");
     
     /* Check bit 1 - should have no edge */
     edge = cfl_hbit_get_bit_edge(tree, CHAINBITTREEDEMO_BS_ALARM, 1,
                                   "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(edge, 0, "bit 1 should have no edge");
     
     cfl_hbit_set_bit(tree, CHAINBITTREEDEMO_BS_ALARM, 0, false,
                      "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(tree);
     
     edge = cfl_hbit_get_bit_edge(tree, CHAINBITTREEDEMO_BS_ALARM, 0,
                                   "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(edge, -1, "should have falling edge on bit 0");
     
     PASS();
 }
 
 static void test_load_from_file(void) {
     TEST("load_from_file");
     
     cfl_hbit_t tree;
     cfl_hbit_status_t status = cfl_hbit_init_from_file(
         &tree,
         "output/generated_ChainBitTreeDemo.bin",
         NULL
     );
     ASSERT_OK(status, "init_from_file failed");
     
     ASSERT_EQ(cfl_hbit_node_count(&tree), CHAINBITTREEDEMO_NODE_COUNT, "node count mismatch");
     ASSERT_EQ(cfl_hbit_bitspace_count(&tree), CHAINBITTREEDEMO_BITSPACE_COUNT, "bitspace count mismatch");
     
     /* Quick functional test */
     cfl_hbit_set_bit(&tree, CHAINBITTREEDEMO_BS_ALARM, 0, true, "Plant.Line1.Cell1.Robot1");
     cfl_hbit_sync(&tree);
     
     int val = cfl_hbit_get_bit(&tree, CHAINBITTREEDEMO_BS_ALARM, 0, "Plant.Line1.Cell1.Robot1");
     ASSERT_EQ(val, 1, "bit should be set");
     
     cfl_hbit_destroy(&tree);
     
     PASS();
 }
 
 /* ============================================ */
 /* Main                                         */
 /* ============================================ */
 
 int main(void) {
     printf("ChainTree Hierarchical Bit Map Runtime Tests\n");
     printf("=============================================\n\n");
     
     printf("Binary info:\n");
     printf("  Descriptor size: %u bytes\n", ChainBitTreeDemo_descriptor_size);
     printf("  Nodes: %d\n", CHAINBITTREEDEMO_NODE_COUNT);
     printf("  Bitspaces: %d\n", CHAINBITTREEDEMO_BITSPACE_COUNT);
     printf("  Classes: %d\n", CHAINBITTREEDEMO_CLASS_COUNT);
     printf("\n");
     
     cfl_hbit_t tree;
     
     printf("Running tests:\n");
     
     test_hash_function();
     test_init(&tree);
     test_path_exists(&tree);
     test_set_get_bit(&tree);
     test_or_propagation(&tree);
     test_latch(&tree);
     test_mask(&tree);
     test_runtime_mask(&tree);
     test_is_leaf(&tree);
     test_change_callback(&tree);
     test_config_access(&tree);
     test_mem_info(&tree);
     test_printf_paths(&tree);
     test_node_indexed(&tree);
     test_edge_detection(&tree);
     test_load_from_file();
     
     printf("\n");
     printf("=============================================\n");
     printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);
     
     cfl_hbit_destroy(&tree);
     
     return tests_failed > 0 ? 1 : 0;
 }