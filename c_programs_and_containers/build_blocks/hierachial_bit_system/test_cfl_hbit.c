/**
 * @file test_cfl_hbit2_api.c
 * @brief Test program for cfl_hbit2 clean API
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 
 #include "cfl_hbit.h"
 #include "output/generated_ChainBitTreeDemo.bin.h"
 #include "output/generated_ChainBitTreeDemo_hashes.h"
 
 /* Exception handler */
 void cfl_exception_handler(const char* file, const char* func, uint16_t line, const char* msg) {
     fprintf(stderr, "EXCEPTION at %s:%s:%u: %s\n", file, func, line, msg);
     abort();
 }
 
 void setup_abort_handler(void) {}
 
 /* Test Macros */
 static int tests_passed = 0;
 static int tests_failed = 0;
 
 #define TEST(name) \
     printf("  Testing %s... ", #name); \
     test_##name(); \
     printf("PASS\n"); \
     tests_passed++;
 
 #define ASSERT(cond, msg) \
     do { if (!(cond)) { printf("FAIL\n    %s\n", msg); tests_failed++; return; } } while(0)
 
 #define ASSERT_EQ(a, b, msg) ASSERT((a) == (b), msg)
 #define ASSERT_OK(s) ASSERT((s) == CFL_HBIT2_OK, "expected OK")
 
 /* Global tree */
 static cfl_hbit2_tree_t g_tree;
 
 /* Tests */
 void test_init(void) {
     cfl_hbit2_status_t s = cfl_hbit2_init(&g_tree, 
         ChainBitTreeDemo_descriptor, sizeof(ChainBitTreeDemo_descriptor));
     ASSERT_OK(s);
     ASSERT_EQ(cfl_hbit2_info_node_count(&g_tree), 13, "13 nodes");
     ASSERT_EQ(cfl_hbit2_info_bitspace_count(&g_tree), 13, "13 bitspaces");
 }
 
 void test_node_lookup(void) {
     int32_t plant = cfl_hbit2_node(&g_tree, "Plant");
     ASSERT(plant >= 0, "Plant found");
     
     int32_t robot1 = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     ASSERT(robot1 >= 0, "Robot1 found");
     
     int32_t r = cfl_hbit2_node(&g_tree, "Plant.Line%d.Cell%d.Robot%d", 1, 1, 1);
     ASSERT_EQ(r, robot1, "printf path match");
     
     int32_t bad = cfl_hbit2_node(&g_tree, "Plant.Line99");
     ASSERT_EQ(bad, -1, "bad path returns -1");
 }
 
 void test_bitspace_lookup(void) {
     int16_t alarm = cfl_hbit2_bitspace(&g_tree, "ALARM");
     ASSERT_EQ(alarm, CHAINBITTREEDEMO_BS_ALARM, "ALARM match");
     
     int16_t bad = cfl_hbit2_bitspace(&g_tree, "NONEXISTENT");
     ASSERT_EQ(bad, -1, "bad bitspace returns -1");
 }
 
 void test_leaf_detection(void) {
     int32_t plant = cfl_hbit2_node(&g_tree, "Plant");
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     
     ASSERT(!cfl_hbit2_info_is_leaf(&g_tree, plant), "Plant not leaf");
     ASSERT(cfl_hbit2_info_is_leaf(&g_tree, robot), "Robot is leaf");
 }
 
 void test_bank_info(void) {
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     ASSERT_EQ(cfl_hbit2_info_bits(&g_tree, robot, alarm), 32, "32 bits");
     ASSERT_EQ(cfl_hbit2_info_bytes(&g_tree, robot, alarm), 4, "4 bytes");
 }
 
 void test_bit_operations(void) {
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     cfl_hbit2_reset(&g_tree);
     cfl_hbit2_sync(&g_tree);
     
     ASSERT_OK(cfl_hbit2_bit_set(&g_tree, robot, alarm, 0, true));
     cfl_hbit2_sync(&g_tree);
     
     ASSERT_EQ(cfl_hbit2_bit_get(&g_tree, robot, alarm, 0), 1, "bit 0 set");
     ASSERT_EQ(cfl_hbit2_bit_get(&g_tree, robot, alarm, 1), 0, "bit 1 clear");
 }
 
 void test_edge_detection(void) {
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     cfl_hbit2_reset(&g_tree);
     cfl_hbit2_sync(&g_tree);
     
     cfl_hbit2_bit_set(&g_tree, robot, alarm, 5, true);
     cfl_hbit2_sync(&g_tree);
     ASSERT_EQ(cfl_hbit2_bit_edge(&g_tree, robot, alarm, 5), 1, "rising");
     
     cfl_hbit2_sync(&g_tree);
     ASSERT_EQ(cfl_hbit2_bit_edge(&g_tree, robot, alarm, 5), 0, "no change");
     
     cfl_hbit2_bit_set(&g_tree, robot, alarm, 5, false);
     cfl_hbit2_sync(&g_tree);
     ASSERT_EQ(cfl_hbit2_bit_edge(&g_tree, robot, alarm, 5), -1, "falling");
 }
 
 void test_bank_operations(void) {
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     cfl_hbit2_reset(&g_tree);
     cfl_hbit2_sync(&g_tree);
     
     uint8_t data[4] = {0xAA, 0x55, 0x00, 0xFF};
     ASSERT_OK(cfl_hbit2_bank_set(&g_tree, robot, alarm, data, 4));
     cfl_hbit2_sync(&g_tree);
     
     const uint8_t* bank = cfl_hbit2_bank_get(&g_tree, robot, alarm);
     ASSERT(bank != NULL, "bank not null");
     ASSERT_EQ(bank[0], 0xAA, "byte 0");
     ASSERT_EQ(bank[1], 0x55, "byte 1");
     
     ASSERT_OK(cfl_hbit2_bank_clear(&g_tree, robot, alarm));
     cfl_hbit2_sync(&g_tree);
     bank = cfl_hbit2_bank_get(&g_tree, robot, alarm);
     ASSERT_EQ(bank[0], 0x00, "cleared");
 }
 
 void test_propagation(void) {
     int32_t robot1 = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int32_t cell = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1");
     int32_t plant = cfl_hbit2_node(&g_tree, "Plant");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     cfl_hbit2_reset(&g_tree);
     cfl_hbit2_sync(&g_tree);
     
     cfl_hbit2_bit_set(&g_tree, robot1, alarm, 0, true);
     cfl_hbit2_sync(&g_tree);
     
     ASSERT_EQ(cfl_hbit2_bit_get(&g_tree, cell, alarm, 0), 1, "cell has alarm");
     ASSERT_EQ(cfl_hbit2_bit_get(&g_tree, plant, alarm, 0), 1, "plant has alarm");
 }
 
 void test_tree_navigation(void) {
     int32_t robot = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1.Robot1");
     int32_t cell = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1");
     int32_t plant = cfl_hbit2_node(&g_tree, "Plant");
     
     ASSERT_EQ(cfl_hbit2_nav_parent(&g_tree, robot), cell, "robot parent");
     ASSERT_EQ(cfl_hbit2_nav_parent(&g_tree, plant), -1, "plant parent -1");
     ASSERT_EQ(cfl_hbit2_nav_child_count(&g_tree, cell), 4, "cell 4 children");
     ASSERT_EQ(cfl_hbit2_nav_child_count(&g_tree, robot), 0, "robot 0 children");
 }
 
 void test_leaf_only_enforcement(void) {
     int32_t cell = cfl_hbit2_node(&g_tree, "Plant.Line1.Cell1");
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     
     ASSERT_EQ(cfl_hbit2_bank_clear(&g_tree, cell, alarm), CFL_HBIT2_ERR_NOT_LEAF, "bank_clear on agg");
     
     uint8_t data[4] = {0};
     ASSERT_EQ(cfl_hbit2_bank_set(&g_tree, cell, alarm, data, 4), CFL_HBIT2_ERR_NOT_LEAF, "bank_set on agg");
 }
 
 void test_mem_info(void) {
     cfl_hbit2_mem_t mem;
     cfl_hbit2_mem(&g_tree, &mem);
     
     ASSERT_EQ(mem.node_count, 13, "13 nodes");
     ASSERT_EQ(mem.leaf_count, 8, "8 leaves");
     
     printf("\n    Memory: desc=%u, arena=%u, mask=%u, total=%u\n",
            mem.descriptor_size, mem.arena_size, mem.mask_size, mem.total_ram);
 }
 
 void test_using_defines(void) {
     int32_t robot = CHAINBITTREEDEMO_NODE_PLANT_LINE1_CELL1_ROBOT1;
     int16_t alarm = CHAINBITTREEDEMO_BS_ALARM;
     int bit = CHAINBITTREEDEMO_BIT_ROBOTARM_ALARM_OVERTORQUE;
     
     cfl_hbit2_reset(&g_tree);
     cfl_hbit2_sync(&g_tree);
     
     ASSERT_OK(cfl_hbit2_bit_set(&g_tree, robot, alarm, bit, true));
     cfl_hbit2_sync(&g_tree);
     
     ASSERT_EQ(cfl_hbit2_bit_get(&g_tree, robot, alarm, bit), 1, "OverTorque set");
 }
 
 void test_init_from_file(void) {
     /* First write descriptor to a temp file */
     FILE* f = fopen("/tmp/test_hbit.bin", "wb");
     ASSERT(f != NULL, "create temp file");
     size_t written = fwrite(ChainBitTreeDemo_descriptor, 1, sizeof(ChainBitTreeDemo_descriptor), f);
     fclose(f);
     ASSERT_EQ(written, sizeof(ChainBitTreeDemo_descriptor), "write descriptor");
     
     /* Now test init from file */
     cfl_hbit2_tree_t file_tree;
     cfl_hbit2_status_t s = cfl_hbit2_init_file(&file_tree, "/tmp/test_hbit.bin");
     ASSERT_OK(s);
     
     /* Verify it works */
     ASSERT_EQ(cfl_hbit2_info_node_count(&file_tree), 13, "13 nodes from file");
     
     int32_t robot = cfl_hbit2_node(&file_tree, "Plant.Line1.Cell1.Robot1");
     ASSERT(robot >= 0, "Robot found in file tree");
     ASSERT(cfl_hbit2_info_is_leaf(&file_tree, robot), "Robot is leaf");
     
     /* Set and read a bit */
     int16_t alarm = cfl_hbit2_bitspace(&file_tree, "ALARM");
     ASSERT(alarm >= 0, "ALARM bitspace found");
     
     ASSERT_OK(cfl_hbit2_bit_set(&file_tree, robot, alarm, 0, true));
     cfl_hbit2_sync(&file_tree);
     ASSERT_EQ(cfl_hbit2_bit_get(&file_tree, robot, alarm, 0), 1, "bit set in file tree");
     
     cfl_hbit2_destroy(&file_tree);
     
     /* Clean up temp file */
     remove("/tmp/test_hbit.bin");
 }
 
 int main(void) {
     printf("CFL_HBIT2 Clean API Tests\n");
     printf("=========================\n\n");
     
     TEST(init);
     TEST(node_lookup);
     TEST(bitspace_lookup);
     TEST(leaf_detection);
     TEST(bank_info);
     TEST(bit_operations);
     TEST(edge_detection);
     TEST(bank_operations);
     TEST(propagation);
     TEST(tree_navigation);
     TEST(leaf_only_enforcement);
     TEST(mem_info);
     TEST(using_defines);
     TEST(init_from_file);
     
     printf("\n=========================\n");
     printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);
     
     cfl_hbit2_destroy(&g_tree);
     return tests_failed > 0 ? 1 : 0;
 }