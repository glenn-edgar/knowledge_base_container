/*
 * main.c - Test program for avro_dsl generated sensor_data files
 * 
 * Tests:
 *   1. Record types and field access
 *   2. Wire packet init/verify helpers
 *   3. Generic packet dispatch
 *   4. Embedded binary schema parsing
 *   5. Binary file loading and parsing
 *   6. Simulated socket send/receive
 *
 * Build:
 *   gcc -Wall -Wextra -o test_sensor main.c
 *   ./test_sensor [sensor_data.bin]
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 #include <time.h>
 
 /* Generated headers */
 #include "sensor_data.h"
 #include "sensor_data_bin.h"
 
 /*============================================================================
  * BINARY SCHEMA PARSER
  *============================================================================*/
 
 /* Binary schema header (matches avro_dsl.lua output) */
 #define SCHEMA_MAGIC   0x41565244  /* "AVRD" little-endian */
 #define SCHEMA_VERSION 1
 
 typedef struct {
     uint32_t magic;
     uint16_t version;
     uint16_t record_count;
     uint32_t schema_hash;
     uint32_t total_size;
 } __attribute__((packed)) schema_header_t;
 
 /* Parse and print schema header */
 static int parse_schema_header(const uint8_t* data, size_t len, schema_header_t* hdr) {
     if (len < sizeof(schema_header_t)) {
         fprintf(stderr, "ERROR: Buffer too small for header\n");
         return -1;
     }
     
     memcpy(hdr, data, sizeof(schema_header_t));
     
     if (hdr->magic != SCHEMA_MAGIC) {
         fprintf(stderr, "ERROR: Invalid magic 0x%08X (expected 0x%08X)\n", 
                 hdr->magic, SCHEMA_MAGIC);
         return -1;
     }
     
     if (hdr->version != SCHEMA_VERSION) {
         fprintf(stderr, "ERROR: Unsupported version %d\n", hdr->version);
         return -1;
     }
     
     return 0;
 }
 
 /* Read null-terminated string, return pointer past it */
 static const uint8_t* read_string(const uint8_t* p, const uint8_t* end, const char** out) {
     *out = (const char*)p;
     while (p < end && *p != '\0') p++;
     if (p >= end) return NULL;
     return p + 1;  /* Skip null terminator */
 }
 
 /* Parse and dump binary schema contents */
 static void dump_binary_schema(const uint8_t* data, size_t len) {
     schema_header_t hdr;
     if (parse_schema_header(data, len, &hdr) != 0) return;
     
     printf("\n=== BINARY SCHEMA DUMP ===\n");
     printf("Magic:        0x%08X ('%.4s')\n", hdr.magic, (char*)&hdr.magic);
     printf("Version:      %d\n", hdr.version);
     printf("Record Count: %d\n", hdr.record_count);
     printf("Schema Hash:  0x%08X\n", hdr.schema_hash);
     printf("Total Size:   %d bytes\n", hdr.total_size);
     
     const uint8_t* p = data + sizeof(schema_header_t);
     const uint8_t* end = data + len;
     
     /* Schema name */
     const char* schema_name;
     p = read_string(p, end, &schema_name);
     if (!p) return;
     printf("Schema Name:  %s\n", schema_name);
     
     /* Enums */
     if (p + 2 > end) return;
     uint16_t enum_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nEnums (%d):\n", enum_count);
     
     for (int i = 0; i < enum_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 5 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         uint8_t value_count = p[4];
         p += 5;
         
         printf("  [%d] %s (hash=0x%08X, values=%d)\n", i, name, hash, value_count);
         
         for (int j = 0; j < value_count; j++) {
             const char* vname;
             p = read_string(p, end, &vname);
             if (!p || p + 4 > end) return;
             
             uint32_t val = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
             p += 4;
             printf("      %s = %d\n", vname, val);
         }
     }
     
     /* Fixed arrays */
     if (p + 2 > end) return;
     uint16_t fixed_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nFixed Arrays (%d):\n", fixed_count);
     
     for (int i = 0; i < fixed_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 6 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         uint16_t size = p[4] | (p[5] << 8);
         p += 6;
         
         printf("  [%d] %s (hash=0x%08X, size=%d)\n", i, name, hash, size);
     }
     
     /* Strings */
     if (p + 2 > end) return;
     uint16_t string_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nStrings (%d):\n", string_count);
     
     for (int i = 0; i < string_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 6 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         uint16_t length = p[4] | (p[5] << 8);
         p += 6;
         
         printf("  [%d] %s (hash=0x%08X, length=%d)\n", i, name, hash, length);
     }
     
     /* Pointers */
     if (p + 2 > end) return;
     uint16_t pointer_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nPointers (%d):\n", pointer_count);
     
     for (int i = 0; i < pointer_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 4 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         p += 4;
         
         printf("  [%d] %s (hash=0x%08X)\n", i, name, hash);
     }
     
     /* Structs */
     if (p + 2 > end) return;
     uint16_t struct_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nStructs (%d):\n", struct_count);
     
     for (int i = 0; i < struct_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 7 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         uint16_t size = p[4] | (p[5] << 8);
         uint8_t field_count = p[6];
         p += 7;
         
         printf("  [%d] %s (hash=0x%08X, size=%d, fields=%d)\n", 
                i, name, hash, size, field_count);
         
         for (int j = 0; j < field_count; j++) {
             const char* fname;
             p = read_string(p, end, &fname);
             if (!p || p + 7 > end) return;
             
             uint8_t type_tag = p[0];
             uint16_t offset = p[1] | (p[2] << 8);
             uint16_t fsize = p[3] | (p[4] << 8);
             uint16_t array_count = p[5] | (p[6] << 8);
             p += 7;
             
             printf("      .%s: tag=%d, offset=%d, size=%d, array=%d\n",
                    fname, type_tag, offset, fsize, array_count);
         }
     }
     
     /* Records */
     if (p + 2 > end) return;
     uint16_t record_count = p[0] | (p[1] << 8);
     p += 2;
     printf("\nRecords (%d):\n", record_count);
     
     for (int i = 0; i < record_count; i++) {
         const char* name;
         p = read_string(p, end, &name);
         if (!p || p + 8 > end) return;
         
         uint32_t hash = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
         uint8_t index = p[4];
         uint16_t size = p[5] | (p[6] << 8);
         uint8_t field_count = p[7];
         p += 8;
         
         printf("  [%d] %s (hash=0x%08X, index=%d, size=%d, fields=%d)\n", 
                i, name, hash, index, size, field_count);
         
         for (int j = 0; j < field_count; j++) {
             const char* fname;
             p = read_string(p, end, &fname);
             if (!p || p + 7 > end) return;
             
             uint8_t type_tag = p[0];
             uint16_t offset = p[1] | (p[2] << 8);
             uint16_t fsize = p[3] | (p[4] << 8);
             uint16_t array_count = p[5] | (p[6] << 8);
             p += 7;
             
             printf("      .%s: tag=%d, offset=%d, size=%d, array=%d\n",
                    fname, type_tag, offset, fsize, array_count);
         }
     }
     
     printf("\nParsed %zu of %zu bytes\n", (size_t)(p - data), len);
 }
 
 /*============================================================================
  * TEST FUNCTIONS
  *============================================================================*/
 
 static int test_count = 0;
 static int assert_count = 0;
 static int pass_count = 0;
 
 #define TEST(name) \
     do { \
         test_count++; \
         printf("\n--- Test: %s ---\n", name); \
     } while(0)
 
 #define ASSERT_EQ(a, b, fmt) \
     do { \
         assert_count++; \
         if ((a) == (b)) { \
             pass_count++; \
             printf("  PASS: " #a " == " #b " (" fmt ")\n", (a)); \
         } else { \
             printf("  FAIL: " #a " (" fmt ") != " #b " (" fmt ")\n", (a), (b)); \
         } \
     } while(0)
 
 #define ASSERT_NOT_NULL(ptr) \
     do { \
         assert_count++; \
         if ((ptr) != NULL) { \
             pass_count++; \
             printf("  PASS: " #ptr " != NULL\n"); \
         } else { \
             printf("  FAIL: " #ptr " == NULL\n"); \
         } \
     } while(0)
 
 #define ASSERT_NULL(ptr) \
     do { \
         assert_count++; \
         if ((ptr) == NULL) { \
             pass_count++; \
             printf("  PASS: " #ptr " == NULL\n"); \
         } else { \
             printf("  FAIL: " #ptr " != NULL\n"); \
         } \
     } while(0)
 
 /* Test 1: Record type sizes and field access */
 static void test_record_types(void) {
     TEST("Record Types and Field Access");
     
     /* sensor_reading_t */
     sensor_reading_t reading = {
         .sensor_id = 42,
         .sensor_type = SENSOR_TYPE_TEMPERATURE,
         .value = 23.5f,
         .timestamp = 1704067200
     };
     
     ASSERT_EQ(reading.sensor_id, 42, "%u");
     ASSERT_EQ(reading.sensor_type, SENSOR_TYPE_TEMPERATURE, "%d");
     ASSERT_EQ(reading.timestamp, 1704067200U, "%u");
     printf("  INFO: sizeof(sensor_reading_t) = %zu\n", sizeof(sensor_reading_t));
     
     /* alarm_event_t */
     alarm_event_t alarm = {
         .sensor_id = 1,
         .level = ALARM_LEVEL_CRITICAL,
         .value = 105.0f,
         .threshold = 100.0f,
         .timestamp = 1704067300
     };
     
     ASSERT_EQ(alarm.level, ALARM_LEVEL_CRITICAL, "%d");
     printf("  INFO: sizeof(alarm_event_t) = %zu\n", sizeof(alarm_event_t));
     
     /* config_update_t */
     config_update_t config = {
         .sensor_id = 5,
         .sample_rate_ms = 1000,
         .threshold_low = 10.0f,
         .threshold_high = 90.0f,
         .enabled = true
     };
     
     ASSERT_EQ(config.sample_rate_ms, 1000, "%u");
     ASSERT_EQ(config.enabled, true, "%d");
     printf("  INFO: sizeof(config_update_t) = %zu\n", sizeof(config_update_t));
     
     /* heartbeat_t */
     heartbeat_t hb = {
         .uptime_sec = 86400,
         .free_heap = 32768,
         .sensor_count = 4,
         .alarm_count = 1
     };
     
     ASSERT_EQ(hb.uptime_sec, 86400U, "%u");
     ASSERT_EQ(hb.sensor_count, 4, "%u");
     printf("  INFO: sizeof(heartbeat_t) = %zu\n", sizeof(heartbeat_t));
 }
 
 /* Test 2: Wire header and schema hash */
 static void test_wire_header(void) {
     TEST("Wire Header and Schema Hash");
     
     printf("  INFO: SENSOR_DATA_SCHEMA_HASH = 0x%08X\n", SENSOR_DATA_SCHEMA_HASH);
     printf("  INFO: SENSOR_DATA_RECORD_COUNT = %d\n", SENSOR_DATA_RECORD_COUNT);
     printf("  INFO: sizeof(sensor_data_wire_header_t) = %zu\n", 
            sizeof(sensor_data_wire_header_t));
     
     /* Verify header size is 16 bytes (naturally aligned) */
     ASSERT_EQ(sizeof(sensor_data_wire_header_t), 16UL, "%zu");
     
     /* Test verify_header */
     sensor_data_wire_header_t good_hdr = {
         .schema_hash = SENSOR_DATA_SCHEMA_HASH,
         .timestamp = 1234567890.123,
         .seq = 100,
         .source_node = 1,
         .index = 0
     };
     
     sensor_data_wire_header_t bad_hdr = {
         .schema_hash = 0xDEADBEEF,  /* Wrong hash */
         .timestamp = 0,
         .seq = 0,
         .source_node = 0,
         .index = 0
     };
     
     ASSERT_EQ(sensor_data_verify_header(&good_hdr), true, "%d");
     ASSERT_EQ(sensor_data_verify_header(&bad_hdr), false, "%d");
 }
 
 /* Test 3: Packet init and verify */
 static void test_packet_init_verify(void) {
     TEST("Packet Init and Verify");
     
     uint8_t source_node = 7;
     
     /* sensor_reading packet */
     sensor_reading_packet_t reading_pkt;
     sensor_reading_wire_t* reading_wire = sensor_reading_packet_init(&reading_pkt, source_node);
     
     ASSERT_NOT_NULL(reading_wire);
     ASSERT_EQ(reading_pkt.header.schema_hash, SENSOR_DATA_SCHEMA_HASH, "0x%08X");
     ASSERT_EQ(reading_pkt.header.source_node, source_node, "%u");
     ASSERT_EQ(reading_pkt.header.index, 0, "%u");
     
     /* Fill in wire data directly */
     reading_wire->sensor_id = 100;
     reading_wire->sensor_type = SENSOR_TYPE_HUMIDITY;
     reading_wire->value = 65.5f;
     reading_wire->timestamp = 1704070000;
     
     /* Verify the packet */
     const sensor_reading_wire_t* verified = sensor_reading_packet_verify(&reading_pkt);
     ASSERT_NOT_NULL(verified);
     ASSERT_EQ(verified->sensor_id, 100, "%u");
     
     /* alarm_event packet */
     alarm_event_packet_t alarm_pkt;
     alarm_event_wire_t* alarm_wire = alarm_event_packet_init(&alarm_pkt, source_node);
     ASSERT_NOT_NULL(alarm_wire);
     ASSERT_EQ(alarm_pkt.header.index, 1, "%u");
     
     /* config_update packet */
     config_update_packet_t config_pkt;
     config_update_wire_t* config_wire = config_update_packet_init(&config_pkt, source_node);
     ASSERT_NOT_NULL(config_wire);
     ASSERT_EQ(config_pkt.header.index, 2, "%u");
     
     /* heartbeat packet */
     heartbeat_packet_t hb_pkt;
     heartbeat_wire_t* hb_wire = heartbeat_packet_init(&hb_pkt, source_node);
     ASSERT_NOT_NULL(hb_wire);
     ASSERT_EQ(hb_pkt.header.index, 3, "%u");
     
     /* Test verify with wrong packet type */
     const alarm_event_wire_t* wrong_type = alarm_event_packet_verify(
         (const alarm_event_packet_t*)&reading_pkt);
     ASSERT_NULL(wrong_type);
     
     /* Test verify with corrupted hash */
     reading_pkt.header.schema_hash = 0xBADBAD;
     const sensor_reading_wire_t* corrupted = sensor_reading_packet_verify(&reading_pkt);
     ASSERT_NULL(corrupted);
 }
 
 /* Test 4: Generic packet dispatch */
 static void test_packet_dispatch(void) {
     TEST("Generic Packet Dispatch");
     
     /* Create packets of each type */
     sensor_reading_packet_t reading_pkt;
     alarm_event_packet_t alarm_pkt;
     config_update_packet_t config_pkt;
     heartbeat_packet_t hb_pkt;
     
     sensor_reading_wire_t* reading_w = sensor_reading_packet_init(&reading_pkt, 1);
     alarm_event_wire_t* alarm_w = alarm_event_packet_init(&alarm_pkt, 2);
     config_update_wire_t* config_w = config_update_packet_init(&config_pkt, 3);
     heartbeat_wire_t* hb_w = heartbeat_packet_init(&hb_pkt, 4);
     
     /* Fill some data */
     reading_w->sensor_id = 111;
     alarm_w->sensor_id = 222;
     config_w->sensor_id = 333;
     hb_w->uptime_sec = 444;
     
     uint8_t source_node;
     const void* data;
     int idx;
     
     /* Dispatch sensor_reading */
     idx = sensor_data_packet_dispatch(&reading_pkt, &source_node, &data);
     ASSERT_EQ(idx, 0, "%d");
     ASSERT_EQ(source_node, 1, "%u");
     ASSERT_EQ(((const sensor_reading_wire_t*)data)->sensor_id, 111, "%u");
     
     /* Dispatch alarm_event */
     idx = sensor_data_packet_dispatch(&alarm_pkt, &source_node, &data);
     ASSERT_EQ(idx, 1, "%d");
     ASSERT_EQ(source_node, 2, "%u");
     
     /* Dispatch config_update */
     idx = sensor_data_packet_dispatch(&config_pkt, &source_node, &data);
     ASSERT_EQ(idx, 2, "%d");
     ASSERT_EQ(source_node, 3, "%u");
     
     /* Dispatch heartbeat */
     idx = sensor_data_packet_dispatch(&hb_pkt, &source_node, &data);
     ASSERT_EQ(idx, 3, "%d");
     ASSERT_EQ(source_node, 4, "%u");
     
     /* Test invalid schema hash */
     reading_pkt.header.schema_hash = 0xDEADBEEF;
     idx = sensor_data_packet_dispatch(&reading_pkt, NULL, NULL);
     ASSERT_EQ(idx, -1, "%d");
     
     /* Test invalid index */
     reading_pkt.header.schema_hash = SENSOR_DATA_SCHEMA_HASH;
     reading_pkt.header.index = 99;
     idx = sensor_data_packet_dispatch(&reading_pkt, NULL, NULL);
     ASSERT_EQ(idx, -1, "%d");
 }
 
 /* Test 5: Size arrays */
 static void test_size_arrays(void) {
     TEST("Size Arrays");
     
     printf("  Wire sizes (packed, cross-platform):\n");
     for (int i = 0; i < SENSOR_DATA_RECORD_COUNT; i++) {
         printf("    [%d] wire=%u, packet=%u\n", 
                i, sensor_data_wire_sizes[i], sensor_data_packet_sizes[i]);
     }
     
     printf("  Native sizes (may have padding):\n");
     printf("    sensor_reading_t = %zu, sensor_reading_wire_t = %zu\n",
            sizeof(sensor_reading_t), sizeof(sensor_reading_wire_t));
     printf("    alarm_event_t = %zu, alarm_event_wire_t = %zu\n",
            sizeof(alarm_event_t), sizeof(alarm_event_wire_t));
     printf("    config_update_t = %zu, config_update_wire_t = %zu\n",
            sizeof(config_update_t), sizeof(config_update_wire_t));
     printf("    heartbeat_t = %zu, heartbeat_wire_t = %zu\n",
            sizeof(heartbeat_t), sizeof(heartbeat_wire_t));
     
     /* Verify wire sizes match packed structs */
     ASSERT_EQ(sensor_data_wire_sizes[0], (uint16_t)sizeof(sensor_reading_wire_t), "%u");
     ASSERT_EQ(sensor_data_wire_sizes[1], (uint16_t)sizeof(alarm_event_wire_t), "%u");
     ASSERT_EQ(sensor_data_wire_sizes[2], (uint16_t)sizeof(config_update_wire_t), "%u");
     ASSERT_EQ(sensor_data_wire_sizes[3], (uint16_t)sizeof(heartbeat_wire_t), "%u");
     
     ASSERT_EQ(sensor_data_packet_sizes[0], (uint16_t)sizeof(sensor_reading_packet_t), "%u");
     ASSERT_EQ(sensor_data_packet_sizes[1], (uint16_t)sizeof(alarm_event_packet_t), "%u");
     ASSERT_EQ(sensor_data_packet_sizes[2], (uint16_t)sizeof(config_update_packet_t), "%u");
     ASSERT_EQ(sensor_data_packet_sizes[3], (uint16_t)sizeof(heartbeat_packet_t), "%u");
 }
 
 /* Test 6: Embedded binary schema */
 static void test_embedded_binary(void) {
     TEST("Embedded Binary Schema");
     
     printf("  INFO: SENSOR_DATA_BIN_SIZE = %d\n", SENSOR_DATA_BIN_SIZE);
     
     /* Verify the embedded binary matches expected size */
     ASSERT_EQ(sizeof(sensor_data_schema_bin), (size_t)SENSOR_DATA_BIN_SIZE, "%zu");
     
     /* Parse header */
     schema_header_t hdr;
     int rc = parse_schema_header(sensor_data_schema_bin, SENSOR_DATA_BIN_SIZE, &hdr);
     ASSERT_EQ(rc, 0, "%d");
     
     /* Verify header fields */
     ASSERT_EQ(hdr.magic, SCHEMA_MAGIC, "0x%08X");
     ASSERT_EQ(hdr.version, SCHEMA_VERSION, "%u");
     ASSERT_EQ(hdr.record_count, SENSOR_DATA_RECORD_COUNT, "%u");
     ASSERT_EQ(hdr.schema_hash, SENSOR_DATA_SCHEMA_HASH, "0x%08X");
     ASSERT_EQ(hdr.total_size, (uint32_t)SENSOR_DATA_BIN_SIZE, "%u");
     
     /* Dump full schema */
     dump_binary_schema(sensor_data_schema_bin, SENSOR_DATA_BIN_SIZE);
 }
 
 /* Test 7: Load binary file (optional) */
 static void test_binary_file(const char* path) {
     TEST("Binary File Loading");
     
     FILE* fp = fopen(path, "rb");
     if (!fp) {
         printf("  SKIP: Could not open '%s'\n", path);
         return;
     }
     
     /* Get file size */
     fseek(fp, 0, SEEK_END);
     long size = ftell(fp);
     fseek(fp, 0, SEEK_SET);
     
     printf("  INFO: File '%s' size = %ld bytes\n", path, size);
     
     /* Allocate and read */
     uint8_t* data = malloc(size);
     if (!data) {
         printf("  FAIL: malloc failed\n");
         fclose(fp);
         return;
     }
     
     size_t nread = fread(data, 1, size, fp);
     fclose(fp);
     
     ASSERT_EQ(nread, (size_t)size, "%zu");
     
     /* Verify it matches embedded binary */
     if (size == SENSOR_DATA_BIN_SIZE) {
         int match = memcmp(data, sensor_data_schema_bin, size) == 0;
         assert_count++;
         if (match) {
             pass_count++;
             printf("  PASS: File matches embedded binary\n");
         } else {
             printf("  FAIL: File does not match embedded binary\n");
         }
     } else {
         printf("  INFO: File size differs from embedded (%ld vs %d)\n",
                size, SENSOR_DATA_BIN_SIZE);
     }
     
     /* Parse and dump */
     dump_binary_schema(data, size);
     
     free(data);
 }
 
 /* Test 8: Simulated socket round-trip */
 static void test_socket_simulation(void) {
     TEST("Simulated Socket Round-Trip");
     
     /* Simulate sending a packet over a socket */
     uint8_t wire_buffer[256];
     
     /* Create and populate a sensor_reading packet */
     sensor_reading_packet_t* tx_pkt = (sensor_reading_packet_t*)wire_buffer;
     sensor_reading_wire_t* tx_data = sensor_reading_packet_init(tx_pkt, 5);
     
     tx_data->sensor_id = 999;
     tx_data->sensor_type = SENSOR_TYPE_PRESSURE;
     tx_data->value = 101.325f;
     tx_data->timestamp = 1704080000;
     
     /* Simulate transport setting seq/timestamp */
     tx_pkt->header.seq = 42;
     tx_pkt->header.timestamp = 1704080000.500;
     
     size_t wire_size = sizeof(sensor_reading_packet_t);
     printf("  INFO: Sending %zu bytes (header=%zu + payload=%zu)\n", 
            wire_size, sizeof(sensor_data_wire_header_t), sizeof(sensor_reading_wire_t));
     
     /* --- "Network" --- */
     /* (In real code: send(sock, wire_buffer, wire_size, 0)) */
     
     /* Simulate receiving */
     /* First read header to determine packet type */
     const sensor_data_wire_header_t* rx_hdr = (const sensor_data_wire_header_t*)wire_buffer;
     
     /* Verify schema */
     ASSERT_EQ(sensor_data_verify_header(rx_hdr), true, "%d");
     
     /* Get record index and size */
     int record_idx = rx_hdr->index;
     ASSERT_EQ(record_idx, 0, "%d");  /* sensor_reading */
     
     uint16_t payload_size = sensor_data_wire_sizes[record_idx];
     printf("  INFO: Received record %d, payload %u bytes\n", record_idx, payload_size);
     
     /* Dispatch and process */
     uint8_t source_node;
     const void* rx_data;
     int idx = sensor_data_packet_dispatch(wire_buffer, &source_node, &rx_data);
     
     ASSERT_EQ(idx, 0, "%d");
     ASSERT_EQ(source_node, 5, "%u");
     
     const sensor_reading_wire_t* reading_wire = (const sensor_reading_wire_t*)rx_data;
     ASSERT_EQ(reading_wire->sensor_id, 999, "%u");
     ASSERT_EQ(reading_wire->sensor_type, SENSOR_TYPE_PRESSURE, "%d");
     ASSERT_EQ(reading_wire->timestamp, 1704080000U, "%u");
     
     /* Also test type-specific verify */
     const sensor_reading_wire_t* verified = sensor_reading_packet_verify(tx_pkt);
     ASSERT_NOT_NULL(verified);
     ASSERT_EQ(verified->sensor_id, 999, "%u");
     
     /* Test conversion to native type */
     sensor_reading_t native;
     sensor_reading_from_wire(verified, &native);
     ASSERT_EQ(native.sensor_id, 999, "%u");
     ASSERT_EQ(native.sensor_type, SENSOR_TYPE_PRESSURE, "%d");
     
     printf("  Round-trip successful!\n");
 }
 
 /* Test 9: Enum Values */
 static void test_enum_values(void) {
     TEST("Enum Values");
     
     ASSERT_EQ(SENSOR_TYPE_TEMPERATURE, 0, "%d");
     ASSERT_EQ(SENSOR_TYPE_HUMIDITY, 1, "%d");
     ASSERT_EQ(SENSOR_TYPE_PRESSURE, 2, "%d");
     ASSERT_EQ(SENSOR_TYPE_FLOW, 3, "%d");
     
     ASSERT_EQ(ALARM_LEVEL_NONE, 0, "%d");
     ASSERT_EQ(ALARM_LEVEL_WARNING, 1, "%d");
     ASSERT_EQ(ALARM_LEVEL_CRITICAL, 2, "%d");
 }
 
 /* Test 10: Cross-platform wire format */
 static void test_cross_platform_wire(void) {
     TEST("Cross-Platform Wire Format");
     
     /* Wire header must be exactly 16 bytes on all platforms */
     ASSERT_EQ(sizeof(sensor_data_wire_header_t), 16UL, "%zu");
     
     /* Wire records must have predictable sizes regardless of platform */
     printf("  Expected wire sizes (packed):\n");
     printf("    sensor_reading_wire_t: 14 bytes (2+4+4+4)\n");
     printf("    alarm_event_wire_t:    18 bytes (2+4+4+4+4)\n");
     printf("    config_update_wire_t:  13 bytes (2+2+4+4+1)\n");
     printf("    heartbeat_wire_t:      10 bytes (4+4+1+1)\n");
     
     /* These sizes MUST be identical on 32-bit and 64-bit */
     ASSERT_EQ(sizeof(sensor_reading_wire_t), 14UL, "%zu");
     ASSERT_EQ(sizeof(alarm_event_wire_t), 18UL, "%zu");
     ASSERT_EQ(sizeof(config_update_wire_t), 13UL, "%zu");
     ASSERT_EQ(sizeof(heartbeat_wire_t), 10UL, "%zu");
     
     /* Packet sizes = header (16) + wire record */
     ASSERT_EQ(sizeof(sensor_reading_packet_t), 30UL, "%zu");
     ASSERT_EQ(sizeof(alarm_event_packet_t), 34UL, "%zu");
     ASSERT_EQ(sizeof(config_update_packet_t), 29UL, "%zu");
     ASSERT_EQ(sizeof(heartbeat_packet_t), 26UL, "%zu");
     
     /* Test native to wire conversion preserves data */
     sensor_reading_t native = {
         .sensor_id = 12345,
         .sensor_type = SENSOR_TYPE_HUMIDITY,
         .value = 55.5f,
         .timestamp = 0xDEADBEEF
     };
     
     sensor_reading_wire_t wire;
     sensor_reading_to_wire(&native, &wire);
     
     ASSERT_EQ(wire.sensor_id, 12345, "%u");
     ASSERT_EQ(wire.sensor_type, SENSOR_TYPE_HUMIDITY, "%d");
     ASSERT_EQ(wire.timestamp, 0xDEADBEEFU, "%u");
     
     /* Test wire to native conversion */
     sensor_reading_t native2;
     sensor_reading_from_wire(&wire, &native2);
     
     ASSERT_EQ(native2.sensor_id, native.sensor_id, "%u");
     ASSERT_EQ(native2.sensor_type, native.sensor_type, "%d");
     ASSERT_EQ(native2.timestamp, native.timestamp, "%u");
     
     printf("  Cross-platform wire format: OK\n");
 }
 
 /* Test 10: Fixed types */
 static void test_fixed_types(void) {
     TEST("Fixed Types");
     
     ASSERT_EQ(sizeof(mac_addr_t), 6UL, "%zu");
     ASSERT_EQ(sizeof(uuid_t), 16UL, "%zu");
     
     /* sensor_name_t struct */
     printf("  INFO: sizeof(sensor_name_t) = %zu\n", sizeof(sensor_name_t));
     
     sensor_name_t name;
     name.max_length = sizeof(name.buffer);
     name.length = snprintf(name.buffer, sizeof(name.buffer), "Temperature_01");
     
     ASSERT_EQ(name.max_length, 32, "%u");
     printf("  INFO: sensor_name = '%s' (len=%u)\n", name.buffer, name.length);
 }
 
 /*============================================================================
  * MAIN
  *============================================================================*/
 
 int main(int argc, char* argv[]) {
     printf("==============================================\n");
     printf("  SENSOR_DATA GENERATED FILES TEST SUITE\n");
     printf("==============================================\n");
     printf("Schema: %s\n", SENSOR_DATA_SCHEMA_FILE);
     printf("Hash:   0x%08X\n", SENSOR_DATA_SCHEMA_HASH);
     printf("Records: %d\n", SENSOR_DATA_RECORD_COUNT);
     printf("\n--- Platform Info ---\n");
     printf("  sizeof(void*)   = %zu (%d-bit pointers)\n", sizeof(void*), (int)(sizeof(void*) * 8));
     printf("  sizeof(size_t)  = %zu\n", sizeof(size_t));
     printf("  sizeof(int)     = %zu\n", sizeof(int));
     printf("  sizeof(long)    = %zu\n", sizeof(long));
     printf("  sizeof(double)  = %zu\n", sizeof(double));
     
     /* Run all tests */
     test_record_types();
     test_wire_header();
     test_packet_init_verify();
     test_packet_dispatch();
     test_size_arrays();
     test_embedded_binary();
     test_enum_values();
     test_cross_platform_wire();
     test_fixed_types();
     test_socket_simulation();
     
     /* Optional: test binary file loading */
     const char* bin_path = (argc > 1) ? argv[1] : "sensor_data.bin";
     test_binary_file(bin_path);
     
     /* Summary */
     printf("\n==============================================\n");
     printf("  TEST SUMMARY\n");
     printf("  Tests:      %d\n", test_count);
     printf("  Assertions: %d/%d passed\n", pass_count, assert_count);
     printf("==============================================\n");
     
     return (pass_count == assert_count) ? 0 : 1;
 }