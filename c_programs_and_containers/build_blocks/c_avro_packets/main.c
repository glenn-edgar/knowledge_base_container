/*
 * main.c - Test program for avro_dsl generated files
 * 
 * Demonstrates:
 * 1. Using schema-only .h file (sensor_msgs.h)
 * 2. Using embedded packet data (_data.h)
 * 3. Loading packet binary data from .bin file at runtime
 * 4. Using packet_verify to validate and access data
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 #include "sensor_msgs.h"
 #include "sensor_data_data.h"
 
 /*----------------------------------------------------------------------------
  * Utility Functions
  *--------------------------------------------------------------------------*/
 
 static void print_hex(const uint8_t *data, size_t len)
 {
     for (size_t i = 0; i < len; i++) {
         printf("%02X ", data[i]);
         if ((i + 1) % 16 == 0) printf("\n");
     }
     if (len % 16 != 0) printf("\n");
 }
 
 static void print_sensor_reading(const sensor_reading_t *r, const char *name)
 {
     printf("Sensor Reading: %s\n", name);
     printf("  Type:      %u\n", r->sensor_type);
     printf("  State:     %u\n", r->sensor_state);
     printf("  Value:     %.3f\n", r->value);
     printf("  Min:       %.3f\n", r->min_value);
     printf("  Max:       %.3f\n", r->max_value);
     printf("\n");
 }
 
 static void print_device_config(const device_config_t *c, const char *name)
 {
     printf("Device Config: %s\n", name);
     printf("  Device ID: 0x%04X\n", c->device_id);
     printf("  MAC:       %02X:%02X:%02X:%02X:%02X:%02X\n",
            c->mac[0], c->mac[1], c->mac[2],
            c->mac[3], c->mac[4], c->mac[5]);
     printf("  Name:      %.16s\n", c->name);
     printf("  Poll(ms):  %u\n", c->poll_interval_ms);
     printf("  Enabled:   %s\n", c->enabled ? "true" : "false");
     printf("\n");
 }
 
 /*----------------------------------------------------------------------------
  * Test 1: Using embedded packet data from _data.h
  *--------------------------------------------------------------------------*/
 
 static void test_embedded_data(void)
 {
     printf("=== Test 1: Embedded Packet Data ===\n\n");
     
     printf("Schema constants from sensor_msgs.h:\n");
     printf("  SENSOR_MSGS_BIN_SIZE:    %d bytes\n", SENSOR_MSGS_BIN_SIZE);
     printf("  SENSOR_MSGS_RECORD_COUNT: %d records\n\n", SENSOR_MSGS_RECORD_COUNT);
     
     printf("Packet offsets:\n");
     printf("  SENSOR_HEADER_PACKET_OFFSET:  %d\n", SENSOR_HEADER_PACKET_OFFSET);
     printf("  SENSOR_READING_PACKET_OFFSET: %d\n", SENSOR_READING_PACKET_OFFSET);
     printf("  DEVICE_CONFIG_PACKET_OFFSET:  %d\n\n", DEVICE_CONFIG_PACKET_OFFSET);
     
     printf("Raw hex dump (first 64 bytes):\n");
     print_hex(sensor_data_bin, 64 < SENSOR_MSGS_BIN_SIZE ? 64 : SENSOR_MSGS_BIN_SIZE);
     printf("...\n\n");
     
     /* Access packets using verify functions with schema-level offsets */
     uint16_t source_node;
     
     const sensor_header_t *header = sensor_header_packet_verify(
         sensor_data_bin + SENSOR_HEADER_PACKET_OFFSET, "sensor_msgs", &source_node);
     if (header) {
         printf("sensor_header (source_node: %u)\n", source_node);
         printf("  device_id:  0x%04X\n", header->device_id);
         printf("  seq:        %u\n", header->seq);
         printf("  timestamp:  %u\n\n", header->timestamp);
     }
     
     const sensor_reading_t *reading = sensor_reading_packet_verify(
         sensor_data_bin + SENSOR_READING_PACKET_OFFSET, "sensor_msgs", &source_node);
     if (reading) {
         printf("sensor_reading (source_node: %u)\n", source_node);
         print_sensor_reading(reading, "  ");
     }
     
     const device_config_t *config = device_config_packet_verify(
         sensor_data_bin + DEVICE_CONFIG_PACKET_OFFSET, "sensor_msgs", &source_node);
     if (config) {
         printf("device_config (source_node: %u)\n", source_node);
         print_device_config(config, "  ");
     }
 }
 
 /*----------------------------------------------------------------------------
  * Test 2: Runtime File Load with Packet Verification
  *--------------------------------------------------------------------------*/
 
 static void test_file_load(const char *packet_filename)
 {
     printf("=== Test 2: Runtime File Load with Packet Verification ===\n\n");
     
     /* Load packets from generator-produced file */
     FILE *f = fopen(packet_filename, "rb");
     if (!f) {
         printf("Error: Could not open %s\n", packet_filename);
         return;
     }
     
     /* Get file size */
     fseek(f, 0, SEEK_END);
     long file_size = ftell(f);
     fseek(f, 0, SEEK_SET);
     
     printf("Loading %s (%ld bytes)\n", packet_filename, file_size);
     printf("Expected size: %d bytes (%d records)\n\n", 
            SENSOR_MSGS_BIN_SIZE, SENSOR_MSGS_RECORD_COUNT);
     
     /* Allocate buffer */
     uint8_t *buffer = (uint8_t *)malloc(file_size);
     if (!buffer) {
         printf("Error: Memory allocation failed\n");
         fclose(f);
         return;
     }
     
     /* Read file */
     size_t bytes_read = fread(buffer, 1, file_size, f);
     fclose(f);
     
     if (bytes_read != (size_t)file_size) {
         printf("Error: Read %zu bytes, expected %ld\n", bytes_read, file_size);
         free(buffer);
         return;
     }
     
     /* Verify file matches expected size */
     if (file_size == SENSOR_MSGS_BIN_SIZE) {
         printf("File size matches schema ✓\n\n");
     } else {
         printf("Warning: File size %ld != expected %d\n\n", file_size, SENSOR_MSGS_BIN_SIZE);
     }
     
     /* Use packet_verify to access each packet using schema-level offsets */
     printf("Verifying packets from file:\n\n");
     
     uint16_t source_node = 0;
     
     const sensor_header_t *header = sensor_header_packet_verify(
         buffer + SENSOR_HEADER_PACKET_OFFSET, 
         "sensor_msgs",
         &source_node);
     if (header) {
         printf("sensor_header verified (source_node: %u)\n", source_node);
         printf("  device_id: 0x%04X\n\n", header->device_id);
     } else {
         printf("sensor_header verification FAILED\n\n");
     }
     
     const sensor_reading_t *reading = sensor_reading_packet_verify(
         buffer + SENSOR_READING_PACKET_OFFSET,
         "sensor_msgs",
         &source_node);
     if (reading) {
         printf("sensor_reading verified (source_node: %u)\n", source_node);
         print_sensor_reading(reading, "  ");
     } else {
         printf("sensor_reading verification FAILED\n\n");
     }
     
     const device_config_t *config = device_config_packet_verify(
         buffer + DEVICE_CONFIG_PACKET_OFFSET,
         "sensor_msgs",
         &source_node);
     if (config) {
         printf("device_config verified (source_node: %u)\n", source_node);
         print_device_config(config, "  ");
     } else {
         printf("device_config verification FAILED\n\n");
     }
     
     /* Test verification failure with wrong schema */
     printf("Testing verification with wrong schema name:\n");
     const sensor_reading_t *bad = sensor_reading_packet_verify(
         buffer + SENSOR_READING_PACKET_OFFSET, 
         "wrong_schema", 
         NULL);
     if (bad) {
         printf("  ERROR: Should have failed!\n\n");
     } else {
         printf("  Correctly rejected ✓\n\n");
     }
     
     free(buffer);
 }
 
 /*----------------------------------------------------------------------------
  * Test 3: Socket simulation (demonstrates wire format compatibility)
  *--------------------------------------------------------------------------*/
 
 static void test_socket_receive(void)
 {
     printf("=== Test 3: Socket/Wire Format Simulation ===\n\n");
     
     /* Simulate receiving a full packet over a socket */
     uint8_t wire_buffer[SENSOR_READING_PACKET_SIZE];
     
     /* Copy packet from embedded data to simulate received data */
     memcpy(wire_buffer, &sensor_data_bin[SENSOR_READING_PACKET_OFFSET], SENSOR_READING_PACKET_SIZE);
     
     printf("Simulated wire receive (%d bytes):\n", SENSOR_READING_PACKET_SIZE);
     print_hex(wire_buffer, SENSOR_READING_PACKET_SIZE);
     printf("\n");
     
     /* Verify and access the received packet */
     uint16_t source_node;
     const sensor_reading_t *received = sensor_reading_packet_verify(
         wire_buffer, "sensor_msgs", &source_node);
     
     if (received) {
         printf("Packet verified (source_node: %u)\n", source_node);
         print_sensor_reading(received, "  ");
     } else {
         printf("Packet verification failed!\n\n");
     }
 }
 
 /*----------------------------------------------------------------------------
  * Test 4: Packet Encode/Verify Functions (with hash-based schema verification)
  *--------------------------------------------------------------------------*/
 
 static void test_packet_functions(void)
 {
     printf("=== Test 4: Packet Encode/Verify Functions ===\n\n");
     
     /* Show the schema hash constant */
     printf("Schema hash constant: 0x%08X\n", SENSOR_MSGS_SCHEMA_HASH);
     printf("Runtime hash of \"sensor_msgs\": 0x%08X\n\n", cfl_avro_hash("sensor_msgs"));
     
     /* Create a packet on the stack */
     sensor_reading_packet_t tx_packet;
     
     /* Encode: initialize header with schema name, get pointer to payload */
     sensor_reading_t* payload = sensor_reading_packet_encode(&tx_packet, "sensor_msgs", 0x0042);
     
     /* Fill in the payload data */
     payload->header.device_id = 0x0042;
     payload->header.seq = 1;
     payload->header.timestamp = 12345678;
     payload->sensor_type = 1;   /* TEMPERATURE */
     payload->sensor_state = 1;  /* ONLINE */
     payload->value = 23.5f;
     payload->min_value = -40.0f;
     payload->max_value = 85.0f;
     
     /* Set timestamp and sequence on the packet header */
     tx_packet.timestamp = 1234567890.123;
     tx_packet.seq = 42;
     
     printf("Encoded packet:\n");
     printf("  schema_hash: 0x%08X\n", tx_packet.schema_hash);
     printf("  timestamp:   %.3f\n", tx_packet.timestamp);
     printf("  seq:         %u\n", tx_packet.seq);
     printf("  source_node: 0x%04X\n", tx_packet.source_node);
     printf("  length:      %u\n", tx_packet.length);
     printf("  index:       %u\n", tx_packet.index);
     printf("  data.value:  %.1f\n\n", tx_packet.data.value);
     
     /* Simulate sending over network (just copy to rx buffer) */
     uint8_t rx_buffer[sizeof(sensor_reading_packet_t)];
     memcpy(rx_buffer, &tx_packet, sizeof(tx_packet));
     
     /* Verify the received packet with correct schema name */
     uint16_t src_node = 0;
     const sensor_reading_t* rx_data = sensor_reading_packet_verify(rx_buffer, "sensor_msgs", &src_node);
     
     if (rx_data) {
         printf("Packet verified successfully!\n");
         printf("  Source node: 0x%04X\n", src_node);
         print_sensor_reading(rx_data, "verified payload");
     } else {
         printf("Packet verification FAILED!\n\n");
     }
     
     /* Test verification failure - wrong schema name */
     printf("Testing verification failure (wrong schema name):\n");
     const sensor_reading_t* bad_data = sensor_reading_packet_verify(rx_buffer, "wrong_schema", NULL);
     if (bad_data) {
         printf("  ERROR: Should have failed!\n\n");
     } else {
         printf("  Correctly rejected (schema hash mismatch) ✓\n\n");
     }
     
     /* Test verification failure - corrupt the index */
     printf("Testing verification failure (corrupted index):\n");
     sensor_reading_packet_t* corrupt = (sensor_reading_packet_t*)rx_buffer;
     corrupt->schema_hash = cfl_avro_hash("sensor_msgs");  /* Restore hash */
     corrupt->index = 99;  /* Wrong index */
     
     bad_data = sensor_reading_packet_verify(rx_buffer, "sensor_msgs", NULL);
     if (bad_data) {
         printf("  ERROR: Should have failed!\n\n");
     } else {
         printf("  Correctly rejected (index mismatch) ✓\n\n");
     }
 }
 
 /*----------------------------------------------------------------------------
  * Test 5: Packet Length and Copy Functions
  *--------------------------------------------------------------------------*/
 
 static void test_packet_length_and_copy(void)
 {
     printf("=== Test 5: Packet Length and Copy Functions ===\n\n");
     
     /* Test packet_length() */
     printf("Packet lengths:\n");
     printf("  sensor_header_packet_length():  %zu bytes\n", sensor_header_packet_length());
     printf("  sensor_reading_packet_length(): %zu bytes\n", sensor_reading_packet_length());
     printf("  device_config_packet_length():  %zu bytes\n\n", device_config_packet_length());
     
     /* Create source packet */
     sensor_reading_packet_t src_packet;
     sensor_reading_t* src_data = sensor_reading_packet_encode(&src_packet, "sensor_msgs", 0x0055);
     src_data->sensor_type = 2;  /* PRESSURE */
     src_data->sensor_state = 1; /* ONLINE */
     src_data->value = 101.5f;
     src_data->min_value = 30.0f;
     src_data->max_value = 110.0f;
     src_packet.timestamp = 9999.99;
     src_packet.seq = 100;
     
     printf("Source packet created:\n");
     printf("  schema_hash: 0x%08X\n", src_packet.schema_hash);
     printf("  index:       %u\n", src_packet.index);
     printf("  value:       %.1f\n\n", src_packet.data.value);
     
     /* Test successful copy */
     uint8_t dst_buffer[sizeof(sensor_reading_packet_t)];
     sensor_reading_t* dst_data = sensor_reading_packet_copy(dst_buffer, &src_packet);
     
     if (dst_data) {
         printf("Packet copy successful!\n");
         sensor_reading_packet_t* dst_pkt = (sensor_reading_packet_t*)dst_buffer;
         printf("  Copied schema_hash: 0x%08X\n", dst_pkt->schema_hash);
         printf("  Copied timestamp:   %.2f\n", dst_pkt->timestamp);
         printf("  Copied seq:         %u\n", dst_pkt->seq);
         printf("  Copied value:       %.1f\n\n", dst_data->value);
     } else {
         printf("Packet copy FAILED!\n\n");
     }
     
     /* Test copy failure - wrong index */
     printf("Testing copy failure (wrong packet type):\n");
     src_packet.index = 99;  /* Corrupt the index */
     
     sensor_reading_t* bad_copy = sensor_reading_packet_copy(dst_buffer, &src_packet);
     if (bad_copy) {
         printf("  ERROR: Should have failed!\n\n");
     } else {
         printf("  Correctly rejected (index mismatch) ✓\n\n");
     }
     
     /* Test copy failure - wrong length */
     printf("Testing copy failure (wrong payload length):\n");
     src_packet.index = SENSOR_READING_INDEX;  /* Restore index */
     src_packet.length = 99;  /* Corrupt the length */
     
     bad_copy = sensor_reading_packet_copy(dst_buffer, &src_packet);
     if (bad_copy) {
         printf("  ERROR: Should have failed!\n\n");
     } else {
         printf("  Correctly rejected (length mismatch) ✓\n\n");
     }
 }
 
 /*----------------------------------------------------------------------------
  * Test 6: Direct struct usage (schema only, no binary)
  *--------------------------------------------------------------------------*/
 
 static void test_direct_struct(void)
 {
     printf("=== Test 6: Direct Struct Usage (Schema Only) ===\n\n");
     
     /* Create instances directly using the generated types */
     sensor_reading_t my_sensor = {
         .sensor_type = 4,   /* LIGHT */
         .sensor_state = 1,  /* ONLINE */
         .value = 500.0f,
         .min_value = 0.0f,
         .max_value = 65535.0f,
     };
     
     print_sensor_reading(&my_sensor, "my_sensor (stack allocated)");
     
     /* Show struct sizes */
     printf("Struct sizes:\n");
     printf("  sensor_header_t:  %zu bytes (expected: %d)\n", 
            sizeof(sensor_header_t), SENSOR_HEADER_SIZE);
     printf("  sensor_reading_t: %zu bytes (expected: %d)\n", 
            sizeof(sensor_reading_t), SENSOR_READING_SIZE);
     printf("  device_config_t:  %zu bytes (expected: %d)\n", 
            sizeof(device_config_t), DEVICE_CONFIG_SIZE);
     printf("\n");
     
     /* Show packet sizes */
     printf("Packet wrapper sizes:\n");
     printf("  sensor_reading_packet_t: %zu bytes\n", sizeof(sensor_reading_packet_t));
     printf("  device_config_packet_t:  %zu bytes\n", sizeof(device_config_packet_t));
     printf("\n");
 }
 
 /*----------------------------------------------------------------------------
  * Main
  *--------------------------------------------------------------------------*/
 
 int main(int argc, char *argv[])
 {
     printf("Avro DSL Test Program\n");
     printf("=====================\n\n");
     
     test_embedded_data();
     
     /* Use command line arg or default packet filename */
     const char *packet_file = (argc > 1) ? argv[1] : "sensor_data.bin";
     test_file_load(packet_file);
     
     test_socket_receive();
     
     test_packet_functions();
     
     test_packet_length_and_copy();
     
     test_direct_struct();
     
     printf("All tests complete.\n");
     return 0;
 }