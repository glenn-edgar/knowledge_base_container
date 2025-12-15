// main.c
// Test program for generated Avro structures

#include <stdio.h>
#include "sensor_msgs.h"

//------------------------------------------------------------------------------
// HANDLERS (user implements these)
//------------------------------------------------------------------------------

static void handle_temp_reading(const temp_reading_t* r) {
    printf("  TEMP: device=%u seq=%u temp=%.2f state=%d\n",
           r->header.device_id, r->header.seq, r->celsius, r->state);
}

static void handle_pressure_reading(const pressure_reading_t* r) {
    printf("  PRESSURE: device=%u seq=%u pascals=%u state=%d\n",
           r->header.device_id, r->header.seq, r->pascals, r->state);
}

static void handle_humidity_reading(const humidity_reading_t* r) {
    printf("  HUMIDITY: device=%u seq=%u percent=%.2f state=%d\n",
           r->header.device_id, r->header.seq, r->percent, r->state);
}

static void handle_sensor_batch(const sensor_batch_t* b) {
    printf("  BATCH: device=%u count=%u temps=[", b->header.device_id, b->count);
    for (int i = 0; i < b->count && i < 8; i++) {
        printf("%.1f%s", b->temps[i], (i < b->count - 1) ? "," : "");
    }
    printf("]\n");
}

static void handle_device_info(const device_info_t* d) {
    printf("  DEVICE: mac=%02X:%02X:%02X:%02X:%02X:%02X name=%.16s\n",
           d->mac[0], d->mac[1], d->mac[2], d->mac[3], d->mac[4], d->mac[5],
           d->name);
}

//------------------------------------------------------------------------------
// MAIN
//------------------------------------------------------------------------------

int main(void) {
    printf("=== Per-Record Packet Test ===\n\n");
    
    // Print structure sizes
    printf("Packet sizes:\n");
    printf("  temp_reading_packet_t:     %zu bytes\n", sizeof(temp_reading_packet_t));
    printf("  pressure_reading_packet_t: %zu bytes\n", sizeof(pressure_reading_packet_t));
    printf("  humidity_reading_packet_t: %zu bytes\n", sizeof(humidity_reading_packet_t));
    printf("  sensor_batch_packet_t:     %zu bytes\n", sizeof(sensor_batch_packet_t));
    printf("  device_info_packet_t:      %zu bytes\n", sizeof(device_info_packet_t));
    printf("\n");

    //--- Test 1: temp_reading ---
    printf("--- temp_reading ---\n");
    temp_reading_packet_t temp_pkt;
    temp_reading_t temp = {
        .header = { .device_id = 42, .seq = 1, .timestamp = 1000000 },
        .celsius = 23.5f,
        .state = SENSOR_STATE_SAMPLING,
    };
    temp_reading_packet_encode(&temp_pkt, &temp);
    printf("Encoded: index=%u length=%u\n", temp_pkt.index, temp_pkt.length);
    handle_temp_reading(&temp_pkt.data);
    printf("\n");

    //--- Test 2: pressure_reading ---
    printf("--- pressure_reading ---\n");
    pressure_reading_packet_t pres_pkt;
    pressure_reading_t pres = {
        .header = { .device_id = 42, .seq = 2, .timestamp = 1000100 },
        .pascals = 101325,
        .state = SENSOR_STATE_IDLE,
    };
    pressure_reading_packet_encode(&pres_pkt, &pres);
    printf("Encoded: index=%u length=%u\n", pres_pkt.index, pres_pkt.length);
    handle_pressure_reading(&pres_pkt.data);
    printf("\n");

    //--- Test 3: sensor_batch ---
    printf("--- sensor_batch ---\n");
    sensor_batch_packet_t batch_pkt;
    sensor_batch_t batch = {
        .header = { .device_id = 42, .seq = 3, .timestamp = 1000200 },
        .count = 4,
        .temps = { 20.0f, 21.5f, 22.0f, 23.5f },
        .pressures = { 101000, 101100, 101200, 101300 },
    };
    sensor_batch_packet_encode(&batch_pkt, &batch);
    printf("Encoded: index=%u length=%u\n", batch_pkt.index, batch_pkt.length);
    handle_sensor_batch(&batch_pkt.data);
    printf("\n");

    //--- Test 4: device_info ---
    printf("--- device_info ---\n");
    device_info_packet_t info_pkt;
    device_info_t info = {
        .mac = { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF },
        .name = "Sensor-Unit-01",
        .firmware_ver = 0x00010203,
        .config = { .sample_rate = 100, .threshold_lo = -10.0f, .threshold_hi = 50.0f, .enabled = true },
    };
    device_info_packet_encode(&info_pkt, &info);
    printf("Encoded: index=%u length=%u\n", info_pkt.index, info_pkt.length);
    handle_device_info(&info_pkt.data);

    return 0;
}