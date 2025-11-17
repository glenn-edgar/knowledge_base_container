// Auto-generated MessagePack runtime helpers
// DO NOT EDIT MANUALLY

#include <stdint.h>
#include "msgpack_arena.h"

// ========== String Hash Functions ==========
// FNV-1a 64-bit hash function
// Matches Python hash_string_64() implementation

// Known string hashes (for debugging)
typedef struct {
    uint64_t hash;
    const char* str;
} HashMapping;

const HashMapping known_hashes[] = {
    {0x1A97F8875C8FDD47ULL, "calibration"},
    {0x4F829F60625CC1BFULL, "device_name"},
    {0x70D885806B56289EULL, "enabled"},
    {0x0A1259F02DA6E2D3ULL, "firmware_version"},
    {0x0DB801B199CB3940ULL, "humidity"},
    {0x1E8E57B2909B2A85ULL, "network"},
    {0x4B1A493507B3A318ULL, "password"},
    {0x90742A2B2B06F5BFULL, "sampling_rate"},
    {0x7C874D9A3405A0F8ULL, "sensors"},
    {0x83C29F19051FB7EEULL, "ssid"},
    {0x556575C1CE107955ULL, "temperature"},
    {0x7C314E706D951238ULL, "threshold"},
    {0x60827E549DE65488ULL, "timeout"},
};

const size_t known_hashes_count = 13;

const char* msgpack_hash_to_string(uint64_t hash) {
    for (size_t i = 0; i < known_hashes_count; i++) {
        if (known_hashes[i].hash == hash) {
            return known_hashes[i].str;
        }
    }
    return NULL;
}

// Hash value macros for compile-time usage
#define HASH_CALIBRATION 0x1A97F8875C8FDD47ULL
#define HASH_DEVICE_NAME 0x4F829F60625CC1BFULL
#define HASH_ENABLED 0x70D885806B56289EULL
#define HASH_FIRMWARE_VERSION 0x0A1259F02DA6E2D3ULL
#define HASH_HUMIDITY 0x0DB801B199CB3940ULL
#define HASH_NETWORK 0x1E8E57B2909B2A85ULL
#define HASH_PASSWORD 0x4B1A493507B3A318ULL
#define HASH_SAMPLING_RATE 0x90742A2B2B06F5BFULL
#define HASH_SENSORS 0x7C874D9A3405A0F8ULL
#define HASH_SSID 0x83C29F19051FB7EEULL
#define HASH_TEMPERATURE 0x556575C1CE107955ULL
#define HASH_THRESHOLD 0x7C314E706D951238ULL
#define HASH_TIMEOUT 0x60827E549DE65488ULL

