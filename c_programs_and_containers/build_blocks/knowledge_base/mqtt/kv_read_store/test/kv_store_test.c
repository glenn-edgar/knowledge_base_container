/**
 * kv_store_test.c - Test driver for the MQTT KV Store library.
 *
 * Uses the writer API to populate the broker with retained test data,
 * then uses the reader API to exercise all read modes (pattern, single,
 * wildcard, sentinel-based).
 *
 * Prerequisites:
 *   - Mosquitto broker running on localhost:1883
 *   - Library built with: make
 *
 * Build standalone:
 *   gcc -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L \
 *       -I../include -o kv_store_test kv_store_test.c \
 *       ../src/kv_store_reader.c ../src/kv_store_writer.c \
 *       -lmosquitto -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <mosquitto.h>

#include "kv_store_reader.h"
#include "kv_store_writer.h"

/* ══════════════════════════════════════════════════════════════════════
 *  Test data
 * ══════════════════════════════════════════════════════════════════════ */

typedef struct {
    const char *topic;
    const char *value;
} test_kv_t;

static const test_kv_t test_data[] = {
    /* Configuration */
    {"kv/example/config/host",          "192.168.1.100"},
    {"kv/example/config/port",          "8080"},
    {"kv/example/config/enabled",       "true"},
    {"kv/example/config/timeout",       "30"},
    {"kv/example/config/retry_count",   "3"},

    /* Status */
    {"kv/example/status/uptime",        "3600"},
    {"kv/example/status/connections",   "42"},
    {"kv/example/status/last_error",    "none"},
    {"kv/example/status/cpu_usage",     "15.7"},

    /* System */
    {"kv/system/version",               "1.2.3"},
    {"kv/system/build",                 "2024.12.20"},
    {"kv/system/hostname",              "mqtt-server-01"},
    {"kv/system/os",                    "Linux 5.15.0"},

    /* Application */
    {"kv/app/users/count",              "1250"},
    {"kv/app/users/active",             "523"},
    {"kv/app/database/connected",       "true"},
    {"kv/app/database/pool_size",       "10"},

    /* Sensors */
    {"kv/sensors/temperature/living_room", "22.5"},
    {"kv/sensors/temperature/bedroom",     "20.1"},
    {"kv/sensors/humidity/living_room",    "45"},
    {"kv/sensors/humidity/bedroom",        "50"},

    /* Sentinels (retained, match various read patterns) */
    {"kv/example/.sentinel",            "done"},
    {"kv/example/config/.sentinel",     "done"},
    {"kv/sensors/.sentinel/1",          "done"},
    {"kv/app/.sentinel/1",              "done"},
    {"kv/.sentinel",                    "done"},
};
static const int test_data_count = sizeof(test_data) / sizeof(test_data[0]);

/* ══════════════════════════════════════════════════════════════════════
 *  write_test_data — populate broker using the writer API
 * ══════════════════════════════════════════════════════════════════════ */

static bool write_test_data(void)
{
    printf("=== Writing Test Data via KVStoreWriter API ===\n\n");

    kvw_config_t cfg;
    kvw_config_init(&cfg);
    strncpy(cfg.client_id, "kv-test-writer", sizeof(cfg.client_id) - 1);

    kvw_store_writer_t writer;
    if (kvw_init(&writer, &cfg) != 0) {
        fprintf(stderr, "Failed to initialise writer\n");
        return false;
    }

    printf("Connecting writer to localhost:1883...\n");
    if (!kvw_connect(&writer, 5.0)) {
        fprintf(stderr, "Writer connect failed\n");
        kvw_destroy(&writer);
        return false;
    }

    /* Write all test data as retained messages */
    printf("Publishing %d retained messages...\n", test_data_count);
    int success_count = 0;

    for (int i = 0; i < test_data_count; i++) {
        if (kvw_write_single(&writer, test_data[i].topic,
                             test_data[i].value, 1, true, 5.0)) {
            success_count++;
        } else {
            fprintf(stderr, "  x Failed: %s\n", test_data[i].topic);
        }
    }

    printf("\nPublished %d/%d messages\n", success_count, test_data_count);

    kvw_disconnect(&writer);
    kvw_destroy(&writer);
    printf("Writer disconnected\n\n");

    return (success_count == test_data_count);
}

/* ══════════════════════════════════════════════════════════════════════
 *  demonstrate_reader — exercise all reader modes
 * ══════════════════════════════════════════════════════════════════════ */

static bool demonstrate_reader(void)
{
    printf("=== Demonstrating KVStoreReader API ===\n\n");

    /* 1. Create reader */
    printf("1. Creating KVStoreReader instance...\n");
    kvr_config_t cfg;
    kvr_config_init(&cfg);
    strncpy(cfg.client_id, "kv-test-reader", sizeof(cfg.client_id) - 1);

    kvr_store_reader_t reader;
    if (kvr_init(&reader, &cfg) != 0) {
        fprintf(stderr, "Failed to initialise reader\n");
        return false;
    }

    /* 2. Connect */
    printf("\n2. Testing connection to broker...\n");
    if (!kvr_connect(&reader, 5.0)) {
        fprintf(stderr, "Failed to connect to broker\n");
        kvr_destroy(&reader);
        return false;
    }
    printf("  Connection status: %s\n",
           kvr_is_connected(&reader) ? "true" : "false");

    kvr_entry_t entries[KVR_MAX_ENTRIES];
    int n;

    /* 3. Read all under kv/example/# with sentinel */
    printf("\n3. Reading all values under 'kv/example/#' (wildcards):\n");
    printf("--------------------------------------------------\n");
    {
        const char *sents[] = {"kv/example/.sentinel", NULL};
        n = kvr_read_pattern(&reader, "kv/example/#", 1, 2.0,
                             sents, true, entries, KVR_MAX_ENTRIES);
        if (n > 0) {
            printf("Found %d entries:\n", n);
            for (int i = 0; i < n; i++) {
                const char *p = entries[i].topic;
                const char *rest = (strlen(p) > 11) ? p + 11 : p;
                printf("  [%s]: %s\n", rest, entries[i].value);
            }
        } else {
            printf("  No retained messages found under kv/example/#\n");
        }
    }

    /* 4. Single-level wildcard kv/example/config/+ with sentinel */
    printf("\n4. Reading config values with 'kv/example/config/+' (single-level wildcard):\n");
    printf("--------------------------------------------------\n");
    {
        const char *sents[] = {"kv/example/config/.sentinel", NULL};
        n = kvr_read_pattern(&reader, "kv/example/config/+", 1, 2.0,
                             sents, true, entries, KVR_MAX_ENTRIES);
        if (n > 0) {
            printf("Configuration parameters:\n");
            for (int i = 0; i < n; i++) {
                const char *param = strrchr(entries[i].topic, '/');
                param = param ? param + 1 : entries[i].topic;
                printf("  %s = %s\n", param, entries[i].value);
            }
        } else {
            printf("  No config values found\n");
        }
    }

    /* 5. Read single value */
    printf("\n5. Reading single value 'kv/system/version' (exact topic):\n");
    printf("--------------------------------------------------\n");
    {
        char val[KVR_MAX_VALUE_LEN];
        if (kvr_read_single(&reader, "kv/system/version", 1.0,
                            val, sizeof(val))) {
            printf("  System version: %s\n", val);
        } else {
            printf("  Version not found\n");
        }
        if (kvr_read_single(&reader, "kv/system/build", 1.0,
                            val, sizeof(val))) {
            printf("  Build date: %s\n", val);
        }
        if (kvr_read_single(&reader, "kv/system/hostname", 1.0,
                            val, sizeof(val))) {
            printf("  Hostname: %s\n", val);
        }
    }

    /* 6. Multiple wildcards kv/sensors/+/+ with sentinel */
    printf("\n6. Reading sensor data 'kv/sensors/+/+' (multiple wildcards):\n");
    printf("--------------------------------------------------\n");
    {
        const char *sents[] = {"kv/sensors/.sentinel/1", NULL};
        n = kvr_read_pattern(&reader, "kv/sensors/+/+", 1, 2.0,
                             sents, true, entries, KVR_MAX_ENTRIES);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                char type[64] = "", location[64] = "";
                const char *p = entries[i].topic;
                if (strlen(p) > 11) {
                    const char *rest = p + 11;
                    const char *slash = strchr(rest, '/');
                    if (slash) {
                        int tlen = (int)(slash - rest);
                        if (tlen > 63) tlen = 63;
                        memcpy(type, rest, tlen);
                        type[tlen] = '\0';
                        strncpy(location, slash + 1, 63);
                        location[63] = '\0';
                    }
                }
                printf("  %s/%s: %s\n", type, location, entries[i].value);
            }
        } else {
            printf("  No sensor data found\n");
        }
    }

    /* 7. ALL retained messages with sentinel */
    printf("\n7. Reading ALL retained messages on broker with '#':\n");
    printf("--------------------------------------------------\n");
    {
        const char *sents[] = {"kv/.sentinel", NULL};
        n = kvr_read_all(&reader, "#", 2.0, sents, true,
                         entries, KVR_MAX_ENTRIES);
        if (n > 0) {
            printf("Total retained messages on broker: %d\n", n);

            /* Count by top-level prefix */
            typedef struct { char prefix[64]; int count; } prefix_count_t;
            prefix_count_t prefixes[32];
            int nprefixes = 0;

            for (int i = 0; i < n; i++) {
                char prefix[64];
                const char *slash = strchr(entries[i].topic, '/');
                if (slash) {
                    int plen = (int)(slash - entries[i].topic);
                    if (plen > 63) plen = 63;
                    memcpy(prefix, entries[i].topic, plen);
                    prefix[plen] = '\0';
                } else {
                    strncpy(prefix, entries[i].topic, 63);
                    prefix[63] = '\0';
                }

                int found = -1;
                for (int j = 0; j < nprefixes; j++) {
                    if (strcmp(prefixes[j].prefix, prefix) == 0) {
                        found = j;
                        break;
                    }
                }
                if (found >= 0) {
                    prefixes[found].count++;
                } else if (nprefixes < 32) {
                    snprintf(prefixes[nprefixes].prefix,
                             sizeof(prefixes[nprefixes].prefix), "%s", prefix);
                    prefixes[nprefixes].count = 1;
                    nprefixes++;
                }
            }

            printf("Message distribution:\n");
            for (int i = 0; i < nprefixes; i++) {
                printf("  %s/: %d messages\n",
                       prefixes[i].prefix, prefixes[i].count);
            }

            printf("\nFirst 5 messages:\n");
            int show = n < 5 ? n : 5;
            for (int i = 0; i < show; i++) {
                char preview[54];
                int vlen = (int)strlen(entries[i].value);
                int clen = vlen < 50 ? vlen : 50;
                memcpy(preview, entries[i].value, clen);
                preview[clen] = '\0';
                printf("  %s = %s%s\n", entries[i].topic, preview,
                       vlen > 50 ? "..." : "");
            }
        } else {
            printf("  No retained messages found on broker\n");
        }
    }

    /* 8. App metrics kv/app/+/+ with sentinel */
    printf("\n8. Reading application metrics 'kv/app/+/+':\n");
    printf("--------------------------------------------------\n");
    {
        const char *sents[] = {"kv/app/.sentinel/1", NULL};
        n = kvr_read_pattern(&reader, "kv/app/+/+", 1, 2.0,
                             sents, true, entries, KVR_MAX_ENTRIES);
        if (n > 0) {
            printf("Application metrics:\n");
            for (int i = 0; i < n; i++) {
                const char *p = entries[i].topic;
                if (strlen(p) > 7) {
                    const char *rest = p + 7;
                    char display[128];
                    strncpy(display, rest, 127);
                    display[127] = '\0';
                    char *s = strchr(display, '/');
                    if (s) *s = '.';
                    printf("  %s = %s\n", display, entries[i].value);
                }
            }
        } else {
            printf("  No application metrics found\n");
        }
    }

    printf("\n* Demonstration completed successfully!\n");

    /* 9. Cleanup */
    printf("\n9. Cleaning up...\n");
    printf("  Final connection status: %s\n",
           kvr_is_connected(&reader) ? "true" : "false");
    kvr_disconnect(&reader);
    printf("  Reader disconnected\n");
    kvr_destroy(&reader);

    return true;
}

/* ══════════════════════════════════════════════════════════════════════
 *  main
 * ══════════════════════════════════════════════════════════════════════ */

int main(void)
{
    printf("============================================================\n");
    printf(" MQTT KV Store - Unified Library Test\n");
    printf("============================================================\n\n");

    mosquitto_lib_init();

    /* Step 1: Write test data using writer API */
    if (!write_test_data()) {
        fprintf(stderr, "Failed to write test data. Exiting.\n");
        mosquitto_lib_cleanup();
        return 1;
    }

    /* Small delay to ensure all messages are processed */
    printf("Waiting for messages to settle...\n\n");
    struct timespec one_sec = {1, 0};
    nanosleep(&one_sec, NULL);

    /* Step 2: Read back using reader API */
    bool ok = demonstrate_reader();

    printf("\n============================================================\n");
    printf(" Test completed! All connections closed.\n");
    printf("============================================================\n");

    mosquitto_lib_cleanup();
    return ok ? 0 : 1;
}

