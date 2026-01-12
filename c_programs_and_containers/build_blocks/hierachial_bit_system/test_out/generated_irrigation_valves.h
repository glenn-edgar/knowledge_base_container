/**
 * @file generated_irrigation_valves.h
 * @brief Hierarchical Bit Map - irrigation_valves v1.0.0
 *
 * Buffer Types:
 *   OR_LATCH - OR merge, bits latch until cleared
 *   OR_MASK  - OR merge with mask for selective propagation
 *   AND      - AND merge, all children must set bit
 *
 * Auto-generated - DO NOT EDIT
 */

#ifndef IRRIGATION_VALVES_H
#define IRRIGATION_VALVES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================ */
/* Schema Info                                  */
/* ============================================ */

#define IRRIGATION_VALVES_VERSION "1.0.0"
#define IRRIGATION_VALVES_BUFFER_COUNT 3
#define IRRIGATION_VALVES_CLASS_COUNT 4
#define IRRIGATION_VALVES_NODE_COUNT 36

/* ============================================ */
/* Buffer Types                                 */
/* ============================================ */

typedef enum {
    IRRIGATION_VALVES_BUF_OR_LATCH = 0,  /* OR merge, bits latch until cleared */
    IRRIGATION_VALVES_BUF_OR_MASK  = 1,  /* OR merge with mask */
    IRRIGATION_VALVES_BUF_AND      = 2,  /* AND merge */
} irrigation_valves_buffer_type_t;

/* ============================================ */
/* Buffer Indices                               */
/* ============================================ */

typedef enum {
    IRRIGATION_VALVES_BUF_ALARM_LATCHED = 0,
    IRRIGATION_VALVES_BUF_ALARM_MASK = 1,
    IRRIGATION_VALVES_BUF_AND_LATCHED = 2,
} irrigation_valves_buffer_id_t;

/* Buffer type for each buffer */
static const irrigation_valves_buffer_type_t irrigation_valves_buffer_types[3] = {
    IRRIGATION_VALVES_BUF_OR_LATCH,  /* ALARM_LATCHED */
    IRRIGATION_VALVES_BUF_OR_MASK,  /* ALARM_MASK */
    IRRIGATION_VALVES_BUF_AND,  /* AND_LATCHED */
};

/* Buffer hash lookup */
typedef struct {
    uint32_t hash;
    int16_t  index;
} irrigation_valves_buffer_hash_entry_t;

static const irrigation_valves_buffer_hash_entry_t irrigation_valves_buffer_hashes[3] = {
    { 0x59C57B7BU, 1 },  /* ALARM_MASK */
    { 0x67B866D6U, 0 },  /* ALARM_LATCHED */
    { 0x7FD46D6AU, 2 },  /* AND_LATCHED */
};

/* Find buffer index by name (e.g., "ALARM_LATCHED") */
static inline int16_t irrigation_valves_find_buffer(const char* name) {
    uint32_t hash = cfl_hbit_hash_string(name);
    int lo = 0, hi = 2;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (irrigation_valves_buffer_hashes[mid].hash == hash) return irrigation_valves_buffer_hashes[mid].index;
        if (irrigation_valves_buffer_hashes[mid].hash < hash) lo = mid + 1; else hi = mid - 1;
    }
    return -1;
}

/* ============================================ */
/* Class Indices                                */
/* ============================================ */

typedef enum {
    IRRIGATION_VALVES_CLASS_VALVE_BANK_LEAF = 0,
    IRRIGATION_VALVES_CLASS_AND_VALVE_BANK_LEAF = 1,
    IRRIGATION_VALVES_CLASS_AND_VALVE_AGGREGATE = 2,
    IRRIGATION_VALVES_CLASS_VALVE_AGGREGATE = 3,
} irrigation_valves_class_id_t;

/* ============================================ */
/* Node Indices                                 */
/* ============================================ */

typedef enum {
    IRRIGATION_VALVES_NODE_VALVE_STATUS = 0,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_1_VALVE_STATUS = 1,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_1_VALVE_STATUS = 2,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_2_VALVE_STATUS = 3,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_3_VALVE_STATUS = 4,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_4_VALVE_STATUS = 5,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_2_VALVE_STATUS = 6,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_1_VALVE_STATUS = 7,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_2_VALVE_STATUS = 8,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_3_VALVE_STATUS = 9,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_3_VALVE_STATUS = 10,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_1_VALVE_STATUS = 11,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_2_VALVE_STATUS = 12,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_3_VALVE_STATUS = 13,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_4_VALVE_STATUS = 14,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_1_VALVE_STATUS = 15,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_2_VALVE_STATUS = 16,
    IRRIGATION_VALVES_NODE_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_3_VALVE_STATUS = 17,
    IRRIGATION_VALVES_NODE_VALVE_STATE = 18,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_1_VALVE_STATE = 19,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_1_VALVE_STATE_BANK_1_VALVE_STATE = 20,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_1_VALVE_STATE_BANK_2_VALVE_STATE = 21,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_1_VALVE_STATE_BANK_3_VALVE_STATE = 22,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_1_VALVE_STATE_BANK_4_VALVE_STATE = 23,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_2_VALVE_STATE = 24,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_2_VALVE_STATE_BANK_1_VALVE_STATE = 25,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_2_VALVE_STATE_BANK_2_VALVE_STATE = 26,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_2_VALVE_STATE_BANK_3_VALVE_STATE = 27,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_3_VALVE_STATE = 28,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_3_VALVE_STATE_BANK_1_VALVE_STATE = 29,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_3_VALVE_STATE_BANK_2_VALVE_STATE = 30,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_3_VALVE_STATE_BANK_3_VALVE_STATE = 31,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_4_VALVE_STATE = 32,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_4_VALVE_STATE_BANK_1_VALVE_STATE = 33,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_4_VALVE_STATE_BANK_2_VALVE_STATE = 34,
    IRRIGATION_VALVES_NODE_VALVE_STATE_STATION_4_VALVE_STATE_BANK_3_VALVE_STATE = 35,
} irrigation_valves_node_id_t;

/* ============================================ */
/* Node Hashes (for cfl_hbit_find_node)         */
/* ============================================ */

#define IRRIGATION_VALVES_HASH_VALVE_STATUS 0xD4D5A144U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_1_VALVE_STATUS 0x7492D1CAU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_1_VALVE_STATUS 0x6B2D261AU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_2_VALVE_STATUS 0x6CEE09A9U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_3_VALVE_STATUS 0xBE5595E8U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_1_VALVE_STATUS_BANK_4_VALVE_STATUS 0x91A2EC47U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_2_VALVE_STATUS 0x4065F0D9U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_1_VALVE_STATUS 0xA3769025U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_2_VALVE_STATUS 0xF5B488C6U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_3_VALVE_STATUS 0xCE8CB14FU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_3_VALVE_STATUS 0x9499C658U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_1_VALVE_STATUS 0xA98D269CU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_2_VALVE_STATUS 0x4BB06657U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_3_VALVE_STATUS_BANK_3_VALVE_STATUS 0x0976EEAEU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_4_VALVE_STATUS 0xC3623077U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_1_VALVE_STATUS 0xA147788FU
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_2_VALVE_STATUS 0x2ACA4FB4U
#define IRRIGATION_VALVES_HASH_VALVE_STATUS_STATION_4_VALVE_STATUS_BANK_3_VALVE_STATUS 0xDA7BDF65U
#define IRRIGATION_VALVES_HASH_VALVE_STATE 0x059ECDAFU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_1_VALVE_STATE 0xD8BFCC4EU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_1_VALVE_STATE_BANK_1_VALVE_STATE 0x940E98F9U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_1_VALVE_STATE_BANK_2_VALVE_STATE 0x90FFB51CU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_1_VALVE_STATE_BANK_3_VALVE_STATE 0x74EDA017U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_1_VALVE_STATE_BANK_4_VALVE_STATE 0x9589A6E2U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_2_VALVE_STATE 0x5AB7C397U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_2_VALVE_STATE_BANK_1_VALVE_STATE 0x4C2970D6U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_2_VALVE_STATE_BANK_2_VALVE_STATE 0x4231B5DFU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_2_VALVE_STATE_BANK_3_VALVE_STATE 0xBB4C9CA4U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_3_VALVE_STATE 0x76C9D89CU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_3_VALVE_STATE_BANK_1_VALVE_STATE 0x9CFDD593U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_3_VALVE_STATE_BANK_2_VALVE_STATE 0xC30961AAU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_3_VALVE_STATE_BANK_3_VALVE_STATE 0x9A4C46F5U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_4_VALVE_STATE 0x435DFC0DU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_4_VALVE_STATE_BANK_1_VALVE_STATE 0x63B3DE00U
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_4_VALVE_STATE_BANK_2_VALVE_STATE 0xFE7C464DU
#define IRRIGATION_VALVES_HASH_VALVE_STATE_STATION_4_VALVE_STATE_BANK_3_VALVE_STATE 0x30358FA2U

/* ============================================ */
/* Bit Definitions                              */
/* ============================================ */

/* Valve_Bank_Leaf */
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_OVERCURRENT 0
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_STUCK_OPEN 1
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_STUCK_CLOSED 2
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_LEAK 3
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_OVERTEMP 4
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_COMM_FAIL 5
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_LOW_PRESSURE 6
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_MASK_HIGH_PRESSURE 7
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_OVERCURRENT 0
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_STUCK_OPEN 1
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_STUCK_CLOSED 2
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LEAK 3
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_OVERTEMP 4
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_COMM_FAIL 5
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_LOW_PRESSURE 6
#define IRRIGATION_VALVES_VALVE_BANK_LEAF_ALARM_LATCHED_HIGH_PRESSURE 7

/* AND_Valve_Bank_Leaf */
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_POWERED 0
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_CALIBRATED 1
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_ENABLED 2
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_READY 3
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_COMM_OK 4
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_PRESSURE_OK 5
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_FLOW_OK 6
#define IRRIGATION_VALVES_AND_VALVE_BANK_LEAF_AND_LATCHED_POSITION_OK 7

/* ============================================ */
/* Bank Sizes (bits per buffer per class)       */
/* ============================================ */

static const uint8_t irrigation_valves_bank_sizes[4][3] = {
    { 8, 8, 0 },  /* Valve_Bank_Leaf */
    { 0, 0, 8 },  /* AND_Valve_Bank_Leaf */
    { 0, 0, 8 },  /* AND_Valve_Aggregate */
    { 8, 8, 0 },  /* Valve_Aggregate */
};

/* ============================================ */
/* Bit Hash Tables (for runtime lookup)         */
/* ============================================ */

typedef struct {
    uint32_t hash;
    uint8_t  bit_index;
} irrigation_valves_bit_hash_entry_t;

static const irrigation_valves_bit_hash_entry_t irrigation_valves_valve_bank_leaf_alarm_latched_bits[8] = {
    { 0x2C1C798CU, 2 },  /* STUCK_CLOSED */
    { 0x55858EB5U, 6 },  /* LOW_PRESSURE */
    { 0x6738D7CEU, 3 },  /* LEAK */
    { 0x89229EBCU, 1 },  /* STUCK_OPEN */
    { 0xC06206F9U, 4 },  /* OVERTEMP */
    { 0xE32C1039U, 7 },  /* HIGH_PRESSURE */
    { 0xE4AC7FC0U, 0 },  /* OVERCURRENT */
    { 0xEABEF842U, 5 },  /* COMM_FAIL */
};

static const irrigation_valves_bit_hash_entry_t irrigation_valves_valve_bank_leaf_alarm_mask_bits[8] = {
    { 0x2C1C798CU, 2 },  /* STUCK_CLOSED */
    { 0x55858EB5U, 6 },  /* LOW_PRESSURE */
    { 0x6738D7CEU, 3 },  /* LEAK */
    { 0x89229EBCU, 1 },  /* STUCK_OPEN */
    { 0xC06206F9U, 4 },  /* OVERTEMP */
    { 0xE32C1039U, 7 },  /* HIGH_PRESSURE */
    { 0xE4AC7FC0U, 0 },  /* OVERCURRENT */
    { 0xEABEF842U, 5 },  /* COMM_FAIL */
};

static const irrigation_valves_bit_hash_entry_t irrigation_valves_and_valve_bank_leaf_and_latched_bits[8] = {
    { 0x07F22354U, 4 },  /* COMM_OK */
    { 0x88C2FFFEU, 2 },  /* ENABLED */
    { 0x9451BA27U, 0 },  /* POWERED */
    { 0x945B1312U, 1 },  /* CALIBRATED */
    { 0x9B444BF8U, 6 },  /* FLOW_OK */
    { 0xBB03D511U, 7 },  /* POSITION_OK */
    { 0xC71642E9U, 5 },  /* PRESSURE_OK */
    { 0xCDDA2CD4U, 3 },  /* READY */
};

typedef struct {
    uint16_t class_idx;
    uint16_t buf_idx;
    uint8_t  count;
    const irrigation_valves_bit_hash_entry_t* entries;
} irrigation_valves_bit_table_t;

static const irrigation_valves_bit_table_t irrigation_valves_bit_tables[3] = {
    { 0, 0, 8, irrigation_valves_valve_bank_leaf_alarm_latched_bits },
    { 0, 1, 8, irrigation_valves_valve_bank_leaf_alarm_mask_bits },
    { 1, 2, 8, irrigation_valves_and_valve_bank_leaf_and_latched_bits },
};

#define IRRIGATION_VALVES_BIT_TABLE_COUNT 3

/* Find bit index by hash for given class and buffer */
static inline int8_t irrigation_valves_find_bit_by_hash(uint16_t class_idx, uint16_t buf_idx, uint32_t hash) {
    for (int i = 0; i < IRRIGATION_VALVES_BIT_TABLE_COUNT; i++) {
        if (irrigation_valves_bit_tables[i].class_idx == class_idx && irrigation_valves_bit_tables[i].buf_idx == buf_idx) {
            const irrigation_valves_bit_hash_entry_t* entries = irrigation_valves_bit_tables[i].entries;
            int lo = 0, hi = irrigation_valves_bit_tables[i].count - 1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                if (entries[mid].hash == hash) return (int8_t)entries[mid].bit_index;
                if (entries[mid].hash < hash) lo = mid + 1; else hi = mid - 1;
            }
            return -1;
        }
    }
    return -1;
}

/* Find bit index by name for given class and buffer */
static inline int8_t irrigation_valves_find_bit(uint16_t class_idx, uint16_t buf_idx, const char* name) {
    return irrigation_valves_find_bit_by_hash(class_idx, buf_idx, cfl_hbit_hash_string(name));
}

/* Find bit index by name for a node */
static inline int8_t irrigation_valves_find_node_bit(const cfl_hbit_instance_t* inst, uint16_t node, uint16_t buf, const char* name) {
    uint16_t class_idx = inst->config->nodes[node].class_index;
    return irrigation_valves_find_bit(class_idx, buf, name);
}

/* ============================================ */
/* Arena Sizes                                  */
/* ============================================ */

#define IRRIGATION_VALVES_ARENA_ALARM_LATCHED_SIZE 18
#define IRRIGATION_VALVES_ARENA_ALARM_LATCHED_SIZE_WITH_LATCH 36
#define IRRIGATION_VALVES_ARENA_ALARM_MASK_SIZE 18
#define IRRIGATION_VALVES_ARENA_AND_LATCHED_SIZE 18

#define IRRIGATION_VALVES_TOTAL_RAM_BYTES 144

/* ============================================ */
/* Node Descriptors                             */
/* ============================================ */

/* Common node structure (compatible with hbit_runtime.h) */
typedef struct {
    uint32_t path_hash;
    uint16_t class_index;
    int16_t  parent_index;   /* -1 if root */
    uint16_t child_count;
    uint16_t first_child;    /* Index of first child, or 0 */
    uint8_t  depth;
    uint8_t  is_leaf;
} irrigation_valves_node_t;

/* Per-buffer arena offsets for each node */
typedef struct {
    uint16_t offset;  /* Byte offset in arena */
    uint8_t  size;    /* Size in bytes */
} irrigation_valves_arena_info_t;

#ifdef __cplusplus
}
#endif

#endif /* IRRIGATION_VALVES_H */
/* ============================================ */
/* Memory Summary                               */
/* ============================================ */
/*
 * Schema: irrigation_valves v1.0.0
 * Nodes: 36, Classes: 4, Buffers: 3
 *
 * Buffer Arenas:
 *   ALARM_LATCHED   72 bytes (type=OR_LATCH, ×2 current/latched, ×2 shadow)
 *   ALARM_MASK     36 bytes (type=OR_MASK, ×2 shadow)
 *   AND_LATCHED    36 bytes (type=AND, ×2 shadow)
 *
 * Total RAM: 144 bytes
 */