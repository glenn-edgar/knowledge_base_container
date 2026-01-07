/* Auto-generated. Do not edit. */
#pragma once
#include <stdint.h>
#include "schema_ids.h"

typedef struct {
  uint16_t node_id;
  uint16_t bitspace_id; /* generator assigns ids */
  uint16_t bits;
  uint8_t  merge;
  uint8_t  boundary;
} schema_bank_desc_t;

typedef struct {
  uint16_t bank_id;
  uint16_t local_idx;
} schema_bit_desc_t;

extern const schema_bank_desc_t g_schema_banks[SCHEMA_BANK_COUNT];
extern const schema_bit_desc_t  g_schema_bits[SCHEMA_BIT_COUNT];
extern const uint16_t          g_schema_parents[SCHEMA_NODE_COUNT];
