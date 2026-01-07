/* bitmask_runtime.h
 *
 * Runtime engine for hierarchical bit masks with shadow buffering.
 * 
 * Design:
 * - Each bank has a "current" bitmask (read-only during tick)
 * - Each bank has a "shadow" bitmask (writes staged here during tick)
 * - On tick end, propagate merges up hierarchy using shadows
 * - Then swap: shadow → current for atomic visibility
 * 
 * Concurrency safety:
 * - Assumes single-writer per bank (or mutex if multi)
 * - Reads always from current (snapshot consistent)
 * - Merges use function-encapsulated access (per your earlier suggestion)
 * 
 * Usage:
 * 1. Init with schema tables/blob
 * 2. Per tick: Write to shadows → call propagate_tick() → swaps happen
 * 
 * Dependencies: schema_tables.h (banks, parents, etc.)
 */

#ifndef BITMASK_RUNTIME_H
#define BITMASK_RUNTIME_H

#include <stdint.h>
#include "schema_tables.h"  // g_schema_banks, etc.

// Per-bank bitmask storage (uint8_t[] for generality; could optimize to uint32_t/64_t)
typedef struct {
  uint8_t *current;  // Visible/read-only during tick
  uint8_t *shadow;   // Staged writes during tick
  uint16_t bytes;    // bits / 8 (padded up)
} bitmask_bank_t;

// Global runtime state
extern bitmask_bank_t g_bitmasks[SCHEMA_BANK_COUNT];

// Init: Allocate masks based on schema
void bitmask_init(void);

// Free allocations
void bitmask_deinit(void);

// Set bit in shadow (encapsulated)
void bitmask_set(uint16_t bank_id, uint16_t bit_idx, bool value);

// Get bit from current (read-only)
bool bitmask_get(uint16_t bank_id, uint16_t bit_idx);

// Per-tick propagation + swap
void bitmask_propagate_tick(void);

#endif  // BITMASK_RUNTIME_H
