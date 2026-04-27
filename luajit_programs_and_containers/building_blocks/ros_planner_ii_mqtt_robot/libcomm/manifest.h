// libcomm/manifest.h
// Slice 1d. Validates the embedded comm_manifest_v1_wire_t blob handed
// to comm_init. Enforces the 8 invariants locked in project memory:
//   1. dongle_count >= 1
//   2. dongles[0].uuid == HOST_INTERNAL_DONGLE (all zeros)
//   3. bus_count <= COMM_BUSES_MAX, slave_count <= COMM_SLAVES_MAX
//   4. every bus's tick_period_ms >= CT_COMM_RX_PERIOD_MS, max_miss > 0
//   5. every slave's dongle_idx < dongle_count
//   6. every slave's bus_id appears in its dongle's bus_local_ids[]
//   7. every slave's addr in {0x01..0xFC, 0xFE}
//   8. mcu non-zero and unique across slaves
//
// Header (schema_hash, packet length) is the entry guard; failures
// surface as COMM_ERR_BAD_MANIFEST. Phase 2 will add runtime-vs-manifest
// dongle enumeration cross-checks (declared-not-found / found-not-declared).

#pragma once

#include "comm.h"
#include "comm_manifest.h"

#ifdef __cplusplus
extern "C" {
#endif

// On success: COMM_OK and *out_data points into the caller's blob (no copy).
// On failure: COMM_ERR_BAD_MANIFEST or COMM_ERR_BAD_ARG.
comm_result_t manifest_validate(const uint8_t *blob,
                                size_t blob_len,
                                const comm_manifest_v1_wire_t **out_data);

// 1 if the 16-byte UUID is the all-zero HOST_INTERNAL_DONGLE sentinel.
int manifest_dongle_is_host(const uint8_t uuid[16]);

// 1 if bus_id appears in d->bus_local_ids[0 .. min(bus_count, 8)].
int manifest_dongle_owns_bus(const manifest_dongle_t *d, uint8_t bus_id);

#ifdef __cplusplus
}
#endif
