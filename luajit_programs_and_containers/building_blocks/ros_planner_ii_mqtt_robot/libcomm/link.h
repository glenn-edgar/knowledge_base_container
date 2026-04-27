// libcomm/link.h
// Slice 1d: per-slave link FSM scaffolding. State enum, per-slave row
// (state, miss_count, last_seen_ms, expected physics_model_id), and
// the build/lookup helpers.
//
// NO transitions are wired in 1d — JOIN_REQ / JOIN_ACK / JOIN_CONFIRM
// processing, miss-count bumping on timeout, and ACK_FLAG_URGENT →
// CMD_DRAIN insertion are all phase-2 work. 1d only stands up the
// data structure that comm_node_state / _physics_model / _miss_count /
// _last_seen_ms read from.

#pragma once

#include "comm.h"
#include "manifest.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t  mcu;                    // 0 = unused row
    uint8_t  state;                  // comm_node_state_t value
    uint8_t  miss_count;
    uint8_t  next_seq;               // per-slave m2s wire sequence counter (slice 1e)
    int8_t   outstanding_slot;       // -1 = no in-flight request; else slot index (slice 1e)
    uint8_t  _pad[3];
    uint32_t last_seen_ms;
    uint32_t physics_model_id;       // expected hash from manifest; verified at JOIN_CONFIRM
} link_slave_state_t;

typedef struct {
    uint8_t            slave_count;
    link_slave_state_t slaves[COMM_SLAVES_MAX];
} link_t;

void link_init(link_t *l);

// Populate per-slave rows from the manifest. All slaves start in
// COMM_NODE_UNKNOWN — phase 2 transitions them to PENDING / LIVE /
// FAULTED via the join handshake.
comm_result_t link_build(link_t *l, const comm_manifest_v1_wire_t *m);

// O(N) lookup by mcu. Returns NULL if mcu is not declared.
link_slave_state_t       *link_find_slave      (link_t       *l, uint8_t mcu);
const link_slave_state_t *link_find_slave_const(const link_t *l, uint8_t mcu);

#ifdef __cplusplus
}
#endif
