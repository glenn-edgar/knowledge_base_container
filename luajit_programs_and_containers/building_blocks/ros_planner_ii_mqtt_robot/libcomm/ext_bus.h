// libcomm/ext_bus.h
// External-bus driver contract — the upper boundary of the per-silicon
// driver layer (continue.md "three-function ext_bus contract").
// Everything above this line is portable: SLIP framing, CRC, ring
// buffers, link FSM, router, slave dispatch. Everything below is
// per-silicon: termios on Linux pty/USB-serial, DMA + idle-line IRQ on
// RP2040/RP2350/MGM240, etc.
//
// Discipline (from continue.md):
//   "above the contract, no caller is allowed to assume anything about
//    where bytes live in memory. The driver owns alignment, cacheability,
//    DMA-vs-CPU ordering. If application code ever needs to know
//    'is this DMA?' — the contract is wrong."
//
// The continue.md sketch wrote the three functions without a handle
// argument because on a single-MCU dongle there's exactly one ext_bus.
// This header takes an opaque ext_bus_t* anyway:
//   - the chain_tree-on-Linux process holds N dongles (= N pty paths)
//     simultaneously, so we need to address each;
//   - on embedded targets the application keeps one ext_bus_t in BSS
//     and never uses more than one — the handle is harmless;
//   - dongles with two UARTs (rare but plausible) work without API
//     change.
//
// The handle is opaque: callers allocate by value (sizeof determined by
// EXT_BUS_STORAGE_BYTES below); the impl casts it to its native struct.

#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque storage. Sized for the largest backend we expect: Linux pty
// (fd + path + flags) is the high-water mark today.
#define EXT_BUS_STORAGE_BYTES   96

typedef struct { uint64_t _opaque[EXT_BUS_STORAGE_BYTES / 8]; } ext_bus_t;

// ============ LIFECYCLE ============
// Linux: ext_bus_open_pty(path) — opens an existing pty/tty path,
// applies cfmakeraw + flock(LOCK_EX|LOCK_NB), sets O_NONBLOCK.
// Embedded: ext_bus_open_uart(uart_idx, baud) — TBD per-silicon. The
// open call is by definition platform-specific (paths don't exist on
// embedded; UART indices don't exist on Linux). Anything ABOVE
// ext_bus_open is portable.
//
// Returns:
//    0  success
//   -1  open / termios / flock failure (errno preserved on Linux)
//   -2  flock contended (someone else holds this path)

int  ext_bus_open_pty(ext_bus_t *bus, const char *path);
void ext_bus_close   (ext_bus_t *bus);

// ============ THE 3-FUNCTION CONTRACT ============

// Write up to n bytes. Non-blocking: never sleeps. Returns the number
// of bytes accepted by the bus (may be 0 if the kernel/HW buffer is
// full). The caller is responsible for re-trying with the leftover.
size_t ext_bus_tx(ext_bus_t *bus, const uint8_t *bytes, size_t n);

// Read up to cap bytes. Non-blocking: returns 0 immediately if no data.
// Returns the number of bytes copied into out.
size_t ext_bus_rx(ext_bus_t *bus, uint8_t *out, size_t cap);

// Block up to timeout_ms waiting for ext_bus_rx to have data ready.
// timeout_ms = 0 returns immediately (poll). Returns:
//    0          data is ready (call ext_bus_rx)
//   -1          timeout expired
//   -2          peer closed (EOF / disconnected)
//   -3          unrecoverable I/O error
int  ext_bus_rx_wait(ext_bus_t *bus, uint32_t timeout_ms);

// ============ DIAGNOSTICS ============
// Path/identifier the bus was opened with — for logging only.
// Returns NULL on closed/unknown bus.
const char *ext_bus_label(const ext_bus_t *bus);

#ifdef __cplusplus
}
#endif
