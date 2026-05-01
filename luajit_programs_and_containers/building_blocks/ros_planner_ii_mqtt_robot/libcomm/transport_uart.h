// libcomm/transport_uart.h
// FD-based byte transport, used by chain_tree_host to talk to a real
// dongle (Phase C: open /dev/ttyUSBn) or — Phase A — to a separate
// process (robot_sim) holding the slave end of a kernel pty.
//
// Same per-endpoint shape as transport_inproc, but where transport_inproc
// owns paired byte rings IN-PROCESS, transport_uart owns one FD and two
// rings staged for non-blocking syscalls. The TX ring stages frame bytes
// produced by frame_encode_m2s; the RX ring buffers bytes read off the FD
// before frame.c's incremental decoder consumes them.
//
// One transport_uart_t == one dongle == one bus. (Multidrop addressing on
// that bus is the master-side slave loop's concern, not the transport's.)

#pragma once

#include "frame.h"
#include "bus_config.h"
#include "ext_bus.h"

#ifdef __cplusplus
extern "C" {
#endif

// Ring sizes (TRANSPORT_UART_TX_SIZE / _RX_SIZE) and path length
// (TRANSPORT_UART_PATH_MAX) live in bus_config.h. Both ring sizes must be
// power-of-2 (frame_ring_init contract).
//
// As of Track A.5, transport_uart_t is a thin staging layer over
// ext_bus_t (libcomm/ext_bus.h). The pty/USB-serial FD specifics are in
// ext_bus_linux_pty.c; on embedded targets a different ext_bus_*.c
// implements the same 3-fn contract behind the ext_bus_t opaque blob.
// `master_fd` is kept as a diagnostic int (mirror of the underlying FD)
// purely so existing logging continues to work — it is no longer the
// owner of the FD.

typedef struct {
    int           master_fd;                      // diagnostic mirror; ext_bus owns the FD
    char          slave_path[TRANSPORT_UART_PATH_MAX];
    ext_bus_t     bus;
    uint8_t       tx_buf[TRANSPORT_UART_TX_SIZE];
    uint8_t       rx_buf[TRANSPORT_UART_RX_SIZE];
    frame_ring_t  tx_ring;                        // m2s bytes pending write to bus
    frame_ring_t  rx_ring;                        // s2m bytes drained from bus
} transport_uart_t;

// Open an existing path (pty in sim, /dev/ttyUSBn in prod), apply
// cfmakeraw, take an exclusive flock(LOCK_EX|LOCK_NB) on the FD to
// enforce the system-wide non-overlap rule. The path is copied into
// t->slave_path for diagnostics. master_fd is left O_NONBLOCK so the
// pump's write side cannot deadlock against a momentarily-full peer
// kernel buffer.
//
// Returns:
//    0  success
//   -1  open(2) or termios syscall failure (errno preserved)
//   -2  flock failed — another chain_tree process owns this path
//       (errno = EWOULDBLOCK on the contended case)
int  transport_uart_init_open(transport_uart_t *t, const char *path);

// Idempotent close. Subsequent pump calls are no-ops.
void transport_uart_close   (transport_uart_t *t);

// Service tick: flush tx_ring → FD (blocking writes; pty kernel buffer is
// 4 KB so this rarely sleeps in practice), then poll FD for readable bytes
// and drain into rx_ring. Both directions advance as far as they can in
// one call.
//
// Returns:
//    0  normal (may have done nothing if there was no work and no input)
//   -1  unrecoverable I/O error (errno set)
//   -2  EOF — peer closed its end of the pty (slave_path holder exited)
int  transport_uart_pump    (transport_uart_t *t);

#ifdef __cplusplus
}
#endif
