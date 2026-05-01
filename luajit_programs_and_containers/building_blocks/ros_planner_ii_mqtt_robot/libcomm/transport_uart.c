// libcomm/transport_uart.c
// Per-dongle staging-ring layer ABOVE the ext_bus.h contract. After
// Track A.5 (continue.md) this file no longer owns the FD or termios —
// those moved to ext_bus_linux_pty.c. This file holds the two byte
// rings (tx_ring / rx_ring) that decouple the pump cadence from the
// frame.c encoder/decoder cadence.
//
// Pump cycle:
//   1. drain tx_ring → ext_bus_tx; on short writes, re-queue the tail.
//   2. ext_bus_rx → fill rx_ring; stop when bus or ring is empty.
//
// On embedded the same shape works: ext_bus_tx hands bytes to a DMA
// transmit ring, ext_bus_rx pulls bytes from a DMA receive ring's tail.

#include "transport_uart.h"
#include "ext_bus.h"

#include <errno.h>
#include <string.h>

int transport_uart_init_open(transport_uart_t *t, const char *path)
{
    if (!t || !path) { errno = EINVAL; return -1; }
    memset(t, 0, sizeof(*t));
    t->master_fd = -1;

    int rc = ext_bus_open_pty(&t->bus, path);
    if (rc != 0) return rc;

    size_t n = strlen(path);
    if (n >= sizeof(t->slave_path)) n = sizeof(t->slave_path) - 1;
    memcpy(t->slave_path, path, n);
    t->slave_path[n] = '\0';
    t->master_fd = 0;     // diagnostic flag: 0 = open, -1 = closed (real FD lives in ext_bus)

    frame_ring_init(&t->tx_ring, t->tx_buf, TRANSPORT_UART_TX_SIZE);
    frame_ring_init(&t->rx_ring, t->rx_buf, TRANSPORT_UART_RX_SIZE);
    return 0;
}

void transport_uart_close(transport_uart_t *t)
{
    if (!t) return;
    if (t->master_fd >= 0) {
        ext_bus_close(&t->bus);
        t->master_fd = -1;
    }
}

int transport_uart_pump(transport_uart_t *t)
{
    if (!t || t->master_fd < 0) return -1;

    uint8_t scratch[FRAME_BUFFER_MAX + 16];

    // 1) drain tx_ring → ext_bus_tx. Re-queue any bytes the bus didn't
    //    accept (full kernel buffer / full DMA ring).
    while (frame_ring_used(&t->tx_ring) > 0) {
        uint32_t n = frame_ring_read_drain(&t->tx_ring, scratch, sizeof(scratch));
        if (n == 0) break;
        size_t accepted = ext_bus_tx(&t->bus, scratch, n);
        if (accepted < n) {
            for (size_t i = accepted; i < n; i++) {
                (void)frame_ring_write_byte(&t->tx_ring, scratch[i]);
            }
            break;
        }
    }

    // 2) ext_bus_rx → fill rx_ring. Stop on first empty read or when
    //    the ring is full. ext_bus_rx_wait(0) gates the read so we
    //    never block on an empty bus.
    while (1) {
        int wrc = ext_bus_rx_wait(&t->bus, 0);
        if (wrc == -1) break;          // no data ready
        if (wrc == -2) return -2;      // peer closed
        if (wrc == -3) return -1;      // unrecoverable

        uint32_t free_n = frame_ring_free(&t->rx_ring);
        if (free_n == 0) break;
        if (free_n > sizeof(scratch)) free_n = sizeof(scratch);

        size_t got = ext_bus_rx(&t->bus, scratch, free_n);
        if (got == 0) break;
        for (size_t i = 0; i < got; i++) {
            (void)frame_ring_write_byte(&t->rx_ring, scratch[i]);
        }
    }

    return 0;
}
