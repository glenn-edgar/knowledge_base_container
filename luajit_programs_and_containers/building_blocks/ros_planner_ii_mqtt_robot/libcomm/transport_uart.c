// libcomm/transport_uart.c
// _XOPEN_SOURCE 700 makes posix_openpt / grantpt / unlockpt / ptsname_r
// visible under -std=c99 (glibc hides them otherwise).

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "transport_uart.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <termios.h>
#include <unistd.h>

int transport_uart_init_open(transport_uart_t *t, const char *path)
{
    if (!t || !path) { errno = EINVAL; return -1; }
    memset(t, 0, sizeof(*t));
    t->master_fd = -1;

    int fd = open(path, O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) return -1;

    // Layer 1 of the non-overlap enforcement: exclusive advisory lock,
    // released automatically when the FD closes. flock will fail with
    // EWOULDBLOCK if another process already holds the lock.
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        int e = errno;
        close(fd);
        errno = e;
        return -2;
    }

    struct termios tio;
    if (tcgetattr(fd, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }
    cfmakeraw(&tio);
    if (tcsetattr(fd, TCSANOW, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }

    t->master_fd = fd;
    size_t n = strlen(path);
    if (n >= sizeof(t->slave_path)) n = sizeof(t->slave_path) - 1;
    memcpy(t->slave_path, path, n);
    t->slave_path[n] = '\0';

    frame_ring_init(&t->tx_ring, t->tx_buf, TRANSPORT_UART_TX_SIZE);
    frame_ring_init(&t->rx_ring, t->rx_buf, TRANSPORT_UART_RX_SIZE);
    return 0;
}

void transport_uart_close(transport_uart_t *t)
{
    if (!t) return;
    if (t->master_fd >= 0) {
        // close() releases the flock implicitly on Linux.
        close(t->master_fd);
        t->master_fd = -1;
    }
}

int transport_uart_pump(transport_uart_t *t)
{
    if (!t || t->master_fd < 0) return -1;

    uint8_t scratch[FRAME_BUFFER_MAX + 16];

    // 1) flush tx_ring → FD. master_fd is O_NONBLOCK so write can return
    //    EAGAIN when the kernel buffer is full; in that case we stash
    //    the unwritten leftover back at the tail of tx_ring (the ring is
    //    SP/SC — we're the sole producer here — so re-pushing bytes we
    //    just drained preserves order). Next pump call retries.
    while (frame_ring_used(&t->tx_ring) > 0) {
        uint32_t n = frame_ring_read_drain(&t->tx_ring, scratch, sizeof(scratch));
        if (n == 0) break;
        size_t off = 0;
        int    bailed = 0;
        while (off < n) {
            ssize_t w = write(t->master_fd, scratch + off, n - off);
            if (w < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    bailed = 1;
                    break;
                }
                return -1;
            }
            off += (size_t)w;
        }
        if (bailed) {
            // Re-queue what we couldn't write. The ring has at least
            // (n - off) bytes free since we just drained n bytes.
            for (size_t i = off; i < n; i++) {
                (void)frame_ring_write_byte(&t->tx_ring, scratch[i]);
            }
            break;
        }
    }

    // 2) drain FD → rx_ring. poll(0 ms) gates the read so we never block
    //    on an empty FD; we keep reading until poll says nothing's there
    //    or the rx_ring runs out of room.
    while (1) {
        struct pollfd pfd = { .fd = t->master_fd, .events = POLLIN };
        int p = poll(&pfd, 1, 0);
        if (p < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (p == 0) break;
        if (!(pfd.revents & POLLIN)) break;

        uint32_t free_n = frame_ring_free(&t->rx_ring);
        if (free_n == 0) break;
        if (free_n > sizeof(scratch)) free_n = sizeof(scratch);

        ssize_t r = read(t->master_fd, scratch, free_n);
        if (r < 0) {
            if (errno == EINTR)  continue;
            if (errno == EAGAIN) break;        // shouldn't happen after poll, but defend
            return -1;
        }
        if (r == 0) return -2;                 // peer closed slave end

        for (ssize_t i = 0; i < r; i++) {
            (void)frame_ring_write_byte(&t->rx_ring, scratch[i]);
        }
    }

    return 0;
}
