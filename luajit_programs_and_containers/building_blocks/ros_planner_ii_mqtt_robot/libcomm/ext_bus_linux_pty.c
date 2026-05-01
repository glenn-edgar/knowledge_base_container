// libcomm/ext_bus_linux_pty.c
// Linux pty/tty backend for ext_bus.h. Replaces the FD-I/O half of the
// old transport_uart.c — ring staging stays one level up in
// transport_uart.c. This file is the per-silicon driver for the
// "Linux waypoint" of continue.md's three-Zephyr-target lineup.
//
// Replaced by per-silicon files on embedded targets:
//   ext_bus_rp2040_uart_dma.c      (RP2040 / Pico)
//   ext_bus_rp2350_uart_dma.c      (RP2350 / Pico 2)
//   ext_bus_mgm240_uart_dma.c      (XIAO Silabs)
// All four implement the same ext_bus.h surface; nothing above this
// line changes.

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "ext_bus.h"
#include "bus_config.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <termios.h>
#include <unistd.h>

typedef struct {
    int   fd;
    int   eof;       // sticky EOF latch — set on read()==0
    char  path[TRANSPORT_UART_PATH_MAX];
} pty_bus_t;

#define BK_STATIC_ASSERT(cond, msg) typedef char eb_assert_##msg[(cond) ? 1 : -1]
BK_STATIC_ASSERT(sizeof(pty_bus_t) <= EXT_BUS_STORAGE_BYTES, pty_bus_fits);

int ext_bus_open_pty(ext_bus_t *opaque, const char *path)
{
    if (!opaque || !path) { errno = EINVAL; return -1; }
    pty_bus_t *b = (pty_bus_t *)opaque;
    memset(b, 0, sizeof(*b));
    b->fd = -1;

    int fd = open(path, O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) return -1;

    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        int e = errno; close(fd); errno = e; return -2;
    }

    struct termios tio;
    if (tcgetattr(fd, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }
    cfmakeraw(&tio);
    if (tcsetattr(fd, TCSANOW, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }

    b->fd = fd;
    size_t n = strlen(path);
    if (n >= sizeof(b->path)) n = sizeof(b->path) - 1;
    memcpy(b->path, path, n);
    b->path[n] = '\0';
    return 0;
}

void ext_bus_close(ext_bus_t *opaque)
{
    if (!opaque) return;
    pty_bus_t *b = (pty_bus_t *)opaque;
    if (b->fd >= 0) {
        close(b->fd);             // releases flock implicitly
        b->fd = -1;
    }
}

size_t ext_bus_tx(ext_bus_t *opaque, const uint8_t *bytes, size_t n)
{
    if (!opaque || !bytes || n == 0) return 0;
    pty_bus_t *b = (pty_bus_t *)opaque;
    if (b->fd < 0) return 0;

    size_t off = 0;
    while (off < n) {
        ssize_t w = write(b->fd, bytes + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return off;           // unrecoverable; caller sees short count
        }
        off += (size_t)w;
    }
    return off;
}

size_t ext_bus_rx(ext_bus_t *opaque, uint8_t *out, size_t cap)
{
    if (!opaque || !out || cap == 0) return 0;
    pty_bus_t *b = (pty_bus_t *)opaque;
    if (b->fd < 0 || b->eof) return 0;

    size_t total = 0;
    while (total < cap) {
        ssize_t r = read(b->fd, out + total, cap - total);
        if (r < 0) {
            if (errno == EINTR)  continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return total;
        }
        if (r == 0) { b->eof = 1; break; }
        total += (size_t)r;
    }
    return total;
}

int ext_bus_rx_wait(ext_bus_t *opaque, uint32_t timeout_ms)
{
    if (!opaque) return -3;
    pty_bus_t *b = (pty_bus_t *)opaque;
    if (b->fd < 0)  return -3;
    if (b->eof)     return -2;

    struct pollfd pfd = { .fd = b->fd, .events = POLLIN };
    int p;
    do {
        p = poll(&pfd, 1, (int)timeout_ms);
    } while (p < 0 && errno == EINTR);
    if (p < 0)  return -3;
    if (p == 0) return -1;
    if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
        // POLLIN may also be set with HUP; let caller drain remaining
        // bytes via ext_bus_rx. The next ext_bus_rx that hits read()==0
        // will latch eof.
        if (!(pfd.revents & POLLIN)) return -2;
    }
    return 0;
}

const char *ext_bus_label(const ext_bus_t *opaque)
{
    if (!opaque) return NULL;
    const pty_bus_t *b = (const pty_bus_t *)opaque;
    return (b->fd >= 0) ? b->path : NULL;
}
