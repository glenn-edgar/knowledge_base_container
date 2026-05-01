// libcomm/test_ext_bus_contract.c
// Boundary regression gate for ext_bus.h — the per-silicon driver
// contract. Passes the same byte sequence through ext_bus_tx → wire →
// ext_bus_rx and verifies byte-for-byte fidelity. Intentionally has NO
// dependency on frame.c, comm.c, or the link layer: it's testing the
// boundary, not the protocol.
//
// On Linux the "wire" is a posix pty pair. The slave side is opened via
// ext_bus_open_pty (the production codepath); the master side is the FD
// returned by posix_openpt and is driven by a small echo thread (built
// on bus_kernel.h's primitives, doubling as integration coverage for
// that abstraction).
//
// On embedded targets the same source compiles unchanged. The "wire"
// becomes a TX→RX jumper between the two pins; the echo thread is
// replaced by a stub (#ifdef __ZEPHYR__ branch on the open path) since
// the loop closes electrically.
//
// What this test guarantees:
//   - ext_bus_open_pty + ext_bus_close are idempotent and balanced.
//   - ext_bus_tx accepts bytes and never drops in steady state.
//   - ext_bus_rx_wait honours its timeout (returns -1 on idle bus).
//   - ext_bus_rx returns exactly the bytes that crossed the wire.
//   - A 4 KB random payload survives a round trip with zero corruption.

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "ext_bus.h"
#include "bus_kernel.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

// ============ pty fixture ============
// posix_openpt + grantpt + unlockpt + ptsname_r — same shape robot_sim
// uses to create its slave end. The test owns the master fd; the echo
// thread loops master_read → master_write so the chain_tree side sees
// its own bytes coming back.

typedef struct {
    int          master_fd;
    char         slave_path[64];
} pty_pair_t;

static int pty_pair_create(pty_pair_t *p)
{
    p->master_fd = posix_openpt(O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (p->master_fd < 0) return -1;
    if (grantpt(p->master_fd) != 0)  { close(p->master_fd); return -1; }
    if (unlockpt(p->master_fd) != 0) { close(p->master_fd); return -1; }
    if (ptsname_r(p->master_fd, p->slave_path, sizeof(p->slave_path)) != 0) {
        close(p->master_fd); return -1;
    }
    return 0;
}

static void pty_pair_destroy(pty_pair_t *p)
{
    if (p->master_fd >= 0) { close(p->master_fd); p->master_fd = -1; }
}

// ============ echo thread ============
// Reads the pty master fd and writes whatever it reads back. Quits when
// `stop` becomes nonzero or the fd closes. Uses poll(2) — the bus
// kernel's bus_thread_sleep_ms isn't tight enough for a byte echo loop.

typedef struct {
    int               fd;
    volatile int      stop;
    volatile uint64_t bytes_echoed;
} echo_ctx_t;

static void echo_entry(void *arg)
{
    echo_ctx_t *e = (echo_ctx_t *)arg;
    uint8_t buf[256];
    while (!e->stop) {
        struct pollfd pfd = { .fd = e->fd, .events = POLLIN };
        int p = poll(&pfd, 1, 50);  // 50 ms wakeup so stop is honoured promptly
        if (p < 0) {
            if (errno == EINTR) continue;
            return;
        }
        if (p == 0) continue;
        if (pfd.revents & (POLLHUP | POLLERR)) return;
        if (!(pfd.revents & POLLIN)) continue;

        ssize_t r = read(e->fd, buf, sizeof(buf));
        if (r <= 0) {
            if (r < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            return;
        }
        ssize_t off = 0;
        while (off < r) {
            ssize_t w = write(e->fd, buf + off, (size_t)(r - off));
            if (w < 0) {
                if (errno == EAGAIN || errno == EINTR) { bus_thread_sleep_ms(1); continue; }
                return;
            }
            off += w;
        }
        e->bytes_echoed += (uint64_t)r;
    }
}

// ============ tests ============

static void test_open_close_idempotent(void)
{
    pty_pair_t pty;
    CHECK(pty_pair_create(&pty) == 0, "pty pair created");

    ext_bus_t bus;
    int rc = ext_bus_open_pty(&bus, pty.slave_path);
    CHECK(rc == 0, "ext_bus_open_pty ok");
    CHECK(ext_bus_label(&bus) != NULL && strcmp(ext_bus_label(&bus), pty.slave_path) == 0,
          "ext_bus_label matches path");

    ext_bus_close(&bus);
    CHECK(ext_bus_label(&bus) == NULL, "label NULL after close");

    ext_bus_close(&bus);   // second close: no-op, no crash
    CHECK(1, "ext_bus_close idempotent");

    pty_pair_destroy(&pty);
}

static void test_rx_wait_timeout(void)
{
    pty_pair_t pty;
    pty_pair_create(&pty);
    ext_bus_t bus;
    ext_bus_open_pty(&bus, pty.slave_path);

    uint32_t t0 = bus_now_ms();
    int rc = ext_bus_rx_wait(&bus, 50);
    uint32_t dt = bus_now_ms() - t0;

    CHECK(rc == -1, "rx_wait on idle bus → -1 (timeout)");
    CHECK(dt >= 40 && dt < 200, "rx_wait honoured 50ms timeout");

    ext_bus_close(&bus);
    pty_pair_destroy(&pty);
}

static void test_short_round_trip(void)
{
    pty_pair_t pty;
    pty_pair_create(&pty);
    ext_bus_t bus;
    ext_bus_open_pty(&bus, pty.slave_path);

    echo_ctx_t echo = { pty.master_fd, 0, 0 };
    bus_thread_t echo_th;
    bus_thread_start(&echo_th, "echo", BUS_PRIO_HIGH, echo_entry, &echo);

    const char *msg = "hello, ext_bus";
    size_t n = strlen(msg);
    size_t accepted = ext_bus_tx(&bus, (const uint8_t *)msg, n);
    CHECK(accepted == n, "tx accepted full short payload");

    // Wait for echo round-trip — generous 200 ms budget.
    int got_data = ext_bus_rx_wait(&bus, 200);
    CHECK(got_data == 0, "rx_wait → ready after round-trip");

    uint8_t out[64] = { 0 };
    size_t got = ext_bus_rx(&bus, out, sizeof(out));
    CHECK(got == n, "rx returned correct byte count");
    CHECK(memcmp(out, msg, n) == 0, "rx payload byte-for-byte equals tx");

    echo.stop = 1;
    bus_thread_join(&echo_th, 1000);
    ext_bus_close(&bus);
    pty_pair_destroy(&pty);
}

static void test_bulk_4k_round_trip(void)
{
    pty_pair_t pty;
    pty_pair_create(&pty);
    ext_bus_t bus;
    ext_bus_open_pty(&bus, pty.slave_path);

    echo_ctx_t echo = { pty.master_fd, 0, 0 };
    bus_thread_t echo_th;
    bus_thread_start(&echo_th, "echo", BUS_PRIO_HIGH, echo_entry, &echo);

    const size_t N = 4096;
    uint8_t *src = malloc(N);
    uint8_t *dst = malloc(N);
    // Deterministic PRNG so failures are reproducible.
    uint32_t s = 0xC0FFEE42u;
    for (size_t i = 0; i < N; i++) {
        s = s * 1664525u + 1013904223u;
        src[i] = (uint8_t)(s >> 16);
    }

    size_t tx_off = 0, rx_off = 0;
    uint32_t deadline = bus_now_ms() + 5000;
    while (rx_off < N) {
        if (tx_off < N) {
            // Bound chunks at 256 B so the kernel pty buffer doesn't fill.
            size_t want = N - tx_off;
            if (want > 256) want = 256;
            size_t got = ext_bus_tx(&bus, src + tx_off, want);
            tx_off += got;
        }
        int rc = ext_bus_rx_wait(&bus, 50);
        if (rc == 0) {
            size_t got = ext_bus_rx(&bus, dst + rx_off, N - rx_off);
            rx_off += got;
        }
        if (bus_now_ms() > deadline) break;
    }

    CHECK(tx_off == N,                  "tx pumped all 4096 bytes");
    CHECK(rx_off == N,                  "rx received all 4096 bytes");
    CHECK(memcmp(src, dst, N) == 0,     "4096-byte payload byte-for-byte equal");

    free(src);
    free(dst);
    echo.stop = 1;
    bus_thread_join(&echo_th, 1000);
    ext_bus_close(&bus);
    pty_pair_destroy(&pty);
}

int main(void)
{
    printf("[ext_bus contract]\n");
    test_open_close_idempotent();
    test_rx_wait_timeout();
    test_short_round_trip();
    test_bulk_4k_round_trip();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
