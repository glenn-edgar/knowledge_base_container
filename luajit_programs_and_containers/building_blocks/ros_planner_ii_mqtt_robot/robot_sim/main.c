// robot_sim/main.c — Phase B stub (one process == one virtual dongle).
//
// Phase B re-architecture: robot_sim CREATES the pty (each process is a
// virtual dongle, mirroring how a real USB dongle plugs in and creates
// /dev/ttyUSBn). Identity = (dongle_type, dongle_instance), passed in
// via argv to mimic what the TBD dongle-commissioning tool will store
// in non-volatile dongle storage. The slave path is published on stdout
// for the orchestrator / test harness to capture and put into
// dongles.json before chain_tree_host is started.
//
// Wire behavior:
//   - addr=0xFE + cmd=DONGLE_HELLO  →  reply DONGLE_IDENT carrying our
//     (type, instance) packed into uuid[0..3], remainder zero.
//   - addr=<our slave addr> + cmd=PING  →  ACK_BARE.
//   - any other cmd                  →  NAK reason 0xFF.
// The PING/NAK behavior is the same Phase A stub policy; Phase B keeps
// it so we can reuse the master-side test patterns. Real slave-class
// dispatch + physics_core integration land later inside this same
// process.
//
// Lifecycle:
//   - Created pty's master FD is held inside this process. The slave
//     end is what we reply on; chain_tree_host opens a copy of the slave
//     end via the published path. So technically the chain_tree side is
//     opening the OTHER end of the pty pair. (Yes — robot_sim is the
//     "controller" of this pty; chain_tree is the "device" from the
//     pty's POV. Naming is just historical; the byte stream is
//     symmetric.)
//   - On EIO/EOF (chain_tree closes its end) or SIGTERM, watcher exits
//     cleanly.
//
// Usage:
//   robot_sim --type <num> --instance <num>
// Optional:
//   --addr <num>      slave bus address to respond to (default 1)
//
// Stdout (line-buffered):
//   PTY=/dev/pts/N
//   READY
// stderr is reserved for diagnostics.

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "frame.h"
#include "comm.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <unistd.h>

static volatile int g_should_exit = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_should_exit = 1;
}

typedef struct {
    int      fd;
    uint16_t dongle_type;
    uint16_t dongle_instance;
    uint8_t  slave_addr;
} watcher_ctx_t;

// Encode-and-write one s2m frame to the pty. Atomic at the frame level.
static int emit_s2m(int fd,
                    uint8_t addr,
                    comm_cmd_t cmd,
                    uint8_t in_seq,
                    uint8_t ack_status,
                    const uint8_t *payload,
                    uint8_t payload_len)
{
    // frame_ring_init contract: size must be power-of-2. Worst-case
    // SLIP-escaped frame is roughly 2*FRAME_BUFFER_MAX + 3 bytes; round
    // up to the next power-of-2 (512 covers all m2s and s2m frames).
    uint8_t      ring_buf[512];
    frame_ring_t ring;
    frame_ring_init(&ring, ring_buf, sizeof(ring_buf));

    frame_meta_t meta;
    memset(&meta, 0, sizeof(meta));
    meta.addr        = addr;
    meta.cmd         = cmd;
    meta.seq         = 0;
    meta.ack_seq     = in_seq;
    meta.ack_status  = ack_status;
    meta.payload_len = payload_len;

    if (frame_encode_s2m(&meta, payload, &ring) != 0) return -1;

    uint8_t  scratch[FRAME_BUFFER_MAX * 2];
    uint32_t n = frame_ring_read_drain(&ring, scratch, sizeof(scratch));
    if (getenv("ROBOT_SIM_TRACE")) {
        fprintf(stderr, "[robot_sim] writing %u bytes:", n);
        for (uint32_t i = 0; i < n; i++) fprintf(stderr, " %02x", scratch[i]);
        fprintf(stderr, "\n");
    }
    size_t   off = 0;
    while (off < n) {
        ssize_t w = write(fd, scratch + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)w;
    }
    return 0;
}

// IDENT payload format (Phase B): 33 bytes total.
//   uuid[16]                   bytes 0-1 = type LE, 2-3 = instance LE, rest zero
//   fw_ver  u32                fixed at 0x00010000 for Phase B
//   bus_count u8               1
//   bus_local_ids[8]           [0]=0, rest 0
//   capabilities u32           0
// Mirrors the wire spec in the locked design (project memory) so the
// master side can pin against it once it stops being a stub on this end.
static void emit_ident(int fd, watcher_ctx_t *ctx, uint8_t in_seq)
{
    uint8_t payload[33];
    memset(payload, 0, sizeof(payload));
    comm_dongle_set_type    (payload + 0, ctx->dongle_type);
    comm_dongle_set_instance(payload + 0, ctx->dongle_instance);
    // fw_ver = 0x00010000 little-endian
    payload[16] = 0x00; payload[17] = 0x00;
    payload[18] = 0x01; payload[19] = 0x00;
    payload[20] = 1;             // bus_count
    payload[21] = 0;             // bus_local_ids[0]
    // capabilities at [29..32] left zero.
    (void)emit_s2m(fd, COMM_ADDR_DONGLE_SELF,
                   COMM_CMD_DONGLE_IDENT, in_seq, 0,
                   payload, sizeof(payload));
}

static void respond(watcher_ctx_t *ctx, const frame_meta_t *in)
{
    // Dongle-self handshake at addr=0xFE.
    if (in->addr == COMM_ADDR_DONGLE_SELF) {
        if (in->cmd == COMM_CMD_DONGLE_HELLO) {
            emit_ident(ctx->fd, ctx, in->seq);
            return;
        }
        // Other cmds at 0xFE not understood by Phase B stub.
        uint8_t reason = COMM_NAK_REASON_UNKNOWN_CMD;
        (void)emit_s2m(ctx->fd, COMM_ADDR_DONGLE_SELF,
                       COMM_CMD_NAK, in->seq, 0, &reason, 1);
        return;
    }

    // Slave-bus traffic addressed to our slave addr.
    if (in->addr == ctx->slave_addr) {
        if (in->cmd == COMM_CMD_PING) {
            (void)emit_s2m(ctx->fd, ctx->slave_addr,
                           COMM_CMD_ACK_BARE, in->seq, 0, NULL, 0);
        } else {
            uint8_t reason = COMM_NAK_REASON_UNKNOWN_CMD;
            (void)emit_s2m(ctx->fd, ctx->slave_addr,
                           COMM_CMD_NAK, in->seq, 0, &reason, 1);
        }
        return;
    }

    // Frame for some other address — not our concern (multidrop bus).
    // Phase B will route this to the appropriate virtual MCU; the stub
    // just drops it.
}

static void *watcher_thread(void *arg)
{
    watcher_ctx_t *ctx = (watcher_ctx_t *)arg;

    sigset_t unblockset;
    sigemptyset(&unblockset);
    sigaddset(&unblockset, SIGTERM);
    sigaddset(&unblockset, SIGINT);
    pthread_sigmask(SIG_UNBLOCK, &unblockset, NULL);

    frame_decoder_t dec;
    frame_decoder_init(&dec, FRAME_DIR_M2S);

    uint8_t      buf[256];
    frame_meta_t fm;
    uint8_t      fp[COMM_PAYLOAD_MAX];

    int trace = getenv("ROBOT_SIM_TRACE") ? 1 : 0;
    while (!g_should_exit) {
        ssize_t r = read(ctx->fd, buf, sizeof(buf));
        if (r < 0) {
            if (errno == EINTR) {
                if (g_should_exit) break;
                continue;
            }
            if (errno != EIO) {
                fprintf(stderr, "robot_sim: pty read errno=%d (%s)\n",
                        errno, strerror(errno));
            }
            break;
        }
        if (r == 0) break;
        if (trace) {
            fprintf(stderr, "[robot_sim] read %ld bytes:", (long)r);
            for (ssize_t i = 0; i < r; i++) fprintf(stderr, " %02x", buf[i]);
            fprintf(stderr, "\n");
        }
        for (ssize_t i = 0; i < r; i++) {
            frame_decode_result_t dr = frame_decoder_feed(&dec, buf[i], &fm, fp);
            if (dr == FRAME_DECODE_FRAME_READY) {
                if (trace) {
                    fprintf(stderr, "[robot_sim] frame addr=%02x cmd=%04x seq=%02x len=%u\n",
                            fm.addr, fm.cmd, fm.seq, fm.payload_len);
                }
                respond(ctx, &fm);
            } else if (dr != FRAME_DECODE_NEED_MORE && trace) {
                fprintf(stderr, "[robot_sim] decode error %d on byte %02x\n",
                        (int)dr, buf[i]);
            }
        }
    }
    g_should_exit = 1;
    return NULL;
}

// Allocate a kernel pty pair, master FD owned by this process.
// Returns master_fd (>=0) on success, or -1 on failure with errno set.
// The slave path is written to slave_path_out (NUL-terminated).
static int create_pty(char *slave_path_out, size_t slave_path_max)
{
    int fd = posix_openpt(O_RDWR | O_NOCTTY);
    if (fd < 0) return -1;
    if (grantpt(fd)  != 0) { int e = errno; close(fd); errno = e; return -1; }
    if (unlockpt(fd) != 0) { int e = errno; close(fd); errno = e; return -1; }

    char name[128];
    if (ptsname_r(fd, name, sizeof(name)) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }

    struct termios tio;
    if (tcgetattr(fd, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }
    cfmakeraw(&tio);
    if (tcsetattr(fd, TCSANOW, &tio) != 0) {
        int e = errno; close(fd); errno = e; return -1;
    }

    size_t n = strlen(name);
    if (n >= slave_path_max) n = slave_path_max - 1;
    memcpy(slave_path_out, name, n);
    slave_path_out[n] = '\0';
    return fd;
}

int main(int argc, char **argv)
{
    long type     = -1;
    long instance = -1;
    long slave_addr = 1;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
            type = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--instance") == 0 && i + 1 < argc) {
            instance = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--addr") == 0 && i + 1 < argc) {
            slave_addr = strtol(argv[++i], NULL, 10);
        } else {
            fprintf(stderr, "robot_sim: unknown arg: %s\n", argv[i]);
            return 2;
        }
    }
    if (type < 1 || type > 0xFFFF || instance < 1 || instance > 0xFFFF) {
        fprintf(stderr, "robot_sim: --type and --instance are required (1..65535)\n");
        return 2;
    }
    if (slave_addr < 1 || slave_addr > 0xFC) {
        fprintf(stderr, "robot_sim: --addr out of range (1..0xFC)\n");
        return 2;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    char pty_path[128];
    int master_fd = create_pty(pty_path, sizeof(pty_path));
    if (master_fd < 0) {
        fprintf(stderr, "robot_sim: create_pty failed errno=%d (%s)\n",
                errno, strerror(errno));
        return 1;
    }

    setvbuf(stdout, NULL, _IOLBF, 0);
    fprintf(stdout, "PTY=%s\n", pty_path);
    fflush(stdout);

    // Route async signals to the watcher thread, not the main thread.
    sigset_t blockset;
    sigemptyset(&blockset);
    sigaddset(&blockset, SIGTERM);
    sigaddset(&blockset, SIGINT);
    pthread_sigmask(SIG_BLOCK, &blockset, NULL);

    watcher_ctx_t ctx = {
        .fd              = master_fd,
        .dongle_type     = (uint16_t)type,
        .dongle_instance = (uint16_t)instance,
        .slave_addr      = (uint8_t)slave_addr,
    };

    pthread_t th;
    if (pthread_create(&th, NULL, watcher_thread, &ctx) != 0) {
        fprintf(stderr, "robot_sim: pthread_create failed\n");
        close(master_fd);
        return 1;
    }

    // READY is published AFTER the watcher thread is up so the
    // orchestrator never opens the pty before robot_sim is actually
    // listening on it.
    fprintf(stdout, "READY\n");
    fflush(stdout);

    pthread_join(th, NULL);
    close(master_fd);
    return 0;
}
