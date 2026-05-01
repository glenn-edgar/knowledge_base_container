// robot_sim/main.c — Slice L4a (four-thread dongle decomposition).
// One process == one virtual dongle, same as before. The big change
// vs. earlier: a single watcher_thread is replaced by three
// infrastructure threads (ext_bus + dongle_manager + internal_bus)
// matching the embedded shape locked at Track C. Identity (type +
// instance) and the slave addr are still passed via argv (Linux
// mirror of Q3's NVS). The pty creation + READY publication is
// unchanged — the existing pty multi-dongle test pins on this output.
//
// Wire behavior is bit-for-bit unchanged from the prior single-thread
// stub:
//   addr=0xFE + cmd=DONGLE_HELLO      →  DONGLE_IDENT
//   addr=<slave_addr> + cmd=PING      →  ACK_BARE
//   anything else                     →  NAK reason 0xFF
// L4b will route non-handshake traffic through internal_bus into a
// drive_base logical_robot.
//
// Usage:
//   robot_sim --type <num> --instance <num> [--addr <num>]
//
// Stdout (line-buffered):  PTY=/dev/pts/N  then  READY

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "dongle_skeleton.h"
#include "comm.h"
#include "bus_kernel.h"
#include "drive_base_robot.h"
#include "logical_robot.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <unistd.h>

static dongle_ctx_t *g_ctx_for_signal;

static void on_signal(int sig)
{
    (void)sig;
    if (g_ctx_for_signal) g_ctx_for_signal->should_exit = 1;
}

// Hardcoded drive_base tunables for the Linux waypoint. Mirrors the
// rover_1 defaults in physics_config.json. On embedded these come
// from NVS via the Q3 schema-versioned blob.
static drive_base_tunables_t default_drive_base_tunables(void)
{
    drive_base_tunables_t t = {0};
    t.schema_version       = DRV_TUNABLES_SCHEMA_VERSION;
    t.wheelbase_m          = 0.30f;
    t.wheel_radius_m       = 0.04f;
    t.mass_kg              = 8.0f;
    t.inertia_kg_m2        = 0.12f;
    t.lin_friction         = 0.8f;
    t.ang_friction         = 0.4f;
    t.max_torque_nm        = 1.5f;
    t.max_wheel_rad_s      = 30.0f;
    t.pid_kp               = 8.0f;
    t.pid_ki               = 4.0f;
    t.pid_kd               = 0.05f;
    t.energy_k             = 1.0f;
    t.lookahead_min_m      = 0.20f;
    t.lookahead_k_v        = 0.40f;
    t.arrival_tol_m        = 0.05f;
    t.heading_tol_rad      = 0.05f;
    t.max_linear_accel     = 1.0f;
    t.max_angular_accel    = 3.0f;
    t.cross_track_abort_m  = 20.0f;
    t.inner_dt_s           = 0.005f;
    t.battery_capacity_j   = 100000.0f;
    t.battery_initial_j    = 100000.0f;
    t.seed                 = 0xC0FFEE42ULL;
    return t;
}

// Allocate a kernel pty pair, master FD owned by this process. Same
// as before; nothing in L4a changes how we set up the pty.
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

// Load tunables from a binary blob laid out as drive_base_tunables_t.
// Mirrors Q3's NVS read path; on embedded the same struct comes back
// from Zephyr settings. Returns 0 on success, -1 on file/size error,
// -2 on schema mismatch. On success, *out is populated.
static int load_tunables_blob(const char *path, drive_base_tunables_t *out)
{
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    size_t got = fread(out, 1, sizeof(*out), f);
    int eof_short = (got != sizeof(*out));
    fclose(f);
    if (eof_short) return -1;
    if (out->schema_version != DRV_TUNABLES_SCHEMA_VERSION) return -2;
    return 0;
}

int main(int argc, char **argv)
{
    long type     = -1;
    long instance = -1;
    long slave_addr = 1;
    const char *tunables_path = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
            type = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--instance") == 0 && i + 1 < argc) {
            instance = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--addr") == 0 && i + 1 < argc) {
            slave_addr = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--tunables") == 0 && i + 1 < argc) {
            tunables_path = argv[++i];
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

    // Per pthread_signal_routing memory: block SIGTERM/SIGINT in main
    // BEFORE pthread_create so workers inherit the block, then
    // selectively unblock in main while we wait. Otherwise pthread_join
    // can deadlock against blocked-in-read workers.
    sigset_t blockset;
    sigemptyset(&blockset);
    sigaddset(&blockset, SIGTERM);
    sigaddset(&blockset, SIGINT);
    pthread_sigmask(SIG_BLOCK, &blockset, NULL);

    static dongle_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.master_fd       = master_fd;
    ctx.dongle_type     = (uint16_t)type;
    ctx.dongle_instance = (uint16_t)instance;
    ctx.slave_addr      = (uint8_t)slave_addr;
    ctx.should_exit     = 0;

    bus_mutex_init(&ctx.pty_write_mu);
    bus_msgq_init (&ctx.mgr_in_q,  ctx.mgr_in_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);
    bus_msgq_init (&ctx.int_bus_q, ctx.int_bus_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);
    bus_msgq_init (&ctx.ext_tx_q,  ctx.ext_tx_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);

    // Configure drive_base instance (slot 0). Tunables come from a
    // binary blob (--tunables) when provided, mirroring the Q3 NVS
    // read path; otherwise fall back to compiled defaults so a bare
    // --type/--instance invocation still works for the wire-level
    // pty multi-dongle test.
    drive_base_tunables_t tun;
    if (tunables_path) {
        int rc = load_tunables_blob(tunables_path, &tun);
        if (rc == -1) {
            fprintf(stderr, "robot_sim: cannot read tunables blob '%s' "
                            "(or wrong size; expected %zu bytes)\n",
                    tunables_path, sizeof(tun));
            close(master_fd);
            return 1;
        }
        if (rc == -2) {
            fprintf(stderr, "robot_sim: tunables schema_version mismatch "
                            "(got %u, expected %u)\n",
                    (unsigned)tun.schema_version,
                    (unsigned)DRV_TUNABLES_SCHEMA_VERSION);
            close(master_fd);
            return 1;
        }
    } else {
        tun = default_drive_base_tunables();
    }
    ctx.drive_base.tun            = tun;
    ctx.drive_base.outbound       = &ctx.ext_tx_q;
    ctx.drive_base.tick_period_ms = drive_base_vtable.tick_period_ms;
    ctx.drive_base.bus_addr       = ctx.slave_addr;

    bus_result_t lr_rc = logical_robot_init(&ctx.drive_base_handle,
                                            "drvbase",
                                            &drive_base_vtable,
                                            &ctx.drive_base,
                                            ctx.drive_base_inbox_buf,
                                            DONGLE_QUEUE_DEPTH);
    if (lr_rc != BUS_OK) {
        fprintf(stderr, "robot_sim: drive_base init failed (%d)\n", (int)lr_rc);
        close(master_fd);
        return 1;
    }
    ctx.robots[0] = &ctx.drive_base_handle;

    g_ctx_for_signal = &ctx;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    bus_result_t r1 = bus_thread_start(&ctx.ext_bus_th,      "ext_bus",
                                        BUS_PRIO_HIGH,
                                        ext_bus_entry, &ctx);
    bus_result_t r2 = bus_thread_start(&ctx.manager_th,      "mgr",
                                        BUS_PRIO_MED,
                                        dongle_manager_entry, &ctx);
    bus_result_t r3 = bus_thread_start(&ctx.internal_bus_th, "int_bus",
                                        BUS_PRIO_MED,
                                        internal_bus_entry, &ctx);
    if (r1 != BUS_OK || r2 != BUS_OK || r3 != BUS_OK) {
        fprintf(stderr, "robot_sim: thread start failed (%d/%d/%d)\n",
                (int)r1, (int)r2, (int)r3);
        ctx.should_exit = 1;
        if (r1 == BUS_OK) bus_thread_join(&ctx.ext_bus_th,      UINT32_MAX);
        if (r2 == BUS_OK) bus_thread_join(&ctx.manager_th,      UINT32_MAX);
        if (r3 == BUS_OK) bus_thread_join(&ctx.internal_bus_th, UINT32_MAX);
        close(master_fd);
        return 1;
    }

    // READY is published AFTER the threads are up so the orchestrator
    // never opens the pty before robot_sim is actually listening.
    fprintf(stdout, "READY\n");
    fflush(stdout);

    // Main thread waits for shutdown. We poll should_exit at 100 ms
    // since we cannot block on threads while still wanting signals to
    // arrive — but on Linux we have already routed signals to the
    // main thread (others have SIGTERM/SIGINT blocked).
    sigset_t empty;
    sigemptyset(&empty);
    while (!ctx.should_exit) {
        // sigsuspend lets us wake on SIGTERM/SIGINT without busy
        // looping. After a signal arrives, on_signal sets
        // should_exit=1 and we fall through to the join.
        sigsuspend(&empty);
    }

    bus_thread_join(&ctx.ext_bus_th,      UINT32_MAX);
    bus_thread_join(&ctx.manager_th,      UINT32_MAX);
    bus_thread_join(&ctx.internal_bus_th, UINT32_MAX);

    // drive_base last — its thread can be blocked in bus_msgq_get
    // forever if no msgs flowed; logical_robot_shutdown posts a
    // sentinel and joins.
    logical_robot_shutdown(&ctx.drive_base_handle);

    close(master_fd);
    return 0;
}
