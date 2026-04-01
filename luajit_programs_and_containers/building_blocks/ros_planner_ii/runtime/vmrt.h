/*
 * vmrt.h -- Multi-VM runtime: socket IPC, process/thread launcher
 *
 * Provides:
 *   - Length-prefixed message send/recv over unix domain sockets
 *   - Bidirectional channel pair (RPC + stream)
 *   - Process launcher for remote robot (with zombie cleanup)
 *   - Thread launcher for hub (pthread + new LuaJIT VM)
 */

#ifndef VMRT_H
#define VMRT_H

#include <stdint.h>
#include <pthread.h>
#include <sys/types.h>

/* -----------------------------------------------------------------------
 * Socket channel: length-prefixed messages over unix socketpair
 * ----------------------------------------------------------------------- */

typedef struct {
    int fd;       /* socket file descriptor */
} vmrt_socket_t;

/* Create a connected socketpair. Returns 0 on success, -1 on failure.
 * a->fd and b->fd are the two ends. */
int vmrt_socketpair(vmrt_socket_t *a, vmrt_socket_t *b);

/* Send a length-prefixed message. Returns 0 on success, -1 on failure. */
int vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len);

/* Receive a length-prefixed message. Returns message length, 0 on EOF,
 * -1 on error. Caller provides buffer. */
int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size);

/* Non-blocking receive. Returns message length, 0 if nothing available,
 * -1 on error or EOF. */
int32_t vmrt_socket_recv_nb(vmrt_socket_t *sock, void *buf, uint32_t buf_size);

/* Close socket. */
void vmrt_socket_close(vmrt_socket_t *sock);

/* -----------------------------------------------------------------------
 * Channel pair: RPC (hub→remote) + Stream (remote→hub)
 * ----------------------------------------------------------------------- */

typedef struct {
    vmrt_socket_t rpc_hub;       /* hub writes RPC commands here */
    vmrt_socket_t rpc_remote;    /* remote reads RPC commands here */
    vmrt_socket_t stream_hub;    /* hub reads stream data here */
    vmrt_socket_t stream_remote; /* remote writes stream data here */
} vmrt_channel_pair_t;

/* Create both socketpairs. Returns 0 on success. */
int vmrt_channel_pair_create(vmrt_channel_pair_t *cp);

/* Close all sockets. */
void vmrt_channel_pair_close(vmrt_channel_pair_t *cp);

/* -----------------------------------------------------------------------
 * Remote process (fork+exec LuaJIT)
 * ----------------------------------------------------------------------- */

typedef struct {
    pid_t         pid;
    vmrt_socket_t rpc_fd;        /* remote's RPC read end */
    vmrt_socket_t stream_fd;     /* remote's stream write end */
    char          script_path[512];
    char          lua_path[2048];
    volatile int  running;
    int           exit_status;
} vmrt_remote_proc_t;

/* Spawn remote as a child process running the given Lua script.
 * Passes RPC and stream socket fds via environment variables.
 * Returns 0 on success, -1 on failure. */
int vmrt_remote_spawn(vmrt_remote_proc_t *proc, const char *script_path,
                      const char *lua_path, vmrt_channel_pair_t *cp);

/* Wait for remote process to exit. Returns exit status.
 * Prevents zombies. */
int vmrt_remote_wait(vmrt_remote_proc_t *proc);

/* Send SIGTERM and wait. Returns exit status. */
int vmrt_remote_stop(vmrt_remote_proc_t *proc);

/* -----------------------------------------------------------------------
 * Hub thread (pthread + new LuaJIT VM)
 * ----------------------------------------------------------------------- */

typedef struct {
    vmrt_socket_t rpc_fd;        /* hub's RPC write end */
    vmrt_socket_t stream_fd;     /* hub's stream read end */
    char          script_path[512];
    char          lua_path[2048];
    volatile int  running;
    volatile int  ready;
    volatile int  exited;
    int           exit_code;
    char          error_msg[256];
    pthread_t     thread;
} vmrt_hub_t;

/* Spawn hub thread. Returns 0 on success, -1 on failure. */
int vmrt_hub_spawn(vmrt_hub_t *hub, const char *script_path,
                   const char *lua_path, vmrt_channel_pair_t *cp);

/* Wait for hub thread to exit. Returns exit_code. */
int vmrt_hub_join(vmrt_hub_t *hub);

/* Request shutdown and join. */
int vmrt_hub_stop(vmrt_hub_t *hub);

/* -----------------------------------------------------------------------
 * Cleanup: call at program exit to reap any children
 * ----------------------------------------------------------------------- */
void vmrt_cleanup(void);

#endif /* VMRT_H */
