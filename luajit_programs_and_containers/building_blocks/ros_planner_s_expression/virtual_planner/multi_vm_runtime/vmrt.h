/*
 * vmrt.h -- Multi-VM runtime: SPSC ringbuffer pipes + thread launcher
 *
 * Provides:
 *   - Lock-free SPSC ringbuffer for message passing between VMs
 *   - Bidirectional pipe (two ringbuffers)
 *   - Thread launcher that creates a new LuaJIT VM per thread
 */

#ifndef VMRT_H
#define VMRT_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

/* -----------------------------------------------------------------------
 * SPSC Ringbuffer
 * ----------------------------------------------------------------------- */

#define VMRT_RINGBUF_DEFAULT_SIZE  (64 * 1024)  /* 64KB */

typedef struct {
    uint8_t  *buf;
    uint32_t  capacity;
    _Atomic uint32_t head;   /* written by producer only */
    _Atomic uint32_t tail;   /* written by consumer only */
} vmrt_ringbuf_t;

/* Create/destroy */
vmrt_ringbuf_t *vmrt_ringbuf_create(uint32_t capacity);
void            vmrt_ringbuf_destroy(vmrt_ringbuf_t *rb);

/* Write a length-prefixed message. Returns 0 on success, -1 if full. */
int vmrt_ringbuf_write(vmrt_ringbuf_t *rb, const void *data, uint32_t len);

/* Read a length-prefixed message. Returns message length, 0 if empty.
 * Caller provides buffer; if buf is NULL, peeks at length only.
 * If buf_size < message length, returns -1 (message too large). */
int32_t vmrt_ringbuf_read(vmrt_ringbuf_t *rb, void *buf, uint32_t buf_size);

/* Bytes available to read */
uint32_t vmrt_ringbuf_available(const vmrt_ringbuf_t *rb);

/* -----------------------------------------------------------------------
 * Bidirectional Pipe
 * ----------------------------------------------------------------------- */

typedef struct {
    vmrt_ringbuf_t *to_hub;     /* main → hub */
    vmrt_ringbuf_t *from_hub;   /* hub → main */
} vmrt_pipe_t;

vmrt_pipe_t *vmrt_pipe_create(uint32_t capacity);
void         vmrt_pipe_destroy(vmrt_pipe_t *pipe);

/* -----------------------------------------------------------------------
 * Hub Thread
 * ----------------------------------------------------------------------- */

typedef struct {
    vmrt_pipe_t *pipe;
    char         script_path[512];
    char         lua_path[2048];
    volatile int running;       /* set to 0 to request shutdown */
    volatile int ready;         /* set to 1 by hub when initialized */
    volatile int exited;        /* set to 1 by hub when thread exits */
    int          exit_code;     /* 0 = success */
    char         error_msg[256];
    pthread_t    thread;
} vmrt_hub_t;

/* Spawn a hub thread. script_path is the Lua entry point.
 * lua_path is the LUA_PATH for the child VM.
 * Returns hub handle, or NULL on failure. */
vmrt_hub_t *vmrt_hub_spawn(const char *script_path, const char *lua_path,
                           vmrt_pipe_t *pipe);

/* Wait for hub thread to exit. Returns exit_code. */
int vmrt_hub_join(vmrt_hub_t *hub);

/* Request shutdown and join. */
int vmrt_hub_stop(vmrt_hub_t *hub);

/* Free hub handle (must be joined first). */
void vmrt_hub_destroy(vmrt_hub_t *hub);

#endif /* VMRT_H */
