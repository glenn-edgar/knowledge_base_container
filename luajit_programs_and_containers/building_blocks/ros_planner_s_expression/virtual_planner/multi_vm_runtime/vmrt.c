/*
 * vmrt.c -- Multi-VM runtime implementation
 */

#include "vmrt.h"
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =======================================================================
 * SPSC Ringbuffer
 * ======================================================================= */

vmrt_ringbuf_t *vmrt_ringbuf_create(uint32_t capacity)
{
    if (capacity == 0) capacity = VMRT_RINGBUF_DEFAULT_SIZE;

    vmrt_ringbuf_t *rb = calloc(1, sizeof(*rb));
    if (!rb) return NULL;

    rb->buf = calloc(1, capacity);
    if (!rb->buf) { free(rb); return NULL; }

    rb->capacity = capacity;
    atomic_store(&rb->head, 0);
    atomic_store(&rb->tail, 0);
    return rb;
}

void vmrt_ringbuf_destroy(vmrt_ringbuf_t *rb)
{
    if (!rb) return;
    free(rb->buf);
    free(rb);
}

/*
 * Message format in ringbuffer:
 *   [uint32_t length][length bytes of data]
 *
 * Wraps around the buffer boundary. Head/tail are monotonically increasing
 * and masked to capacity for actual position.
 */

static inline uint32_t rb_mask(const vmrt_ringbuf_t *rb, uint32_t pos)
{
    return pos % rb->capacity;
}

static inline uint32_t rb_used(const vmrt_ringbuf_t *rb)
{
    return atomic_load_explicit(&rb->head, memory_order_acquire)
         - atomic_load_explicit(&rb->tail, memory_order_acquire);
}

static inline uint32_t rb_free(const vmrt_ringbuf_t *rb)
{
    return rb->capacity - rb_used(rb);
}

/* Write bytes wrapping around the ring */
static void rb_write_bytes(vmrt_ringbuf_t *rb, uint32_t pos,
                           const void *data, uint32_t len)
{
    uint32_t off = rb_mask(rb, pos);
    uint32_t first = rb->capacity - off;
    if (first >= len) {
        memcpy(rb->buf + off, data, len);
    } else {
        memcpy(rb->buf + off, data, first);
        memcpy(rb->buf, (const uint8_t *)data + first, len - first);
    }
}

/* Read bytes wrapping around the ring */
static void rb_read_bytes(const vmrt_ringbuf_t *rb, uint32_t pos,
                          void *data, uint32_t len)
{
    uint32_t off = rb_mask(rb, pos);
    uint32_t first = rb->capacity - off;
    if (first >= len) {
        memcpy(data, rb->buf + off, len);
    } else {
        memcpy(data, rb->buf + off, first);
        memcpy((uint8_t *)data + first, rb->buf, len - first);
    }
}

int vmrt_ringbuf_write(vmrt_ringbuf_t *rb, const void *data, uint32_t len)
{
    uint32_t needed = sizeof(uint32_t) + len;
    if (rb_free(rb) < needed) return -1;

    uint32_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);

    /* Write length prefix */
    rb_write_bytes(rb, head, &len, sizeof(uint32_t));
    head += sizeof(uint32_t);

    /* Write payload */
    if (len > 0) {
        rb_write_bytes(rb, head, data, len);
        head += len;
    }

    atomic_store_explicit(&rb->head, head, memory_order_release);
    return 0;
}

int32_t vmrt_ringbuf_read(vmrt_ringbuf_t *rb, void *buf, uint32_t buf_size)
{
    uint32_t avail = rb_used(rb);
    if (avail < sizeof(uint32_t)) return 0;  /* empty */

    uint32_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);

    /* Read length prefix */
    uint32_t msg_len;
    rb_read_bytes(rb, tail, &msg_len, sizeof(uint32_t));

    if (buf == NULL) return (int32_t)msg_len;  /* peek */

    if (buf_size < msg_len) return -1;  /* caller buffer too small */

    tail += sizeof(uint32_t);
    rb_read_bytes(rb, tail, buf, msg_len);
    tail += msg_len;

    atomic_store_explicit(&rb->tail, tail, memory_order_release);
    return (int32_t)msg_len;
}

uint32_t vmrt_ringbuf_available(const vmrt_ringbuf_t *rb)
{
    return rb_used(rb);
}

/* =======================================================================
 * Bidirectional Pipe
 * ======================================================================= */

vmrt_pipe_t *vmrt_pipe_create(uint32_t capacity)
{
    vmrt_pipe_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->to_hub   = vmrt_ringbuf_create(capacity);
    p->from_hub = vmrt_ringbuf_create(capacity);

    if (!p->to_hub || !p->from_hub) {
        vmrt_pipe_destroy(p);
        return NULL;
    }
    return p;
}

void vmrt_pipe_destroy(vmrt_pipe_t *pipe)
{
    if (!pipe) return;
    vmrt_ringbuf_destroy(pipe->to_hub);
    vmrt_ringbuf_destroy(pipe->from_hub);
    free(pipe);
}

/* =======================================================================
 * Hub Thread
 * ======================================================================= */

static void *hub_thread_entry(void *arg)
{
    vmrt_hub_t *hub = (vmrt_hub_t *)arg;

    lua_State *L = luaL_newstate();
    if (!L) {
        snprintf(hub->error_msg, sizeof(hub->error_msg), "failed to create Lua state");
        hub->exit_code = 1;
        hub->exited = 1;
        return NULL;
    }
    luaL_openlibs(L);

    /* Set LUA_PATH in the child VM */
    if (hub->lua_path[0]) {
        lua_getglobal(L, "package");
        lua_pushstring(L, hub->lua_path);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);
    }

    /* Pass pipe pointer and hub pointer as globals */
    lua_pushlightuserdata(L, hub->pipe);
    lua_setglobal(L, "_VMRT_PIPE");

    lua_pushlightuserdata(L, hub);
    lua_setglobal(L, "_VMRT_HUB");

    /* Run the hub script */
    int err = luaL_dofile(L, hub->script_path);
    if (err) {
        const char *msg = lua_tostring(L, -1);
        if (msg) {
            snprintf(hub->error_msg, sizeof(hub->error_msg), "%.255s", msg);
        }
        hub->exit_code = 1;
    }

    lua_close(L);
    hub->exited = 1;
    return NULL;
}

vmrt_hub_t *vmrt_hub_spawn(const char *script_path, const char *lua_path,
                           vmrt_pipe_t *pipe)
{
    vmrt_hub_t *hub = calloc(1, sizeof(*hub));
    if (!hub) return NULL;

    hub->pipe = pipe;
    hub->running = 1;
    snprintf(hub->script_path, sizeof(hub->script_path), "%s", script_path);
    if (lua_path) {
        snprintf(hub->lua_path, sizeof(hub->lua_path), "%s", lua_path);
    }

    int rc = pthread_create(&hub->thread, NULL, hub_thread_entry, hub);
    if (rc != 0) {
        free(hub);
        return NULL;
    }

    return hub;
}

int vmrt_hub_join(vmrt_hub_t *hub)
{
    pthread_join(hub->thread, NULL);
    return hub->exit_code;
}

int vmrt_hub_stop(vmrt_hub_t *hub)
{
    hub->running = 0;
    return vmrt_hub_join(hub);
}

void vmrt_hub_destroy(vmrt_hub_t *hub)
{
    if (!hub) return;
    free(hub);
}
