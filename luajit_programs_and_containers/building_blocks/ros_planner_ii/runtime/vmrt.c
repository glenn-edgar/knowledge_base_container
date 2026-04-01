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
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

/* =======================================================================
 * Socket channel
 * ======================================================================= */

int vmrt_socketpair(vmrt_socket_t *a, vmrt_socket_t *b)
{
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0)
        return -1;
    a->fd = fds[0];
    b->fd = fds[1];
    return 0;
}

/* Send all bytes, handling partial writes */
static int send_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, p + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += n;
    }
    return 0;
}

/* Recv all bytes, handling partial reads */
static int recv_all(int fd, void *buf, size_t len)
{
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, p + got, len - got);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -2;  /* EOF */
        got += n;
    }
    return 0;
}

int vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len)
{
    /* Length prefix */
    if (send_all(sock->fd, &len, sizeof(len)) < 0) {
        fprintf(stderr, "vmrt_socket_send: length prefix failed fd=%d errno=%d (%s)\n",
                sock->fd, errno, strerror(errno));
        return -1;
    }
    /* Payload */
    if (len > 0 && send_all(sock->fd, data, len) < 0) {
        fprintf(stderr, "vmrt_socket_send: payload failed fd=%d errno=%d (%s)\n",
                sock->fd, errno, strerror(errno));
        return -1;
    }
    return 0;
}

int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size)
{
    uint32_t msg_len;
    int rc = recv_all(sock->fd, &msg_len, sizeof(msg_len));
    if (rc == -2) return 0;   /* EOF */
    if (rc < 0) return -1;

    if (msg_len > buf_size) return -1;  /* too large */

    if (msg_len > 0) {
        rc = recv_all(sock->fd, buf, msg_len);
        if (rc < 0) return (rc == -2) ? 0 : -1;
    }
    return (int32_t)msg_len;
}

int32_t vmrt_socket_recv_nb(vmrt_socket_t *sock, void *buf, uint32_t buf_size)
{
    /* Set non-blocking temporarily */
    int flags = fcntl(sock->fd, F_GETFL, 0);
    fcntl(sock->fd, F_SETFL, flags | O_NONBLOCK);

    uint32_t msg_len;
    ssize_t n = read(sock->fd, &msg_len, sizeof(msg_len));

    /* Restore blocking */
    fcntl(sock->fd, F_SETFL, flags);

    if (n == 0) return 0;  /* EOF */
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    if (n < (ssize_t)sizeof(msg_len)) {
        /* Partial header read — read rest in blocking mode */
        int rc = recv_all(sock->fd, ((uint8_t*)&msg_len) + n, sizeof(msg_len) - n);
        if (rc < 0) return -1;
    }

    if (msg_len > buf_size) return -1;

    if (msg_len > 0) {
        int rc = recv_all(sock->fd, buf, msg_len);
        if (rc < 0) return -1;
    }
    return (int32_t)msg_len;
}

void vmrt_socket_close(vmrt_socket_t *sock)
{
    if (sock->fd >= 0) {
        close(sock->fd);
        sock->fd = -1;
    }
}

/* =======================================================================
 * Channel pair
 * ======================================================================= */

int vmrt_channel_pair_create(vmrt_channel_pair_t *cp)
{
    if (vmrt_socketpair(&cp->rpc_hub, &cp->rpc_remote) < 0)
        return -1;
    if (vmrt_socketpair(&cp->stream_remote, &cp->stream_hub) < 0) {
        vmrt_socket_close(&cp->rpc_hub);
        vmrt_socket_close(&cp->rpc_remote);
        return -1;
    }
    return 0;
}

void vmrt_channel_pair_close(vmrt_channel_pair_t *cp)
{
    vmrt_socket_close(&cp->rpc_hub);
    vmrt_socket_close(&cp->rpc_remote);
    vmrt_socket_close(&cp->stream_hub);
    vmrt_socket_close(&cp->stream_remote);
}

/* =======================================================================
 * Remote process
 * ======================================================================= */

int vmrt_remote_spawn(vmrt_remote_proc_t *proc, const char *script_path,
                      const char *lua_path, vmrt_channel_pair_t *cp)
{
    pid_t pid = fork();
    if (pid < 0) return -1;

    if (pid == 0) {
        /* Child process */

        /* Close hub's ends of the sockets */
        close(cp->rpc_hub.fd);
        close(cp->stream_hub.fd);

        /* Pass fds via environment */
        char rpc_fd_str[16], stream_fd_str[16];
        snprintf(rpc_fd_str, sizeof(rpc_fd_str), "%d", cp->rpc_remote.fd);
        snprintf(stream_fd_str, sizeof(stream_fd_str), "%d", cp->stream_remote.fd);
        setenv("VMRT_RPC_FD", rpc_fd_str, 1);
        setenv("VMRT_STREAM_FD", stream_fd_str, 1);

        if (lua_path && lua_path[0]) {
            setenv("LUA_PATH", lua_path, 1);
        }

        /* exec luajit with the script */
        execlp("luajit", "luajit", script_path, (char *)NULL);

        /* If exec fails */
        perror("vmrt_remote_spawn: execlp failed");
        _exit(127);
    }

    /* Parent */
    proc->pid = pid;
    proc->rpc_fd = cp->rpc_remote;
    proc->stream_fd = cp->stream_remote;
    proc->running = 1;
    snprintf(proc->script_path, sizeof(proc->script_path), "%s", script_path);
    if (lua_path) {
        snprintf(proc->lua_path, sizeof(proc->lua_path), "%s", lua_path);
    }

    /* Close remote's ends in the parent */
    close(cp->rpc_remote.fd);
    cp->rpc_remote.fd = -1;
    close(cp->stream_remote.fd);
    cp->stream_remote.fd = -1;

    return 0;
}

int vmrt_remote_wait(vmrt_remote_proc_t *proc)
{
    if (proc->pid <= 0) return -1;

    int status;
    pid_t rc;
    do {
        rc = waitpid(proc->pid, &status, 0);
    } while (rc < 0 && errno == EINTR);

    proc->running = 0;
    if (WIFEXITED(status)) {
        proc->exit_status = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        proc->exit_status = 128 + WTERMSIG(status);
    } else {
        proc->exit_status = -1;
    }
    proc->pid = 0;
    return proc->exit_status;
}

int vmrt_remote_stop(vmrt_remote_proc_t *proc)
{
    if (proc->pid > 0) {
        kill(proc->pid, SIGTERM);
    }
    return vmrt_remote_wait(proc);
}

/* =======================================================================
 * Hub thread
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

    /* Set LUA_PATH */
    if (hub->lua_path[0]) {
        lua_getglobal(L, "package");
        lua_pushstring(L, hub->lua_path);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);
    }

    /* Pass socket fds as globals */
    lua_pushinteger(L, hub->rpc_fd.fd);
    lua_setglobal(L, "_VMRT_RPC_FD");

    lua_pushinteger(L, hub->stream_fd.fd);
    lua_setglobal(L, "_VMRT_STREAM_FD");

    lua_pushlightuserdata(L, hub);
    lua_setglobal(L, "_VMRT_HUB");

    /* Run script */
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

int vmrt_hub_spawn(vmrt_hub_t *hub, const char *script_path,
                   const char *lua_path, vmrt_channel_pair_t *cp)
{
    memset(hub, 0, sizeof(*hub));
    hub->rpc_fd = cp->rpc_hub;
    hub->stream_fd = cp->stream_hub;
    hub->running = 1;
    snprintf(hub->script_path, sizeof(hub->script_path), "%s", script_path);
    if (lua_path) {
        snprintf(hub->lua_path, sizeof(hub->lua_path), "%s", lua_path);
    }

    int rc = pthread_create(&hub->thread, NULL, hub_thread_entry, hub);
    if (rc != 0) return -1;

    return 0;
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

/* =======================================================================
 * Cleanup
 * ======================================================================= */

void vmrt_cleanup(void)
{
    /* Reap any zombie children */
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;
}
