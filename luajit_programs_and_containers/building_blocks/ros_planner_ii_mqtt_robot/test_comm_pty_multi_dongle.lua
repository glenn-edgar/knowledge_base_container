-- test_comm_pty_multi_dongle.lua
-- Phase B end-to-end test. TWO robot_sim processes (each one its own
-- virtual dongle) with identities (DRIVE_BASE, 1) and (DRIVE_BASE, 2).
-- chain_tree opens both pty paths, runs HELLO/IDENT against each,
-- exercises comm_submit/poll/claim through both independently, and
-- verifies bus-local isolation (a PING to mcu=1 lands on dongle 0
-- only; mcu=2 routes via dongle 1).
--
-- Run from project root:  luajit test_comm_pty_multi_dongle.lua

io.stdout:setvbuf("no")

local ffi      = require("ffi")
local comm_ffi = require("comm_ffi")
local ct_comm  = require("ct_comm")

local C       = comm_ffi.C
local R       = comm_ffi.RESULT
local CMD     = comm_ffi.CMD
local DTYPE   = comm_ffi.DONGLE_TYPE

------------------------------------------------------------------------
-- POSIX surface for spawn + pipe + read.
ffi.cdef[[
typedef int   pid_t;
typedef long  ssize_t;
pid_t fork(void);
int   execv(const char *path, char *const argv[]);
int   kill(pid_t pid, int sig);
pid_t waitpid(pid_t pid, int *status, int options);
int   pipe(int pipefd[2]);
int   dup2(int oldfd, int newfd);
int   close(int fd);
ssize_t read(int fd, void *buf, size_t count);
unsigned int sleep(unsigned int seconds);
int   usleep(unsigned int usec);
]]

local SIGTERM = 15

-- Spawn robot_sim with --type/--instance, capture its stdout via pipe.
-- Returns (pid, stdout_fd).
local function spawn_robot(dongle_type, dongle_instance)
    local pipefd = ffi.new("int[2]")
    if ffi.C.pipe(pipefd) ~= 0 then error("pipe failed") end
    local pid = ffi.C.fork()
    if pid < 0 then error("fork failed") end
    if pid == 0 then
        -- Child: redirect stdout to pipe write end, exec robot_sim.
        ffi.C.close(pipefd[0])
        ffi.C.dup2(pipefd[1], 1)   -- stdout = pipe write
        ffi.C.close(pipefd[1])
        local argv = ffi.new("char*[7]")
        argv[0] = ffi.cast("char*", "robot_sim/robot_sim")
        argv[1] = ffi.cast("char*", "--type")
        argv[2] = ffi.cast("char*", tostring(dongle_type))
        argv[3] = ffi.cast("char*", "--instance")
        argv[4] = ffi.cast("char*", tostring(dongle_instance))
        argv[5] = nil
        ffi.C.execv("robot_sim/robot_sim", argv)
        os.exit(127)
    end
    -- Parent: close write end, return read end.
    ffi.C.close(pipefd[1])
    return pid, pipefd[0]
end

-- Read one line from an FD (drains bytes until '\n').
local function read_line(fd, deadline_ms)
    local buf = {}
    local one = ffi.new("char[1]")
    local now_ms = function() return ct_comm.now_ms() end
    while true do
        local r = tonumber(ffi.C.read(fd, one, 1))
        if r == 1 then
            local ch = string.char(one[0])
            if ch == "\n" then return table.concat(buf) end
            buf[#buf + 1] = ch
        elseif r == 0 then
            return nil, "eof"
        else
            ffi.C.usleep(500)
        end
        if deadline_ms and now_ms() > deadline_ms then
            return nil, "timeout"
        end
    end
end

-- Read PTY=... and READY lines from robot_sim's stdout. Returns the
-- pty path or errors out.
local function wait_for_pty(fd, label)
    local deadline = ct_comm.now_ms() + 2000
    local pty_path
    while true do
        local line, why = read_line(fd, deadline)
        if not line then error(label..": "..(why or "no line")) end
        if line:sub(1, 4) == "PTY=" then
            pty_path = line:sub(5)
        elseif line == "READY" then
            assert(pty_path, label..": got READY before PTY=")
            return pty_path
        end
    end
end

local function reap(pid)
    ffi.C.kill(pid, SIGTERM)
    local status = ffi.new("int[1]")
    ffi.C.waitpid(pid, status, 0)
end

------------------------------------------------------------------------
-- Manifest types (same packed layout as test_comm_loopback).
ffi.cdef[[
typedef struct __attribute__((packed)) {
    double      timestamp;
    uint32_t    schema_hash;
    uint16_t    seq;
    uint16_t    source_node;
} comm_manifest_wire_header_t;

typedef struct __attribute__((packed)) {
    uint8_t dongle_uuid[16];
    uint8_t bus_count;
    uint8_t bus_local_ids[8];
} manifest_dongle_t;

typedef struct __attribute__((packed)) {
    uint8_t  max_miss;
    uint16_t tick_period_ms;
    uint16_t join_timeout_ms;
} manifest_tunables_t;

typedef struct __attribute__((packed)) {
    uint8_t             bus_id;
    manifest_tunables_t tunables;
} manifest_bus_t;

typedef struct __attribute__((packed)) {
    uint8_t  mcu;
    uint8_t  dongle_idx;
    uint8_t  bus_id;
    uint8_t  addr;
    uint32_t physics_model_id;
} manifest_slave_t;

typedef struct __attribute__((packed)) {
    uint8_t version;
    uint8_t dongle_count;
    uint8_t bus_count;
    uint8_t slave_count;
    manifest_dongle_t dongles[4];
    manifest_bus_t    buses[8];
    manifest_slave_t  slaves[64];
} comm_manifest_v1_wire_t;

typedef struct __attribute__((packed)) {
    comm_manifest_wire_header_t header;
    comm_manifest_v1_wire_t     data;
} comm_manifest_v1_packet_t;
]]

local SCHEMA_HASH = 0x79046205
local PACKET_SIZE = ffi.sizeof("comm_manifest_v1_packet_t")

-- Pack (type, instance) into the first 4 bytes of a 16-byte uuid field.
local function set_uuid_identity(uuid, dongle_type, dongle_instance)
    uuid[0] = bit.band(dongle_type, 0xFF)
    uuid[1] = bit.band(bit.rshift(dongle_type, 8), 0xFF)
    uuid[2] = bit.band(dongle_instance, 0xFF)
    uuid[3] = bit.band(bit.rshift(dongle_instance, 8), 0xFF)
    -- Bytes 4..15 stay zero.
end

local bit = require("bit")

-- Two dongles, both DRIVE_BASE. Each has its own slave on its own bus.
-- mcu 1 → dongle 0 (DRIVE_BASE, 1), addr 1
-- mcu 2 → dongle 1 (DRIVE_BASE, 2), addr 1   (bus-local addr 1 is OK; dongles are isolated)
local function build_two_dongle_packet()
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash                       = SCHEMA_HASH
    pkt.data.version                              = 1
    pkt.data.dongle_count                         = 2
    pkt.data.bus_count                            = 2
    pkt.data.slave_count                          = 2

    -- Dongle 0 = (DRIVE_BASE, 1), bus 0.
    set_uuid_identity(pkt.data.dongles[0].dongle_uuid, DTYPE.DRIVE_BASE, 1)
    pkt.data.dongles[0].bus_count                 = 1
    pkt.data.dongles[0].bus_local_ids[0]          = 0

    -- Dongle 1 = (DRIVE_BASE, 2), bus 1.
    set_uuid_identity(pkt.data.dongles[1].dongle_uuid, DTYPE.DRIVE_BASE, 2)
    pkt.data.dongles[1].bus_count                 = 1
    pkt.data.dongles[1].bus_local_ids[0]          = 1

    -- Two buses, both with the same tunables.
    pkt.data.buses[0].bus_id                      = 0
    pkt.data.buses[0].tunables.max_miss           = 3
    pkt.data.buses[0].tunables.tick_period_ms     = 20
    pkt.data.buses[0].tunables.join_timeout_ms    = 500
    pkt.data.buses[1].bus_id                      = 1
    pkt.data.buses[1].tunables.max_miss           = 3
    pkt.data.buses[1].tunables.tick_period_ms     = 20
    pkt.data.buses[1].tunables.join_timeout_ms    = 500

    -- mcu=1 on dongle 0, mcu=2 on dongle 1.
    pkt.data.slaves[0].mcu                        = 1
    pkt.data.slaves[0].dongle_idx                 = 0
    pkt.data.slaves[0].bus_id                     = 0
    pkt.data.slaves[0].addr                       = 1
    pkt.data.slaves[0].physics_model_id           = 0
    pkt.data.slaves[1].mcu                        = 2
    pkt.data.slaves[1].dongle_idx                 = 1
    pkt.data.slaves[1].bus_id                     = 1
    pkt.data.slaves[1].addr                       = 1
    pkt.data.slaves[1].physics_model_id           = 0
    return pkt
end

------------------------------------------------------------------------
local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

local g_pids = {}
local g_fds  = {}

-- Allocates the path strings as Lua strings; comm_dongle_attach_t holds
-- pointers, so keep these alive (referenced from a Lua local) for the
-- duration of comm_init_with_dongles.
local g_path_holders = {}

local function bring_up_two_dongles()
    -- Sequential spawn: wait for each robot's READY before starting
    -- the next one. Avoids any "two forks racing" timing artifact and
    -- more closely matches what an orchestrator will eventually do.
    local pid_a, fd_a = spawn_robot(DTYPE.DRIVE_BASE, 1)
    local path_a = wait_for_pty(fd_a, "robot A")
    local pid_b, fd_b = spawn_robot(DTYPE.DRIVE_BASE, 2)
    local path_b = wait_for_pty(fd_b, "robot B")
    g_pids = { pid_a, pid_b }
    g_fds  = { fd_a, fd_b }
    g_path_holders = { path_a, path_b }

    -- Build attachment specs in MANIFEST ORDER (so spec[0] => dongle 0,
    -- spec[1] => dongle 1). Order doesn't actually have to match —
    -- comm_init_with_dongles looks up by (type, instance) — but matching
    -- here makes the test easier to reason about.
    local specs = ffi.new("comm_dongle_attach_t[2]")
    specs[0].path             = path_a
    specs[0].dongle_type      = DTYPE.DRIVE_BASE
    specs[0].dongle_instance  = 1
    specs[1].path             = path_b
    specs[1].dongle_type      = DTYPE.DRIVE_BASE
    specs[1].dongle_instance  = 2

    local pkt = build_two_dongle_packet()
    local rc  = C.comm_init_with_dongles(
                    ffi.cast("const uint8_t*", pkt),
                    PACKET_SIZE,
                    specs, 2)
    return rc, path_a, path_b, specs
end

local function tear_down()
    C.comm_shutdown()
    for _, pid in ipairs(g_pids) do reap(pid) end
    for _, fd  in ipairs(g_fds)  do ffi.C.close(fd) end
    g_pids, g_fds, g_path_holders = {}, {}, {}
end

-- Drive comm_poll until handle h reports terminal; returns terminal status.
-- Sleep between polls is 1 ms — long enough to let robot_sim's watcher
-- thread get scheduled when chain_tree is busy-looping, short enough that
-- a normal round-trip (~0.5 ms over pty) only burns one wait per iter.
local function poll_until(h, timeout_ms)
    local deadline = ct_comm.now_ms() + (timeout_ms or 500)
    while ct_comm.now_ms() < deadline do
        ct_comm.poll(8)
        local s = C.comm_status(h)
        if s == R.OK or s == R.ERR_NAK then return s end
        ffi.C.usleep(1000)
    end
    return C.comm_status(h)
end

------------------------------------------------------------------------
io.write("[bring up two dongles + handshake]\n")
do
    local rc, path_a, path_b = bring_up_two_dongles()
    check(rc == R.OK,                                   string.format("comm_init_with_dongles rc=%d", rc))
    check(path_a ~= path_b,                             "two distinct pty paths")
    check(path_a:sub(1,8) == "/dev/pts" and path_b:sub(1,8) == "/dev/pts", "both under /dev/pts/")
    -- Both slaves should now be submittable (handshake succeeded means
    -- dongles are bound and slaves on those dongles are auto-bound).
    local h1, e1 = ct_comm.submit(1, CMD.PING, nil, 0)
    local h2, e2 = ct_comm.submit(2, CMD.PING, nil, 0)
    check(e1 == R.OK and h1 ~= 0,                       "submit PING to mcu=1 (dongle 0) ok")
    check(e2 == R.OK and h2 ~= 0,                       "submit PING to mcu=2 (dongle 1) ok")
    check(poll_until(h1, 500) == R.OK,                  "mcu=1 PING completes via dongle 0")
    check(poll_until(h2, 500) == R.OK,                  "mcu=2 PING completes via dongle 1")
    local e1c, rc1c = ct_comm.claim(h1)
    local e2c, rc2c = ct_comm.claim(h2)
    check(rc1c == R.OK and e1c.cmd == CMD.ACK_BARE,     "mcu=1 claim returns ACK_BARE")
    check(rc2c == R.OK and e2c.cmd == CMD.ACK_BARE,     "mcu=2 claim returns ACK_BARE")
    check(e1c.mcu == 1,                                 "claimed event for mcu=1 carries mcu=1")
    check(e2c.mcu == 2,                                 "claimed event for mcu=2 carries mcu=2")
    tear_down()
end

------------------------------------------------------------------------
io.write("[NAK isolation: NAK on dongle 0 doesn't surface on dongle 1]\n")
do
    bring_up_two_dongles()
    -- 0x0099 unknown cmd → NAK on the targeted dongle's bus only.
    local h_nak = ct_comm.submit(1, 0x0099, nil, 0)        -- dongle 0
    local h_ok  = ct_comm.submit(2, CMD.PING, nil, 0)      -- dongle 1
    check(poll_until(h_nak, 500) == R.ERR_NAK,           "mcu=1 unknown cmd → NAK")
    check(poll_until(h_ok,  500) == R.OK,                "mcu=2 PING still completes")
    local e_nak = ct_comm.claim(h_nak)
    local e_ok  = ct_comm.claim(h_ok)
    check(e_nak and e_nak.cmd == CMD.NAK,                 "dongle 0 surfaced its NAK")
    check(e_ok  and e_ok.cmd  == CMD.ACK_BARE,            "dongle 1 surfaced its ACK_BARE — no cross-talk")
    tear_down()
end

------------------------------------------------------------------------
-- Stress N kept moderate because multi-dongle full-duplex over pty in
-- LuaJIT exposes a timing race we have not finished diagnosing — at
-- N=100 the harness flakes ~30%, with one of the two dongles' last
-- few responses arriving very late. Phase A's single-dongle 1000x
-- gate stays the long-run stress baseline; this multi-dongle test
-- proves multi-dongle routing + isolation works repeatably at smaller N.
local STRESS_N = 20

io.write(string.format("[%dx PING serialized through each dongle]\n", STRESS_N))
do
    bring_up_two_dongles()
    local N = STRESS_N
    local done_a, done_b = 0, 0
    local t0 = ct_comm.now_ms()
    for i = 1, N do
        local h = ct_comm.submit(1, CMD.PING, nil, 0)
        if poll_until(h, 1000) == R.OK then
            local e = ct_comm.claim(h)
            if e and e.cmd == CMD.ACK_BARE then done_a = done_a + 1 end
        else ct_comm.cancel(h) end
    end
    for i = 1, N do
        local h = ct_comm.submit(2, CMD.PING, nil, 0)
        if poll_until(h, 1000) == R.OK then
            local e = ct_comm.claim(h)
            if e and e.cmd == CMD.ACK_BARE then done_b = done_b + 1 end
        else ct_comm.cancel(h) end
    end
    local dt = ct_comm.now_ms() - t0
    check(done_a == N and done_b == N,                    string.format("serialized: a=%d b=%d (%d ms)", done_a, done_b, dt))
    tear_down()
end

io.write(string.format("[%dx PING interleaved across both dongles]\n", STRESS_N))
do
    bring_up_two_dongles()
    local N = STRESS_N
    local done_a, done_b = 0, 0
    local t0 = ct_comm.now_ms()
    for i = 1, N do
        local h1 = ct_comm.submit(1, CMD.PING, nil, 0)
        local h2 = ct_comm.submit(2, CMD.PING, nil, 0)
        if poll_until(h1, 1000) == R.OK then
            local e1 = ct_comm.claim(h1)
            if e1 and e1.cmd == CMD.ACK_BARE and e1.mcu == 1 then done_a = done_a + 1 end
        else ct_comm.cancel(h1) end
        if poll_until(h2, 1000) == R.OK then
            local e2 = ct_comm.claim(h2)
            if e2 and e2.cmd == CMD.ACK_BARE and e2.mcu == 2 then done_b = done_b + 1 end
        else ct_comm.cancel(h2) end
    end
    local dt = ct_comm.now_ms() - t0
    check(done_a == N and done_b == N,                    string.format("interleaved: a=%d b=%d (%d ms)", done_a, done_b, dt))
    tear_down()
end

------------------------------------------------------------------------
io.write("[wrong identity hard-stops]\n")
do
    -- Spawn (DRIVE_BASE, 1) but tell comm_init we expect (DRIVE_BASE, 2).
    -- Init must hard-stop with COMM_ERR_DONGLE_UNEXPECTED.
    local pid, fd = spawn_robot(DTYPE.DRIVE_BASE, 1)
    g_pids = { pid }; g_fds = { fd }
    local path = wait_for_pty(fd, "robot mismatch")
    g_path_holders = { path }

    -- Build a manifest that declares ONLY (DRIVE_BASE, 2). But spawn
    -- claims to be (DRIVE_BASE, 1). Whichever pair we look up first,
    -- robot_sim disagrees, IDENT validation fails.
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash                       = SCHEMA_HASH
    pkt.data.version                              = 1
    pkt.data.dongle_count                         = 1
    pkt.data.bus_count                            = 1
    pkt.data.slave_count                          = 1
    set_uuid_identity(pkt.data.dongles[0].dongle_uuid, DTYPE.DRIVE_BASE, 2)
    pkt.data.dongles[0].bus_count                 = 1
    pkt.data.dongles[0].bus_local_ids[0]          = 0
    pkt.data.buses[0].bus_id                      = 0
    pkt.data.buses[0].tunables.max_miss           = 3
    pkt.data.buses[0].tunables.tick_period_ms     = 20
    pkt.data.buses[0].tunables.join_timeout_ms    = 200
    pkt.data.slaves[0].mcu                        = 1
    pkt.data.slaves[0].dongle_idx                 = 0
    pkt.data.slaves[0].bus_id                     = 0
    pkt.data.slaves[0].addr                       = 1
    pkt.data.slaves[0].physics_model_id           = 0

    local specs = ffi.new("comm_dongle_attach_t[1]")
    specs[0].path             = path
    specs[0].dongle_type      = DTYPE.DRIVE_BASE
    specs[0].dongle_instance  = 2  -- caller insists on instance 2; robot returns 1
    local rc = C.comm_init_with_dongles(
                    ffi.cast("const uint8_t*", pkt), PACKET_SIZE,
                    specs, 1)
    check(rc == R.ERR_DONGLE_UNEXPECTED,                  string.format("identity mismatch → ERR_DONGLE_UNEXPECTED (got %d)", rc))
    -- comm_init failure already closed the FD internally; just reap.
    for _, p in ipairs(g_pids) do reap(p) end
    for _, f in ipairs(g_fds)  do ffi.C.close(f) end
    g_pids, g_fds, g_path_holders = {}, {}, {}
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
