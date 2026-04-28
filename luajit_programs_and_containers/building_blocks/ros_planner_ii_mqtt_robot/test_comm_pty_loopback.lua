-- test_comm_pty_loopback.lua
-- Single-dongle pty loopback (Phase B compatible). Migrated from the
-- legacy comm_init_with_uart path to comm_init_with_dongles with one
-- (DRIVE_BASE, 1) dongle so we can retire the legacy entry point.
-- Mirrors the slice-1e in-proc loopback shape but over a real pty,
-- proving the master-side libcomm path works irrespective of transport.
--
-- The Phase A multi-dongle test (test_comm_pty_multi_dongle.lua) covers
-- the multi-dongle end-to-end shape; this file covers single-dongle
-- stress in isolation.
--
-- Run from project root:  luajit test_comm_pty_loopback.lua

io.stdout:setvbuf("no")

local ffi      = require("ffi")
local bit      = require("bit")
local comm_ffi = require("comm_ffi")
local ct_comm  = require("ct_comm")

local C     = comm_ffi.C
local R     = comm_ffi.RESULT
local CMD   = comm_ffi.CMD
local DTYPE = comm_ffi.DONGLE_TYPE

------------------------------------------------------------------------
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

local function spawn_robot(dongle_type, dongle_instance)
    local pipefd = ffi.new("int[2]")
    if ffi.C.pipe(pipefd) ~= 0 then error("pipe failed") end
    local pid = ffi.C.fork()
    if pid < 0 then error("fork failed") end
    if pid == 0 then
        ffi.C.close(pipefd[0])
        ffi.C.dup2(pipefd[1], 1)
        ffi.C.close(pipefd[1])
        local argv = ffi.new("char*[6]")
        argv[0] = ffi.cast("char*", "robot_sim/robot_sim")
        argv[1] = ffi.cast("char*", "--type")
        argv[2] = ffi.cast("char*", tostring(dongle_type))
        argv[3] = ffi.cast("char*", "--instance")
        argv[4] = ffi.cast("char*", tostring(dongle_instance))
        argv[5] = nil
        ffi.C.execv("robot_sim/robot_sim", argv)
        os.exit(127)
    end
    ffi.C.close(pipefd[1])
    return pid, pipefd[0]
end

local function read_line(fd, deadline_ms)
    local buf = {}
    local one = ffi.new("char[1]")
    while true do
        local r = tonumber(ffi.C.read(fd, one, 1))
        if r == 1 then
            local ch = string.char(one[0])
            if ch == "\n" then return table.concat(buf) end
            buf[#buf + 1] = ch
        elseif r == 0 then return nil, "eof"
        else ffi.C.usleep(500) end
        if deadline_ms and ct_comm.now_ms() > deadline_ms then return nil, "timeout" end
    end
end

local function wait_for_pty(fd, label)
    local deadline = ct_comm.now_ms() + 2000
    local pty_path
    while true do
        local line, why = read_line(fd, deadline)
        if not line then error(label..": "..(why or "no line")) end
        if line:sub(1,4) == "PTY=" then pty_path = line:sub(5)
        elseif line == "READY" then
            assert(pty_path, label..": got READY before PTY=")
            return pty_path
        end
    end
end

local function reap(pid)
    ffi.C.kill(pid, SIGTERM)
    local st = ffi.new("int[1]")
    ffi.C.waitpid(pid, st, 0)
end

------------------------------------------------------------------------
-- Manifest packed types (same as multi-dongle test).
ffi.cdef[[
typedef struct __attribute__((packed)) {
    double timestamp; uint32_t schema_hash; uint16_t seq; uint16_t source_node;
} comm_manifest_wire_header_t;
typedef struct __attribute__((packed)) {
    uint8_t dongle_uuid[16]; uint8_t bus_count; uint8_t bus_local_ids[8];
} manifest_dongle_t;
typedef struct __attribute__((packed)) {
    uint8_t  max_miss; uint16_t tick_period_ms; uint16_t join_timeout_ms;
} manifest_tunables_t;
typedef struct __attribute__((packed)) {
    uint8_t bus_id; manifest_tunables_t tunables;
} manifest_bus_t;
typedef struct __attribute__((packed)) {
    uint8_t mcu, dongle_idx, bus_id, addr; uint32_t physics_model_id;
} manifest_slave_t;
typedef struct __attribute__((packed)) {
    uint8_t version, dongle_count, bus_count, slave_count;
    manifest_dongle_t dongles[4]; manifest_bus_t buses[8]; manifest_slave_t slaves[64];
} comm_manifest_v1_wire_t;
typedef struct __attribute__((packed)) {
    comm_manifest_wire_header_t header; comm_manifest_v1_wire_t data;
} comm_manifest_v1_packet_t;
]]

local SCHEMA_HASH = 0x79046205
local PACKET_SIZE = ffi.sizeof("comm_manifest_v1_packet_t")

local function set_uuid_identity(uuid, dongle_type, dongle_instance)
    uuid[0] = bit.band(dongle_type, 0xFF)
    uuid[1] = bit.band(bit.rshift(dongle_type, 8), 0xFF)
    uuid[2] = bit.band(dongle_instance, 0xFF)
    uuid[3] = bit.band(bit.rshift(dongle_instance, 8), 0xFF)
end

local function build_one_dongle_packet()
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash                       = SCHEMA_HASH
    pkt.data.version                              = 1
    pkt.data.dongle_count                         = 1
    pkt.data.bus_count                            = 1
    pkt.data.slave_count                          = 1
    set_uuid_identity(pkt.data.dongles[0].dongle_uuid, DTYPE.DRIVE_BASE, 1)
    pkt.data.dongles[0].bus_count                 = 1
    pkt.data.dongles[0].bus_local_ids[0]          = 0
    pkt.data.buses[0].bus_id                      = 0
    pkt.data.buses[0].tunables.max_miss           = 3
    pkt.data.buses[0].tunables.tick_period_ms     = 20
    pkt.data.buses[0].tunables.join_timeout_ms    = 500
    pkt.data.slaves[0].mcu                        = 1
    pkt.data.slaves[0].dongle_idx                 = 0
    pkt.data.slaves[0].bus_id                     = 0
    pkt.data.slaves[0].addr                       = 1
    pkt.data.slaves[0].physics_model_id           = 0
    return pkt
end

------------------------------------------------------------------------
local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

local g_pid, g_fd
local g_pathstr           -- keep alive; comm_dongle_attach_t holds a pointer

local function setup()
    local pid, fd = spawn_robot(DTYPE.DRIVE_BASE, 1)
    g_pid, g_fd = pid, fd
    local path = wait_for_pty(fd, "robot")
    g_pathstr = path
    local specs = ffi.new("comm_dongle_attach_t[1]")
    specs[0].path             = path
    specs[0].dongle_type      = DTYPE.DRIVE_BASE
    specs[0].dongle_instance  = 1
    local pkt = build_one_dongle_packet()
    local rc  = C.comm_init_with_dongles(
                    ffi.cast("const uint8_t*", pkt), PACKET_SIZE, specs, 1)
    assert(rc == R.OK, string.format("comm_init_with_dongles rc=%d", rc))
    return path
end

local function teardown()
    C.comm_shutdown()
    if g_pid then reap(g_pid); g_pid = nil end
    if g_fd  then ffi.C.close(g_fd); g_fd = nil end
    g_pathstr = nil
end

local function poll_until(h, timeout_ms)
    local deadline = ct_comm.now_ms() + (timeout_ms or 500)
    while ct_comm.now_ms() < deadline do
        ct_comm.poll(8)
        local s = C.comm_status(h)
        if s == R.OK or s == R.ERR_NAK then return s end
        ffi.C.usleep(500)
    end
    return C.comm_status(h)
end

------------------------------------------------------------------------
io.write("[uart bring-up]\n")
do
    local pty_path = setup()
    check(pty_path ~= nil and #pty_path > 0,                   "spawn returns a non-empty pty path")
    check(pty_path:sub(1, 8) == "/dev/pts",                    "pty path lives under /dev/pts/")
    teardown()
end

------------------------------------------------------------------------
io.write("[single PING round-trip over pty]\n")
do
    setup()
    local t0 = ct_comm.now_ms()
    local h, err = ct_comm.submit(1, CMD.PING, nil, 0)
    check(err == R.OK and h ~= 0,                              "submit PING ok")
    check(poll_until(h, 500) == R.OK,                          "PING completes (status OK)")
    local e, claim_rc = ct_comm.claim(h)
    check(claim_rc == R.OK and e ~= nil,                       "claim returns OK + event")
    if e then
        check(e.handle == h,                                   "event handle matches submitted handle")
        check(e.mcu == 1,                                      "event mcu == 1")
        check(e.cmd == CMD.ACK_BARE,                           "event.cmd == ACK_BARE")
        check(e.ack_status == 0,                               "event.ack_status == 0")
        check(e.payload_len == 0,                              "event.payload_len == 0 (bare ack)")
        check(e.elapsed_ms <= (ct_comm.now_ms() - t0) + 50,    "elapsed_ms is plausible")
    end
    teardown()
end

------------------------------------------------------------------------
io.write("[NAK on unknown cmd over pty]\n")
do
    setup()
    local h, err = ct_comm.submit(1, 0x0099, nil, 0)
    check(err == R.OK,                                         "submit unknown cmd accepted by master")
    check(poll_until(h, 500) == R.ERR_NAK,                     "status reports NAK after slave rejects")
    local e, claim_rc = ct_comm.claim(h)
    check(claim_rc == R.ERR_NAK,                               "claim returns ERR_NAK")
    check(e ~= nil and e.cmd == CMD.NAK,                       "claimed event.cmd == CMD.NAK")
    check(e and e.payload_len == 1 and e.payload[0] == 0xFF,   "NAK payload reason byte == 0xFF")
    teardown()
end

------------------------------------------------------------------------
io.write("[100x PING burst]\n")
do
    setup()
    local N    = 100
    local t0   = ct_comm.now_ms()
    local done = 0
    for i = 1, N do
        local h = ct_comm.submit(1, CMD.PING, nil, 0)
        if poll_until(h, 1000) ~= R.OK then break end
        local e = ct_comm.claim(h)
        if e and e.cmd == CMD.ACK_BARE then done = done + 1 end
    end
    local dt = ct_comm.now_ms() - t0
    check(done == N,                                           string.format("%d/%d PINGs completed (%d ms)", done, N, dt))
    teardown()
end

------------------------------------------------------------------------
io.write("[1000x PING stress (single dongle)]\n")
do
    setup()
    local N    = 1000
    local t0   = ct_comm.now_ms()
    local done = 0
    for i = 1, N do
        local h = ct_comm.submit(1, CMD.PING, nil, 0)
        if poll_until(h, 1000) ~= R.OK then break end
        local e = ct_comm.claim(h)
        if e and e.cmd == CMD.ACK_BARE then done = done + 1 end
    end
    local dt = ct_comm.now_ms() - t0
    check(done == N,                                           string.format("stress: %d/%d completed in %d ms (%.1f Hz)", done, N, dt, (done * 1000) / math.max(dt, 1)))
    teardown()
end

------------------------------------------------------------------------
io.write("[128 B max-payload NAK round-trip]\n")
do
    setup()
    local big = ffi.new("uint8_t[?]", 128)
    for i = 0, 127 do
        big[i] = (i % 4 == 0) and 0xC0
              or (i % 4 == 1) and 0xDB
              or (i % 4 == 2) and 0x55
              or 0xAA
    end
    local h, err = ct_comm.submit(1, 0x0099, big, 128)
    check(err == R.OK,                                         "submit 128 B unknown cmd accepted")
    check(poll_until(h, 500) == R.ERR_NAK,                     "128 B request → NAK")
    local e, claim_rc = ct_comm.claim(h)
    check(claim_rc == R.ERR_NAK,                               "claim 128 B NAK ok")
    check(e and e.cmd == CMD.NAK and e.payload_len == 1 and e.payload[0] == 0xFF,
                                                               "128 B NAK carries reason 0xFF")
    teardown()
end

------------------------------------------------------------------------
io.write("[clean teardown + re-init]\n")
do
    setup()
    local h = ct_comm.submit(1, CMD.PING, nil, 0)
    poll_until(h, 500)
    ct_comm.claim(h)
    teardown()

    setup()
    local h2 = ct_comm.submit(1, CMD.PING, nil, 0)
    check(poll_until(h2, 500) == R.OK,                          "second init/teardown cycle works")
    ct_comm.claim(h2)
    teardown()
end

------------------------------------------------------------------------
io.write(string.format("\n[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
