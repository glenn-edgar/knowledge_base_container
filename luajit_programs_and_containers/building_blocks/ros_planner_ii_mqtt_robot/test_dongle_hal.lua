-- test_dongle_hal.lua
-- Slice L5.5 smoke test: spawn robot_sim, init dongle_hal against
-- it, send PUSH_LINE via the HAL, verify the rover moves and
-- last_done_seg_id eventually equals what push_line returned.
--
-- Mirrors the lifecycle test_random_paths.lua will see when the
-- planner runs, but in a single-file standalone form so we can
-- iterate on the HAL without the whole MQTT stack.

io.stdout:setvbuf("no")

local ffi = require("ffi")
ffi.cdef[[
    int    pipe(int *fd);
    int    fork(void);
    int    dup2(int oldfd, int newfd);
    int    close(int fd);
    int    execvp(const char *file, char *const argv[]);
    int    kill(int pid, int sig);
    int    waitpid(int pid, int *status, int options);
    int    usleep(unsigned int usec);
    long   read(int fd, void *buf, long count);
]]
local SIGTERM = 15

local function spawn_robot_sim(args)
    local pipefd = ffi.new("int[2]")
    if ffi.C.pipe(pipefd) ~= 0 then error("pipe failed") end
    local pid = ffi.C.fork()
    if pid < 0 then error("fork failed") end
    if pid == 0 then
        ffi.C.dup2(pipefd[1], 1)        -- stdout → write end
        ffi.C.close(pipefd[0])
        ffi.C.close(pipefd[1])
        local argv = ffi.new("char*[?]", #args + 2)
        argv[0] = ffi.cast("char*", "./robot_sim/robot_sim")
        for i, a in ipairs(args) do argv[i] = ffi.cast("char*", a) end
        argv[#args + 1] = nil
        ffi.C.execvp("./robot_sim/robot_sim", argv)
        os.exit(127)
    end
    ffi.C.close(pipefd[1])
    return pid, pipefd[0]
end

local function read_line(fd, deadline_ms)
    local buf = {}
    local one = ffi.new("char[1]")
    local function now() return os.time() * 1000 + math.floor((os.clock() - math.floor(os.clock())) * 1000) end
    while true do
        local r = tonumber(ffi.C.read(fd, one, 1))
        if r == 1 then
            local ch = string.char(one[0])
            if ch == "\n" then return table.concat(buf) end
            buf[#buf + 1] = ch
        elseif r == 0 then return nil, "eof"
        else ffi.C.usleep(500) end
    end
end

local function wait_for_pty(fd)
    while true do
        local line = read_line(fd, nil)
        if not line then error("robot_sim: stdout closed") end
        if line:sub(1, 4) == "PTY=" then return line:sub(5) end
    end
end

local function reap(pid)
    ffi.C.kill(pid, SIGTERM)
    local st = ffi.new("int[1]")
    ffi.C.waitpid(pid, st, 0)
end

------------------------------------------------------------------------
local pass, fail = 0, 0
local function check(cond, msg)
    if cond then pass = pass + 1; io.write("  PASS  "..msg.."\n")
    else        fail = fail + 1; io.write("  FAIL  "..msg.."\n") end
end

io.write("[dongle_hal smoke]\n")

local pid, fd = spawn_robot_sim({"--type", "1", "--instance", "1", "--addr", "1"})
local pty_path = wait_for_pty(fd)
check(pty_path:sub(1,9) == "/dev/pts/", "robot_sim opened a pty: "..pty_path)

local robot_hal_mod = require("robot_hal")
local hal = robot_hal_mod.new({
    mode            = "dongle",
    pty_path        = pty_path,
    dongle_type     = 1,
    dongle_instance = 1,
    slave_addr      = 1,
    mcu             = 1,
})
check(hal.mode == "dongle", "HAL initialised in dongle mode")

-- Allow a couple of telemetry events to land before the first read.
ffi.C.usleep(100 * 1000)
hal:step(0)

local pose = hal:read_pose()
check(math.abs(pose.x) < 0.01 and math.abs(pose.y) < 0.01,
      string.format("initial pose at origin (got %.3f,%.3f)", pose.x, pose.y))

-- Push a 1m line at 0.5 m/s.
local seg_id = hal:push_line(0, 0, 1, 0, 0, 0, 0.5)
check(seg_id ~= nil and seg_id > 0, "push_line returned seg_id="..tostring(seg_id))

-- Wait up to 4 seconds for the segment to complete.
local deadline = os.time() + 4
local done = false
while os.time() < deadline do
    hal:step(0)
    local st = hal:read_path_status()
    if st.last_done_seg_id == seg_id then done = true; break end
    ffi.C.usleep(50 * 1000)
end
check(done, "SEG_DONE arrived for our seg_id")

local final = hal:read_pose()
check(final.x > 0.5,
      string.format("rover advanced past x=0.5m (got %.3f)", final.x))
check(math.abs(final.y) < 0.10,
      string.format("rover stayed near y axis (got %.3f)", final.y))

robot_hal_mod = nil   -- (avoid keeping ref before shutdown)
require("dongle_hal").shutdown()
reap(pid)

io.write(string.format("[summary] %d passed, %d failed\n", pass, fail))
os.exit(fail == 0 and 0 or 1)
