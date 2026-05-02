--[[
    test_random_paths.lua -- Generates random missions, drives them
    against a rover, summarises kb_dones.

    After the planner_test_peer extraction, this script is a scenario
    generator + thin runner. The peer handles the link handshake +
    heartbeat keep-alive + dispatch + done collection.

    --self-host (default: false): if set, the peer brings the rover live
                                  itself instead of relying on a
                                  separately-running test_mock_planner.

    Usage:
       luajit test_random_paths.lua [--robot rover_1]
                                    [--site moonbase.alpha.surface_ops]
                                    [--host localhost] [--port 1883]
                                    [--seed 42] [--count 20]
                                    [--mode mixed|paths_only|single_action]
                                    [--workspace 3.0]
                                    [--wait 90] [--self-host]
                                    [--verbose]
]]

local planner_test_peer = require("planner_test_peer")

-- ---------- arg parse ----------
local args = { robot = "rover_1", site = "moonbase.alpha.surface_ops",
               host = "localhost", port = 1883, seed = 1, count = 20,
               mode = "mixed", verbose = false, wait = 90,
               workspace = 3.0, self_host = false }
do
    local i = 1
    while i <= #arg do
        local a = arg[i]
        if a == "--robot"   then args.robot = arg[i+1]; i = i + 2
        elseif a == "--site" then args.site = arg[i+1]; i = i + 2
        elseif a == "--host" then args.host = arg[i+1]; i = i + 2
        elseif a == "--port" then args.port = tonumber(arg[i+1]); i = i + 2
        elseif a == "--seed" then args.seed = tonumber(arg[i+1]); i = i + 2
        elseif a == "--count" then args.count = tonumber(arg[i+1]); i = i + 2
        elseif a == "--mode" then args.mode = arg[i+1]; i = i + 2
        elseif a == "--verbose" then args.verbose = true; i = i + 1
        elseif a == "--wait" then args.wait = tonumber(arg[i+1]); i = i + 2
        elseif a == "--workspace" then args.workspace = tonumber(arg[i+1]); i = i + 2
        elseif a == "--self-host" then args.self_host = true; i = i + 1
        else
            io.stderr:write("unknown arg: " .. a .. "\n"); os.exit(1)
        end
    end
end

math.randomseed(args.seed)

io.stderr:write(string.format(
    "test_random_paths: robot=%s host=%s:%d seed=%d count=%d mode=%s workspace=%.1fm\n",
    args.robot, args.host, args.port, args.seed, args.count, args.mode, args.workspace))

-- ---------- scenario builders (unchanged from pre-extraction) ----------

local T = {
    INIT_CHECK      = 1,
    PATH_SPLINE     = 2,
    PATH_LINE       = 3,
    PATH_ROTATE     = 5,
    DELIVER_PART    = 6,
    PAINT_SAMPLE    = 7,
    LOAD_SHIPPING   = 8,
    PASS_GATE       = 9,
    INSPECTION_SCAN = 10,
    IDLE            = 11,
    RECHARGE        = 12,
}

local function rand_range(a, b) return a + math.random() * (b - a) end
local pose = { x = 0, y = 0, h = 0 }
local commands = {}

local function emit_line(speed)
    local dx = rand_range(-args.workspace, args.workspace)
    local dy = rand_range(-args.workspace, args.workspace)
    if math.abs(dx) + math.abs(dy) < 0.3 then dx = dx + 1.0 end
    local to_x, to_y = pose.x + dx, pose.y + dy
    local to_h = math.atan2(to_y - pose.y, to_x - pose.x)
    commands[#commands+1] = {
        packet_type = T.PATH_LINE,
        params = { from_x = pose.x, from_y = pose.y,
                   to_x = to_x,     to_y = to_y,
                   from_heading = pose.h, to_heading = to_h,
                   speed = speed or 0.5 },
        energy = 200,
    }
    pose.x, pose.y, pose.h = to_x, to_y, to_h
end

local function emit_spline(speed)
    local dx = rand_range(-args.workspace, args.workspace)
    local dy = rand_range(-args.workspace, args.workspace)
    if math.abs(dx) + math.abs(dy) < 0.3 then dx = dx + 1.0 end
    local to_x, to_y = pose.x + dx, pose.y + dy
    local chord_h = math.atan2(to_y - pose.y, to_x - pose.x)
    local to_h = chord_h + rand_range(-0.3, 0.3)
    commands[#commands+1] = {
        packet_type = T.PATH_SPLINE,
        params = { from_x = pose.x, from_y = pose.y,
                   to_x = to_x,     to_y = to_y,
                   from_heading = pose.h, to_heading = to_h,
                   speed = speed or 0.5 },
        energy = 250,
    }
    pose.x, pose.y, pose.h = to_x, to_y, to_h
end

local function emit_action(kind)
    if kind == "idle" then
        commands[#commands+1] = { packet_type = T.IDLE, params = {}, energy = 10 }
    elseif kind == "scan" then
        commands[#commands+1] = { packet_type = T.INSPECTION_SCAN,
            params = { sensor_port = 1, sensor_type = 2 }, energy = 30 }
    elseif kind == "deliver" then
        commands[#commands+1] = { packet_type = T.DELIVER_PART,
            params = { arm_target = 90, arm_speed = 60, arm_return = 0, payload_type = 1 },
            energy = 100 }
    elseif kind == "paint" then
        commands[#commands+1] = { packet_type = T.PAINT_SAMPLE,
            params = { arm_target = 75, arm_speed = 60, arm_return = 0, hold_time = 1.0 },
            energy = 100 }
    end
end

local function build_mixed()
    commands[#commands+1] = { packet_type = T.INIT_CHECK, params = {}, energy = 50 }
    for i = 1, args.count do
        local r = math.random()
        if r < 0.20 then emit_line()
        elseif r < 0.80 then emit_spline()
        elseif r < 0.90 then emit_action("idle")
        elseif r < 0.96 then emit_action("scan")
        else              emit_action("paint")
        end
    end
    commands[#commands+1] = { packet_type = T.IDLE, params = {}, energy = 10 }
end

local function build_paths_only()
    commands[#commands+1] = { packet_type = T.INIT_CHECK, params = {}, energy = 50 }
    for i = 1, args.count do
        if math.random() < 0.5 then emit_line() else emit_spline() end
    end
end

local function build_single_action() emit_action("idle") end

if     args.mode == "paths_only"    then build_paths_only()
elseif args.mode == "single_action" then build_single_action()
else                                     build_mixed()
end

io.stderr:write(string.format("  built %d commands\n", #commands))

-- ---------- run ----------

local peer = planner_test_peer.new{
    robot   = args.robot, site = args.site,
    host    = args.host,  port = args.port,
    verbose = args.verbose,
}

if args.self_host then
    -- Run-as-planner: bring the rover live ourselves. Otherwise assume
    -- a test_mock_planner is already running on the broker.
    local ok, err = peer:bring_robot_live(15)
    if not ok then
        io.stderr:write("test_random_paths: " .. err .. "\n")
        peer:close(); os.exit(2)
    end
end

peer:send_batch(commands)
io.stderr:write(string.format("  listening %ds for stream events...\n", args.wait))
local stats = peer:wait_for_dones(#commands, args.wait)

io.stderr:write(string.format(
    "\nSummary: cmds=%d  ack=%d  hb=%d  done=%d (%d ok / %d fail)\n",
    #commands, stats.ack, stats.hb, stats.done, stats.ok, stats.fail))

peer:close()

if stats.done < #commands then
    io.stderr:write("WARN: not all commands completed within wait window\n")
    os.exit(2)
end
if stats.fail > 0 then
    io.stderr:write("WARN: some commands failed\n")
    os.exit(3)
end
