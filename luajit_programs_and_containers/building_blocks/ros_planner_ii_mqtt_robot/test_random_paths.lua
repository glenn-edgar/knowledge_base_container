--[[
    test_random_paths.lua -- Mimic a planner: publish random command
    sequences to the robot's RPC topic. Subscribe to stream_bus to print
    heartbeats + kb_done messages.

    Usage:
       luajit test_random_paths.lua [--robot rover_1] [--site ...]
                                    [--host localhost] [--port 1883]
                                    [--seed 42] [--count 20] [--mode mixed]
                                    [--verbose]

    --mode  mixed | paths_only | single_action
]]

local json_util = require("json_util")
local pubsub    = require("lib.mqtt_pubsub")

-- ---------- arg parse ----------
local args = { robot = "rover_1", site = "moonbase.alpha.surface_ops",
               host = "localhost", port = 1883, seed = 1, count = 20,
               mode = "mixed", verbose = false, wait = 90,
               workspace = 3.0 }
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
        else
            io.stderr:write("unknown arg: " .. a .. "\n"); os.exit(1)
        end
    end
end

math.randomseed(args.seed)

local site_path = args.site:gsub("%.", "/")
local rpc_topic    = site_path .. "/robots/" .. args.robot .. "/rpc"
local stream_topic = site_path .. "/robots/" .. args.robot .. "/stream_bus"

io.stderr:write(string.format(
    "test_random_paths: robot=%s host=%s:%d seed=%d count=%d mode=%s workspace=%.1fm\n",
    args.robot, args.host, args.port, args.seed, args.count, args.mode, args.workspace))

local ps = pubsub.PubSub.new(args.host, args.port, "test_random_" .. args.seed)
ps:connect(5000)
ps:subscribe(stream_topic, 1)

-- ---------- generators ----------

-- Type IDs (mirror command_packets)
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

-- Seed current pose (keep segments continuous)
local pose = { x = 0, y = 0, h = 0 }
local seq, test_id = 0, 2000

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
    -- bias to_heading toward chord direction so it looks natural
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

-- ---------- build the mission ----------

local function build_mixed()
    -- init_check first
    commands[#commands+1] = { packet_type = T.INIT_CHECK, params = {}, energy = 50 }
    for i = 1, args.count do
        local r = math.random()
        -- Prefer splines heavily; line segments with sharp heading changes
        -- cause corner-cutting that drifts from the harness's pose model.
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

local function build_single_action()
    emit_action("idle")
end

if args.mode == "paths_only"   then build_paths_only()
elseif args.mode == "single_action" then build_single_action()
else                                 build_mixed()
end

-- Annotate seq/test_id
for i, c in ipairs(commands) do
    c.seq = seq; c.test_id = test_id
    seq = seq + 1; test_id = test_id + 1
end

io.stderr:write(string.format("  built %d commands\n", #commands))

-- ---------- publish all commands (burst) ----------
for _, c in ipairs(commands) do
    ps:publish(rpc_topic, json_util.encode(c), 1, false)
    if args.verbose then
        io.stderr:write("  -> " .. json_util.encode(c) .. "\n")
    end
end

-- ---------- listen for stream responses ----------

local deadline = os.time() + args.wait
local stats = { ack = 0, heartbeat = 0, kb_done = 0, success = 0, fail = 0 }
local expected = #commands - 1  -- init_check may or may not return kb_done; we tally actuals
local seen_done = 0

io.stderr:write(string.format("  listening %ds for stream events...\n", args.wait))
while os.time() < deadline do
    local msgs = ps:poll(200)
    for _, m in ipairs(msgs) do
        local ok, ev = pcall(json_util.decode, m.payload)
        if ok and ev then
            if ev.type == "ack" then
                stats.ack = stats.ack + 1
                if args.verbose then
                    io.stderr:write(string.format("  ACK seq=%s test=%s\n", tostring(ev.seq), tostring(ev.test_id)))
                end
            elseif ev.type == "heartbeat" then
                stats.heartbeat = stats.heartbeat + 1
                if args.verbose and stats.heartbeat % 20 == 1 then
                    io.stderr:write(string.format("  HB phase=%s worker=%s x=%.2f y=%.2f h=%.2f\n",
                        tostring(ev.phase), tostring(ev.worker),
                        tonumber(ev.global_x) or 0,
                        tonumber(ev.global_y) or 0,
                        tonumber(ev.global_heading) or 0))
                end
            elseif ev.type == "kb_done" then
                stats.kb_done = stats.kb_done + 1
                seen_done = seen_done + 1
                if ev.success then stats.success = stats.success + 1 else stats.fail = stats.fail + 1 end
                io.stderr:write(string.format("  DONE test=%s success=%s dx=%s dy=%s dh=%s energy=%s%s\n",
                    tostring(ev.test_id), tostring(ev.success),
                    tostring(ev.delta_x), tostring(ev.delta_y), tostring(ev.delta_heading),
                    tostring(ev.energy_remaining),
                    ev.fault_reason and (" fault=" .. ev.fault_reason) or ""))
            end
        end
    end
    if seen_done >= #commands then break end
end

io.stderr:write(string.format(
    "\nSummary: cmds=%d  ack=%d  hb=%d  done=%d (%d ok / %d fail)\n",
    #commands, stats.ack, stats.heartbeat, stats.kb_done, stats.success, stats.fail))

pcall(function() ps:disconnect() end)
pcall(function() ps:destroy() end)

if stats.kb_done < #commands then
    io.stderr:write("WARN: not all commands completed within wait window\n")
    os.exit(2)
end
if stats.fail > 0 then
    io.stderr:write("WARN: some commands failed\n")
    os.exit(3)
end
