--[[
    test_mock_planner.lua -- Minimal planner side for e2e smoke tests.

    Accepts link_announce from a robot, sends link_bridge_ack, then sends
    periodic link_bridge_heartbeat so the robot stays in "live" state. Does
    not issue commands itself (use test_random_paths.lua for that).

    Usage: luajit test_mock_planner.lua [--robot rover_1]
                                        [--site moonbase.alpha.surface_ops]
                                        [--host localhost] [--port 1883]
                                        [--duration 120]
]]

local json_util = require("json_util")
local pubsub    = require("lib.mqtt_pubsub")

local args = { robot = "rover_1", site = "moonbase.alpha.surface_ops",
               host = "localhost", port = 1883, duration = 120 }
do
    local i = 1
    while i <= #arg do
        local a = arg[i]
        if a == "--robot" then args.robot = arg[i+1]; i = i + 2
        elseif a == "--site" then args.site = arg[i+1]; i = i + 2
        elseif a == "--host" then args.host = arg[i+1]; i = i + 2
        elseif a == "--port" then args.port = tonumber(arg[i+1]); i = i + 2
        elseif a == "--duration" then args.duration = tonumber(arg[i+1]); i = i + 2
        else io.stderr:write("bad arg: " .. a .. "\n"); os.exit(1) end
    end
end

local site_path = args.site:gsub("%.", "/")
local link_in   = site_path .. "/robots/" .. args.robot .. "/link"
local planner_prefix = site_path .. "/robots/" .. args.robot .. "/planner/"
local stream    = site_path .. "/robots/" .. args.robot .. "/stream_bus"

io.stderr:write(string.format(
    "mock_planner: robot=%s host=%s:%d duration=%ds\n",
    args.robot, args.host, args.port, args.duration))

local ps = pubsub.PubSub.new(args.host, args.port, "mock_planner_" .. args.robot)
ps:connect(5000)
ps:subscribe(link_in, 1)
ps:subscribe(stream, 1)

local ack_seq = 0
local hb_seq  = 0
local state   = "idle"   -- idle -> ack_sent -> live
local last_hb_wall = 0

local function send_ack()
    ack_seq = ack_seq + 1
    local p = json_util.encode({
        type      = "link_bridge_ack",
        robot_id  = args.robot,
        bridge_id = "planner",
        ack_seq   = ack_seq,
        seq       = hb_seq,
        ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })
    ps:publish(planner_prefix .. "ack", p, 1, false)
    io.stderr:write(string.format("  SENT  link_bridge_ack seq=%d\n", ack_seq))
end

local function send_hb()
    hb_seq = hb_seq + 1
    local p = json_util.encode({
        type      = "link_bridge_heartbeat",
        robot_id  = args.robot,
        bridge_id = "planner",
        seq       = hb_seq,
        ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })
    ps:publish(planner_prefix .. "hb", p, 1, false)
end

local deadline = os.time() + args.duration
while os.time() < deadline do
    local msgs = ps:poll(200)
    for _, m in ipairs(msgs) do
        local ok, ev = pcall(json_util.decode, m.payload)
        if ok and ev then
            if m.topic == link_in then
                if ev.type == "link_announce" then
                    io.stderr:write(string.format(
                        "  RECV  link_announce seq=%s energy=%s\n",
                        tostring(ev.seq), tostring(ev.energy_remaining)))
                    send_ack()
                    state = "live"
                elseif ev.type == "link_confirm" then
                    io.stderr:write(string.format(
                        "  RECV  link_confirm (%d capabilities)\n",
                        ev.capabilities and #ev.capabilities or 0))
                elseif ev.type == "link_heartbeat" then
                    -- ignore keepalive
                end
            elseif m.topic == stream then
                -- summarise robot->planner events (kb_done mostly)
                if ev.type == "kb_done" then
                    io.stderr:write(string.format(
                        "  STREAM kb_done test=%s success=%s energy=%s%s\n",
                        tostring(ev.test_id), tostring(ev.success),
                        tostring(ev.energy_remaining),
                        ev.fault_reason and (" fault=" .. ev.fault_reason) or ""))
                end
            end
        end
    end

    local now = os.time()
    if state == "live" and now - last_hb_wall >= 1 then
        send_hb()
        last_hb_wall = now
    end
end

pcall(function() ps:disconnect() end)
pcall(function() ps:destroy() end)
io.stderr:write("mock_planner: done\n")
