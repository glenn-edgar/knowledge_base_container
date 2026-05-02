--[[
    test_mock_planner.lua -- Long-running mission-side keepalive.

    After the planner_test_peer extraction, this is a thin wrapper:
    bring rover live, then pump heartbeats for `--duration` seconds.
    Used by run_tests.sh as a background process so test_random_paths
    can drive commands against an already-live rover.

    Usage: luajit test_mock_planner.lua [--robot rover_1]
                                        [--site moonbase.alpha.surface_ops]
                                        [--host localhost] [--port 1883]
                                        [--duration 120]
]]

local planner_test_peer = require("planner_test_peer")

local args = { robot = "rover_1", site = "moonbase.alpha.surface_ops",
               host = "localhost", port = 1883, duration = 120 }
do
    local i = 1
    while i <= #arg do
        local a = arg[i]
        if a == "--robot"    then args.robot    = arg[i+1]; i = i + 2
        elseif a == "--site" then args.site     = arg[i+1]; i = i + 2
        elseif a == "--host" then args.host     = arg[i+1]; i = i + 2
        elseif a == "--port" then args.port     = tonumber(arg[i+1]); i = i + 2
        elseif a == "--duration" then args.duration = tonumber(arg[i+1]); i = i + 2
        else io.stderr:write("bad arg: " .. a .. "\n"); os.exit(1) end
    end
end

io.stderr:write(string.format(
    "mock_planner: robot=%s host=%s:%d duration=%ds\n",
    args.robot, args.host, args.port, args.duration))

local peer = planner_test_peer.new{
    robot = args.robot, site = args.site,
    host  = args.host,  port = args.port,
    verbose = true,
}

local ok, err = peer:bring_robot_live(args.duration)
if not ok then
    io.stderr:write("mock_planner: " .. err .. "\n")
    peer:close()
    os.exit(1)
end

peer:pump_for(args.duration)
peer:close()
io.stderr:write("mock_planner: done\n")
