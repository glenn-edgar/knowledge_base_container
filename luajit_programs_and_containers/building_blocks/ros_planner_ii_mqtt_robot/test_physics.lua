--[[
    test_physics.lua -- Standalone physics/HAL tests (no MQTT, no ChainTree).

    Run:   LUA_PATH="<json_util path>;./?.lua;;" luajit test_physics.lua
]]

local hal_mod = require("robot_hal")

local tests_passed, tests_failed = 0, 0

local function ok(cond, msg)
    if cond then
        tests_passed = tests_passed + 1
        io.stderr:write("  PASS  " .. msg .. "\n")
    else
        tests_failed = tests_failed + 1
        io.stderr:write("  FAIL  " .. msg .. "\n")
    end
end

local function approx(a, b, tol) return math.abs(a - b) <= tol end

-- Advance sim until cond() is true, or t_max seconds
local function run_until(hal, cond, t_max, step_s)
    step_s = step_s or 0.1
    t_max  = t_max or 30.0
    local t0 = hal:sim_time()
    while hal:sim_time() - t0 < t_max do
        hal:step(step_s)
        if cond() then return true end
    end
    return false
end

-- =========================================================================
io.stderr:write("\n[T1] Single-segment line: robot arrives at endpoint\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    local id  = hal:push_line(0, 0, 2.0, 0, 0, 0, 0.5)
    ok(id > 0, "push_line returned id>0")
    ok(hal:queue_depth() == 1, "queue_depth==1 after push")

    local done = run_until(hal, function()
        return hal:last_done_seg_id() == id and hal:is_stopped()
    end, 15.0)
    ok(done, "segment completes within 15s")
    local pose = hal:read_pose_truth()
    ok(approx(pose.x, 2.0, 0.15), string.format("final x=%.3f near 2.0", pose.x))
    ok(approx(pose.y, 0.0, 0.10), string.format("final y=%.3f near 0.0", pose.y))
    ok(hal:queue_depth() == 0, "queue empty after completion")
end

-- =========================================================================
io.stderr:write("\n[T2] Continuous 3-segment path (no stopping between)\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    local s1 = hal:push_line(0, 0, 2, 0, 0, 0, 0.5)
    local s2 = hal:push_spline(2, 0, 2, 2, 0, math.pi/2, 0.5)
    local s3 = hal:push_line(2, 2, 4, 2, math.pi/2, 0, 0.5)
    ok(hal:queue_depth() == 3, "all 3 segments queued")

    local motion_paused = false
    run_until(hal, function()
        if not motion_paused then
            local pose = hal:read_pose_truth()
            -- Check we never come fully to rest mid-path
            if hal:queue_depth() > 0 and hal:is_stopped() and hal:sim_time() > 1.0 then
                motion_paused = true
            end
        end
        return hal:last_done_seg_id() == s3 and hal:is_stopped()
    end, 40.0)
    ok(not motion_paused, "robot never fully stopped mid-path")
    ok(hal:last_done_seg_id() == s3, "all three segments completed")
    local pose = hal:read_pose_truth()
    ok(approx(pose.x, 4.0, 0.30), string.format("final x=%.3f near 4.0", pose.x))
end

-- =========================================================================
io.stderr:write("\n[T3] Stop + release resumes motion\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    hal:push_line(0, 0, 5, 0, 0, 0, 0.5)
    run_until(hal, function() return hal:sim_time() >= 3.0 end, 5.0)
    local p_mid = hal:read_pose_truth()
    ok(p_mid.v > 0.3, string.format("moving mid-path (v=%.2f)", p_mid.v))

    hal:request_stop()
    run_until(hal, function() return hal:is_stopped() end, 5.0)
    ok(hal:is_stopped(), "stopped after request_stop")
    local p_stop = hal:read_pose_truth()
    ok(p_stop.x < 5.0, string.format("stopped before endpoint (x=%.2f)", p_stop.x))

    hal:release_stop()
    run_until(hal, function()
        return hal:queue_depth() == 0 and hal:is_stopped()
    end, 20.0)
    local p_final = hal:read_pose_truth()
    ok(approx(p_final.x, 5.0, 0.15), string.format("resumed and reached endpoint (x=%.3f)", p_final.x))
end

-- =========================================================================
io.stderr:write("\n[T4] Arm tool: revolute ramp to target + back\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    local target_rad = math.rad(90)
    ok(hal:begin_tool_move("arm", target_rad, math.rad(60)) == 0, "begin_tool_move accepted")
    run_until(hal, function()
        local ts = hal:read_tool_status(0)
        return bit.band(ts.flags, hal.TOOL_F.AT_TARGET) ~= 0
    end, 5.0)
    local ts1 = hal:read_tool_status(0)
    ok(approx(ts1.value, target_rad, 0.01), string.format("arm at target (%.3f)", ts1.value))

    -- Back to 0
    hal:begin_tool_move("arm", 0, math.rad(60))
    run_until(hal, function()
        local ts = hal:read_tool_status(0)
        return bit.band(ts.flags, hal.TOOL_F.AT_TARGET) ~= 0 and math.abs(ts.value) < 0.01
    end, 5.0)
    local ts2 = hal:read_tool_status(0)
    ok(approx(ts2.value, 0, 0.01), string.format("arm back to 0 (%.3f)", ts2.value))
end

-- =========================================================================
io.stderr:write("\n[T5] Charger dock + charge\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    -- Drive to charger at (8, 0, 0)
    hal:push_line(0, 0, 8, 0, 0, 0, 0.5)
    run_until(hal, function() return hal:queue_depth() == 0 and hal:is_stopped() end, 25.0)
    local pose = hal:read_pose_truth()
    ok(approx(pose.x, 8.0, 0.20), string.format("docked near charger (x=%.3f)", pose.x))

    local si = hal:station_at_pose("charger")
    ok(si >= 0, "station_at_pose(charger) found (idx=" .. si .. ")")

    -- Begin charge to capacity
    local b0 = hal:battery_j()
    -- Drain a bit first so we see charging
    -- (there's been some energy used already during the drive)
    ok(b0 < 100000, string.format("battery already drained a bit (%.0f J)", b0))
    local rc = hal:begin_charge("charge_port", 100000.0)
    ok(rc == 0, "begin_charge accepted")
    run_until(hal, function()
        local ts = hal:read_tool_status(2)
        return bit.band(ts.flags, hal.TOOL_F.AT_TARGET) ~= 0
    end, 60.0)
    local b1 = hal:battery_j()
    ok(b1 > b0, string.format("battery charged up (%.0f -> %.0f J)", b0, b1))
    ok(approx(b1, 100000, 100), "battery near capacity")
end

-- =========================================================================
io.stderr:write("\n[T6] Payload pickup increases effective mass\n")
-- =========================================================================
do
    local hal = hal_mod.new({ dir = ".", mode = "sim" })
    -- Drive to shipping dock at (6, 6, pi/2)
    hal:push_line(0, 0, 6, 0, 0, 0, 0.5)
    hal:push_line(6, 0, 6, 6, 0, math.pi/2, 0.5)
    run_until(hal, function() return hal:queue_depth() == 0 and hal:is_stopped() end, 40.0)
    local pose = hal:read_pose_truth()
    io.stderr:write(string.format("    pose after drive: x=%.2f y=%.2f h=%.2f\n",
        pose.x, pose.y, pose.heading))
    local si = hal:station_at_pose("load_dock")
    ok(si >= 0, "at load_dock station")

    if si >= 0 then
        ok(hal:payload_mass() == 0, "no payload before grip")
        hal:begin_grip("gripper")
        run_until(hal, function()
            local ts = hal:read_tool_status(1)
            return bit.band(ts.flags, hal.TOOL_F.GRASPED) ~= 0
        end, 3.0)
        ok(hal:payload_mass() > 0, string.format("payload acquired (%.1f kg)", hal:payload_mass()))
    end
end

-- =========================================================================
io.stderr:write("\n" .. string.format("%d passed, %d failed\n", tests_passed, tests_failed))
if tests_failed > 0 then os.exit(1) end
