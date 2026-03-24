------------------------------------------------------------------------
-- Mission DSL — FIRST LEGO League Match Strategy
--
-- Defines: match plan, run sequences, action steps, and scoring goals.
-- Changes when: strategy changes (mission order, run grouping, timing).
--
-- Key constraint: missions are grouped into runs by ATTACHMENT JIG.
-- All missions in a run must be accomplishable with the same jig.
-- Between runs the robot returns to launch and the kids swap jigs.
--
-- Execution model: a single se_state_machine reads the "run_select"
-- blackboard field (set by hub button press in the launch area).
-- Each se_case corresponds to a run/jig pair. The state machine
-- re-enters after each run completes, ready for the next menu pick.
--
-- References:
--   Equipment base_slots  — chassis leaf nodes (equipment_dsl.lua)
--   Equipment jigs         — per-jig slots and motor configs
--   Map waypoints          — navigation targets (map_dsl.lua)
--   Map missions           — physical models to interact with
--
-- Compiles to:
--   1. S-expression tree with se_state_machine dispatcher
--   2. KB mission plan rows (ltree path: plan.run.<N>.step.<M>)
--   3. Planner precondition/effect rows for future auto-planning
------------------------------------------------------------------------

local Mission = {}

------------------------------------------------------------------------
-- 1. MATCH CONFIGURATION
------------------------------------------------------------------------

Mission.match = {
    total_time_s    = 150,                -- 2.5 minute match
    runs            = 3,                  -- planned number of launch-return cycles
    reserve_s       = 10,                 -- time buffer for final return to launch
}

------------------------------------------------------------------------
-- 2. SAFETY WRAPPER
--    Applied to every run automatically. The mission compiler wraps
--    each run body inside this guard structure.
------------------------------------------------------------------------

Mission.safety = {

    guards = {
        { name   = "battery_critical",
          fault  = "FAULT_BATTERY_CRIT",
          action = { "EMERGENCY_STOP" },
          abort  = true },

        { name   = "comm_timeout",
          fault  = "FAULT_COMM_TIMEOUT",
          action = { "BRAKE_ALL" },
          abort  = true },

        { name   = "tilt_detect",
          fault  = "FAULT_TILT",
          action = { "STOP_ALL" },
          abort  = false },

        { name   = "bump_detect",
          fault  = "FAULT_BUMP",
          action = { "BRAKE_ALL", "WAIT_SECONDS", 0.5 },
          abort  = false },

        { name   = "battery_low",
          fault  = "FAULT_BATTERY_LOW",
          action = {},
          abort  = false },
    },

    finally = { "STOP_ALL" },
}

------------------------------------------------------------------------
-- 3. NAVIGATION HELPERS
------------------------------------------------------------------------

Mission.nav = {}

function Mission.nav.goto(from, to)
    return { op = "navigate", from = from, to = to }
end

function Mission.nav.align_wall(speed)
    return { op = "sequence", steps = {
        { slot = "ALIGN_TO_WALL",     params = { speed = speed or 80 } },
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = -30 } },
        { slot = "RESET_DRIVEBASE" },
        { slot = "RESET_HEADING" },
    }}
end

function Mission.nav.find_line()
    return { op = "sequence", steps = {
        { slot = "DRIVE_TO_LINE" },
        { slot = "RESET_DRIVEBASE" },
    }}
end

-- spline_route: follow a named spline route from Map.spline_routes.
-- The compiler fits a Catmull-Rom spline through the route waypoints
-- and emits a single SPLINE_FOLLOW slot with cubic Bezier control points.
-- The robot curves smoothly through all waypoints without stopping.
function Mission.nav.spline_route(route_name)
    return { op = "spline_route", route = route_name }
end

------------------------------------------------------------------------
-- 4. MISSION ACTION TEMPLATES
--    Each template maps a mission interact type to a slot sequence.
--    Templates are tagged with required_jig — the compiler validates
--    that the run's jig provides the slots the template uses.
------------------------------------------------------------------------

Mission.actions = {}

-- push_lever: passive pusher, drive into lever, reverse out
-- Works with ANY jig (only uses base drivebase slots)
function Mission.actions.push_lever(mission)
    return { op = "sequence", required_jig = nil, steps = {
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = mission.push_mm + 20 } },
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = -(mission.push_mm + 20) } },
    }}
end

-- drive_over: cross elevated structure, drivebase only
-- Works with ANY jig
function Mission.actions.drive_over(mission)
    return { op = "sequence", required_jig = nil, steps = {
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = mission.drive_mm } },
    }}
end

-- spin_wheel: push against wheel, wait, reverse
-- Works with ANY jig (passive contact via chassis)
function Mission.actions.spin_wheel(mission)
    return { op = "sequence", required_jig = nil, steps = {
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = mission.contact_mm } },
        { slot = "WAIT_SECONDS",      params = { seconds = 1.0 } },
        { slot = "DRIVE_STRAIGHT",    params = { distance_mm = -mission.contact_mm } },
    }}
end

-- grip_and_carry: lower arm, close gripper, lift
-- REQUIRES gripper_arm jig
function Mission.actions.grip_and_carry(mission)
    return { op = "sequence", required_jig = "gripper_arm", steps = {
        { slot = "ARM_TO_ANGLE",      params = { angle_deg = mission.grip_angle or 140 } },
        { slot = "GRIP_CLOSE",        params = { torque_pct = 40 } },
        { slot = "ARM_HOME" },
    }}
end

-- deliver_payload: open gripper to drop a carried piece
-- REQUIRES gripper_arm jig
function Mission.actions.deliver_payload(mission)
    return { op = "sequence", required_jig = "gripper_arm", steps = {
        { slot = "ARM_TO_ANGLE",      params = { angle_deg = 40 } },
        { slot = "GRIP_OPEN" },
        { slot = "WAIT_SECONDS",      params = { seconds = 0.3 } },
        { slot = "ARM_HOME" },
    }}
end

-- tray_deliver: tilt cargo tray to dump contents
-- REQUIRES cargo_tray jig
function Mission.actions.tray_deliver(mission)
    return { op = "sequence", required_jig = "cargo_tray", steps = {
        { slot = "TRAY_DUMP" },
        { slot = "WAIT_SECONDS",      params = { seconds = 0.5 } },
        { slot = "TRAY_HOME" },
    }}
end

------------------------------------------------------------------------
-- 5. RUN DEFINITIONS
--    Each run specifies:
--      jig         — which attachment jig is mounted (from Equipment.jigs)
--      menu_id     — integer written to run_select field (hub button)
--      preload     — pieces loaded on robot before this run
--      steps       — mission + navigation sequence
--
--    The compiler groups runs by jig to validate slot usage.
--    At runtime, se_state_machine dispatches on menu_id.
------------------------------------------------------------------------

Mission.runs = {

    ---------- RUN 1: Pusher jig — west side rams + spins ----------------
    -- Missions: m6 (push lever), m4 (spin wheel), m1 (push lever)
    -- All use passive contact — no motor-driven attachment needed.
    -- Also does m2 (drive_over) on the way back since it's drivebase-only.
    {
        name          = "run_1_pusher",
        jig           = "pusher",
        menu_id       = 1,
        time_budget_s = 50,
        preload       = {},

        steps = {
            { slot = "RESET_DRIVEBASE" },
            { slot = "RESET_HEADING" },

            -- Smooth curve from launch to spine
            Mission.nav.spline_route("launch_to_spine"),

            ---- Mission 6: flip switch (10 pts) ----
            Mission.nav.goto("spine_west", "m6_approach"),
            { op = "do_mission", mission = "m6_flip_switch" },
            Mission.nav.goto("m6_approach", "spine_west"),

            ---- Mission 4: wheel spin (15 pts) ----
            Mission.nav.goto("spine_west", "branch_n_base"),
            Mission.nav.goto("branch_n_base", "m4_approach"),
            { op = "do_mission", mission = "m4_wheel_spin" },
            Mission.nav.goto("m4_approach", "branch_n_base"),

            ---- Mission 1: solar panel push (20 pts) ----
            Mission.nav.goto("branch_n_base", "branch_n_mid"),
            Mission.nav.goto("branch_n_mid", "m1_approach"),
            { op = "do_mission", mission = "m1_solar_panel" },
            Mission.nav.goto("m1_approach", "branch_n_mid"),

            ---- Mission 2: bridge cross (30 pts, drivebase only) ----
            Mission.nav.goto("branch_n_mid", "branch_n_base"),
            Mission.nav.goto("branch_n_base", "spine_center"),
            Mission.nav.goto("spine_center", "m2_approach"),
            { op = "do_mission", mission = "m2_bridge_cross" },
            Mission.nav.goto("m2_approach", "spine_center"),

            -- Smooth return to launch
            Mission.nav.goto("spine_center", "branch_n_base"),
            Mission.nav.goto("branch_n_base", "spine_west"),
            Mission.nav.spline_route("spine_to_home"),
        },
        expected_points = 75,             -- m6(10) + m4(15) + m1(20) + m2(30)
    },

    ---------- RUN 2: Gripper arm jig — collect sample -------------------
    -- Missions: m5 (grip_and_carry)
    -- Requires gripper arm for picking up water sample.
    {
        name          = "run_2_gripper",
        jig           = "gripper_arm",
        menu_id       = 2,
        time_budget_s = 50,
        preload       = {},

        steps = {
            { slot = "RESET_DRIVEBASE" },
            { slot = "RESET_HEADING" },

            -- Smooth curve from launch to spine
            Mission.nav.spline_route("launch_to_spine"),

            -- Smooth traverse to east side
            Mission.nav.spline_route("spine_to_south"),
            Mission.nav.goto("branch_s_base", "loop_e_entry"),

            ---- Mission 5: collect sample (25 pts) ----
            Mission.nav.goto("loop_e_entry", "loop_e_far"),
            Mission.nav.goto("loop_e_far", "m5_approach"),
            { op = "do_mission", mission = "m5_collect_sample" },
            Mission.nav.goto("m5_approach", "loop_e_far"),

            -- Return via east loop and spine (carrying water_sample)
            Mission.nav.goto("loop_e_far", "loop_e_entry"),
            Mission.nav.goto("loop_e_entry", "branch_s_base"),
            Mission.nav.goto("branch_s_base", "spine_center"),
            Mission.nav.goto("spine_center", "branch_n_base"),
            Mission.nav.goto("branch_n_base", "spine_west"),

            -- Smooth return to launch
            Mission.nav.spline_route("spine_to_home"),
        },
        expected_points = 25,             -- m5(25)
    },

    ---------- RUN 3: Cargo tray jig — deliver energy unit ---------------
    -- Missions: m3 (tray_deliver)
    -- Pre-loaded energy_unit tipped out via cargo tray.
    {
        name          = "run_3_cargo",
        jig           = "cargo_tray",
        menu_id       = 3,
        time_budget_s = 40,
        preload       = { "energy_unit" },

        steps = {
            { slot = "RESET_DRIVEBASE" },
            { slot = "RESET_HEADING" },

            -- Smooth curve from launch to spine
            Mission.nav.spline_route("launch_to_spine"),

            -- Smooth traverse to south branch
            Mission.nav.spline_route("spine_to_south"),

            ---- Mission 3: cargo deliver (20 pts) ----
            Mission.nav.goto("branch_s_base", "branch_s_mid"),
            Mission.nav.goto("branch_s_mid", "m3_approach"),
            { op = "do_mission", mission = "m3_cargo_deliver" },
            Mission.nav.goto("m3_approach", "branch_s_mid"),

            -- Return to launch
            Mission.nav.goto("branch_s_mid", "branch_s_base"),
            Mission.nav.goto("branch_s_base", "spine_center"),
            Mission.nav.goto("spine_center", "branch_n_base"),
            Mission.nav.goto("branch_n_base", "spine_west"),
            Mission.nav.spline_route("spine_to_home"),
        },
        expected_points = 20,             -- m3(20)
    },
}

------------------------------------------------------------------------
-- 6. MISSION-TO-JIG COMPATIBILITY
--    Which missions can be done with which jig.
--    The compiler uses this + action template required_jig to validate.
--    Also enables the planner to reassign missions between runs.
------------------------------------------------------------------------

Mission.jig_compat = {
    -- pusher: all drive-contact missions
    pusher      = { "m1_solar_panel", "m2_bridge_cross", "m4_wheel_spin", "m6_flip_switch" },
    -- gripper_arm: pickup/delivery missions
    gripper_arm = { "m5_collect_sample", "m3_cargo_deliver" },
    -- cargo_tray: delivery missions (different mechanism)
    cargo_tray  = { "m3_cargo_deliver" },
}

------------------------------------------------------------------------
-- 7. MISSION-TO-ACTION OVERRIDE
--    When the same mission can be done by different jigs using
--    different action templates. Default is Map.missions.interact.
------------------------------------------------------------------------

Mission.action_override = {
    -- m3 uses deliver_payload with gripper_arm, tray_deliver with cargo_tray
    m3_cargo_deliver = {
        gripper_arm = "deliver_payload",
        cargo_tray  = "tray_deliver",
    },
}

------------------------------------------------------------------------
-- 8. PRECONDITIONS AND EFFECTS
------------------------------------------------------------------------

Mission.preconditions = {
    m1_solar_panel = {
        requires  = { at = "m1_approach" },
        produces  = { m1_done = true },
    },
    m2_bridge_cross = {
        requires  = { at = "m2_approach" },
        produces  = { m2_done = true },
    },
    m3_cargo_deliver = {
        requires  = { at = "m3_approach", carrying = "energy_unit" },
        produces  = { m3_done = true, carrying = nil },
    },
    m4_wheel_spin = {
        requires  = { at = "m4_approach" },
        produces  = { m4_done = true },
    },
    m5_collect_sample = {
        requires  = { at = "m5_approach", carrying = nil },
        produces  = { m5_done = true, carrying = "water_sample" },
    },
    m6_flip_switch = {
        requires  = { at = "m6_approach" },
        produces  = { m6_done = true },
    },
}

------------------------------------------------------------------------
-- 9. TIME GUARDS
------------------------------------------------------------------------

Mission.timing = {
    step_timeout_s      = 8,
    nav_timeout_per_mm  = 0.015,
    nav_timeout_min_s   = 3,
    action_timeout_s    = 12,
    run_overtime_action = "return_home",
}

------------------------------------------------------------------------
-- 10. MATCH PLAN SUMMARY
------------------------------------------------------------------------

Mission.plan_summary = {
    total_expected_points = 120,          -- 75 + 25 + 20
    total_time_budget_s   = 140,          -- 50 + 50 + 40 (10s reserve)
    run_order = {
        { run = "run_1_pusher",   jig = "pusher",      menu_id = 1, pts = 75 },
        { run = "run_2_gripper",  jig = "gripper_arm",  menu_id = 2, pts = 25 },
        { run = "run_3_cargo",    jig = "cargo_tray",   menu_id = 3, pts = 20 },
    },
    -- Strategy: run 1 does all pusher-compatible missions (75 pts)
    --           run 2 does grip-and-carry (25 pts)
    --           run 3 does cargo delivery (20 pts, drop first if time short)
}

return Mission
