------------------------------------------------------------------------
-- Equipment & I/O DSL — FIRST LEGO League SPIKE Prime Robot
--
-- Defines: ports, motors, sensors, drivebase geometry, guard thresholds,
--          virtual function slots, and status stream fields.
-- Changes when: the physical robot is rebuilt.
--
-- This file compiles into:
--   1. Blackboard field definitions (status stream layout)
--   2. Virtual function slot table (leaf node vocabulary)
--   3. Guard threshold constants
--   4. Equipment rows in robot_kb.sqlite
------------------------------------------------------------------------

local Equipment = {}

------------------------------------------------------------------------
-- 1. ROBOT IDENTITY
------------------------------------------------------------------------

Equipment.robot = {
    name        = "fll_runner",
    hub         = "SPIKE_PRIME",          -- SPIKE Prime or SPIKE Essential
    firmware    = "pybricks",             -- pybricks 3.x MicroPython
    season      = "2025-2026",
}

------------------------------------------------------------------------
-- 2. PORT ASSIGNMENTS
--    Physical port letter -> logical role.
--    Chassis ports (drive + sensors) are fixed across all runs.
--    Attachment ports (C, D) change per jig configuration.
------------------------------------------------------------------------

Equipment.chassis_ports = {
    -- Drive motors (paired into drivebase) — never change
    { port = "A",  role = "drive_left",   device = "MOTOR_L" },
    { port = "B",  role = "drive_right",  device = "MOTOR_L" },

    -- Sensors — never change
    { port = "E",  role = "line_sensor",  device = "COLOR" },
    { port = "F",  role = "range_sensor", device = "ULTRASONIC" },
}

------------------------------------------------------------------------
-- 3. DRIVEBASE GEOMETRY
--    Pybricks DriveBase constructor parameters.
--    All dimensions in millimeters, angles in degrees.
------------------------------------------------------------------------

Equipment.drivebase = {
    left_motor      = "A",                -- port letter
    right_motor     = "B",
    wheel_diameter  = 56.0,               -- mm (SPIKE large wheel)
    axle_track      = 128.0,              -- mm center-to-center
    straight_speed  = 200,                -- mm/s default
    straight_accel  = 400,                -- mm/s^2
    turn_rate       = 150,                -- deg/s default
    turn_accel      = 300,                -- deg/s^2
    positive_dir    = "clockwise",        -- motor positive direction
}

------------------------------------------------------------------------
-- 4. GUARD THRESHOLDS
--    Constants consumed by guard_check() on every 10ms monitor tick.
--    Each threshold implicitly defines a fault predicate slot.
------------------------------------------------------------------------

Equipment.guards = {
    battery_min_mv      = 6800,           -- millivolts — warn below this
    battery_critical_mv = 6200,           -- millivolts — emergency stop
    tilt_max_deg        = 45,             -- degrees from level — tip-over
    bump_accel_mg       = 2500,           -- milli-g spike — collision
    bump_window_ms      = 50,             -- samples to confirm bump
    comm_timeout_ms     = 500,            -- BLE silence -> fault
    stall_torque_pct    = 80,             -- motor duty % at stall
    stall_confirm_ms    = 200,            -- stall must persist this long
    overcurrent_ma      = 1500,           -- per-motor current limit
    runaway_speed_pct   = 120,            -- speed > target% -> fault
}

------------------------------------------------------------------------
-- 5. ATTACHMENT JIGS
--    Named physical mechanisms that clip onto the robot chassis.
--    Kids swap jigs between runs in the launch area.
--    Each jig defines what is on ports C and D, motor limits,
--    and which additional virtual function slots it provides.
--
--    Jig slots supplement the base slots (section 8).
--    The compiler validates that each run only uses slots available
--    from its jig + the base set.
------------------------------------------------------------------------

Equipment.jigs = {

    -- Jig 1: Pusher — passive flat blade, no motors needed
    -- Used to ram levers and spin wheels by driving into them.
    pusher = {
        menu_id  = 1,
        ports    = {},                    -- no motors, passive attachment
        motors   = {},
        slots    = {},                    -- no extra slots beyond base
    },

    -- Jig 2: Gripper arm — lift motor (C) + gripper motor (D)
    -- Used to pick up and carry objects, deliver payloads.
    gripper_arm = {
        menu_id  = 2,
        ports    = {
            { port = "C", role = "arm_lift", device = "MOTOR_M" },
            { port = "D", role = "gripper",  device = "MOTOR_M" },
        },
        motors   = {
            arm_lift = {
                port          = "C",
                min_angle     = 0,
                max_angle     = 160,
                home_angle    = 0,
                default_speed = 400,
                default_torque = 60,
            },
            gripper = {
                port          = "D",
                min_angle     = 0,
                max_angle     = 90,
                home_angle    = 0,
                default_speed = 300,
                default_torque = 40,
            },
        },
        slots    = {
            -- Predicates available only with this jig
            { name = "IS_ARM_DONE",     kind = "predicate", reads = "arm_lift.done" },
            { name = "IS_GRIP_DONE",    kind = "predicate", reads = "gripper.done" },
            { name = "IS_ARM_STALLED",  kind = "predicate", reads = "arm_lift.stalled" },
            -- Oneshots
            { name = "ARM_STOP",        kind = "oneshot",  targets = {"arm_lift"} },
            { name = "ARM_HOLD",        kind = "oneshot",  targets = {"arm_lift"} },
            { name = "GRIP_STOP",       kind = "oneshot",  targets = {"gripper"} },
            { name = "GRIP_HOLD",       kind = "oneshot",  targets = {"gripper"} },
            -- Main functions
            { name = "ARM_TO_ANGLE",    kind = "main", params = {"angle_deg"},
              completes_on = "IS_ARM_DONE" },
            { name = "ARM_HOME",        kind = "main", params = {},
              completes_on = "IS_ARM_DONE" },
            { name = "GRIP_CLOSE",      kind = "main", params = {"torque_pct"},
              completes_on = "IS_GRIP_DONE" },
            { name = "GRIP_OPEN",       kind = "main", params = {},
              completes_on = "IS_GRIP_DONE" },
        },
    },

    -- Jig 3: Cargo tray — single lift motor (C), no gripper
    -- Used to carry and tip pre-loaded cargo into delivery zones.
    cargo_tray = {
        menu_id  = 3,
        ports    = {
            { port = "C", role = "tray_lift", device = "MOTOR_M" },
        },
        motors   = {
            tray_lift = {
                port          = "C",
                min_angle     = 0,
                max_angle     = 120,
                home_angle    = 0,
                default_speed = 300,
                default_torque = 50,
            },
        },
        slots    = {
            { name = "IS_TRAY_DONE",    kind = "predicate", reads = "tray_lift.done" },
            { name = "TRAY_STOP",       kind = "oneshot",   targets = {"tray_lift"} },
            { name = "TRAY_HOLD",       kind = "oneshot",   targets = {"tray_lift"} },
            { name = "TRAY_TO_ANGLE",   kind = "main", params = {"angle_deg"},
              completes_on = "IS_TRAY_DONE" },
            { name = "TRAY_HOME",       kind = "main", params = {},
              completes_on = "IS_TRAY_DONE" },
            { name = "TRAY_DUMP",       kind = "main", params = {},
              completes_on = "IS_TRAY_DONE" },
        },
    },
}

------------------------------------------------------------------------
-- 6. SENSOR CONFIGURATION
--    Mode and threshold settings per sensor.
--    Each entry produces sensor-specific predicate slots.
------------------------------------------------------------------------

Equipment.sensors = {
    line_sensor = {
        port            = "E",
        mode            = "REFLECTION",   -- Pybricks Color.REFLECTION
        line_threshold  = 30,             -- reflection < this = on black line
        white_threshold = 70,             -- reflection > this = on white
    },
    range_sensor = {
        port            = "F",
        mode            = "DISTANCE",     -- Pybricks Ultrasonic.distance()
        close_mm        = 50,             -- "obstacle close" predicate
        far_mm          = 300,            -- "clear ahead" predicate
    },
}

------------------------------------------------------------------------
-- 7. STATUS STREAM FIELDS
--    Declares every field pushed at 10Hz from hub to host blackboard.
--    Grouped by device. Types: int32, uint32, uint16, float, bool.
--    Order here = order in the JSON status packet.
------------------------------------------------------------------------

Equipment.status_stream = {

    -- Per drive motor (x2: left, right)
    drive_left  = {
        { field = "angle",    type = "int32"  },  -- cumulative degrees
        { field = "speed",    type = "int16"  },  -- current deg/s
        { field = "load",     type = "int16"  },  -- duty cycle %
        { field = "stalled",  type = "bool"   },
    },
    drive_right = {
        { field = "angle",    type = "int32"  },
        { field = "speed",    type = "int16"  },
        { field = "load",     type = "int16"  },
        { field = "stalled",  type = "bool"   },
    },

    -- Per attachment motor
    arm_lift = {
        { field = "angle",    type = "int32"  },
        { field = "speed",    type = "int16"  },
        { field = "load",     type = "int16"  },
        { field = "stalled",  type = "bool"   },
        { field = "done",     type = "bool"   },
    },
    gripper = {
        { field = "angle",    type = "int32"  },
        { field = "speed",    type = "int16"  },
        { field = "load",     type = "int16"  },
        { field = "stalled",  type = "bool"   },
        { field = "done",     type = "bool"   },
    },

    -- Drivebase aggregate
    drivebase = {
        { field = "distance", type = "int32"  },  -- mm since reset
        { field = "angle",    type = "int32"  },  -- heading deg since reset
        { field = "speed",    type = "int16"  },  -- current mm/s
        { field = "done",     type = "bool"   },
        { field = "stalled",  type = "bool"   },
    },

    -- Color sensor
    line_sensor = {
        { field = "reflection", type = "uint16" },  -- 0-100
        { field = "hue",        type = "uint16" },  -- 0-359
        { field = "saturation", type = "uint16" },  -- 0-100
        { field = "value",      type = "uint16" },  -- 0-100 (HSV V)
        { field = "color_id",   type = "uint16" },  -- Pybricks Color enum
    },

    -- Ultrasonic sensor
    range_sensor = {
        { field = "distance_mm", type = "uint16" },  -- 0-2000, 2001=no echo
    },

    -- IMU (built into hub)
    imu = {
        { field = "heading",  type = "float"  },  -- degrees, yaw
        { field = "pitch",    type = "float"  },  -- degrees
        { field = "roll",     type = "float"  },  -- degrees
        { field = "accel_x",  type = "float"  },  -- milli-g
        { field = "accel_y",  type = "float"  },
        { field = "accel_z",  type = "float"  },
        { field = "ready",    type = "bool"   },  -- gyro calibrated
    },

    -- System
    system = {
        { field = "battery_mv",     type = "uint16" },
        { field = "hub_temp_c",     type = "uint16" },
        { field = "tick_seq",       type = "uint32" },  -- monotonic counter
        { field = "comm_last_tick", type = "uint32" },  -- last good rx tick
    },
}

------------------------------------------------------------------------
-- 8. BASE VIRTUAL FUNCTION SLOTS
--    Slots available to ALL runs regardless of jig.
--    These are chassis, drivebase, sensor, and safety slots.
--    Jig-specific slots are in Equipment.jigs[name].slots.
------------------------------------------------------------------------

Equipment.base_slots = {

    -- Predicates — chassis sensors
    { name = "IS_ON_LINE",          kind = "predicate",
      reads = "line_sensor.reflection", compare = "lt", threshold = "line_threshold" },
    { name = "IS_ON_WHITE",         kind = "predicate",
      reads = "line_sensor.reflection", compare = "gt", threshold = "white_threshold" },
    { name = "IS_OBSTACLE_CLOSE",   kind = "predicate",
      reads = "range_sensor.distance_mm", compare = "lt", threshold = "close_mm" },
    { name = "IS_PATH_CLEAR",       kind = "predicate",
      reads = "range_sensor.distance_mm", compare = "gt", threshold = "far_mm" },
    { name = "IS_DRIVE_DONE",       kind = "predicate",
      reads = "drivebase.done" },
    { name = "IS_DRIVE_STALLED",    kind = "predicate",
      reads = "drivebase.stalled" },
    { name = "IS_IMU_READY",        kind = "predicate",
      reads = "imu.ready" },
    { name = "IS_HEADING_AT",       kind = "predicate",
      reads = "imu.heading", compare = "near", tolerance = 3.0 },

    -- Fault predicates
    { name = "FAULT_BATTERY_LOW",   kind = "predicate",
      reads = "system.battery_mv", compare = "lt", threshold = "battery_min_mv" },
    { name = "FAULT_BATTERY_CRIT",  kind = "predicate",
      reads = "system.battery_mv", compare = "lt", threshold = "battery_critical_mv" },
    { name = "FAULT_TILT",          kind = "predicate",
      reads = "imu.pitch", compare = "abs_gt", threshold = "tilt_max_deg" },
    { name = "FAULT_BUMP",          kind = "predicate",
      reads = "_derived.bump_detected" },
    { name = "FAULT_COMM_TIMEOUT",  kind = "predicate",
      reads = "_derived.comm_timeout" },

    -- Oneshots — chassis only (no attachment motors)
    { name = "STOP_ALL",            kind = "oneshot",  targets = {"drivebase"} },
    { name = "BRAKE_ALL",           kind = "oneshot",  targets = {"drivebase"} },
    { name = "RESET_DRIVEBASE",     kind = "oneshot",  targets = {"drivebase"} },
    { name = "RESET_HEADING",       kind = "oneshot",  targets = {"imu"} },
    { name = "EMERGENCY_STOP",      kind = "oneshot",  targets = {"all"}, priority = "critical" },
    { name = "READ_SENSORS",        kind = "oneshot",  targets = {"all"} },

    -- Main functions — drivebase and navigation
    { name = "DRIVE_STRAIGHT",      kind = "main",  params = {"distance_mm"} },
    { name = "DRIVE_TURN",          kind = "main",  params = {"angle_deg"} },
    { name = "DRIVE_CURVE",         kind = "main",  params = {"radius_mm", "angle_deg"} },
    { name = "DRIVE_TO_LINE",       kind = "main",  params = {},
      completes_on = "IS_ON_LINE" },
    { name = "LINE_FOLLOW",         kind = "main",  params = {"distance_mm"},
      completes_on = "IS_DRIVE_DONE" },
    { name = "WALL_RIDE",           kind = "main",  params = {"distance_mm", "wall_side"},
      completes_on = "IS_DRIVE_DONE" },
    { name = "WAIT_SECONDS",        kind = "main",  params = {"seconds"} },
    { name = "ALIGN_TO_WALL",       kind = "main",  params = {"speed"},
      completes_on = "IS_DRIVE_STALLED" },
    { name = "BUMP_DETECT",         kind = "main",  params = {},
      completes_on = "FAULT_BUMP" },

    -- Spline path following — smooth curve through (x,y) control points.
    -- Hub evaluates cubic Bezier segments, computes v_left/v_right per tick
    -- using IMU heading + drivebase odometry feedback.
    { name = "SPLINE_FOLLOW",       kind = "main",
      params = {"segment_count", "control_points"},
      completes_on = "IS_DRIVE_DONE" },
}

------------------------------------------------------------------------
-- 8b. SPLINE CONFIGURATION
--     Parameters for the Catmull-Rom spline path generator.
------------------------------------------------------------------------

Equipment.spline = {
    tau             = 0.5,                -- Catmull-Rom tension (0.5 = standard)
    sample_interval_mm = 5.0,             -- distance between path samples sent to hub
    base_speed_mm_s = 150,                -- default cruise speed on spline paths
    min_speed_pct   = 30,                 -- minimum speed in tight curves (% of base)
    lookahead_mm    = 40,                 -- how far ahead the hub looks for heading correction
    heading_kp      = 2.0,               -- proportional gain for heading error correction
}

------------------------------------------------------------------------
-- 9. MENU FIELD
--    Blackboard field that selects which run/jig to execute.
--    Written by the launch-area operator (kid pressing hub button).
--    Read by the se_state_machine dispatcher.
------------------------------------------------------------------------

Equipment.menu = {
    field_name  = "run_select",
    field_type  = "int32",
    default     = 0,                      -- 0 = idle, waiting for selection
}

------------------------------------------------------------------------
-- 10. COMPETITION-SPECIFIC CONSTANTS
--    FLL table dimensions and rules.
------------------------------------------------------------------------

Equipment.field = {
    table_length_mm     = 2362,           -- 8 ft inner
    table_width_mm      = 1143,           -- 4 ft inner
    wall_height_mm      = 89,             -- 3.5 inches
    launch_area_mm      = { x = 0, y = 0, w = 450, h = 450 },
    mat_surface         = "vinyl",        -- affects wheel grip
    precision_tokens    = 6,              -- max tokens per match
    match_duration_s    = 150,            -- 2.5 minutes
}

return Equipment
