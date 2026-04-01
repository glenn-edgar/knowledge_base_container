-- robot_dsl.lua — Mobile robot equipment definition
--
-- Defines the SPIKE Prime robot: drivebase geometry, port assignments,
-- and guard thresholds. Compiles to kb table rows under "robot.*" paths.

robot = {
  name = "factory_bot",
  hub  = "spike_prime",

  drivebase = {
    wheel_diameter = 56,    -- mm
    axle_track     = 112,   -- mm
    straight_speed = 150,   -- mm/s (default cruise speed)
    turn_rate      = 90,    -- deg/s
  },

  ports = {
    A = { device = "medium_motor",  role = "left_drive"  },
    B = { device = "medium_motor",  role = "right_drive" },
    C = { device = "medium_motor",  role = "arm"         },
    D = { device = "color_sensor",  role = "line_detect" },
    E = { device = "color_sensor",  role = "inspect"     },
  },

  -- Guard thresholds — monitored by the runtime, feed into
  -- ChainTree exception handlers when violated.
  -- action: what the guard triggers
  --   "abort"  — raise exception to nearest handler, terminate subtree
  --   "warn"   — write to KB, no exception (informational)
  --   "pause"  — halt execution, wait for condition to clear
  guards = {
    battery_critical = { threshold = 6200, unit = "mv",  action = "abort" },
    battery_low      = { threshold = 6800, unit = "mv",  action = "warn"  },
    tilt_max         = { threshold = 45,   unit = "deg", action = "pause" },
    comm_timeout     = { threshold = 5000, unit = "ms",  action = "pause" },
  },
}

return robot
