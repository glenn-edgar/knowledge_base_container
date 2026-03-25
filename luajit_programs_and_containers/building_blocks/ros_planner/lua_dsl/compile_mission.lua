#!/usr/bin/env luajit
------------------------------------------------------------------------
-- compile_mission.lua — Mission DSL Compiler (v2: jig-aware)
--
-- Loads the three DSL files (Equipment, Map, Mission), resolves
-- navigation paths, expands mission templates per-jig, validates
-- slot usage, wraps safety guards, and produces all output artifacts:
--
--   1. Human-readable expanded plan   (stdout + plan_expanded.txt)
--   2. S-expression tree .lua file    (mission_tree.lua)
--      — single se_state_machine, menu field selects run/jig
--   3. SQLite knowledge base          (robot_kb.sqlite)
--   4. Bindings JSON                  (bindings.json)
--
-- Usage:
--   luajit compile_mission.lua <equipment> <map> <mission> <output_dir> [flags]
--
-- Arguments:
--   equipment   — path to equipment_dsl.lua
--   map         — path to map_dsl.lua
--   mission     — path to mission_dsl.lua
--   output_dir  — directory for generated artifacts
--   flags       — --plan --tree --kb --bindings --all (default: --all)
--
-- Called by s_build.sh in each mission directory.
------------------------------------------------------------------------

local ffi = require("ffi")

------------------------------------------------------------------------
-- 0. LOAD DSL FILES
------------------------------------------------------------------------

local compiler_dir = arg[0]:match("(.*/)")  or "./"

if #arg < 4 then
    io.stderr:write("Usage: luajit compile_mission.lua <equipment> <map> <mission> <output_dir> [flags]\n")
    os.exit(1)
end

local equipment_path = arg[1]
local map_path       = arg[2]
local mission_path   = arg[3]
local output_dir     = arg[4]
-- Ensure output_dir ends with /
if output_dir:sub(-1) ~= "/" then output_dir = output_dir .. "/" end

local Equipment = dofile(equipment_path)
local Map       = dofile(map_path)
local Mission   = dofile(mission_path)
local Spline    = dofile(compiler_dir .. "spline.lua")

------------------------------------------------------------------------
-- PARSE COMMAND LINE (flags start at arg[5])
------------------------------------------------------------------------

local function parse_flags()
    local flags = { plan = false, tree = false, kb = false, bindings = false }
    local has_flags = false
    for i = 5, #arg do
        has_flags = true
        local a = arg[i]
        if a == "--all"      then flags.plan=true; flags.tree=true; flags.kb=true; flags.bindings=true
        elseif a == "--plan"     then flags.plan     = true
        elseif a == "--tree"     then flags.tree     = true
        elseif a == "--kb"       then flags.kb       = true
        elseif a == "--bindings" then flags.bindings = true
        end
    end
    if not has_flags then
        flags.plan = true; flags.tree = true; flags.kb = true; flags.bindings = true
    end
    return flags
end

local FLAGS = parse_flags()

------------------------------------------------------------------------
-- BUILD SLOT INDEX — base + per-jig available slots
------------------------------------------------------------------------

local BASE_SLOT_NAMES = {}
for _, s in ipairs(Equipment.base_slots) do
    BASE_SLOT_NAMES[s.name] = true
end

-- Returns set of all slot names available for a given jig
local function slots_for_jig(jig_name)
    local avail = {}
    for name, _ in pairs(BASE_SLOT_NAMES) do
        avail[name] = true
    end
    local jig = Equipment.jigs[jig_name]
    if jig then
        for _, s in ipairs(jig.slots) do
            avail[s.name] = true
        end
    end
    return avail
end

------------------------------------------------------------------------
-- 1. GRAPH RESOLVER — Dijkstra over Map.edges
------------------------------------------------------------------------

local Graph = {}

function Graph.build(waypoints, edges)
    local g = { nodes = {}, adj = {} }
    for _, wp in ipairs(waypoints) do
        g.nodes[wp.name] = wp
        g.adj[wp.name] = {}
    end
    for _, e in ipairs(edges) do
        table.insert(g.adj[e.from], {
            to = e.to, dist = e.distance_mm,
            method = e.method, wall_side = e.wall_side,
        })
        if e.bidirectional then
            table.insert(g.adj[e.to], {
                to = e.from, dist = e.distance_mm,
                method = e.method, wall_side = e.wall_side,
            })
        end
    end
    return g
end

function Graph.shortest_path(g, from, to)
    local dist, prev, edge_used, visited = {}, {}, {}, {}
    for name, _ in pairs(g.nodes) do dist[name] = math.huge end
    dist[from] = 0

    while true do
        local u, u_dist = nil, math.huge
        for name, d in pairs(dist) do
            if not visited[name] and d < u_dist then u, u_dist = name, d end
        end
        if u == nil or u == to then break end
        visited[u] = true
        for _, edge in ipairs(g.adj[u] or {}) do
            local alt = u_dist + edge.dist
            if alt < dist[edge.to] then
                dist[edge.to] = alt
                prev[edge.to] = u
                edge_used[edge.to] = edge
            end
        end
    end

    if dist[to] == math.huge then
        return nil, "no path from " .. from .. " to " .. to
    end

    local path = {}
    local node = to
    while node ~= from do
        local e = edge_used[node]
        table.insert(path, 1, {
            from = prev[node], to = node,
            dist = e.dist, method = e.method, wall_side = e.wall_side,
        })
        node = prev[node]
    end
    return path, nil
end

local GRAPH = Graph.build(Map.waypoints, Map.edges)

------------------------------------------------------------------------
-- 2. NAV EXPANDER
------------------------------------------------------------------------

local NavExpand = {}

local function heading_diff(from_hdg, to_hdg)
    local d = to_hdg - from_hdg
    while d >  180 do d = d - 360 end
    while d < -180 do d = d + 360 end
    return d
end

function NavExpand.edge_to_slots(edge, from_wp, to_wp)
    local steps = {}
    local turn = heading_diff(from_wp.heading, to_wp.heading)
    if math.abs(turn) > 2 then
        table.insert(steps, {
            slot = "DRIVE_TURN", params = { angle_deg = turn },
            note = string.format("turn %.0f deg at %s", turn, from_wp.name),
        })
    end
    if edge.method == "line_follow" then
        table.insert(steps, {
            slot = "LINE_FOLLOW", params = { distance_mm = edge.dist },
            note = string.format("line-follow %d mm %s -> %s", edge.dist, edge.from, edge.to),
        })
    elseif edge.method == "wall_ride" then
        table.insert(steps, {
            slot = "WALL_RIDE",
            params = { distance_mm = edge.dist, wall_side = edge.wall_side or "right" },
            note = string.format("wall-ride %d mm %s -> %s", edge.dist, edge.from, edge.to),
        })
    elseif edge.method == "direct" then
        table.insert(steps, {
            slot = "DRIVE_STRAIGHT", params = { distance_mm = edge.dist },
            note = string.format("drive straight %d mm %s -> %s", edge.dist, edge.from, edge.to),
        })
    end
    return steps
end

function NavExpand.resolve(from_name, to_name)
    local path, err = Graph.shortest_path(GRAPH, from_name, to_name)
    if not path then
        return {{ slot = "ERROR", params = {}, note = "NAV ERROR: " .. err }}
    end
    local steps = {}
    for _, edge in ipairs(path) do
        local from_wp = GRAPH.nodes[edge.from]
        local to_wp   = GRAPH.nodes[edge.to]
        for _, s in ipairs(NavExpand.edge_to_slots(edge, from_wp, to_wp)) do
            table.insert(steps, s)
        end
    end
    return steps
end

------------------------------------------------------------------------
-- 2b. SPLINE ROUTE EXPANDER
------------------------------------------------------------------------

-- Index waypoints by name for coordinate lookup
local WP_INDEX = {}
for _, wp in ipairs(Map.waypoints) do WP_INDEX[wp.name] = wp end

function NavExpand.spline_route(route_name)
    local route = Map.spline_routes[route_name]
    if not route then
        return {{ slot = "ERROR", params = {}, note = "UNKNOWN SPLINE ROUTE: " .. route_name }}
    end

    -- Build (x,y) point list from waypoint names
    local points = {}
    local wp_names = {}
    for _, wp_name in ipairs(route.waypoints) do
        local wp = WP_INDEX[wp_name]
        if not wp then
            return {{ slot = "ERROR", params = {},
                note = "SPLINE ROUTE " .. route_name .. ": unknown waypoint " .. wp_name }}
        end
        table.insert(points, { x = wp.x, y = wp.y })
        table.insert(wp_names, wp_name)
    end

    if #points < 2 then
        return {{ slot = "ERROR", params = {},
            note = "SPLINE ROUTE " .. route_name .. ": need at least 2 waypoints" }}
    end

    -- Fit Catmull-Rom spline
    local tau = Equipment.spline and Equipment.spline.tau or 0.5
    local segments = Spline.fit(points, tau)
    local total_len = Spline.total_length(segments)

    -- Flatten control points for the hub
    local flat = Spline.flatten(segments)

    -- Build control_points as a flat array of numbers
    local cp = {}
    for _, seg in ipairs(flat) do
        for _, v in ipairs(seg) do
            table.insert(cp, math.floor(v + 0.5))  -- round to integer mm
        end
    end

    return {{
        slot   = "SPLINE_FOLLOW",
        params = {
            segment_count  = #segments,
            control_points = cp,
            total_length_mm = math.floor(total_len + 0.5),
        },
        note = string.format("spline '%s': %s, %d segs, %d mm",
            route_name, table.concat(wp_names, "->"),
            #segments, math.floor(total_len + 0.5)),
    }}
end

------------------------------------------------------------------------
-- 3. MISSION TEMPLATE EXPANDER (jig-aware)
------------------------------------------------------------------------

local MissionExpand = {}

local MISSION_INDEX = {}
for _, m in ipairs(Map.missions) do MISSION_INDEX[m.name] = m end

-- Resolve action template for a mission given the current jig
function MissionExpand.expand(mission_name, jig_name)
    local m = MISSION_INDEX[mission_name]
    if not m then
        return {{ slot = "ERROR", params = {}, note = "UNKNOWN MISSION: " .. mission_name }}
    end

    -- Check for jig-specific action override
    local interact = m.interact
    local overrides = Mission.action_override[mission_name]
    if overrides and overrides[jig_name] then
        interact = overrides[jig_name]
    end

    local template_fn = Mission.actions[interact]
    if not template_fn then
        return {{ slot = "ERROR", params = {}, note = "NO TEMPLATE: " .. interact }}
    end

    local tmpl = template_fn(m)

    -- Validate required_jig
    if tmpl.required_jig and tmpl.required_jig ~= jig_name then
        return {{ slot = "ERROR", params = {},
            note = string.format("MISSION %s requires jig '%s' but run uses '%s'",
                                  mission_name, tmpl.required_jig, jig_name) }}
    end

    local steps = {}
    for _, s in ipairs(tmpl.steps) do
        table.insert(steps, {
            slot   = s.slot,
            params = s.params or {},
            note   = string.format("mission %s: %s", mission_name, s.slot),
        })
    end
    return steps
end

------------------------------------------------------------------------
-- 4. STEP EXPANDER (jig-aware)
------------------------------------------------------------------------

local function expand_steps(raw_steps, jig_name)
    local expanded = {}
    for _, step in ipairs(raw_steps) do
        if step.op == "navigate" then
            for _, s in ipairs(NavExpand.resolve(step.from, step.to)) do
                table.insert(expanded, s)
            end
        elseif step.op == "spline_route" then
            for _, s in ipairs(NavExpand.spline_route(step.route)) do
                table.insert(expanded, s)
            end
        elseif step.op == "do_mission" then
            for _, s in ipairs(MissionExpand.expand(step.mission, jig_name)) do
                table.insert(expanded, s)
            end
        elseif step.op == "sequence" then
            for _, s in ipairs(expand_steps(step.steps, jig_name)) do
                table.insert(expanded, s)
            end
        elseif step.slot then
            table.insert(expanded, {
                slot = step.slot, params = step.params or {}, note = step.slot,
            })
        else
            table.insert(expanded, {
                slot = "UNKNOWN", params = step, note = "unrecognized step type",
            })
        end
    end
    return expanded
end

------------------------------------------------------------------------
-- 5. SLOT VALIDATOR
------------------------------------------------------------------------

local validation_errors = {}

local function validate_slots(steps, jig_name, run_name)
    local avail = slots_for_jig(jig_name)
    for i, step in ipairs(steps) do
        if step.slot ~= "ERROR" and step.slot ~= "UNKNOWN" and not avail[step.slot] then
            local msg = string.format(
                "ERROR: run '%s' (jig=%s) step %d uses slot '%s' not available for this jig",
                run_name, jig_name, i, step.slot)
            table.insert(validation_errors, msg)
            io.stderr:write(msg .. "\n")
        end
    end
end

------------------------------------------------------------------------
-- 6. TIMEOUT INSERTER
------------------------------------------------------------------------

local function compute_timeout(step)
    local t = Mission.timing
    if step.slot == "LINE_FOLLOW" or step.slot == "DRIVE_STRAIGHT"
       or step.slot == "WALL_RIDE" or step.slot == "DRIVE_CURVE" then
        local dist = step.params.distance_mm or 200
        return math.max(t.nav_timeout_min_s, math.abs(dist) * t.nav_timeout_per_mm)
    end
    if step.slot == "SPLINE_FOLLOW" then
        local dist = step.params.total_length_mm or 500
        return math.max(t.nav_timeout_min_s, dist * t.nav_timeout_per_mm)
    end
    if step.slot == "DRIVE_TURN" then return 4.0 end
    if step.note and step.note:match("^mission ") then return t.action_timeout_s end
    return t.step_timeout_s
end

local function insert_timeouts(steps)
    for _, step in ipairs(steps) do step.timeout_s = compute_timeout(step) end
    return steps
end

------------------------------------------------------------------------
-- 7. SAFETY GUARD WRAPPER
------------------------------------------------------------------------

local function build_guard_tree(run_name, steps)
    return {
        type     = "guarded_run",
        run_name = run_name,
        guards   = Mission.safety.guards,
        finally  = Mission.safety.finally,
        body     = steps,
    }
end

------------------------------------------------------------------------
-- COMPILE ALL RUNS
------------------------------------------------------------------------

local function compile_all_runs()
    local compiled_runs = {}

    for i, run in ipairs(Mission.runs) do
        local jig_name = run.jig

        -- Expand nav + mission templates (jig-aware)
        local expanded = expand_steps(run.steps, jig_name)

        -- Validate slot usage against jig
        validate_slots(expanded, jig_name, run.name)

        -- Insert timeouts
        insert_timeouts(expanded)

        -- Wrap in safety guards
        local guarded = build_guard_tree(run.name, expanded)
        guarded.time_budget_s   = run.time_budget_s
        guarded.expected_points = run.expected_points
        guarded.preload         = run.preload or {}
        guarded.run_index       = i
        guarded.jig_name        = jig_name
        guarded.menu_id         = run.menu_id

        table.insert(compiled_runs, guarded)
    end

    if #validation_errors > 0 then
        io.stderr:write(string.format(
            "\n[compiler] %d SLOT VALIDATION ERRORS — fix before deploying\n",
            #validation_errors))
    end

    return compiled_runs
end

local COMPILED = compile_all_runs()

------------------------------------------------------------------------
-- OUTPUT 1: HUMAN-READABLE PLAN
------------------------------------------------------------------------

local function emit_plan(runs)
    local lines = {}
    local function p(fmt, ...) table.insert(lines, string.format(fmt, ...)) end

    p("=" .. string.rep("=", 71))
    p("  COMPILED MISSION PLAN — %s", Equipment.robot.name)
    p("  Match time: %ds | Runs: %d | Max points: %d",
      Mission.match.total_time_s, #runs, Map.scoring.max_points)
    p("  Execution: se_state_machine on field '%s'",
      Equipment.menu.field_name)
    p("=" .. string.rep("=", 71))
    p("")

    p("MENU DISPATCH:")
    for _, run in ipairs(runs) do
        p("  %d = %-20s jig=%-15s budget=%ds  expected=%d pts",
          run.menu_id, run.run_name, run.jig_name,
          run.time_budget_s, run.expected_points)
    end
    p("  0 = idle (waiting for selection)")
    p("")

    p("SAFETY GUARDS (applied to every run):")
    for _, g in ipairs(Mission.safety.guards) do
        p("  [%s] fault=%-22s abort=%-5s action=%s",
          g.name, g.fault, tostring(g.abort),
          table.concat(g.action, ", "))
    end
    p("  [finally] %s", table.concat(Mission.safety.finally, ", "))
    p("")

    if #validation_errors > 0 then
        p("VALIDATION ERRORS:")
        for _, e in ipairs(validation_errors) do p("  %s", e) end
        p("")
    end

    local total_steps, total_dist, total_time = 0, 0, 0

    for _, run in ipairs(runs) do
        p("-" .. string.rep("-", 71))
        p("  RUN %d: %s  [menu_id=%d, jig=%s]",
          run.run_index, run.run_name, run.menu_id, run.jig_name)
        p("  Budget: %ds | Expected: %d pts | Preload: %s",
          run.time_budget_s, run.expected_points,
          #run.preload > 0 and table.concat(run.preload, ", ") or "(none)")
        p("  Jig ports: %s",
          (function()
              local jig = Equipment.jigs[run.jig_name]
              if not jig or #jig.ports == 0 then return "(none — passive)" end
              local parts = {}
              for _, pp in ipairs(jig.ports) do
                  table.insert(parts, pp.port .. "=" .. pp.role)
              end
              return table.concat(parts, ", ")
          end)())
        p("-" .. string.rep("-", 71))

        local run_dist, run_time = 0, 0
        for j, step in ipairs(run.body) do
            local param_str = ""
            if step.params then
                local parts = {}
                for k, v in pairs(step.params) do
                    -- Skip large arrays in display
                    if type(v) == "table" then
                        table.insert(parts, string.format("%s=[%d values]", k, #v))
                    else
                        table.insert(parts, string.format("%s=%s", k, tostring(v)))
                    end
                end
                if #parts > 0 then param_str = "(" .. table.concat(parts, ", ") .. ")" end
            end
            p("  %3d. %-24s %-30s timeout=%.1fs", j, step.slot, param_str, step.timeout_s)
            if step.note and step.note ~= step.slot then
                p("       -- %s", step.note)
            end
            local d = step.params and (step.params.distance_mm or step.params.total_length_mm)
            if d then run_dist = run_dist + math.abs(d) end
            run_time = run_time + step.timeout_s
            total_steps = total_steps + 1
        end
        p("")
        p("  Run summary: %d steps, %d mm travel, %.1fs max timeout sum",
          #run.body, run_dist, run_time)
        p("")
        total_dist = total_dist + run_dist
        total_time = total_time + run_time
    end

    p("=" .. string.rep("=", 71))
    p("  TOTALS: %d steps | %d mm travel | %.1fs sum of timeouts",
      total_steps, total_dist, total_time)
    p("  Expected points: %d / %d",
      Mission.plan_summary.total_expected_points, Map.scoring.max_points)
    p("=" .. string.rep("=", 71))

    return table.concat(lines, "\n")
end

if FLAGS.plan then
    local plan_text = emit_plan(COMPILED)
    print(plan_text)
    local f = io.open(output_dir .. "plan_expanded.txt", "w")
    if f then f:write(plan_text .. "\n"); f:close()
        io.stderr:write("[compiler] wrote plan_expanded.txt\n") end
end

------------------------------------------------------------------------
-- OUTPUT 2: S-EXPRESSION TREE (se_state_machine dispatcher)
------------------------------------------------------------------------

local function emit_se_tree(runs)
    local lines = {}
    local function p(fmt, ...) table.insert(lines, string.format(fmt, ...)) end

    p("------------------------------------------------------------------------")
    p("-- mission_tree.lua — AUTO-GENERATED by compile_mission.lua")
    p("-- DO NOT EDIT — regenerate from the DSL files.")
    p("--")
    p("-- Structure: single se_state_machine on '%s' field.", Equipment.menu.field_name)
    p("-- Hub button sets field to menu_id, state machine dispatches to run.")
    p("------------------------------------------------------------------------")
    p("")
    p('dofile("se_lego_spike.lua")')
    p("")
    p("spike_define_blackboard()")
    p("")
    p("-- Menu selection field")
    p('define_field("%s", "int32", 0)', Equipment.menu.field_name)
    p("")

    -- Slot -> se_lego_spike function mapping (base + all jigs merged)
    local slot_map = {
        RESET_DRIVEBASE   = { fn = "spike_drivebase_reset",    kind = "oneshot" },
        RESET_HEADING     = { fn = "spike_imu_reset_heading",  kind = "oneshot" },
        STOP_ALL          = { fn = "spike_drivebase_stop",     kind = "oneshot" },
        BRAKE_ALL         = { fn = "spike_drivebase_brake",    kind = "oneshot" },
        EMERGENCY_STOP    = { fn = "spike_emergency_stop",     kind = "oneshot" },
        READ_SENSORS      = { fn = "spike_read_sensors",       kind = "oneshot" },
        ARM_STOP          = { fn = "spike_motor_stop",         kind = "oneshot", port = "SPIKE_PORT_C" },
        ARM_HOLD          = { fn = "spike_motor_hold",         kind = "oneshot", port = "SPIKE_PORT_C" },
        GRIP_STOP         = { fn = "spike_motor_stop",         kind = "oneshot", port = "SPIKE_PORT_D" },
        GRIP_HOLD         = { fn = "spike_motor_hold",         kind = "oneshot", port = "SPIKE_PORT_D" },
        TRAY_STOP         = { fn = "spike_motor_stop",         kind = "oneshot", port = "SPIKE_PORT_C" },
        TRAY_HOLD         = { fn = "spike_motor_hold",         kind = "oneshot", port = "SPIKE_PORT_C" },
    }

    local function emit_slot_call(step, indent)
        local pfx = string.rep("    ", indent)
        local m = slot_map[step.slot]

        if m then
            if m.port then
                p("%s%s(%s)", pfx, m.fn, m.port)
            else
                p("%s%s()", pfx, m.fn)
            end
            return
        end

        -- Main function dispatch
        if step.slot == "DRIVE_STRAIGHT" or step.slot == "LINE_FOLLOW"
           or step.slot == "WALL_RIDE" then
            p("%sspike_drivebase_straight(%d)", pfx, step.params.distance_mm or 0)
        elseif step.slot == "DRIVE_TURN" then
            p("%sspike_drivebase_turn(%d)", pfx, step.params.angle_deg or 0)
        elseif step.slot == "DRIVE_CURVE" then
            p("%sspike_drivebase_curve(%d, %d)", pfx,
              step.params.radius_mm or 200, step.params.angle_deg or 90)
        elseif step.slot == "WAIT_SECONDS" then
            p("%sse_tick_delay(%d)", pfx, math.floor((step.params.seconds or 1) * 100))
        elseif step.slot == "ARM_TO_ANGLE" then
            local jig = Equipment.jigs.gripper_arm
            p("%sspike_motor_run_target(SPIKE_PORT_C, %d, %d)", pfx,
              jig.motors.arm_lift.default_speed, step.params.angle_deg or 0)
        elseif step.slot == "ARM_HOME" then
            local jig = Equipment.jigs.gripper_arm
            p("%sspike_motor_run_target(SPIKE_PORT_C, %d, 0)", pfx,
              jig.motors.arm_lift.default_speed)
        elseif step.slot == "GRIP_CLOSE" then
            local jig = Equipment.jigs.gripper_arm
            p("%sspike_motor_run_target(SPIKE_PORT_D, %d, %d)", pfx,
              jig.motors.gripper.default_speed, jig.motors.gripper.max_angle)
        elseif step.slot == "GRIP_OPEN" then
            local jig = Equipment.jigs.gripper_arm
            p("%sspike_motor_run_target(SPIKE_PORT_D, %d, 0)", pfx,
              jig.motors.gripper.default_speed)
        elseif step.slot == "TRAY_TO_ANGLE" then
            local jig = Equipment.jigs.cargo_tray
            p("%sspike_motor_run_target(SPIKE_PORT_C, %d, %d)", pfx,
              jig.motors.tray_lift.default_speed, step.params.angle_deg or 0)
        elseif step.slot == "TRAY_HOME" then
            local jig = Equipment.jigs.cargo_tray
            p("%sspike_motor_run_target(SPIKE_PORT_C, %d, 0)", pfx,
              jig.motors.tray_lift.default_speed)
        elseif step.slot == "TRAY_DUMP" then
            local jig = Equipment.jigs.cargo_tray
            p("%sspike_motor_run_target(SPIKE_PORT_C, %d, %d)", pfx,
              jig.motors.tray_lift.default_speed, jig.motors.tray_lift.max_angle)
        elseif step.slot == "ALIGN_TO_WALL" then
            p("%sspike_motor_run_until_stalled(SPIKE_PORT_A, %d)", pfx,
              step.params.speed or 80)
        elseif step.slot == "SPLINE_FOLLOW" then
            -- Emit control points as a flat array the hub evaluates
            local cp = step.params.control_points or {}
            local segs = step.params.segment_count or 0
            p("%s-- spline: %d segments, %d mm", pfx, segs,
              step.params.total_length_mm or 0)
            p("%sspline_follow({", pfx)
            p("%s    segments = %d,", pfx, segs)
            p("%s    speed = %d,", pfx,
              Equipment.spline and Equipment.spline.base_speed_mm_s or 150)
            p("%s    cp = {", pfx)
            -- 8 values per segment (b0x,b0y,b1x,b1y,b2x,b2y,b3x,b3y)
            for si = 1, segs do
                local base = (si - 1) * 8
                local vals = {}
                for vi = 1, 8 do
                    table.insert(vals, tostring(cp[base + vi] or 0))
                end
                p("%s        %s%s", pfx, table.concat(vals, ", "),
                  si < segs and "," or "")
            end
            p("%s    }", pfx)
            p("%s})", pfx)
        else
            p("%s-- TODO: unmapped slot %s", pfx, step.slot)
        end
    end

    -- Guard emitter
    local pred_map = {
        FAULT_BATTERY_CRIT = string.format(
            "spike_battery_low(%.1f)", Equipment.guards.battery_critical_mv / 1000),
        FAULT_BATTERY_LOW  = string.format(
            "spike_battery_low(%.1f)", Equipment.guards.battery_min_mv / 1000),
        FAULT_COMM_TIMEOUT = string.format(
            "spike_comm_timeout(%d)", Equipment.guards.comm_timeout_ms / 10),
        FAULT_TILT         = string.format(
            "spike_tilt_exceeded(%d)", Equipment.guards.tilt_max_deg),
        FAULT_BUMP         = "spike_bump_detected()",
    }
    local recovery_map = {
        EMERGENCY_STOP = "spike_emergency_stop()",
        BRAKE_ALL      = "spike_drivebase_brake()",
        STOP_ALL       = "spike_drivebase_stop()",
    }

    local function emit_guard_open(guards, indent)
        local pfx = string.rep("    ", indent)
        p("%sspike_multi_guard({", pfx)
        for _, g in ipairs(guards) do
            local pred_call = pred_map[g.fault] or ("-- unknown fault: " .. g.fault)
            local rec_calls = {}
            local ai = 1
            while ai <= #g.action do
                local a = g.action[ai]
                if a == "WAIT_SECONDS" and type(g.action[ai+1]) == "number" then
                    table.insert(rec_calls, string.format("se_tick_delay(%d)",
                        math.floor(g.action[ai+1] * 100)))
                    ai = ai + 2
                else
                    table.insert(rec_calls, recovery_map[a] or ("-- unknown: " .. tostring(a)))
                    ai = ai + 1
                end
            end
            p("%s    { pred = function() %s end,", pfx, pred_call)
            p("%s      recovery = function()", pfx)
            for _, rc in ipairs(rec_calls) do
                p("%s          %s", pfx, rc)
            end
            p("%s      end },", pfx)
        end
        p("%s}, function()", pfx)
    end

    -- Emit the top-level state machine tree
    p('define_tree("match_dispatcher")')
    p("")
    p('    se_state_machine("%s", function()', Equipment.menu.field_name)
    p("")

    -- Case 0: idle — do nothing, wait for menu selection
    p("        -- menu_id 0: idle")
    p("        se_case(0, function()")
    p("            se_nop()")
    p("        end)")
    p("")

    -- One case per run
    for _, run in ipairs(runs) do
        p("        -- menu_id %d: %s (jig=%s, %d pts)",
          run.menu_id, run.run_name, run.jig_name, run.expected_points)
        p("        se_case(%d, function()", run.menu_id)

        -- Open safety guard wrapper
        emit_guard_open(run.guards, 3)

        -- Body sequence
        p("                se_sequence(")
        for _, step in ipairs(run.body) do
            if step.note and step.note ~= step.slot then
                p("                    -- %s", step.note)
            end
            p("                    function()")
            emit_slot_call(step, 6)
            p("                    end,")
        end
        p("                )")  -- close se_sequence

        -- Close guard
        p("            end)")   -- close multi_guard action
        p("")

        -- Finally
        p("            -- finally: cleanup")
        for _, f in ipairs(run.finally) do
            local m = slot_map[f]
            if m then p("            %s()", m.fn) end
        end
        p("")

        -- Reset menu to idle after run completes
        p("            -- reset menu to idle for next run")
        p('            set_field("%s", 0)', Equipment.menu.field_name)

        p("        end)")
        p("")
    end

    -- Default case
    p("        -- default: unknown menu_id, treat as idle")
    p("        se_case(\"default\", function()")
    p("            se_nop()")
    p("        end)")
    p("")

    p("    end)")  -- close se_state_machine
    p("")
    p("end_tree()")

    return table.concat(lines, "\n")
end

if FLAGS.tree then
    local tree_text = emit_se_tree(COMPILED)
    local f = io.open(output_dir .. "mission_tree.lua", "w")
    if f then f:write(tree_text .. "\n"); f:close()
        io.stderr:write("[compiler] wrote mission_tree.lua\n") end
end

------------------------------------------------------------------------
-- OUTPUT 3: SQLITE KNOWLEDGE BASE
------------------------------------------------------------------------

local function emit_kb(runs)
    local stmts = {}
    local function q(fmt, ...) table.insert(stmts, string.format(fmt, ...)) end
    local function esc(s) return tostring(s):gsub("'", "''") end

    q("-- robot_kb.sql — AUTO-GENERATED by compile_mission.lua")
    q("CREATE TABLE IF NOT EXISTS kb (")
    q("    path      TEXT PRIMARY KEY,")
    q("    data_type TEXT NOT NULL DEFAULT 'text',")
    q("    value     TEXT,")
    q("    num_value REAL")
    q(");")
    q("")
    q("BEGIN TRANSACTION;")
    q("")

    -- Robot identity
    q("-- Robot identity")
    for k, v in pairs(Equipment.robot) do
        q("INSERT INTO kb(path, value) VALUES('robot.%s', '%s');", k, esc(v))
    end
    q("")

    -- Chassis ports
    q("-- Chassis ports (fixed)")
    for _, p in ipairs(Equipment.chassis_ports) do
        local base = string.format("equipment.port.%s", p.port:lower())
        q("INSERT INTO kb(path, value) VALUES('%s.role', '%s');", base, esc(p.role))
        q("INSERT INTO kb(path, value) VALUES('%s.device', '%s');", base, esc(p.device))
    end
    q("")

    -- Jigs
    q("-- Attachment jigs")
    for jname, jig in pairs(Equipment.jigs) do
        local base = string.format("equipment.jig.%s", jname)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.menu_id', 'number', %d);", base, jig.menu_id)
        for _, pp in ipairs(jig.ports) do
            q("INSERT INTO kb(path, value) VALUES('%s.port.%s.role', '%s');", base, pp.port:lower(), esc(pp.role))
            q("INSERT INTO kb(path, value) VALUES('%s.port.%s.device', '%s');", base, pp.port:lower(), esc(pp.device))
        end
        for mname, motor in pairs(jig.motors) do
            for mk, mv in pairs(motor) do
                local mbase = string.format("%s.motor.%s.%s", base, mname, mk)
                if type(mv) == "number" then
                    q("INSERT INTO kb(path, data_type, num_value) VALUES('%s', 'number', %s);", mbase, mv)
                else
                    q("INSERT INTO kb(path, value) VALUES('%s', '%s');", mbase, esc(mv))
                end
            end
        end
        for _, s in ipairs(jig.slots) do
            q("INSERT INTO kb(path, value) VALUES('%s.slot.%s', '%s');", base, s.name:lower(), esc(s.kind))
        end
    end
    q("")

    -- Drivebase
    q("-- Drivebase geometry")
    for k, v in pairs(Equipment.drivebase) do
        if type(v) == "number" then
            q("INSERT INTO kb(path, data_type, num_value) VALUES('equipment.drivebase.%s', 'number', %s);", k, v)
        else
            q("INSERT INTO kb(path, value) VALUES('equipment.drivebase.%s', '%s');", k, esc(v))
        end
    end
    q("")

    -- Guards
    q("-- Guard thresholds")
    for k, v in pairs(Equipment.guards) do
        q("INSERT INTO kb(path, data_type, num_value) VALUES('equipment.guard.%s', 'number', %s);", k, v)
    end
    q("")

    -- Base slots
    q("-- Base virtual function slots")
    for _, s in ipairs(Equipment.base_slots) do
        local base = string.format("equipment.slot.%s", s.name:lower())
        q("INSERT INTO kb(path, value) VALUES('%s.kind', '%s');", base, esc(s.kind))
        if s.reads then q("INSERT INTO kb(path, value) VALUES('%s.reads', '%s');", base, esc(s.reads)) end
        if s.compare then q("INSERT INTO kb(path, value) VALUES('%s.compare', '%s');", base, esc(s.compare)) end
    end
    q("")

    -- Menu
    q("-- Menu field")
    q("INSERT INTO kb(path, value) VALUES('equipment.menu.field_name', '%s');", esc(Equipment.menu.field_name))
    q("INSERT INTO kb(path, data_type, num_value) VALUES('equipment.menu.default', 'number', %d);", Equipment.menu.default)
    q("")

    -- Map: waypoints, edges, zones, missions, lines
    q("-- Map waypoints")
    for _, wp in ipairs(Map.waypoints) do
        local base = string.format("map.waypoint.%s", wp.name)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.x', 'number', %s);", base, wp.x)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.y', 'number', %s);", base, wp.y)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.heading', 'number', %s);", base, wp.heading)
        q("INSERT INTO kb(path, value) VALUES('%s.nav_method', '%s');", base, esc(wp.nav_method))
    end
    q("")

    q("-- Map edges")
    for _, e in ipairs(Map.edges) do
        local base = string.format("map.edge.%s_%s", e.from, e.to)
        q("INSERT INTO kb(path, value) VALUES('%s.method', '%s');", base, esc(e.method))
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.distance_mm', 'number', %s);", base, e.distance_mm)
    end
    q("")

    q("-- Map zones")
    for _, z in ipairs(Map.zones) do
        local base = string.format("map.zone.%s", z.name)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.x', 'number', %s);", base, z.x)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.y', 'number', %s);", base, z.y)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.w', 'number', %s);", base, z.w)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.h', 'number', %s);", base, z.h)
    end
    q("")

    q("-- Map missions")
    for _, m in ipairs(Map.missions) do
        local base = string.format("map.mission.%s", m.name)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.x', 'number', %s);", base, m.x)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.y', 'number', %s);", base, m.y)
        q("INSERT INTO kb(path, value) VALUES('%s.approach', '%s');", base, esc(m.approach))
        q("INSERT INTO kb(path, value) VALUES('%s.interact', '%s');", base, esc(m.interact))
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.points', 'number', %s);", base, m.points)
    end
    q("")

    q("-- Scoring")
    for k, v in pairs(Map.scoring) do
        if type(v) == "number" then
            q("INSERT INTO kb(path, data_type, num_value) VALUES('map.scoring.%s', 'number', %s);", k, v)
        end
    end
    q("")

    -- Jig compatibility
    q("-- Jig compatibility")
    for jname, missions in pairs(Mission.jig_compat) do
        q("INSERT INTO kb(path, value) VALUES('plan.jig_compat.%s', '%s');",
          jname, esc(table.concat(missions, ",")))
    end
    q("")

    -- Compiled plan steps
    q("-- Compiled plan steps")
    for _, run in ipairs(runs) do
        local rbase = string.format("plan.run.%d", run.run_index)
        q("INSERT INTO kb(path, value) VALUES('%s.name', '%s');", rbase, esc(run.run_name))
        q("INSERT INTO kb(path, value) VALUES('%s.jig', '%s');", rbase, esc(run.jig_name))
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.menu_id', 'number', %d);", rbase, run.menu_id)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.time_budget_s', 'number', %s);", rbase, run.time_budget_s)
        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.expected_points', 'number', %s);", rbase, run.expected_points)
        for j, step in ipairs(run.body) do
            local sbase = string.format("%s.step.%03d", rbase, j)
            q("INSERT INTO kb(path, value) VALUES('%s.slot', '%s');", sbase, esc(step.slot))
            q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.timeout_s', 'number', %.1f);", sbase, step.timeout_s)
            if step.params then
                for pk, pv in pairs(step.params) do
                    if type(pv) == "number" then
                        q("INSERT INTO kb(path, data_type, num_value) VALUES('%s.param.%s', 'number', %s);", sbase, pk, pv)
                    else
                        q("INSERT INTO kb(path, value) VALUES('%s.param.%s', '%s');", sbase, pk, esc(pv))
                    end
                end
            end
        end
    end
    q("")

    -- Preconditions
    q("-- Preconditions and effects")
    for mname, pe in pairs(Mission.preconditions) do
        local base = string.format("plan.precond.%s", mname)
        if pe.requires then
            for k, v in pairs(pe.requires) do
                q("INSERT INTO kb(path, value) VALUES('%s.requires.%s', '%s');", base, k, esc(tostring(v)))
            end
        end
        if pe.produces then
            for k, v in pairs(pe.produces) do
                q("INSERT INTO kb(path, value) VALUES('%s.produces.%s', '%s');", base, k, esc(tostring(v)))
            end
        end
    end
    q("")

    q("COMMIT;")
    return table.concat(stmts, "\n")
end

if FLAGS.kb then
    local sql_text = emit_kb(COMPILED)
    local f = io.open(output_dir .. "robot_kb.sql", "w")
    if f then f:write(sql_text .. "\n"); f:close()
        io.stderr:write("[compiler] wrote robot_kb.sql\n") end
    local rc = os.execute(string.format(
        "cd '%s' && rm -f robot_kb.sqlite && sqlite3 robot_kb.sqlite < robot_kb.sql 2>/dev/null",
        output_dir))
    if rc == 0 or rc == true then
        io.stderr:write("[compiler] wrote robot_kb.sqlite\n")
    else
        io.stderr:write("[compiler] sqlite3 not available — use robot_kb.sql manually\n")
    end
end

------------------------------------------------------------------------
-- OUTPUT 4: BINDINGS JSON (per-jig port mappings)
------------------------------------------------------------------------

local function emit_bindings()
    local entries = {}
    local function e(fmt, ...) table.insert(entries, string.format(fmt, ...)) end

    e("{")
    e('  "robot": "%s",', Equipment.robot.name)
    e('  "hub": "%s",', Equipment.robot.hub)
    e('  "protocol": "json_newline",')
    e('  "transport": "ble_uart",')
    e('  "menu_field": "%s",', Equipment.menu.field_name)
    e("")

    -- Chassis ports (fixed)
    e('  "chassis_ports": {')
    for i, p in ipairs(Equipment.chassis_ports) do
        local comma = (i < #Equipment.chassis_ports) and "," or ""
        e('    "%s": { "port": "%s", "device": "%s" }%s', p.role, p.port, p.device, comma)
    end
    e("  },")
    e("")

    e('  "drivebase": {')
    e('    "left_motor": "%s",', Equipment.drivebase.left_motor)
    e('    "right_motor": "%s",', Equipment.drivebase.right_motor)
    e('    "wheel_diameter_mm": %.1f,', Equipment.drivebase.wheel_diameter)
    e('    "axle_track_mm": %.1f', Equipment.drivebase.axle_track)
    e("  },")
    e("")

    -- Per-jig port configs
    e('  "jigs": {')
    local jig_list = {}
    for name, _ in pairs(Equipment.jigs) do table.insert(jig_list, name) end
    table.sort(jig_list)
    for ji, jname in ipairs(jig_list) do
        local jig = Equipment.jigs[jname]
        local jcomma = (ji < #jig_list) and "," or ""
        e('    "%s": {', jname)
        e('      "menu_id": %d,', jig.menu_id)
        e('      "ports": {')
        for pi, pp in ipairs(jig.ports) do
            local pcomma = (pi < #jig.ports) and "," or ""
            e('        "%s": { "port": "%s", "device": "%s" }%s', pp.role, pp.port, pp.device, pcomma)
        end
        e("      },")
        -- Jig-specific slot bindings
        e('      "slots": [')
        for si, s in ipairs(jig.slots) do
            local scomma = (si < #jig.slots) and "," or ""
            e('        "%s"%s', s.name, scomma)
        end
        e("      ]")
        e('    }%s', jcomma)
    end
    e("  },")
    e("")

    -- Base slot bindings
    e('  "base_bindings": {')
    for i, s in ipairs(Equipment.base_slots) do
        local comma = (i < #Equipment.base_slots) and "," or ""
        if s.kind == "predicate" then
            e('    "%s": { "kind": "predicate", "rpc": false, "reads": "%s"%s }%s',
              s.name, s.reads or "",
              s.compare and string.format(', "compare": "%s"', s.compare) or "",
              comma)
        else
            local params_str = ""
            if s.params and #s.params > 0 then
                params_str = string.format(', "params": ["%s"]', table.concat(s.params, '", "'))
            end
            e('    "%s": { "kind": "%s", "rpc": true, "command": "%s"%s }%s',
              s.name, s.kind, s.name:lower(), params_str, comma)
        end
    end
    e("  }")
    e("}")

    return table.concat(entries, "\n")
end

if FLAGS.bindings then
    local json_text = emit_bindings()
    local f = io.open(output_dir .. "bindings.json", "w")
    if f then f:write(json_text .. "\n"); f:close()
        io.stderr:write("[compiler] wrote bindings.json\n") end
end

------------------------------------------------------------------------
-- DONE
------------------------------------------------------------------------
io.stderr:write(string.format("[compiler] done — %d runs, %d total steps, %d validation errors\n",
    #COMPILED,
    (function() local n=0; for _,r in ipairs(COMPILED) do n=n+#r.body end; return n end)(),
    #validation_errors
))
