------------------------------------------------------------------------
-- spline.lua — Catmull-Rom Spline Library for Path Planning
--
-- Generates smooth curves through a sequence of (x,y) waypoints.
-- Output is a list of cubic Bezier segments that the hub evaluates
-- per-tick to compute differential wheel speeds.
--
-- Catmull-Rom properties:
--   - Curve passes through every waypoint (interpolating)
--   - C1 continuous (smooth velocity, no sudden direction changes)
--   - Local control (moving one point only affects nearby segments)
--   - tau parameter controls tightness (0.5 = centripetal, default)
--
-- Hub-side evaluation:
--   For each segment, the hub steps t from 0 to 1 at its tick rate.
--   At each t it computes (x,y) and the tangent (dx,dy).
--   The tangent gives the desired heading. Combined with the drivebase
--   axle_track, this gives v_left and v_right.
------------------------------------------------------------------------

local Spline = {}

------------------------------------------------------------------------
-- Catmull-Rom to cubic Bezier conversion
--
-- Given 4 Catmull-Rom control points P0,P1,P2,P3:
--   The curve segment goes from P1 to P2.
--   P0 and P3 influence the tangents at P1 and P2.
--
-- Convert to cubic Bezier control points B0,B1,B2,B3:
--   B0 = P1
--   B1 = P1 + (P2 - P0) / (6 * tau)
--   B2 = P2 - (P3 - P1) / (6 * tau)
--   B3 = P2
--
-- tau = 0.5 gives the standard Catmull-Rom (alpha=0, uniform).
------------------------------------------------------------------------

local function catmull_to_bezier(p0, p1, p2, p3, tau)
    tau = tau or 0.5
    local d = 6 * tau

    return {
        -- B0
        { x = p1.x, y = p1.y },
        -- B1
        { x = p1.x + (p2.x - p0.x) / d,
          y = p1.y + (p2.y - p0.y) / d },
        -- B2
        { x = p2.x - (p3.x - p1.x) / d,
          y = p2.y - (p3.y - p1.y) / d },
        -- B3
        { x = p2.x, y = p2.y },
    }
end

------------------------------------------------------------------------
-- Fit a Catmull-Rom spline through a list of (x,y) waypoints.
--
-- Input:  points = { {x=..., y=...}, {x=..., y=...}, ... }
--         At least 2 points required.
--
-- Output: segments = list of cubic Bezier segments.
--         Each segment = { b0, b1, b2, b3 } where bi = {x,y}.
--         Segment i goes from points[i] to points[i+1].
--
-- Endpoint handling: ghost points are reflected beyond the first
-- and last waypoints so the curve starts/ends with the right tangent.
------------------------------------------------------------------------

function Spline.fit(points, tau)
    tau = tau or 0.5
    local n = #points
    assert(n >= 2, "spline.fit: need at least 2 points")

    -- Build extended point list with ghost endpoints
    local pts = {}

    -- Ghost point before first: reflect point[2] across point[1]
    pts[1] = {
        x = 2 * points[1].x - points[2].x,
        y = 2 * points[1].y - points[2].y,
    }

    -- Copy actual points
    for i = 1, n do
        pts[i + 1] = points[i]
    end

    -- Ghost point after last: reflect point[n-1] across point[n]
    pts[n + 2] = {
        x = 2 * points[n].x - points[n - 1].x,
        y = 2 * points[n].y - points[n - 1].y,
    }

    -- Generate Bezier segments
    local segments = {}
    for i = 1, n - 1 do
        local seg = catmull_to_bezier(
            pts[i], pts[i + 1], pts[i + 2], pts[i + 3], tau)
        table.insert(segments, seg)
    end

    return segments
end

------------------------------------------------------------------------
-- Evaluate a cubic Bezier segment at parameter t (0..1)
-- Returns position (x,y) and tangent (dx,dy)
------------------------------------------------------------------------

function Spline.eval(seg, t)
    local b0, b1, b2, b3 = seg[1], seg[2], seg[3], seg[4]
    local u = 1 - t

    -- Position: B(t) = (1-t)^3*B0 + 3*(1-t)^2*t*B1 + 3*(1-t)*t^2*B2 + t^3*B3
    local x = u*u*u*b0.x + 3*u*u*t*b1.x + 3*u*t*t*b2.x + t*t*t*b3.x
    local y = u*u*u*b0.y + 3*u*u*t*b1.y + 3*u*t*t*b2.y + t*t*t*b3.y

    -- Tangent: B'(t) = 3*(1-t)^2*(B1-B0) + 6*(1-t)*t*(B2-B1) + 3*t^2*(B3-B2)
    local dx = 3*u*u*(b1.x-b0.x) + 6*u*t*(b2.x-b1.x) + 3*t*t*(b3.x-b2.x)
    local dy = 3*u*u*(b1.y-b0.y) + 6*u*t*(b2.y-b1.y) + 3*t*t*(b3.y-b2.y)

    return x, y, dx, dy
end

------------------------------------------------------------------------
-- Compute heading angle (degrees) from tangent vector
------------------------------------------------------------------------

function Spline.heading(dx, dy)
    return math.deg(math.atan2(dy, dx))
end

------------------------------------------------------------------------
-- Estimate arc length of a segment by sampling
------------------------------------------------------------------------

function Spline.arc_length(seg, samples)
    samples = samples or 50
    local total = 0
    local px, py = Spline.eval(seg, 0)
    for i = 1, samples do
        local t = i / samples
        local nx, ny = Spline.eval(seg, t)
        local dx, dy = nx - px, ny - py
        total = total + math.sqrt(dx*dx + dy*dy)
        px, py = nx, ny
    end
    return total
end

------------------------------------------------------------------------
-- Total arc length of all segments
------------------------------------------------------------------------

function Spline.total_length(segments)
    local total = 0
    for _, seg in ipairs(segments) do
        total = total + Spline.arc_length(seg)
    end
    return total
end

------------------------------------------------------------------------
-- Sample a spline path at fixed distance intervals.
-- Returns a list of { x, y, heading_deg, v_ratio } samples.
--
-- v_ratio = curvature-based speed scaling (1.0 = straight, lower = tighter curve).
-- The hub uses this to scale base_speed for each tick.
--
-- axle_track_mm: distance between wheel centers (for v_left/v_right calc)
------------------------------------------------------------------------

function Spline.sample_path(segments, interval_mm, axle_track_mm)
    interval_mm = interval_mm or 5.0
    axle_track_mm = axle_track_mm or 128.0

    local samples = {}
    local accum = 0

    for si, seg in ipairs(segments) do
        local seg_len = Spline.arc_length(seg)
        local n_steps = math.max(1, math.floor(seg_len / 1.0))  -- 1mm resolution

        local px, py = Spline.eval(seg, 0)
        for i = 1, n_steps do
            local t = i / n_steps
            local nx, ny, dx, dy = Spline.eval(seg, t)
            local step_len = math.sqrt((nx-px)*(nx-px) + (ny-py)*(ny-py))
            accum = accum + step_len

            if accum >= interval_mm then
                local heading = Spline.heading(dx, dy)

                -- Curvature approximation: kappa = |cross(v, a)| / |v|^3
                -- Simplified: use tangent change rate
                local speed = math.sqrt(dx*dx + dy*dy)
                local radius = (speed > 0.001) and (speed / 0.01) or 999999
                -- v_ratio: slow down in tight curves
                local v_ratio = math.min(1.0, math.sqrt(radius / 200.0))

                table.insert(samples, {
                    x = nx, y = ny,
                    heading_deg = heading,
                    v_ratio = v_ratio,
                })
                accum = accum - interval_mm
            end

            px, py = nx, ny
        end
    end

    return samples
end

------------------------------------------------------------------------
-- Compute differential wheel speeds for a sample point.
--
-- base_speed: desired center-of-robot speed (mm/s)
-- heading_deg: desired heading at this point
-- axle_track: wheel center-to-center distance (mm)
-- angular_rate: heading change rate (deg/s), computed from consecutive samples
--
-- Returns v_left, v_right (mm/s)
------------------------------------------------------------------------

function Spline.wheel_speeds(base_speed, angular_rate_deg_s, axle_track)
    local omega = math.rad(angular_rate_deg_s)  -- rad/s
    local half_track = axle_track / 2

    local v_left  = base_speed - omega * half_track
    local v_right = base_speed + omega * half_track

    return v_left, v_right
end

------------------------------------------------------------------------
-- Flatten segments into a compact control point array for the hub.
-- Format: list of { b0x, b0y, b1x, b1y, b2x, b2y, b3x, b3y }
-- Each segment is 8 numbers. The hub evaluates these directly.
------------------------------------------------------------------------

function Spline.flatten(segments)
    local flat = {}
    for _, seg in ipairs(segments) do
        table.insert(flat, {
            seg[1].x, seg[1].y,
            seg[2].x, seg[2].y,
            seg[3].x, seg[3].y,
            seg[4].x, seg[4].y,
        })
    end
    return flat
end

------------------------------------------------------------------------
-- Pretty-print a spline for debugging
------------------------------------------------------------------------

function Spline.dump(segments)
    for i, seg in ipairs(segments) do
        local len = Spline.arc_length(seg)
        local _, _, dx0, dy0 = Spline.eval(seg, 0)
        local _, _, dx1, dy1 = Spline.eval(seg, 1)
        io.write(string.format(
            "  seg %d: (%.0f,%.0f)->(%.0f,%.0f)  len=%.0fmm  hdg=%.0f->%.0f\n",
            i, seg[1].x, seg[1].y, seg[4].x, seg[4].y, len,
            Spline.heading(dx0, dy0), Spline.heading(dx1, dy1)))
    end
end

return Spline
