-- upward_peer.lua -- Phase 1 stub (no-op).
--
-- In Phase 1 the rover talks directly to the existing mission planner
-- over MQTT (link_client inside mqtt_robot_main owns the connection).
-- This module is the seam where Phase 2 will plug in a robot_controller
-- client (KB-via-controller, fleet membership, exception sink to PG).
--
-- Class image doesn't change between phases — rebuild robot_base only.

local M = {}

function M.new(_ctx) return setmetatable({}, { __index = M }) end

function M:register()    return true, "phase 1 stub" end
function M:tick()        return true end
function M:on_shutdown() return true end

return M
