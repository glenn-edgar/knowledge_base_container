--[[
    test_link_client.lua -- Unit test for link_client (robot-side link protocol).
    Uses mock transport. No external dependencies.

    Tests:
      - Announce → ack → confirm → live
      - Keepalive heartbeats sent while live
      - Planner heartbeat loss → planner_lost → re-announce
      - Planner seq reset detection
      - Planner disconnect → re-announce
      - Robot shutdown
]]

print("=== Link Client Unit Test ===\n")

local json_util = require("json_util")

local passed, failed = 0, 0
local function check(name, cond, detail)
    if cond then
        print("  PASS: " .. name)
        passed = passed + 1
    else
        print("  FAIL: " .. name .. (detail and (" — " .. detail) or ""))
        failed = failed + 1
    end
end

-- Mock transport
local function make_mock_transport()
    local tx = {
        sent_link = {},     -- messages robot sent
        inbox = {},         -- messages from planner (inject here)
    }
    function tx:send_link(json_str)
        self.sent_link[#self.sent_link + 1] = json_str
    end
    function tx:recv_link()
        if #self.inbox == 0 then return nil end
        local result = self.inbox
        self.inbox = {}
        return result
    end
    function tx:last_sent()
        if #self.sent_link == 0 then return nil end
        return json_util.decode(self.sent_link[#self.sent_link])
    end
    function tx:clear()
        self.sent_link = {}
        self.inbox = {}
    end
    return tx
end

-- Planner lost tracker
local lost_events = {}
local function on_planner_lost(reason)
    lost_events[#lost_events + 1] = reason
end

---------------------------------------------------------------------------

local link_client = require("link_client")

print("--- Initial State ---")

local tx = make_mock_transport()
local lc = link_client.new({
    robot_id     = "rover_1",
    class_name   = "lunar_rover",
    transport    = tx,
    wire_format  = "cbor",
    capabilities = {"init_check", "path_spline"},
    energy_max   = 10000,
    on_planner_lost = on_planner_lost,
})

check("starts offline", lc:get_state() == "offline")
check("not live", not lc:is_live())

print("\n--- First Tick: Sends Announce ---")

lc:tick()

check("state is announcing", lc:get_state() == "announcing")
check("announce sent", #tx.sent_link == 1)
local announce = tx:last_sent()
check("announce type", announce.type == "link_announce")
check("announce robot_id", announce.robot_id == "rover_1")
check("announce class_name", announce.class_name == "lunar_rover")
check("announce has seq", announce.seq == 1)

print("\n--- Second Tick: No duplicate announce (interval not reached) ---")

tx:clear()
lc:tick()
check("no duplicate announce", #tx.sent_link == 0)
check("still announcing", lc:get_state() == "announcing")

print("\n--- Planner Sends Bridge Ack ---")

tx:clear()
tx.inbox = { json_util.encode({
    type     = "link_bridge_ack",
    robot_id = "rover_1",
    ack_seq  = 1,
    seq      = 0,
    ts       = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }

lc:tick()

check("state is live", lc:get_state() == "live")
check("is_live", lc:is_live())
check("confirm sent", #tx.sent_link == 1)
local confirm = tx:last_sent()
check("confirm type", confirm.type == "link_confirm")
check("confirm class_name", confirm.class_name == "lunar_rover")
check("confirm wire_format", confirm.wire_format == "cbor")
check("confirm capabilities", #confirm.capabilities == 2)
check("confirm energy_max", confirm.energy_max == 10000)

print("\n--- Live: Receives Planner Heartbeats ---")

tx:clear()
tx.inbox = { json_util.encode({
    type      = "link_bridge_heartbeat",
    bridge_id = "planner",
    seq       = 1,
    ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }

lc:tick()
check("still live after heartbeat", lc:is_live())
check("no planner lost", #lost_events == 0)

print("\n--- Live: Sends Keepalive Heartbeats ---")

tx:clear()
-- Force keepalive by setting last_keepalive far in past
lc.last_keepalive = os.time() - 100
lc:tick()

check("keepalive sent", #tx.sent_link == 1)
local keepalive = tx:last_sent()
check("keepalive type is link_heartbeat", keepalive.type == "link_heartbeat")
check("keepalive robot_id", keepalive.robot_id == "rover_1")

print("\n--- Energy Update ---")

lc:set_energy(7500)
tx:clear()
lc.last_keepalive = os.time() - 100
lc:tick()

local hb = tx:last_sent()
check("heartbeat has updated energy", hb.energy_remaining == 7500)

print("\n--- Planner Heartbeat Timeout → planner_lost ---")

tx:clear()
lost_events = {}
-- Simulate planner heartbeat timeout
lc.last_planner_hb = os.time() - 100  -- far in past

lc:tick()

check("planner lost fired", #lost_events == 1)
check("reason is heartbeat_timeout", lost_events[1] == "heartbeat_timeout")
check("state is announcing (re-announce)", lc:get_state() == "announcing")
check("announce sent after planner loss", #tx.sent_link > 0)
local reannounce = tx:last_sent()
check("re-announce type", reannounce.type == "link_announce")

print("\n--- Re-registration After Planner Loss ---")

tx:clear()
tx.inbox = { json_util.encode({
    type     = "link_bridge_ack",
    robot_id = "rover_1",
    ack_seq  = 2,
    seq      = 0,
    ts       = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }

lc:tick()
check("live again after re-registration", lc:is_live())

print("\n--- Planner Seq Reset Detection ---")

tx:clear()
lost_events = {}

-- Receive heartbeats with increasing seq
tx.inbox = { json_util.encode({
    type = "link_bridge_heartbeat", bridge_id = "planner",
    seq = 5, ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }
lc:tick()
check("still live with seq=5", lc:is_live())

-- Seq resets to lower value → planner restarted
tx.inbox = { json_util.encode({
    type = "link_bridge_heartbeat", bridge_id = "planner",
    seq = 1, ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }
lc:tick()

check("planner restart detected", #lost_events == 1)
check("reason is planner_restart", lost_events[1] == "planner_restart")
check("back to announcing", lc:get_state() == "announcing")

print("\n--- Planner Disconnect Message ---")

-- Re-register first
tx:clear()
tx.inbox = { json_util.encode({
    type = "link_bridge_ack", robot_id = "rover_1",
    ack_seq = 3, seq = 0,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }
lc:tick()
check("live again", lc:is_live())

lost_events = {}
tx:clear()
tx.inbox = { json_util.encode({
    type      = "link_bridge_disconnect",
    bridge_id = "planner",
    reason    = "shutdown",
    ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }

lc:tick()

check("planner disconnect detected", #lost_events == 1)
check("reason is shutdown", lost_events[1] == "shutdown")
check("announcing after planner disconnect", lc:get_state() == "announcing")

print("\n--- Robot Shutdown ---")

-- Re-register
tx:clear()
tx.inbox = { json_util.encode({
    type = "link_bridge_ack", robot_id = "rover_1",
    ack_seq = 4, seq = 0,
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
}) }
lc:tick()
check("live for shutdown test", lc:is_live())

tx:clear()
lc:shutdown()

check("offline after shutdown", lc:get_state() == "offline")
check("disconnect sent", #tx.sent_link == 1)
local disc = tx:last_sent()
check("disconnect type", disc.type == "link_disconnect")
check("disconnect reason", disc.reason == "robot_shutdown")

---------------------------------------------------------------------------

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", passed, failed))
if failed > 0 then print("FAILED"); os.exit(1) else print("PASSED") end
