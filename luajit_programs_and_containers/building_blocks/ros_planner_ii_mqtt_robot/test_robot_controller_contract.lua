-- test_robot_controller_contract.lua -- Self-test for the
-- robot_controller_test_peer fixture. Exercises every contract verb
-- against an in-process peer and asserts behavior matches
-- docs/controller/contract.md. Lets us catch contract drift between
-- the document and the harness without standing up a transport.
--
-- Usage:  luajit test_robot_controller_contract.lua

local rc_peer = require("robot_controller_test_peer")

local pass_count, fail_count = 0, 0

local function check(name, ok, detail)
    if ok then
        pass_count = pass_count + 1
        io.stderr:write(string.format("  PASS  %s\n", name))
    else
        fail_count = fail_count + 1
        io.stderr:write(string.format("  FAIL  %s   %s\n",
                                      name, tostring(detail or "")))
    end
end

local function assert_eq(name, got, want)
    check(name, got == want,
          string.format("got=%s want=%s", tostring(got), tostring(want)))
end

-- ---------- 1. REGISTER happy path ----------

local ctrl = rc_peer.new()

local ack = ctrl:handle{
    type             = "register",
    robot_id         = "rover_1",
    robot_class      = "lunar_rover",
    dongle_instance  = 1,
    capabilities     = { "init_check", "path_line", "path_spline" },
    energy_max       = 10000,
    energy_remaining = 10000,
    boot_ts          = "2026-05-02T00:00:00Z",
    client_version   = "robot_base:1.0+L6",
}
assert_eq("register: ack",          ack.ack,          true)
assert_eq("register: ack type",     ack.type,         "register_ack")
check    ("register: session_id",   type(ack.session_id) == "string"
                                    and #ack.session_id > 0)
check    ("register: kb_root",      ack.kb_root == "rovers.rover_1")
check    ("register: policy",       type(ack.policy) == "table"
                                    and ack.policy.heartbeat_period_s == 10)
local session_id = ack.session_id

-- ---------- 2. dongle_collision ----------

local ack_dup = ctrl:handle{
    type            = "register",
    robot_id        = "rover_2_clone",        -- different robot_id, same dongle
    robot_class     = "lunar_rover",
    dongle_instance = 1,
    capabilities    = {},
    energy_max      = 10000,
    client_version  = "robot_base:1.0+L6",
}
assert_eq("collision: ack false",   ack_dup.ack,    false)
assert_eq("collision: reason",      ack_dup.reason, "dongle_collision")
check    ("collision: conflict id", ack_dup.conflict_session_id == session_id)

-- ---------- 3. HEARTBEAT ----------

local hb_ack = ctrl:handle{
    type             = "heartbeat",
    session_id       = session_id,
    seq              = 1,
    energy_remaining = 9876,
    fault_active     = false,
    current_phase    = "monitor",
    active_workers   = { "worker_idle" },
    ts               = "2026-05-02T00:00:01Z",
}
assert_eq("heartbeat: ok",          hb_ack.ok,  true)
assert_eq("heartbeat: seq echoed",  hb_ack.seq, 1)

-- bad session
local bad_hb = ctrl:handle{
    type = "heartbeat", session_id = "bogus", seq = 1,
}
assert_eq("heartbeat: bad session", bad_hb.ok, false)

-- ---------- 4. EXCEPTION (idempotent) ----------

local exc_req = {
    type         = "exception",
    session_id   = session_id,
    exception_id = "01H-test-0001",
    severity     = "ERROR",
    kind         = "tool_fault",
    subject      = "worker_recharge",
    details      = { fault_reason = "charger_fault" },
    ts           = "2026-05-02T00:00:02Z",
}
local exc_ack1 = ctrl:handle(exc_req)
local exc_ack2 = ctrl:handle(exc_req)   -- dedupe
assert_eq("exception: 1st ok",      exc_ack1.ok, true)
assert_eq("exception: 2nd ok",      exc_ack2.ok, true)
assert_eq("exception: id echoed",   exc_ack1.exception_id, "01H-test-0001")
assert_eq("exception: deduped",     #ctrl.exceptions_seen,  1)

-- ---------- 5. KB_READ ----------

ctrl.kb_store["rovers.rover_1.config.tunables_v2"] = {
    value     = { mass_kg = 8, energy_max = 10000 },
    version   = "v3",
    version_n = 3,
}

local rd = ctrl:handle{
    type       = "kb_read",
    session_id = session_id,
    request_id = "r1",
    path       = "rovers.rover_1.config.tunables_v2",
}
assert_eq("kb_read: ok",            rd.ok,      true)
assert_eq("kb_read: version",       rd.version, "v3")
check    ("kb_read: value mass",    rd.value and rd.value.mass_kg == 8)

-- field projection
local rd_f = ctrl:handle{
    type       = "kb_read",
    session_id = session_id,
    request_id = "r2",
    path       = "rovers.rover_1.config.tunables_v2",
    fields     = { "mass_kg" },
}
check    ("kb_read: field projected", rd_f.value and rd_f.value.mass_kg == 8
                                     and rd_f.value.energy_max == nil)

-- not found
local rd_nf = ctrl:handle{
    type = "kb_read", session_id = session_id, request_id = "r3",
    path = "rovers.rover_1.does_not_exist",
}
assert_eq("kb_read: not_found",     rd_nf.error, "not_found")

-- ---------- 6. KB_WRITE: scoped ----------

local wr = ctrl:handle{
    type       = "kb_write",
    session_id = session_id,
    request_id = "w1",
    path       = "rovers.rover_1.state.last_mission",
    value      = { kind = "paths_only", at = 1234 },
}
assert_eq("kb_write: ok",           wr.ok,      true)
assert_eq("kb_write: version",      wr.version, "v1")

-- denied: outside kb_root
local wr_denied = ctrl:handle{
    type       = "kb_write",
    session_id = session_id,
    request_id = "w2",
    path       = "rovers.other_rover.state",
    value      = {},
}
assert_eq("kb_write: denied",       wr_denied.error, "denied")

-- CAS miss
local wr_cas = ctrl:handle{
    type       = "kb_write",
    session_id = session_id,
    request_id = "w3",
    path       = "rovers.rover_1.state.last_mission",
    value      = { kind = "stale" },
    if_match   = "v999",
}
assert_eq("kb_write: cas miss",     wr_cas.error,   "version_mismatch")
assert_eq("kb_write: cas current",  wr_cas.version, "v1")

-- ---------- 7. SHUTDOWN_NOTICE ----------

local sd = ctrl:handle{
    type       = "shutdown_notice",
    session_id = session_id,
    reason     = "sigterm",
}
assert_eq("shutdown: ok",           sd.ok, true)
assert_eq("shutdown: live count",   ctrl:live_count(), 0)

-- After shutdown, dongle_instance is free for re-registration.
local re_ack = ctrl:handle{
    type            = "register",
    robot_id        = "rover_1_replacement",
    robot_class     = "lunar_rover",
    dongle_instance = 1,
    capabilities    = {},
    energy_max      = 10000,
    client_version  = "robot_base:1.0+L6",
}
assert_eq("re-register: ok",        re_ack.ack, true)

-- ---------- 8. controller-initiated verb shapes ----------

local drain = ctrl:make_drain(session_id, "2026-05-02T00:05:00Z")
assert_eq("drain: type",            drain.type,         "drain")
assert_eq("drain: session_id",      drain.session_id,   session_id)
assert_eq("drain: deadline",        drain.deadline,     "2026-05-02T00:05:00Z")

local pause = ctrl:make_pause(session_id, "p1")
assert_eq("pause: type",            pause.type,         "pause")
assert_eq("pause: request_id",      pause.request_id,   "p1")

local inv = ctrl:make_kb_invalidate(session_id, { "x", "y" })
assert_eq("invalidate: type",       inv.type,           "kb_invalidate")
assert_eq("invalidate: paths len",  #inv.paths,          2)

-- ---------- 9. unknown verb ----------

local resp, err = ctrl:handle{ type = "ufo" }
check    ("unknown verb: nil resp", resp == nil)
check    ("unknown verb: errstr",   type(err) == "string"
                                    and err:match("^unknown_verb"))

-- ---------- summary ----------

io.stderr:write(string.format(
    "\n%s  %d passed, %d failed\n",
    fail_count == 0 and "[PASS]" or "[FAIL]",
    pass_count, fail_count))
os.exit(fail_count == 0 and 0 or 1)
