--[[
    mission_client.lua -- Submit a mission and track its progress.

    Reads a mission JSON file, submits it to the planner's NATS job queue,
    then polls NATS KV for status updates and the final result.

    Usage: luajit mission_client.lua <mission.json>
    (normally called via submit_mission.sh)

    Exit codes:
      0 = mission completed successfully
      1 = mission failed or error
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util = require("json_util")

local nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site        = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

---------------------------------------------------------------------------
-- Read mission JSON file
---------------------------------------------------------------------------
local mission_file = arg and arg[1]
if not mission_file then
    io.stderr:write("Usage: luajit mission_client.lua <mission.json>\n")
    os.exit(1)
end

local f = io.open(mission_file, "r")
if not f then
    io.stderr:write("Error: cannot open " .. mission_file .. "\n")
    os.exit(1)
end
local mission_json = f:read("*a")
f:close()

local ok, mission_cmd = pcall(json_util.decode, mission_json)
if not ok or not mission_cmd then
    io.stderr:write("Error: invalid JSON in " .. mission_file .. "\n")
    os.exit(1)
end

local robot_id = mission_cmd.robot_id
if not robot_id then
    io.stderr:write("Error: mission JSON missing robot_id\n")
    os.exit(1)
end

print(string.format("=== Mission Client ==="))
print(string.format("Robot:   %s", robot_id))
print(string.format("Board:   %s", mission_cmd.board or "default"))
print(string.format("Start:   %s", mission_cmd.start or "?"))
print(string.format("Stops:   %d", mission_cmd.stops and #mission_cmd.stops or 0))
print(string.format("Server:  %s", nats_server))
print("")

---------------------------------------------------------------------------
-- Connect to NATS
---------------------------------------------------------------------------
local ks_lib = require("lib.nats_key_store")
local jq_lib = require("lib.nats_job_queue")

local site_bucket = site:gsub("%.", "_")

-- Job queue for submission
local submit_ks = ks_lib.KeyStore.new({
    server        = nats_server,
    bucket        = site_bucket .. "_action_server",
    description   = "Action server mission queue: " .. site,
    create_bucket = true,
    history       = 1,
    client_name   = "mission_client",
})
submit_ks:connect()
local jq = jq_lib.JobQueue.new(submit_ks:handle(), "mission_client")

-- KV reader for status/result polling
local status_ks = ks_lib.KeyStore.new({
    server      = nats_server,
    bucket      = site_bucket .. "_action_server",
    history     = 1,
    client_name = "mission_client_reader",
})
status_ks:connect()

---------------------------------------------------------------------------
-- Submit mission to job queue
---------------------------------------------------------------------------
local queue_name = site .. ".action_server.missions"
print("Submitting mission...")

local submit_ok, job_id = pcall(jq.submit, jq, mission_json, queue_name, 5, 1, 600)
if not submit_ok then
    io.stderr:write("Error: failed to submit mission: " .. tostring(job_id) .. "\n")
    submit_ks:disconnect()
    submit_ks:destroy()
    status_ks:disconnect()
    status_ks:destroy()
    os.exit(1)
end

print(string.format("Submitted (job_id=%s)", tostring(job_id)))
print("")

---------------------------------------------------------------------------
-- Poll for status and result
---------------------------------------------------------------------------
local status_key = site .. ".action_server." .. robot_id .. ".status"
local result_key = site .. ".action_server." .. robot_id .. ".result"

local last_state = ""
local timeout = 300  -- 5 minute timeout
local start_time = os.time()

print("Tracking mission progress...")

while true do
    -- Check timeout
    if os.time() - start_time > timeout then
        io.stderr:write("\nError: mission timed out after " .. timeout .. "s\n")
        break
    end

    -- Poll status
    local val = status_ks:get(status_key)
    if val then
        local s_ok, status = pcall(json_util.decode, val)
        if s_ok and status and status.state ~= last_state then
            last_state = status.state
            local detail = ""
            if status.actions then
                detail = string.format(" (%d actions)", status.actions)
            end
            if status.error then
                detail = detail .. " — " .. status.error
            end
            print(string.format("  Status: %s%s", status.state, detail))

            -- Terminal states
            if status.state == "completed" or status.state == "failed" then
                break
            end
        end
    end

    ffi.C.usleep(200000)  -- 200ms poll interval
end

---------------------------------------------------------------------------
-- Read final result
---------------------------------------------------------------------------
print("")

local result_val = status_ks:get(result_key)
local exit_code = 1

if result_val then
    local r_ok, result = pcall(json_util.decode, result_val)
    if r_ok and result then
        print("=== Mission Result ===")
        print(string.format("Success:   %s", tostring(result.success)))
        print(string.format("Completed: %s/%s actions",
            tostring(result.completed), tostring(result.total)))

        if result.elapsed_ms then
            print(string.format("Elapsed:   %dms", result.elapsed_ms))
        end
        if result.replans then
            print(string.format("Replans:   %d", result.replans))
        end

        if result.final_pose then
            local p = result.final_pose
            print(string.format("Pose:      x=%.0f y=%.0f heading=%.0f arm=%.0f",
                p.x or 0, p.y or 0, p.heading or 0, p.arm_angle or 0))
        end

        if result.fault then
            print(string.format("Fault:     %s", result.fault.reason or "unknown"))
            if result.fault.detail then
                print(string.format("Detail:    %s", result.fault.detail))
            end
        end

        if result.success then
            exit_code = 0
        end
    else
        io.stderr:write("Error: could not parse result\n")
    end
else
    io.stderr:write("Error: no result found in KV store\n")
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
pcall(function() jq:destroy() end)
pcall(function() submit_ks:disconnect() end)
pcall(function() submit_ks:destroy() end)
pcall(function() status_ks:disconnect() end)
pcall(function() status_ks:destroy() end)

os.exit(exit_code)
