--[[
    nats_transport.lua -- NATS-based hub↔robot transport using JobQueue.

    Two job queues per robot, backed by one KeyStore bucket:
      rpc queue:        hub submits commands, robot claims
      stream_bus queue: robot submits updates, hub claims

    Queue names follow KB namespace:
      {site}.robots.{id}.rpc
      {site}.robots.{id}.stream_bus

    No callbacks, no pub/sub, no threading issues.
    Pure synchronous claim/complete lifecycle.

    Hub side:
      tx:send_rpc(json)      -- submit command to rpc queue
      tx:recv_stream()       -- claim next stream_bus job (nil if empty)

    Remote side:
      tx:recv_rpc()          -- claim next rpc job (nil if empty)
      tx:send_stream(json)   -- submit update to stream_bus queue
]]

local M = {}

local NATS_SERVER = "nats://127.0.0.1:4222"

---------------------------------------------------------------------------
-- Shared: create KeyStore + JobQueue for a robot
---------------------------------------------------------------------------

local function make_jq(site, robot_id, worker_name, server)
    local ks_lib = require("lib.nats_key_store")
    local jq_lib = require("lib.nats_job_queue")

    local site_bucket = site:gsub("%.", "_")
    local ks = ks_lib.KeyStore.new({
        server = server,
        bucket = site_bucket .. "_transport",
        create_bucket = true,
        history = 1,
    })
    ks:connect()

    local jq = jq_lib.JobQueue.new(ks:handle(), worker_name)
    return ks, jq
end

-- Drain all stale jobs from queues (call before test runs)
local function flush_queue(jq, queue_names)
    for _, q in ipairs(queue_names) do
        for _ = 1, 100 do  -- safety limit
            local ok, job = pcall(jq.claim_job, jq, { q })
            if not ok or not job then break end
            pcall(jq.complete_job, jq, job.id, "")
        end
    end
end

---------------------------------------------------------------------------
-- Hub side
---------------------------------------------------------------------------

function M.hub_side(robot_id, server, site)
    server = server or NATS_SERVER
    site = site or "moonbase.alpha.surface_ops"

    local ks, jq = make_jq(site, robot_id, "hub_" .. robot_id, server)

    local rpc_queue    = site .. ".robots." .. robot_id .. ".rpc"
    local stream_queue = site .. ".robots." .. robot_id .. ".stream_bus"

    local tx = {}

    function tx:flush()
        flush_queue(jq, { rpc_queue, stream_queue })
    end

    function tx:send_rpc(json_str)
        local ok, job_id = pcall(jq.submit, jq, json_str, rpc_queue, 5, 1, 30)
        return ok and job_id or nil
    end

    function tx:recv_stream()
        local ok, job = pcall(jq.claim_job, jq, { stream_queue })
        if ok and job then
            local payload = job.payload_json
            pcall(jq.complete_job, jq, job.id, "")
            return payload
        end
        return nil
    end

    function tx:close()
        pcall(function() jq:destroy() end)
        pcall(function() ks:disconnect() end)
        pcall(function() ks:destroy() end)
    end

    return tx
end

---------------------------------------------------------------------------
-- Remote side
---------------------------------------------------------------------------

function M.remote_side(robot_id, server, site)
    server = server or NATS_SERVER
    site = site or "moonbase.alpha.surface_ops"

    local ks, jq = make_jq(site, robot_id, "robot_" .. robot_id, server)

    local rpc_queue    = site .. ".robots." .. robot_id .. ".rpc"
    local stream_queue = site .. ".robots." .. robot_id .. ".stream_bus"

    local tx = {}

    function tx:flush()
        flush_queue(jq, { rpc_queue, stream_queue })
    end

    function tx:recv_rpc()
        local ok, job = pcall(jq.claim_job, jq, { rpc_queue })
        if ok and job then
            local payload = job.payload_json
            local job_id = job.id
            pcall(jq.complete_job, jq, job_id, "")
            return payload
        end
        return nil
    end

    function tx:send_stream(json_str)
        local ok, job_id = pcall(jq.submit, jq, json_str, stream_queue, 5, 1, 30)
        return ok and job_id or nil
    end

    function tx:close()
        pcall(function() jq:destroy() end)
        pcall(function() ks:disconnect() end)
        pcall(function() ks:destroy() end)
    end

    return tx
end

---------------------------------------------------------------------------
-- Loopback (in-process, no NATS)
---------------------------------------------------------------------------

function M.loopback()
    local rpc_queue = {}
    local stream_queue = {}

    local hub = {}
    function hub:send_rpc(json)
        rpc_queue[#rpc_queue + 1] = json
        return "ok"
    end
    function hub:recv_stream()
        if #stream_queue == 0 then return nil end
        return table.remove(stream_queue, 1)
    end
    function hub:close() end

    local remote = {}
    function remote:recv_rpc()
        if #rpc_queue == 0 then return nil end
        return table.remove(rpc_queue, 1)
    end
    function remote:send_stream(json)
        stream_queue[#stream_queue + 1] = json
        return "ok"
    end
    function remote:close() end

    return hub, remote
end

return M
