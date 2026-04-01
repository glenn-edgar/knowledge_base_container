--[[
  nats.lua — Unified entry point for all NATS LuaJIT bindings

  Usage:
    local nats = require("lib.nats")

    local ks  = nats.KeyStore.new({ bucket = "test" })
    local kb  = nats.KbStore.new("nats://127.0.0.1:4222", "my_kb", "desc")
    local jq  = nats.JobQueue.new(ks:handle(), "worker-1")
    local cli = nats.RpcClient.new({ server = "nats://127.0.0.1:4222" })
    local ps  = nats.PubSub.new({ server = "nats://127.0.0.1:4222" })
]]

local key_store = require("lib.nats_key_store")
local kb_store  = require("lib.nats_kb_store")
local job_queue = require("lib.nats_job_queue")
local rpc       = require("lib.nats_rpc")
local pubsub    = require("lib.nats_pubsub")
local stream    = require("lib.nats_stream")

return {
    -- Classes
    KeyStore     = key_store.KeyStore,
    KbStore      = kb_store.KbStore,
    JobQueue     = job_queue.JobQueue,
    RpcServer    = rpc.RpcServer,
    RpcClient    = rpc.RpcClient,
    PubSub       = pubsub.PubSub,
    StreamBuffer = stream.StreamBuffer,

    -- Sub-modules (for status codes, raw C handles, etc.)
    key_store = key_store,
    kb_store  = kb_store,
    job_queue = job_queue,
    rpc       = rpc,
    pubsub    = pubsub,
    stream    = stream,
}
