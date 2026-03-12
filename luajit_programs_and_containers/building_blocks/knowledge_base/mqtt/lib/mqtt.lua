--[[
  mqtt.lua — Unified entry point for MQTT LuaJIT bindings
]]
local kv     = require("lib.mqtt_kv_store")
local queue  = require("lib.mqtt_queue")
local pubsub = require("lib.mqtt_pubsub")

return {
    KvWriter    = kv.Writer,
    KvReader    = kv.Reader,
    Publisher   = queue.Publisher,
    QueueReader = queue.Reader,
    PubSub      = pubsub.PubSub,
    lib_init    = kv.lib_init,
    lib_cleanup = kv.lib_cleanup,
    kv_store = kv, queue = queue, pubsub = pubsub,
}

