-- hub_dsl.lua — Stationary hub with pseudo-RPC services
--
-- Defines a fixed SPIKE Prime hub that acts as an RPC server.
-- The robot calls services by name; the hub executes actuator actions
-- and returns structured results.
--
-- Transport (BLE, UART, MQTT, etc.) is NOT specified here.
-- The DSL defines only the service interface: procedures, parameters,
-- return types, timeouts, and fault codes.
--
-- At the ChainTree level, each service compiles to a hidden server node
-- inside a controlled_node_container. The robot's rpc_call action
-- activates the server via the client/server request_port/response_port
-- pattern.

hub = {
  name = "gate_controller",
  hub  = "spike_prime",

  ports = {
    A = { device = "medium_motor", role = "gate_motor" },
  },

  -- Actuators: physical devices the hub controls.
  -- Keyed by name, referenced by services below.
  actuators = {
    gate = {
      port = "A",
      positions = {
        open   = { angle = 90, speed = 50 },
        closed = { angle = 0,  speed = 50 },
      },
    },
  },

  -- Pseudo-RPC service definitions.
  -- Dot notation names map to KB paths: hub.service.gate.open
  --
  -- Fields:
  --   actuator — which actuator this service drives (nil for read-only)
  --   action   — target position name from actuator.positions (nil for queries)
  --   params   — input parameters the caller must provide
  --   returns  — fields in the response
  --   timeout  — ms before caller receives a timeout fault
  --   faults   — possible fault codes the caller must handle
  services = {
    ["gate.open"] = {
      actuator = "gate",
      action   = "open",
      params   = {},
      returns  = { "status" },
      timeout  = 3000,
      faults   = { "jammed", "timeout" },
    },

    ["gate.close"] = {
      actuator = "gate",
      action   = "closed",
      params   = {},
      returns  = { "status" },
      timeout  = 3000,
      faults   = { "jammed", "timeout" },
    },

    ["gate.status"] = {
      actuator = "gate",
      action   = nil,                    -- read-only query, no actuation
      params   = {},
      returns  = { "status", "angle" },
      timeout  = 1000,
      faults   = { "timeout" },
    },
  },
}

return hub
