-- env_validate.lua -- robot_base ENV gate.
--
-- Required:  ROBOT_ID, ROBOT_CLASS, DONGLE_INSTANCE
-- Optional:  MQTT_HOST=localhost, MQTT_PORT=1883,
--            VMRT_KB_SITE=moonbase.alpha.surface_ops,
--            DONGLE_TYPE=1, SLAVE_ADDR=1, SPEED_FACTOR=1.0,
--            HAL_MODE=dongle, ENERGY_MAX=10000,
--            ENERGY_INFINITE=false, WIRE_FORMAT=json
--
-- Class image MUST set ROBOT_CLASS_BAKED via Dockerfile ENV; mismatch
-- with runtime ROBOT_CLASS = fail-stop.

local M = {}

local DEFAULTS = {
    MQTT_HOST       = "localhost",
    MQTT_PORT       = "1883",
    VMRT_KB_SITE    = "moonbase.alpha.surface_ops",
    DONGLE_TYPE     = "1",
    SLAVE_ADDR      = "1",
    SPEED_FACTOR    = "1.0",
    HAL_MODE        = "dongle",
    ENERGY_MAX      = "10000",
    ENERGY_INFINITE = "false",
    WIRE_FORMAT     = "json",
}

local REQUIRED = { "ROBOT_ID", "ROBOT_CLASS", "DONGLE_INSTANCE" }

function M.gather()
    local env, missing = {}, {}
    for _, k in ipairs(REQUIRED) do
        local v = os.getenv(k)
        if not v or v == "" then
            missing[#missing + 1] = k
        else
            env[k] = v
        end
    end
    if #missing > 0 then
        return nil, "missing required env: " .. table.concat(missing, ",")
    end
    for k, dv in pairs(DEFAULTS) do
        local v = os.getenv(k)
        env[k] = (v ~= nil and v ~= "") and v or dv
    end
    local baked = os.getenv("ROBOT_CLASS_BAKED")
    if baked and baked ~= "" and baked ~= env.ROBOT_CLASS then
        return nil, string.format(
            "ROBOT_CLASS mismatch: baked=%s runtime=%s", baked, env.ROBOT_CLASS)
    end
    env.ROBOT_CLASS_BAKED = baked or env.ROBOT_CLASS
    return env
end

return M
