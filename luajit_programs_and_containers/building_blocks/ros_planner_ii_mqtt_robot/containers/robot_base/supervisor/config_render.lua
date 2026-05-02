-- config_render.lua -- ${VAR} substitution from a flat env table.
--
-- Reads a JSON-shaped template (any text), substitutes ${VAR} tokens
-- against env, decodes to verify it's still valid JSON, writes target.
-- Empty env value is allowed; unresolved ${VAR} fails.

local M = {}

local function read_file(path)
    local f, err = io.open(path, "rb")
    if not f then return nil, err end
    local d = f:read("*a"); f:close()
    return d
end

local function write_file(path, data)
    local f, err = io.open(path, "wb")
    if not f then return nil, err end
    f:write(data); f:close()
    return true
end

local function substitute(template, env)
    local unresolved = {}
    local out = template:gsub("%${([A-Za-z_][A-Za-z0-9_]*)}", function(k)
        local v = env[k]
        if v == nil then
            unresolved[#unresolved + 1] = k
            return "${" .. k .. "}"
        end
        return tostring(v)
    end)
    if #unresolved > 0 then
        return nil, "unresolved tokens: " .. table.concat(unresolved, ",")
    end
    return out
end

function M.render(template_path, target_path, env)
    local t, err = read_file(template_path)
    if not t then return nil, "read template: " .. tostring(err) end
    local rendered, serr = substitute(t, env)
    if not rendered then return nil, serr end

    -- Validate as JSON to catch template typos at boot, not at first use.
    local json = require("json_util")
    local ok, decoded = pcall(json.decode, rendered)
    if not ok or decoded == nil then
        return nil, "rendered config is not valid JSON: " ..
                    tostring(decoded or "<nil>")
    end

    local wok, werr = write_file(target_path, rendered)
    if not wok then return nil, "write config: " .. tostring(werr) end
    return target_path
end

return M
