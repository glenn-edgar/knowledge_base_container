-- plan_yaml.lua -- YAML serializer for planner output
--
-- Handles numbers as native YAML numbers (not quoted strings).

local M = {}

local function is_array(t)
  if type(t) ~= "table" then return false end
  local n = #t
  if n == 0 then
    for _ in pairs(t) do return false end
    return true
  end
  local count = 0
  for _ in pairs(t) do count = count + 1 end
  return count == n
end

local function sorted_keys(t)
  local keys = {}
  for k in pairs(t) do keys[#keys + 1] = k end
  table.sort(keys, function(a, b)
    local ta, tb = type(a), type(b)
    if ta ~= tb then return ta < tb end
    return a < b
  end)
  return keys
end

local function needs_quoting(s)
  if s == "" then return true end
  if s:match("^[%s]") or s:match("[%s]$") then return true end
  if s:match("[:#{}&*!|>'\"%@`,%[%]{}]") then return true end
  if s:match("^%-%-") then return true end
  if s == "true" or s == "false" or s == "null" or s == "~" then return true end
  return false
end

local function format_scalar(v)
  local vtype = type(v)
  if vtype == "number" then
    if v == math.floor(v) and math.abs(v) < 2^53 then
      return string.format("%d", v)
    else
      return string.format("%.6g", v)
    end
  elseif vtype == "boolean" then
    return v and "true" or "false"
  elseif v == nil then
    return "null"
  else
    local s = tostring(v)
    if needs_quoting(s) then
      s = s:gsub("\\", "\\\\"):gsub('"', '\\"')
      return '"' .. s .. '"'
    end
    return s
  end
end

local serialize  -- forward decl

serialize = function(buf, value, indent)
  local prefix = string.rep("  ", indent)

  if type(value) == "table" then
    if is_array(value) then
      if #value == 0 then
        buf[#buf + 1] = " []\n"
        return
      end
      buf[#buf + 1] = "\n"
      for i = 1, #value do
        buf[#buf + 1] = prefix .. "- "
        if type(value[i]) == "table" then
          if is_array(value[i]) then
            -- Inline short numeric arrays (like waypoint pairs)
            if #value[i] <= 4 and type(value[i][1]) == "number" then
              local parts = {}
              for j = 1, #value[i] do
                parts[j] = format_scalar(value[i][j])
              end
              buf[#buf + 1] = "[" .. table.concat(parts, ", ") .. "]\n"
            else
              serialize(buf, value[i], indent + 1)
            end
          else
            buf[#buf + 1] = "\n"
            local keys = sorted_keys(value[i])
            for _, k in ipairs(keys) do
              buf[#buf + 1] = prefix .. "  " .. format_scalar(k) .. ":"
              serialize(buf, value[i][k], indent + 2)
            end
          end
        else
          buf[#buf + 1] = format_scalar(value[i]) .. "\n"
        end
      end
    else
      local keys = sorted_keys(value)
      if #keys == 0 then
        buf[#buf + 1] = " {}\n"
        return
      end
      buf[#buf + 1] = "\n"
      for _, k in ipairs(keys) do
        buf[#buf + 1] = prefix .. format_scalar(k) .. ":"
        serialize(buf, value[k], indent + 1)
      end
    end
  else
    buf[#buf + 1] = " " .. format_scalar(value) .. "\n"
  end
end

function M.to_yaml(data)
  local buf = { "# Global Plan YAML Output\n" }
  buf[#buf + 1] = "# Generated: " .. os.date("!%Y-%m-%dT%H:%M:%SZ") .. "\n"
  buf[#buf + 1] = "---\n"

  if type(data) == "table" then
    local keys = sorted_keys(data)
    for _, k in ipairs(keys) do
      buf[#buf + 1] = format_scalar(k) .. ":"
      serialize(buf, data[k], 1)
    end
  else
    buf[#buf + 1] = tostring(data) .. "\n"
  end

  return table.concat(buf)
end

function M.dump_to_file(data, filepath)
  local yaml_str = M.to_yaml(data)
  local fh, err = io.open(filepath, "w")
  if not fh then
    error(string.format("plan_yaml: cannot open '%s': %s", filepath, err))
  end
  fh:write(yaml_str)
  fh:close()
  print(string.format("[plan_yaml] Wrote %d bytes to %s", #yaml_str, filepath))
end

return M
