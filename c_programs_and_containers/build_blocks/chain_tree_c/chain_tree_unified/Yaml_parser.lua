--[[
Simple YAML Parser for LuaJIT

This is a minimal YAML parser designed specifically for ChainTree YAML files.
It handles the subset of YAML features used by the ChainTree DSL generator:
  - Key-value pairs
  - Nested dictionaries
  - Lists (arrays)
  - Strings, numbers, booleans, null
  - Quoted strings

Does NOT support:
  - Anchors and aliases (&, *)
  - Multi-line strings (|, >)
  - Complex keys
  - Tags
]]

local M = {}

-- Helper: trim whitespace
local function trim(s)
    return s:match("^%s*(.-)%s*$")
end

-- Helper: count leading spaces
local function count_indent(line)
    local spaces = line:match("^( *)")
    return spaces and #spaces or 0
end

-- Helper: parse a scalar value
local function parse_value(s)
    s = trim(s)
    
    -- Empty or null
    if s == "" or s == "null" or s == "~" then
        return nil
    end
    
    -- Empty inline array
    if s == "[]" then
        return {}
    end
    
    -- Empty inline object
    if s == "{}" then
        return {}
    end
    
    -- Boolean
    if s == "true" or s == "True" or s == "TRUE" then
        return true
    end
    if s == "false" or s == "False" or s == "FALSE" then
        return false
    end
    
    -- Quoted string (single or double)
    local quoted = s:match("^['\"](.*)['\"']$")
    if quoted then
        -- Unescape basic escapes
        quoted = quoted:gsub("\\n", "\n")
        quoted = quoted:gsub("\\t", "\t")
        quoted = quoted:gsub("\\\\", "\\")
        quoted = quoted:gsub("\\'", "'")
        quoted = quoted:gsub('\\"', '"')
        return quoted
    end
    
    -- Number (integer)
    local int_val = s:match("^%-?%d+$")
    if int_val then
        return tonumber(int_val)
    end
    
    -- Number (float)
    local float_val = s:match("^%-?%d+%.%d+$")
    if float_val then
        return tonumber(float_val)
    end
    
    -- Hex number
    local hex_val = s:match("^0x(%x+)$")
    if hex_val then
        return tonumber(hex_val, 16)
    end
    
    -- Plain string
    return s
end

-- Helper: check if line is a list item
local function is_list_item(line)
    return line:match("^%s*%- ")
end

-- Helper: check if line is a key-value pair
local function is_key_value(line)
    -- Match "key:" with optional value
    return line:match("^%s*[%w_][%w_%.-]*%s*:")
end

-- Helper: extract key from "key: value" line
local function extract_key_value(line)
    local key, value = line:match("^%s*([%w_][%w_%.-]*)%s*:%s*(.*)$")
    if key then
        return key, trim(value)
    end
    return nil, nil
end

-- Helper: extract list item content
local function extract_list_item(line)
    local content = line:match("^%s*%-%s+(.*)$")
    if content then
        return trim(content)
    end
    -- Handle "- " with nothing after
    if line:match("^%s*%-%s*$") then
        return ""
    end
    return nil
end

-- Main parser class
local Parser = {}
Parser.__index = Parser

function Parser:new(lines)
    local self = setmetatable({}, Parser)
    self.lines = lines
    self.pos = 1
    return self
end

function Parser:current_line()
    return self.lines[self.pos]
end

function Parser:advance()
    self.pos = self.pos + 1
end

function Parser:at_end()
    return self.pos > #self.lines
end

function Parser:peek_indent()
    while not self:at_end() do
        local line = self:current_line()
        -- Skip empty lines and comments
        if line:match("^%s*$") or line:match("^%s*#") then
            self:advance()
        else
            return count_indent(line)
        end
    end
    return -1
end

function Parser:skip_empty()
    while not self:at_end() do
        local line = self:current_line()
        if line:match("^%s*$") or line:match("^%s*#") then
            self:advance()
        else
            break
        end
    end
end

function Parser:parse_dict(min_indent)
    local dict = {}
    
    while not self:at_end() do
        self:skip_empty()
        if self:at_end() then break end
        
        local line = self:current_line()
        local indent = count_indent(line)
        
        -- If we've dedented past our level, we're done
        if indent < min_indent then
            break
        end
        
        -- Must be at exactly our indent level for keys
        if indent ~= min_indent then
            -- Might be nested content, handled elsewhere
            break
        end
        
        local key, value_str = extract_key_value(line)
        if key then
            self:advance()
            
            if value_str ~= "" then
                -- Inline value
                dict[key] = parse_value(value_str)
            else
                -- Check what follows
                self:skip_empty()
                if self:at_end() then
                    dict[key] = nil
                else
                    local next_indent = count_indent(self:current_line())
                    if next_indent > min_indent then
                        -- Nested content
                        if is_list_item(self:current_line()) then
                            dict[key] = self:parse_list(next_indent)
                        else
                            dict[key] = self:parse_dict(next_indent)
                        end
                    else
                        dict[key] = nil
                    end
                end
            end
        else
            -- Not a key-value line at our indent, done with this dict
            break
        end
    end
    
    return dict
end

function Parser:parse_list(min_indent)
    local list = {}
    
    while not self:at_end() do
        self:skip_empty()
        if self:at_end() then break end
        
        local line = self:current_line()
        local indent = count_indent(line)
        
        -- If we've dedented, we're done
        if indent < min_indent then
            break
        end
        
        if not is_list_item(line) then
            break
        end
        
        local content = extract_list_item(line)
        self:advance()
        
        if content and content ~= "" then
            -- Check if content is a key-value (nested dict in list item)
            local key, val = content:match("^([%w_][%w_%.-]*)%s*:%s*(.*)$")
            if key then
                -- This is the start of a dict within the list item
                local item_dict = {}
                if val ~= "" then
                    item_dict[key] = parse_value(val)
                else
                    -- Check for nested content
                    self:skip_empty()
                    if not self:at_end() then
                        local next_indent = count_indent(self:current_line())
                        if next_indent > indent then
                            if is_list_item(self:current_line()) then
                                item_dict[key] = self:parse_list(next_indent)
                            else
                                item_dict[key] = self:parse_dict(next_indent)
                            end
                        else
                            item_dict[key] = nil
                        end
                    end
                end
                
                -- Continue parsing more keys at the same indent + 2
                self:skip_empty()
                while not self:at_end() do
                    local next_line = self:current_line()
                    local next_indent = count_indent(next_line)
                    
                    if next_indent <= indent or is_list_item(next_line) then
                        break
                    end
                    
                    local k2, v2 = extract_key_value(next_line)
                    if k2 then
                        self:advance()
                        if v2 ~= "" then
                            item_dict[k2] = parse_value(v2)
                        else
                            self:skip_empty()
                            if not self:at_end() then
                                local ni = count_indent(self:current_line())
                                if ni > next_indent then
                                    if is_list_item(self:current_line()) then
                                        item_dict[k2] = self:parse_list(ni)
                                    else
                                        item_dict[k2] = self:parse_dict(ni)
                                    end
                                else
                                    item_dict[k2] = nil
                                end
                            end
                        end
                    else
                        break
                    end
                    self:skip_empty()
                end
                
                table.insert(list, item_dict)
            else
                -- Simple scalar value
                table.insert(list, parse_value(content))
            end
        else
            -- Empty list item or nested content
            self:skip_empty()
            if not self:at_end() then
                local next_indent = count_indent(self:current_line())
                if next_indent > indent then
                    if is_list_item(self:current_line()) then
                        table.insert(list, self:parse_list(next_indent))
                    else
                        table.insert(list, self:parse_dict(next_indent))
                    end
                else
                    table.insert(list, nil)
                end
            end
        end
    end
    
    return list
end

function Parser:parse()
    self:skip_empty()
    if self:at_end() then
        return {}
    end
    
    local first_indent = count_indent(self:current_line())
    
    if is_list_item(self:current_line()) then
        return self:parse_list(first_indent)
    else
        return self:parse_dict(first_indent)
    end
end

--- Parse YAML from a string
-- @param yaml_str YAML content as string
-- @return Parsed data structure
function M.parse(yaml_str)
    -- Split into lines
    local lines = {}
    for line in yaml_str:gmatch("[^\r\n]*") do
        table.insert(lines, line)
    end
    
    local parser = Parser:new(lines)
    return parser:parse()
end

--- Load and parse YAML from a file
-- @param filename Path to YAML file
-- @return Parsed data structure
function M.load_file(filename)
    local f = io.open(filename, "r")
    if not f then
        error("Cannot open file: " .. filename)
    end
    
    local content = f:read("*all")
    f:close()
    
    return M.parse(content)
end

return M