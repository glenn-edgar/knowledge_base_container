--[[
  KB_Bit_Structures - Bit structure management, S-expression processor,
  and KB_BIT_DATA record type.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local kb_bit = require("kb_bit_structures")

    -- Data record
    local data = kb_bit.KB_BIT_DATA.new()

    -- S-expression evaluation
    local proc = kb_bit.SExpressionProcessor.new()
    local result = proc:execute("(and user1:flag1 user2:flag2)", kb_data)

    -- Full bit-structure manager (inherits SExpressionProcessor)
    local bs = kb_bit.KB_Bit_Structures.new(kb_search, "my_database")
]]

local dkjson = require("dkjson")
local BitMaskOperations = require("bit_mask_operations")

local M = {}   -- module table

---------------------------------------------------------------------------
-- KB_BIT_DATA  (plain record / dataclass equivalent)
---------------------------------------------------------------------------

local KB_BIT_DATA = {}
KB_BIT_DATA.__index = KB_BIT_DATA

function KB_BIT_DATA.new(init)
  init = init or {}
  local self = setmetatable({}, KB_BIT_DATA)
  self.user_name   = init.user_name   or ""
  self.bit_size    = init.bit_size    or 1
  self.flags       = init.flags       or {}
  self.flags_mask  = init.flags_mask  or {}
  self.flag_data   = init.flag_data   or {}
  self.flag_change = init.flag_change or {}
  self.bit_mask    = init.bit_mask    or 0
  self.node_id     = init.node_id     or ""
  return self
end

M.KB_BIT_DATA = KB_BIT_DATA

---------------------------------------------------------------------------
-- Token types (enum-style constants)
---------------------------------------------------------------------------

local TokenType = {
  LPAREN    = "LPAREN",
  RPAREN    = "RPAREN",
  OPERATOR  = "OPERATOR",
  REFERENCE = "REFERENCE",
  KEYWORD   = "KEYWORD",
}
M.TokenType = TokenType

---------------------------------------------------------------------------
-- Token record
---------------------------------------------------------------------------

local Token = {}
Token.__index = Token
function Token.new(tt, val) return setmetatable({ type = tt, value = val }, Token) end
M.Token = Token

---------------------------------------------------------------------------
-- AST node types
---------------------------------------------------------------------------

local SExpNode = {}
SExpNode.__index = SExpNode
function SExpNode.new(operator, operands)
  return setmetatable({ operator = operator, operands = operands or {} }, SExpNode)
end
M.SExpNode = SExpNode

local PlainList = {}
PlainList.__index = PlainList
function PlainList.new(items)
  return setmetatable({ items = items or {} }, PlainList)
end
M.PlainList = PlainList

---------------------------------------------------------------------------
-- SExpressionProcessor
---------------------------------------------------------------------------

local VALID_OPERATORS = { bit_changed = true, ["and"] = true, ["or"] = true, ["if"] = true, cond = true }
local KEYWORDS        = { ["else"] = true }

local SExpressionProcessor = {}
SExpressionProcessor.__index = SExpressionProcessor

function SExpressionProcessor.new()
  local self = setmetatable({}, SExpressionProcessor)
  self.tokens   = {}
  self.position = 0
  return self
end

--- Tokenize an S-expression string.
function SExpressionProcessor:tokenize(s_expr)
  local tokens = {}
  s_expr = s_expr:match("^%s*(.-)%s*$")  -- trim
  local i = 1
  local len = #s_expr

  while i <= len do
    local ch = s_expr:sub(i, i)

    -- whitespace
    if ch:match("%s") then
      i = i + 1

    -- parens
    elseif ch == "(" then
      tokens[#tokens + 1] = Token.new(TokenType.LPAREN, "(")
      i = i + 1
    elseif ch == ")" then
      tokens[#tokens + 1] = Token.new(TokenType.RPAREN, ")")
      i = i + 1

    -- symbol
    else
      local start = i
      while i <= len and not s_expr:sub(i, i):match("[%s%(%)]") do
        i = i + 1
      end
      local symbol = s_expr:sub(start, i - 1)

      if VALID_OPERATORS[symbol] then
        tokens[#tokens + 1] = Token.new(TokenType.OPERATOR, symbol)
      elseif KEYWORDS[symbol] then
        tokens[#tokens + 1] = Token.new(TokenType.KEYWORD, symbol)
      elseif symbol:find(":") then
        tokens[#tokens + 1] = Token.new(TokenType.REFERENCE, symbol)
      else
        error(string.format(
          "Invalid symbol: '%s'. Must be operator, keyword, or user_name:flag_name reference.", symbol))
      end
    end
  end
  return tokens
end

--- Parse token list into AST (internal).
function SExpressionProcessor:_parse_tokens(tokens)
  self.tokens   = tokens
  self.position = 1  -- Lua is 1-indexed
  return self:_parse_expression()
end

function SExpressionProcessor:_parse_expression()
  if self.position > #self.tokens then
    error("Unexpected end of expression")
  end

  local tok = self.tokens[self.position]

  if tok.type == TokenType.LPAREN then
    self.position = self.position + 1
    if self.position > #self.tokens then error("Expected content after '('") end

    local next_tok = self.tokens[self.position]

    if next_tok.type == TokenType.OPERATOR then
      -- operator expression
      local operator = next_tok.value
      self.position = self.position + 1

      local operands = {}
      while self.position <= #self.tokens and self.tokens[self.position].type ~= TokenType.RPAREN do
        operands[#operands + 1] = self:_parse_expression()
      end
      if self.position > #self.tokens then error("Missing closing parenthesis") end
      self.position = self.position + 1  -- consume RPAREN
      return SExpNode.new(operator, operands)
    else
      -- plain list
      local items = {}
      while self.position <= #self.tokens and self.tokens[self.position].type ~= TokenType.RPAREN do
        items[#items + 1] = self:_parse_expression()
      end
      if self.position > #self.tokens then error("Missing closing parenthesis") end
      self.position = self.position + 1
      return PlainList.new(items)
    end

  elseif tok.type == TokenType.REFERENCE then
    self.position = self.position + 1
    return tok.value

  elseif tok.type == TokenType.KEYWORD then
    self.position = self.position + 1
    return tok.value

  else
    error("Unexpected token: " .. tok.value)
  end
end

--- Execute an S-expression against a kb_data dictionary.
-- @param s_expr  string or token list
-- @param kb_data table mapping user_name → KB_BIT_DATA
-- @return boolean
function SExpressionProcessor:execute(s_expr, kb_data)
  local tokens
  if type(s_expr) == "string" then
    tokens = self:tokenize(s_expr)
  else
    tokens = s_expr
  end
  local tree = self:_parse_tokens(tokens)
  return self:_evaluate(tree, kb_data)
end

function SExpressionProcessor:_evaluate(node, kb_data)
  if getmetatable(node) == PlainList then
    error("Plain list used in invalid context")
  end

  -- leaf reference string
  if type(node) == "string" then
    if KEYWORDS[node] then error("Keyword '" .. node .. "' used in invalid context") end
    return self:_lookup_reference(node, kb_data, true, false)
  end

  -- operator node
  local op = node.operator
  local ops = node.operands

  if op == "bit_changed" then
    for _, o in ipairs(ops) do
      if not self:_check_bit_changed(o, kb_data) then return false end
    end
    return true

  elseif op == "and" then
    for _, o in ipairs(ops) do
      if not self:_evaluate(o, kb_data) then return false end
    end
    return true

  elseif op == "or" then
    for _, o in ipairs(ops) do
      if self:_evaluate(o, kb_data) then return true end
    end
    return false

  elseif op == "if" then
    if #ops ~= 3 then
      error(string.format("'if' requires exactly 3 operands, got %d", #ops))
    end
    if self:_evaluate(ops[1], kb_data) then
      return self:_evaluate(ops[2], kb_data)
    else
      return self:_evaluate(ops[3], kb_data)
    end

  elseif op == "cond" then
    for _, operand in ipairs(ops) do
      if getmetatable(operand) ~= PlainList then
        error("'cond' clauses must be lists")
      end
      if #operand.items ~= 2 then
        error(string.format("'cond' clause must have 2 elements, got %d", #operand.items))
      end
      local test = operand.items[1]
      local expr = operand.items[2]
      if type(test) == "string" and test == "else" then
        return self:_evaluate(expr, kb_data)
      end
      if self:_evaluate(test, kb_data) then
        return self:_evaluate(expr, kb_data)
      end
    end
    error("'cond' expression: no conditions matched and no 'else' clause provided")

  else
    error("Unknown operator: " .. op)
  end
end

function SExpressionProcessor:_check_bit_changed(operand, kb_data)
  if type(operand) == "string" then
    return self:_lookup_reference(operand, kb_data, false, true)
  elseif getmetatable(operand) == PlainList then
    error("Plain list used in bit_changed context")
  else
    return self:_evaluate(operand, kb_data)
  end
end

function SExpressionProcessor:_lookup_reference(reference, kb_data, need_value, need_change)
  if not reference:find(":") then
    error(string.format("Invalid reference format: '%s'. Expected 'user_name:flag_name'", reference))
  end
  local user_name, flag_name = reference:match("^([^:]+):(.+)$")
  if not kb_data[user_name] then
    error(string.format("User '%s' not found in KB data", user_name))
  end
  local entry = kb_data[user_name]

  if need_change then
    if entry.flag_change[flag_name] == nil then
      error(string.format("Flag '%s' not found in flag_change for user '%s'", flag_name, user_name))
    end
    return entry.flag_change[flag_name]
  end

  if need_value then
    if entry.flag_data[flag_name] == nil then
      error(string.format("Flag '%s' not found in flag_data for user '%s'", flag_name, user_name))
    end
    return entry.flag_data[flag_name] == 1
  end

  -- default
  if entry.flag_data[flag_name] == nil then
    error(string.format("Flag '%s' not found in flag_data for user '%s'", flag_name, user_name))
  end
  return entry.flag_data[flag_name] == 1
end

M.SExpressionProcessor = SExpressionProcessor

---------------------------------------------------------------------------
-- KB_Bit_Structures  (inherits SExpressionProcessor behaviour via delegation)
---------------------------------------------------------------------------

local KB_Bit_Structures = {}
KB_Bit_Structures.__index = KB_Bit_Structures

function KB_Bit_Structures.new(kb_search, database)
  local self = setmetatable({}, KB_Bit_Structures)
  self.kb_search          = kb_search
  self.database           = database
  self.dbh                = kb_search:get_connection()
  self.bit_mask_operations = BitMaskOperations.new(self.dbh)
  -- Embed an SExpressionProcessor for S-expr methods
  self._sexpr             = SExpressionProcessor.new()
  return self
end

-- Delegate S-expression methods
function KB_Bit_Structures:tokenize(s)      return self._sexpr:tokenize(s) end
function KB_Bit_Structures:execute(s, kb)   return self._sexpr:execute(s, kb) end

---------------------------------------------------------------------------
-- Node discovery
---------------------------------------------------------------------------

function KB_Bit_Structures:find_bit_structure_id(kb, node_name, properties, node_path)
  local results = self:find_bit_structure_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No bit structure found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple bit structures (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results[1]
end

function KB_Bit_Structures:find_bit_structure_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_BIT_MASK")

  if kb        then self.kb_search:search_kb(kb) end
  if node_name then self.kb_search:search_name(node_name) end
  if properties and type(properties) == "table" then
    for k, v in pairs(properties) do
      self.kb_search:search_property_value(k, v)
    end
  end
  if node_path then self.kb_search:search_path(node_path) end

  local rows = self.kb_search:execute_query()
  if not rows or #rows == 0 then
    error(string.format("No bit structures found: name=%s", tostring(node_name)))
  end
  return rows
end

---------------------------------------------------------------------------
-- Assemble bit data from KB rows
---------------------------------------------------------------------------

--- Assemble a single KB_BIT_DATA from a knowledge-base row.
function KB_Bit_Structures:assemble_bit_data(row)
  local rd = KB_BIT_DATA.new()
  local props = row.properties
  if type(props) == "string" then props = dkjson.decode(props) or {} end

  rd.user_name = props.user_name or ""
  rd.bit_size  = tonumber(props.mask_size) or 1
  rd.node_id   = props.record_id or ""

  -- flags from the stored JSON string
  local flags_raw = props.flag_dictionary
  if type(flags_raw) == "string" then
    rd.flags = dkjson.decode(flags_raw) or {}
  elseif type(flags_raw) == "table" then
    rd.flags = flags_raw
  else
    rd.flags = {}
  end

  -- build per-flag masks
  for flag_name, flag_info in pairs(rd.flags) do
    local bit_pos = tonumber(flag_info.bit) or 0
    rd.flags_mask[flag_name] = math.pow(2, bit_pos)  -- 1 << bit
  end

  -- read current mask from DB
  local bm = self.bit_mask_operations:get_bit_mask(props.record_id)
  rd.bit_mask = bm or 0

  for flag_name, _ in pairs(rd.flags) do
    local masked = rd.bit_mask % (rd.flags_mask[flag_name] * 2)
    -- integer test: (bit_mask & mask) ~= 0
    if masked >= rd.flags_mask[flag_name] then
      rd.flag_data[flag_name] = 1
    else
      rd.flag_data[flag_name] = 0
    end
    rd.flag_change[flag_name] = false
  end

  return rd
end

--- Build a dict of user_name → KB_BIT_DATA from query rows.
function KB_Bit_Structures:find_assemble_bit_data(table_dict_rows, clear_flag_data, user_names)
  if not table_dict_rows or #table_dict_rows == 0 then return {} end

  if user_names then
    if #user_names ~= #table_dict_rows then
      error("Number of user names must match number of table dict rows")
    end
    for i, name in ipairs(user_names) do
      local props = table_dict_rows[i].properties
      if type(props) == "string" then
        props = dkjson.decode(props) or {}
        table_dict_rows[i].properties = props
      end
      props.user_name = name
    end
  end

  local rv = {}
  for _, row in ipairs(table_dict_rows) do
    if clear_flag_data then
      local props = row.properties
      if type(props) == "string" then props = dkjson.decode(props) or {} end
      self.bit_mask_operations:set_bit_mask(props.record_id, 0, -1)
    end
    local dc = self:assemble_bit_data(row)
    rv[dc.user_name] = dc
  end
  return rv
end

---------------------------------------------------------------------------
-- Convenience wrappers
---------------------------------------------------------------------------

function KB_Bit_Structures:get_bit_mask(node_id)
  return self.bit_mask_operations:get_bit_mask(node_id)
end

function KB_Bit_Structures:set_bit_mask(node_id, new_bits, change_mask)
  return self.bit_mask_operations:set_bit_mask(node_id, new_bits, change_mask or -1)
end

function KB_Bit_Structures:set_all_ones(node_id)
  return self.bit_mask_operations:set_bit_mask(node_id, -1, -1)
end

function KB_Bit_Structures:set_all_zeros(node_id)
  return self.bit_mask_operations:set_bit_mask(node_id, 0, -1)
end

--- Set specific flags by name.
-- @param data_class KB_BIT_DATA instance
-- @param flag_vals  table { flag_name = 0|1, ... }
function KB_Bit_Structures:set_flag_data(data_class, flag_vals)
  local mask = 0
  local change_mask = 0

  for flag_name, val in pairs(flag_vals) do
    if not data_class.flags[flag_name] then
      error(string.format("Flag '%s' not found in data class", flag_name))
    end
    if val ~= 0 and val ~= 1 then
      error(string.format("Flag data must be 0 or 1, got %s", tostring(val)))
    end
    local fm = data_class.flags_mask[flag_name]
    if val == 1 then
      mask = mask + fm   -- set bit
    end
    change_mask = change_mask + fm
  end

  -- NOTE: original Python passes (node_id, change_mask, mask) – args swapped
  self.bit_mask_operations:set_bit_mask(data_class.node_id, change_mask, mask)
end

--- Refresh flag_data / flag_change from the database.
function KB_Bit_Structures:get_flag_data(data_class)
  local bm = self.bit_mask_operations:get_bit_mask(data_class.node_id)
  data_class.bit_mask = bm or 0

  for flag_name, _ in pairs(data_class.flags) do
    local fm = data_class.flags_mask[flag_name]
    local masked = data_class.bit_mask % (fm * 2)
    local current = (masked >= fm) and 1 or 0

    if current ~= data_class.flag_data[flag_name] then
      data_class.flag_change[flag_name] = true
    else
      data_class.flag_change[flag_name] = false
    end
    data_class.flag_data[flag_name] = current
  end

  return data_class.flag_data
end

M.KB_Bit_Structures = KB_Bit_Structures

return M


