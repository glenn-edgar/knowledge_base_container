#!/usr/bin/env luajit
-- =============================================================================
-- test_container_spec_validator.lua -- unit tests for spec validation.
-- =============================================================================

local SCRIPT_DIR = (arg[0] or ""):match("(.+)/[^/]+$") or "."
package.path = SCRIPT_DIR .. "/../?.lua;" .. package.path

local validator = require("container_spec_validator")

local pass, fail = 0, 0
local function expect(cond, msg)
  if cond then pass = pass + 1; print("  PASS: " .. msg)
  else fail = fail + 1; print("  FAIL: " .. msg) end
end

local function assert_invalid(spec, name, expected_substring)
  local ok, err = validator.validate(spec, name)
  expect(not ok, "invalid spec rejected: " .. name)
  expect(err and err:find(expected_substring, 1, true),
    "error mentions '" .. expected_substring .. "' (got: " .. tostring(err) .. ")")
end

-- valid spec
do
  local ok, err = validator.validate({
    class = "x", image = "y:1", kind = "application",
    port_spec = { ui = { internal = 8080, protocol = "tcp", purpose = "ui" } },
    env_required = { "PG_HOST" },
    volumes = { "/foo:/bar:ro" },
  }, "ok_app")
  expect(ok, "valid spec accepted")
  expect(err == nil, "no error: " .. tostring(err))
end

-- minimal valid (just required fields)
do
  local ok = validator.validate({
    class = "x", image = "y:1", kind = "application",
  }, "minimal")
  expect(ok, "minimal spec accepted (port_spec/env/volumes optional)")
end

-- not a table
assert_invalid("not-a-table", "not_a_table_app", "must return a table")

-- missing class
assert_invalid({ image = "y", kind = "application" }, "no_class_app",
  "class must be a non-empty string")

-- empty class
assert_invalid({ class = "", image = "y", kind = "application" },
  "empty_class_app", "class must be a non-empty string")

-- invalid kind
assert_invalid({ class = "x", image = "y", kind = "bogus" },
  "bad_kind_app", "kind must be one of")

-- duplicate internal port
assert_invalid({
  class = "x", image = "y", kind = "application",
  port_spec = {
    ui     = { internal = 8080 },
    health = { internal = 8080 },
  },
}, "dup_port_app", "both claim internal port 8080")

-- bad protocol
assert_invalid({
  class = "x", image = "y", kind = "application",
  port_spec = { ui = { internal = 8080, protocol = "icmp" } },
}, "bad_proto_app", "protocol must be tcp|udp")

-- bad purpose
assert_invalid({
  class = "x", image = "y", kind = "application",
  port_spec = { ui = { internal = 8080, purpose = "magic" } },
}, "bad_purpose_app", "purpose must be ui|service")

-- env_required not a table
assert_invalid({
  class = "x", image = "y", kind = "application",
  env_required = "PG_HOST",
}, "bad_env_app", "env_required must be an array")

-- env_required entry not a string
assert_invalid({
  class = "x", image = "y", kind = "application",
  env_required = { "OK_VAR", 42 },
}, "bad_env_entry_app", "env_required[2] must be a non-empty string")

-- volumes not a table
assert_invalid({
  class = "x", image = "y", kind = "application",
  volumes = "/foo:/bar",
}, "bad_vols_app", "volumes must be an array")

print("")
print(string.format("=== %d passed, %d failed ===", pass, fail))
os.exit(fail == 0 and 0 or 1)
