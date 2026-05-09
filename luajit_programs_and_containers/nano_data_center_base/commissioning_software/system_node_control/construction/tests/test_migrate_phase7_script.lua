#!/usr/bin/env luajit
-- =============================================================================
-- test_migrate_phase7_script.lua -- Phase 7 C1 acceptance for the
-- migrate_phase7.sh drop+rebuild script.
--
-- Live-cluster behavior is validated by running --dry-run manually
-- against pg + NATS. This test guards the script's STRUCTURE so
-- safety guards can't be silently removed in a future edit:
--
--   - bash syntax valid (bash -n)
--   - --help, --apply, --dry-run flag handling present
--   - default mode is dry-run (no implicit destructive action)
--   - --apply requires "YES, WIPE" confirmation gate
--   - APP_SYSTEM / APP_SITE / PG_PASSWORD env preflight present
--   - deprecated bucket + path lists target ONLY the right things
--     (boards, robots, action_server bucket, mission_log bucket;
--      NOT planner.<ns>.* / app_containers.* / infrastructure.*)
--   - script is executable (chmod +x landed)
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../"
local SCRIPT     = REPO_ROOT .. "construction/scripts/migrate_phase7.sh"

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

local function shell(cmd)
  local p = io.popen(cmd .. " 2>&1; echo EXIT:$?", "r")
  if not p then return nil, "popen failed" end
  local out = p:read("*a"); p:close()
  local exit = tonumber(out:match("EXIT:(%d+)") or "-1")
  out = out:gsub("EXIT:%d+\n?$", "")
  return out, exit
end

------------------------------------------------------------------------
print("== file present + executable + parses ==")
------------------------------------------------------------------------

local src = read_file(SCRIPT)
ok("script file exists", src ~= nil, "missing: " .. SCRIPT)
ok("script non-empty", src and #src > 1000)
ok("starts with #!/usr/bin/env bash",
   src and src:sub(1, 19) == "#!/usr/bin/env bash")

-- Execute permission
local stat_out = shell("test -x '" .. SCRIPT .. "' && echo executable")
ok("script is executable (chmod +x)",
   stat_out and stat_out:find("executable") ~= nil)

-- bash -n syntax check
local _, syntax_exit = shell("bash -n '" .. SCRIPT .. "'")
ok("script passes bash -n syntax check", syntax_exit == 0)

------------------------------------------------------------------------
print()
print("== safety guards present in source ==")
------------------------------------------------------------------------

if src then
  -- Default mode is dry-run
  ok("APPLY=0 default (dry-run)",
     src:find("APPLY=0", 1, true) ~= nil)

  -- Flag parsing covers both --apply and --dry-run
  ok("--apply flag parsed", src:find("%-%-apply") ~= nil)
  ok("--dry-run flag parsed", src:find("%-%-dry%-run") ~= nil)
  ok("--help flag parsed",
     src:find("%-%-help") ~= nil or src:find("|-h)") ~= nil)

  -- Confirmation gate uses "YES, WIPE" literal
  ok("confirmation requires 'YES, WIPE' literal",
     src:find('"YES, WIPE"', 1, true) ~= nil)
  ok("confirmation read with `read -r`",
     src:find("read %-r CONFIRM") ~= nil)
  ok("aborts on wrong confirmation",
     src:find('Aborted', 1, true) ~= nil)

  -- Env preflight
  for _, var in ipairs({ "APP_SYSTEM", "APP_SITE", "PG_PASSWORD" }) do
    ok(var .. " preflight check",
       src:find(var, 1, true) ~= nil and
       src:find(var .. " env var required") ~= nil)
  end

  -- Dry-run early exit before any DELETE / nats kv del
  local dry_block = src:find('DRY%-RUN complete', 1, false)
  local first_delete = src:find("DELETE FROM", 1, true)
  ok("dry-run exit comes BEFORE any DELETE FROM in script source",
     dry_block ~= nil and first_delete ~= nil and dry_block < first_delete,
     "dry_block_pos=" .. tostring(dry_block) ..
     " first_delete_pos=" .. tostring(first_delete))

  -- Confirm gate comes before destructive ops too
  local confirm_pos = src:find('YES, WIPE', 1, true)
  ok("confirmation prompt comes BEFORE any DELETE FROM",
     confirm_pos ~= nil and first_delete ~= nil and confirm_pos < first_delete)
end

------------------------------------------------------------------------
print()
print("== deprecated targets are correct + don't include new paths ==")
------------------------------------------------------------------------

if src then
  -- DEPRECATED targets present
  ok("targets <site>_action_server bucket",
     src:find('SITE_BUCKET}_action_server', 1, true) ~= nil)
  ok("targets <site>_mission_log bucket",
     src:find('SITE_BUCKET}_mission_log', 1, true) ~= nil)
  ok("targets boards subtree",
     src:find('site.${SITE}.boards', 1, true) ~= nil)
  ok("targets robots subtree",
     src:find('site.${SITE}.robots', 1, true) ~= nil)

  -- NEW paths must NOT be in deprecated lists -- script must not
  -- nuke the post-Phase-7 schema if accidentally re-run after kb_build.
  -- Locate the DEPRECATED_BUCKETS array and DEPRECATED_PG_PATHS array
  -- regions, then check that planner / app_containers / infra are
  -- absent from THOSE specific arrays.
  local function array_block(start_marker)
    local s = src:find(start_marker, 1, true)
    if not s then return nil end
    local e = src:find(")", s, true)
    if not e then return nil end
    return src:sub(s, e)
  end
  local bucket_block = array_block("DEPRECATED_BUCKETS=(")
  local path_block   = array_block("DEPRECATED_PG_PATHS=(")
  ok("DEPRECATED_BUCKETS array present", bucket_block ~= nil)
  ok("DEPRECATED_PG_PATHS array present", path_block ~= nil)

  if bucket_block and path_block then
    local both = bucket_block .. path_block
    ok("deprecated lists do NOT mention planner.<ns>",
       both:find("planner_", 1, true) == nil and
       both:find("planner%.") == nil,
       "planner.<ns> path leaked into deprecated lists")
    ok("deprecated lists do NOT mention app_containers",
       both:find("app_containers", 1, true) == nil)
    ok("deprecated lists do NOT mention infrastructure",
       both:find("infrastructure", 1, true) == nil)
    ok("deprecated lists do NOT mention cpu.",
       both:find("%.cpu%.") == nil and both:find("/cpu/") == nil)
  end

  -- Orphan blob cleanup is post-DELETE so refs are correctly orphaned
  local fs_node_del_pos = src:find("DELETE FROM knowledge_base_fs_node", 1, true)
  local blob_del_pos    = src:find("DELETE FROM knowledge_base_fs_blob", 1, true)
  ok("orphan blob delete comes AFTER fs_node delete",
     fs_node_del_pos and blob_del_pos and fs_node_del_pos < blob_del_pos)
end

------------------------------------------------------------------------
print()
print("== --help works without env vars ==")
------------------------------------------------------------------------

do
  -- --help should not require APP_SYSTEM etc. (it's a help screen)
  local out, code = shell("env -u APP_SYSTEM -u APP_SITE -u PG_PASSWORD '" ..
                          SCRIPT .. "' --help")
  ok("--help exits 0 without env vars", code == 0,
     "exit=" .. tostring(code))
  ok("--help mentions usage",
     out and out:find("usage:") ~= nil)
  ok("--help mentions --apply",
     out and out:find("%-%-apply") ~= nil)
  ok("--help documents env vars",
     out and out:find("APP_SYSTEM") ~= nil)
end

------------------------------------------------------------------------
print()
print("== refuses to run without env vars ==")
------------------------------------------------------------------------

do
  local out, code = shell("env -u APP_SYSTEM -u APP_SITE -u PG_PASSWORD '" ..
                          SCRIPT .. "'")
  ok("no APP_SYSTEM -> non-zero exit", code ~= 0)
  ok("no APP_SYSTEM -> error message",
     out and out:find("APP_SYSTEM") ~= nil and out:find("required") ~= nil)
end

-- env(1) wants ALL -u flags BEFORE any NAME=VALUE assignments;
-- otherwise the -u tokens are treated as the command name.

do
  local out, code = shell("env -u APP_SITE -u PG_PASSWORD APP_SYSTEM=foo '" ..
                          SCRIPT .. "'")
  ok("no APP_SITE -> non-zero exit", code ~= 0)
  ok("no APP_SITE -> error message",
     out and out:find("APP_SITE") ~= nil and out:find("required") ~= nil,
     "out: " .. tostring(out))
end

do
  local out, code = shell("env -u PG_PASSWORD APP_SYSTEM=foo APP_SITE=bar '" ..
                          SCRIPT .. "'")
  ok("no PG_PASSWORD -> non-zero exit", code ~= 0)
  ok("no PG_PASSWORD -> error message",
     out and out:find("PG_PASSWORD") ~= nil and out:find("required") ~= nil,
     "out: " .. tostring(out))
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
