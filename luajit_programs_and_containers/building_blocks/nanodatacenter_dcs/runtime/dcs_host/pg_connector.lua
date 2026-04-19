-- =============================================================================
-- pg_connector.lua -- Postgres connect helper for the DCS host process.
--
-- Single short-lived try_connect call per VERIFY_PG invocation. The
-- chain-tree's asm_verify_timeout drives retries (calls VERIFY_PG every
-- tick until the window elapses), so we don't loop inside this module.
--
-- Secrets: POSTGRES_PASSWORD (aka PG_PASSWORD) comes from host env
-- (sourced from ~/.config/nanodatacenter/secrets.env by start.sh).
-- Never baked into any file.
-- =============================================================================

local DBI = require("DBI")

local M = {}

-- Try to open a new DBI connection using cfg fields. Returns (conn, nil)
-- on success or (nil, err_str) on failure. Caller owns the conn; close
-- with conn:close() when done (VERIFY_PG hands it off to ctx.connectors.pg).
function M.try_connect(cfg, password)
  if not password or password == "" then
    return nil, "no password (PG_PASSWORD/POSTGRES_PASSWORD not in env)"
  end
  local conn, err = DBI.Connect("PostgreSQL",
    cfg.pg_db, cfg.pg_user, password,
    cfg.pg_host, tostring(cfg.pg_port))
  if not conn then return nil, tostring(err) end

  -- Sanity query. If the server isn't fully ready yet (pg accepts
  -- connections a moment before it's queryable), this fails and the
  -- caller treats it as not-yet-up.
  local ok, sth_or_err = pcall(function()
    local sth = conn:prepare("SELECT 1")
    assert(sth, "prepare SELECT 1 failed")
    assert(sth:execute(), "execute SELECT 1 failed")
    sth:close()
  end)
  if not ok then
    conn:close()
    return nil, "sanity query failed: " .. tostring(sth_or_err)
  end

  conn:autocommit(true)
  return conn, nil
end

return M
