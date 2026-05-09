-- planner_ui :: render helpers (Phase 5b C1).
--
-- Tiny rendering helpers shared across handlers. Pure functions; no
-- ngx.* calls so it's host-testable via `luajit -e require ...`.
--
-- Kept intentionally minimal: html_escape + a context-builder that
-- pulls planner identity from env vars (PLANNER_NAMESPACE,
-- CONTAINER_NAME, APP_SITE). Later sub-commits will add fragment
-- helpers (htmx swaps), JSON encoders, etc.

local M = {}

-- HTML-escape a value for safe interpolation into shell pages.
-- Returns "" for nil so concatenation never errors on a missing env.
function M.html_escape(s)
  if s == nil then return "" end
  s = tostring(s)
  s = s:gsub("&", "&amp;")
  s = s:gsub("<", "&lt;")
  s = s:gsub(">", "&gt;")
  s = s:gsub('"', "&quot;")
  s = s:gsub("'", "&#39;")
  return s
end

-- Build the planner-identity context from env. Used by the shell page
-- header. Falls back to "(unset)" so the placeholder is visible during
-- bring-up if an env var hasn't been wired yet.
function M.context()
  local function env_or(name, fallback)
    local v = os.getenv(name)
    if v == nil or v == "" then return fallback end
    return v
  end
  return {
    container_name    = env_or("CONTAINER_NAME",    "(unset)"),
    planner_namespace = env_or("PLANNER_NAMESPACE", "(unset)"),
    site              = env_or("APP_SITE",          "(unset)"),
    system            = env_or("APP_SYSTEM",        "(unset)"),
  }
end

return M
