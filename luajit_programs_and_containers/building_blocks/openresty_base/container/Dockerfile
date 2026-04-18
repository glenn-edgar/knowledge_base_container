# nanodatacenter/openresty-base -- luajit-base + OpenResty + pgmoon + vendored htmx.
#
# App images that include web processes FROM this image.
# The luajit-base supervisor remains PID 1; openresty runs as a supervised
# child process launched from the app's manifest (argv points at openresty).
#
# Exposed filesystem conventions (contract for app images):
#   /usr/local/openresty/          -- openresty install prefix
#   /usr/local/openresty/site/     -- opm-installed lua modules (pgmoon)
#   /usr/local/openresty/site/assets/   -- vendored browser assets (htmx)
#   /opt/openresty_base/conf.d/    -- app nginx configs drop here
#   /opt/openresty_base/logs/      -- per-process log dir (pid, error, access)

FROM nanodatacenter/luajit-base:latest

# 1. Install OpenResty from its apt repo.
#    Note: openresty publishes amd64 under .../package/ubuntu and arm64 under
#    .../package/arm64/ubuntu -- pick per host arch.
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates \
    && wget -qO - https://openresty.org/package/pubkey.gpg \
        | gpg --dearmor -o /usr/share/keyrings/openresty.gpg \
    && . /etc/os-release \
    && ARCH="$(dpkg --print-architecture)" \
    && case "$ARCH" in \
         amd64) REPO_SUBPATH="ubuntu" ;; \
         arm64) REPO_SUBPATH="arm64/ubuntu" ;; \
         *)     echo "unsupported arch: $ARCH" >&2; exit 1 ;; \
       esac \
    && echo "deb [signed-by=/usr/share/keyrings/openresty.gpg] http://openresty.org/package/${REPO_SUBPATH} ${VERSION_CODENAME} main" \
        > /etc/apt/sources.list.d/openresty.list \
    && apt-get update && apt-get install -y --no-install-recommends openresty \
    && apt-get purge -y wget gnupg && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# 2. pgmoon (pure-lua Postgres client) -- vendored from a github release
#    tarball into prebuilt_openresty_lualib/ by docker_build.sh. Goes under
#    site/lualib so openresty's default package.path picks it up.
COPY prebuilt_openresty_lualib/ /usr/local/openresty/site/lualib/

# 3. Vendored web assets (htmx + anything shared across app UIs).
COPY prebuilt_web_assets/ /usr/local/openresty/site/assets/

# 4. Scaffolding dirs. App images drop their nginx.conf into conf.d/.
RUN mkdir -p /opt/openresty_base/conf.d /opt/openresty_base/logs

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
