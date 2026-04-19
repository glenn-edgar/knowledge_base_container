#!/usr/bin/env bash
# Interactive setup for nanodatacenter operator secrets.
# Writes ~/.config/nanodatacenter/secrets.env (mode 600).
# Run on the operator laptop only. Source the resulting file before provisioning.

set -euo pipefail

CONFIG_DIR="${HOME}/.config/nanodatacenter"
SECRETS_FILE="${CONFIG_DIR}/secrets.env"
FORCE=0

for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        -h|--help)
            cat <<EOF
Usage: $(basename "$0") [--force]

Creates ${SECRETS_FILE} with broker passwords for provisioning.
Prompts for each password twice (silent input). Refuses to overwrite an
existing file unless --force is given.
EOF
            exit 0
            ;;
        *)
            echo "unknown argument: $arg" >&2
            exit 2
            ;;
    esac
done

if [[ -e "$SECRETS_FILE" && $FORCE -ne 1 ]]; then
    echo "refusing to overwrite existing ${SECRETS_FILE} (use --force)" >&2
    exit 1
fi

prompt_password() {
    local var_name="$1"
    local label="$2"
    local pw1 pw2
    while true; do
        printf '%s: ' "$label" >&2
        IFS= read -rs pw1
        printf '\n' >&2
        if [[ -z "$pw1" ]]; then
            echo "  empty password rejected, try again" >&2
            continue
        fi
        printf 'confirm %s: ' "$label" >&2
        IFS= read -rs pw2
        printf '\n' >&2
        if [[ "$pw1" != "$pw2" ]]; then
            echo "  passwords did not match, try again" >&2
            continue
        fi
        printf -v "$var_name" '%s' "$pw1"
        break
    done
}

prompt_password PG_PASSWORD "Postgres password"
# NATS and MQTT brokers currently run without authentication; add prompts here
# when that changes.

umask 077
mkdir -p "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

tmp_file="$(mktemp "${SECRETS_FILE}.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

shell_escape() {
    printf "'%s'" "${1//\'/\'\\\'\'}"
}

{
    echo "# nanodatacenter operator secrets"
    echo "# generated $(date -u +%Y-%m-%dT%H:%M:%SZ) by setup_secrets.sh"
    echo "# source this file before running provision_all.lua"
    echo "export PG_PASSWORD=$(shell_escape "$PG_PASSWORD")"
} > "$tmp_file"

chmod 600 "$tmp_file"
mv "$tmp_file" "$SECRETS_FILE"
trap - EXIT

cat <<EOF

wrote ${SECRETS_FILE} (mode 600)

next:
  source ${SECRETS_FILE}
  # then run provision_all.lua

reminder: this file is the ONLY copy of these passwords. back it up
to your password manager or encrypted storage.
EOF
