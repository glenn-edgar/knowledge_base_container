#!/bin/bash
CURSOR_EXE="/mnt/c/Users/glenn/AppData/Local/Programs/cursor/_/Cursor.exe"

if [[ -z "$1" || "$1" == -* ]]; then
    nohup "$CURSOR_EXE" --disable-gpu "$@" >/dev/null 2>&1 &
else
    # For --remote wsl+, pass the LINUX path, not the Windows path
    LINUX_PATH="$(realpath "$1")"
    nohup "$CURSOR_EXE" --disable-gpu --reuse-window --folder-uri "vscode-remote://wsl+${WSL_DISTRO_NAME}${LINUX_PATH}" >/dev/null 2>&1 &
fi
disown


