#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-/tmp/cairnkv-cluster}"

if [[ ! -d "$BASE" ]]; then
    echo "cluster dir not found: $BASE"
    exit 0
fi

for pid_file in "$BASE"/*.pid; do
    [[ -e "$pid_file" ]] || continue
    pid="$(cat "$pid_file")"
    kill "$pid" 2>/dev/null || true
done

sleep 0.3

for pid_file in "$BASE"/*.pid; do
    [[ -e "$pid_file" ]] || continue
    pid="$(cat "$pid_file")"
    kill -9 "$pid" 2>/dev/null || true
done

rm -f "$BASE"/*.pid
echo "cluster stopped"
