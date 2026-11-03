#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/build/cairnkd}"
BASE="${BASE:-/tmp/cairnkv-cluster}"

if [[ ! -x "$BIN" ]]; then
    echo "missing cairnkd binary: $BIN" >&2
    echo "build it first, or set BIN=/path/to/cairnkd" >&2
    exit 1
fi

rm -rf "$BASE"
mkdir -p "$BASE"

start_node() {
    local id="$1"
    local listen="$2"
    local peers="$3"
    "$BIN" --id "$id" --listen "$listen" --data-dir "$BASE/$id" \
        --peers "$peers" > "$BASE/$id.log" 2>&1 &
    echo $! > "$BASE/$id.pid"
    echo "started $id pid=$(cat "$BASE/$id.pid") listen=$listen"
}

start_node node-1 127.0.0.1:6666 "node-2=127.0.0.1:6667,node-3=127.0.0.1:6668"
start_node node-2 127.0.0.1:6667 "node-1=127.0.0.1:6666,node-3=127.0.0.1:6668"
start_node node-3 127.0.0.1:6668 "node-1=127.0.0.1:6666,node-2=127.0.0.1:6667"

echo "cluster started; logs: $BASE/node-*.log"
