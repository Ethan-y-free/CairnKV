#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTL="${CTL:-$ROOT_DIR/build/tools/cairnctl/cairnctl}"
BASE="${BASE:-/tmp/cairnkv-cluster}"

if [[ ! -x "$CTL" ]]; then
    echo "missing cairnctl: $CTL" >&2
    exit 1
fi

leader_addr() {
    for addr in 127.0.0.1:6666 127.0.0.1:6667 127.0.0.1:6668; do
        out="$("$CTL" --addr "$addr" status 2>/dev/null || true)"
        if grep -q '^role=leader' <<<"$out"; then
            echo "$addr"
            return 0
        fi
    done
    return 1
}

wait_leader() {
    for _ in $(seq 1 100); do
        if leader_addr >/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

if ! wait_leader; then
    echo "no leader elected" >&2
    exit 1
fi

addr="$(leader_addr)"
echo "leader at $addr"
"$CTL" --addr "$addr" put smoke:key smoke-value
"$CTL" --addr "$addr" get smoke:key
"$CTL" --addr "$addr" status

leader_id="$("$CTL" --addr "$addr" status | awk -F= '/^leader_id=/{print $2}')"
echo "killing leader $leader_id"
kill "$(cat "$BASE/$leader_id.pid")" 2>/dev/null || true

if ! wait_leader; then
    echo "no leader after failover" >&2
    exit 1
fi

addr="$(leader_addr)"
echo "new leader at $addr"
"$CTL" --addr "$addr" get smoke:key
echo "smoke OK"
