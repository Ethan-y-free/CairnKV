#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTL="${CTL:-$ROOT_DIR/build/tools/cairnctl/cairnctl}"

IDS=(node-1 node-2 node-3)
ADDRS=(127.0.0.1:6666 127.0.0.1:6667 127.0.0.1:6668)

if [[ ! -x "$CTL" ]]; then
    echo "missing cairnctl: $CTL" >&2
    exit 1
fi

status_of() {
    "$CTL" --addr "$1" status 2>/dev/null || true
}

leader_index() {
    local exclude="${1:-}"
    local i
    for i in 0 1 2; do
        [[ "$i" == "$exclude" ]] && continue
        if grep -q '^role=leader' <<<"$(status_of "${ADDRS[$i]}")"; then
            echo "$i"
            return 0
        fi
    done
    return 1
}

wait_leader() {
    local exclude="${1:-}"
    local _
    for _ in $(seq 1 80); do
        if leader_index "$exclude" >/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

if ! wait_leader; then
    echo "no leader; start the cluster first" >&2
    exit 1
fi

leader=$(leader_index)
echo "leader: ${IDS[$leader]} (${ADDRS[$leader]})"

"$CTL" --addr "${ADDRS[$leader]}" put partition:key partition-value || exit 1

followers=()
for i in 0 1 2; do
    [[ "$i" == "$leader" ]] && continue
    followers+=("$i")
done
f1="${followers[0]}"
f2="${followers[1]}"

echo "partition ${IDS[$leader]} <-> ${IDS[$f1]},${IDS[$f2]}"
"$CTL" --addr "${ADDRS[$leader]}" partition "${IDS[$f1]},${IDS[$f2]}" || exit 1
"$CTL" --addr "${ADDRS[$f1]}" partition "${IDS[$leader]}" || exit 1
"$CTL" --addr "${ADDRS[$f2]}" partition "${IDS[$leader]}" || exit 1

if ! wait_leader "$leader"; then
    echo "followers did not elect a new leader" >&2
    exit 1
fi

new_leader=$(leader_index "$leader")
echo "new leader: ${IDS[$new_leader]} (${ADDRS[$new_leader]})"
"$CTL" --addr "${ADDRS[$new_leader]}" put partition:new new-value || exit 1
"$CTL" --addr "${ADDRS[$new_leader]}" get partition:new || exit 1

sleep 0.5
stale=$("$CTL" --addr "${ADDRS[$leader]}" --stale get partition:key 2>/dev/null || true)
echo "isolated leader stale read: ${stale}"
if [[ "$stale" != "partition-value" ]]; then
    echo "stale read did not return the local applied value" >&2
    exit 1
fi

linear_out=$(timeout 3 "$CTL" --addr "${ADDRS[$leader]}" get partition:key 2>&1)
linear_rc=$?
echo "isolated leader linearizable read rc=${linear_rc}: ${linear_out}"
if [[ "$linear_rc" -eq 0 ]]; then
    echo "isolated leader served a linearizable read" >&2
    exit 1
fi

echo "unpartitioning"
for i in 0 1 2; do
    "$CTL" --addr "${ADDRS[$i]}" unpartition >/dev/null || true
done
sleep 2

echo "final statuses:"
for i in 0 1 2; do
    echo "--- ${IDS[$i]}"
    status_of "${ADDRS[$i]}"
done

echo "partition smoke OK"
