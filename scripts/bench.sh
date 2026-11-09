#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
BIN="${BIN:-$BUILD_DIR/cairnkd}"
CTL="${CTL:-$BUILD_DIR/tools/cairnctl/cairnctl}"
BENCH="${BENCH:-$BUILD_DIR/tools/cairnkv-bench/cairnkv-bench}"
BASE="${BASE:-/tmp/cairnkv-bench}"
MODE="${1:-single}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/docs/benchmarks/results/$MODE}"

DURATION="${DURATION:-10}"
WARMUP="${WARMUP:-2}"
VALUE_SIZE="${VALUE_SIZE:-64}"
THREADS_LIST="${THREADS_LIST:-1 8 32}"
READ_THREADS="${READ_THREADS:-8}"

if [[ "$MODE" != "single" && "$MODE" != "cluster" ]]; then
    echo "usage: $0 [single|cluster]" >&2
    exit 1
fi

for binary in "$BIN" "$CTL" "$BENCH"; do
    if [[ ! -x "$binary" ]]; then
        echo "missing binary: $binary" >&2
        echo "build first, or set BIN/CTL/BENCH" >&2
        exit 1
    fi
done

mkdir -p "$OUT_DIR"

NODE_PID=""

cleanup() {
    if [[ "$MODE" == "single" && -n "$NODE_PID" ]]; then
        kill "$NODE_PID" 2>/dev/null || true
        sleep 0.2
        kill -9 "$NODE_PID" 2>/dev/null || true
    fi
    if [[ "$MODE" == "cluster" ]]; then
        BASE="$BASE" "$ROOT_DIR/scripts/stop-cluster.sh" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [[ "$MODE" == "single" ]]; then
    ADDRS=(127.0.0.1:6666)
else
    ADDRS=(127.0.0.1:6666 127.0.0.1:6667 127.0.0.1:6668)
fi

leader_addr() {
    for addr in "${ADDRS[@]}"; do
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

run_case() {
    local label="$1"
    local addr="$2"
    local op="$3"
    local threads="$4"
    local out="$OUT_DIR/$label.json"

    echo "== $label (op=$op threads=$threads) =="
    "$BENCH" --addr "$addr" \
        --op "$op" \
        --threads "$threads" \
        --duration "$DURATION" \
        --warmup "$WARMUP" \
        --value-size "$VALUE_SIZE" \
        --label "$label" \
        --json | tee "$out"
    echo
}

if [[ "$MODE" == "single" ]]; then
    rm -rf "$BASE"
    mkdir -p "$BASE/node-1"
    "$BIN" --id node-1 --listen 127.0.0.1:6666 \
        --data-dir "$BASE/node-1" --io-threads 4 \
        > "$BASE/node-1.log" 2>&1 &
    NODE_PID=$!
    echo "started node-1 pid=$NODE_PID"
else
    BASE="$BASE" BIN="$BIN" "$ROOT_DIR/scripts/run-cluster.sh"
fi

if ! wait_leader; then
    echo "no leader elected" >&2
    exit 1
fi
addr="$(leader_addr)"
echo "leader at $addr"
echo "results -> $OUT_DIR"
echo

for threads in $THREADS_LIST; do
    run_case "put-${threads}t" "$addr" put "$threads"
done

run_case "get-${READ_THREADS}t" "$addr" get "$READ_THREADS"
run_case "stale-get-${READ_THREADS}t" "$addr" stale-get "$READ_THREADS"

# Keep server-side logs next to the client results: the per-second raft
# metrics lines (sync_calls, sync_avg_us, proposals) are what the baseline
# analysis is built from, and /tmp is wiped between runs.
if [[ "$MODE" == "single" ]]; then
    cp "$BASE/node-1.log" "$OUT_DIR/node-1.log" 2>/dev/null || true
else
    for log_file in "$BASE"/node-*.log; do
        [[ -e "$log_file" ]] || continue
        cp "$log_file" "$OUT_DIR/"
    done
fi

echo "benchmark matrix done"
