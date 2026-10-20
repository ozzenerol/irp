#!/usr/bin/env bash
#
# Keeps heavy, mixed Redis traffic on the proxy for a while, like a busy
# site: reads from a big pool of app connections, writes from another, and
# clients that keep connecting and leaving. The proxy and the backends must
# already be running.
#
#   ./scripts/soak.sh                  # 7 minutes
#   ./scripts/soak.sh --minutes=10

set -euo pipefail

MINUTES=7

for arg in "$@"; do
    case "$arg" in
        --minutes=*) MINUTES="${arg#*=}" ;;
        -h|--help)   echo "usage: $0 [--minutes=N]"; exit 0 ;;
        *)           echo "unknown argument: $arg" >&2; exit 1 ;;
    esac
done

if ! [[ "$MINUTES" =~ ^[1-9][0-9]*$ ]]; then
    echo "--minutes must be a positive integer" >&2
    exit 1
fi

command -v go >/dev/null || { echo "go is not installed" >&2; exit 1; }

DIR=$(cd "$(dirname "$0")" && pwd)
TMP=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$TMP"' EXIT

# Built once, so stopping it with SIGINT reaches the load tool itself.
go build -C "$DIR/redisload" -o "$TMP/redisload" .
LOAD="$TMP/redisload"
END=$(( $(date +%s) + MINUTES * 60 ))
SECONDS_LEFT=$(( MINUTES * 60 ))

echo "soaking for $MINUTES minutes"

# Reads: 2000 users sharing 200 connections.
timeout -s INT "$SECONDS_LEFT" "$LOAD" --clients=2000 --requests=100000000 --pool=200 \
    --command="GET user:42" --every=1h > "$TMP/reads.log" 2>&1 &

# Writes: 500 users sharing 50 connections.
timeout -s INT "$SECONDS_LEFT" "$LOAD" --clients=500 --requests=100000000 --pool=50 \
    --command="SET user:42 online" --every=1h > "$TMP/writes.log" 2>&1 &

# Churn: 200 users at a time connect, send 20 requests and leave.
(
    rounds=0
    while (( $(date +%s) < END )); do
        "$LOAD" --clients=200 --requests=20 --every=1h > /dev/null 2>&1 || echo "churn round failed" >&2
        rounds=$((rounds + 1))
    done
    echo "churn: $rounds rounds, $((rounds * 200)) connections"
) > "$TMP/churn.log" 2>&1 &

wait

echo
echo "reads:  $(grep -h '^done\|^stopped' "$TMP/reads.log" || tail -1 "$TMP/reads.log")"
echo "writes: $(grep -h '^done\|^stopped' "$TMP/writes.log" || tail -1 "$TMP/writes.log")"
echo "$(cat "$TMP/churn.log")"
