#!/usr/bin/env bash
#
# Sends X requests from N concurrent clients to the proxy with pgbench.
# Every request opens a new connection, so each one goes through the
# proxy's routing.
#
#   ./scripts/test_postgres.sh --clients=10 --requests=1000

set -euo pipefail

CLIENTS=1
REQUESTS=100
CONFIG="$(dirname "$0")/../config.json"

PG_USER="irp"
PG_PASSWORD="irp"
PG_DB="irp"

usage() {
  cat <<EOF
usage: $0 --clients=N --requests=X

  --clients=N    number of concurrent clients (default 1)
  --requests=X   total number of requests, split across the clients (default 100)
EOF
  exit "${1:-0}"
}

for arg in "$@"; do
  case "$arg" in
    --clients=*)  CLIENTS="${arg#*=}" ;;
    --requests=*) REQUESTS="${arg#*=}" ;;
    -h|--help)    usage 0 ;;
    *)            echo "unknown argument: $arg" >&2; usage 1 ;;
  esac
done

for v in "$CLIENTS" "$REQUESTS"; do
  if ! [[ "$v" =~ ^[1-9][0-9]*$ ]]; then
    echo "--clients and --requests must be positive integers" >&2
    exit 1
  fi
done

if (( REQUESTS < CLIENTS )); then
  echo "--requests must be at least --clients" >&2
  exit 1
fi

command -v pgbench >/dev/null || { echo "pgbench is not installed (apt install postgresql-client)" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is not installed" >&2; exit 1; }

HOST=$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | grep -m1 '^192\.168\.1\.' || true)
if [[ -z "$HOST" ]]; then
  echo "no 192.168.1.x address found on this machine" >&2
  exit 1
fi

PORT=$(jq -r '.proxy.listen_port' "$CONFIG")

PER_CLIENT=$(( (REQUESTS + CLIENTS - 1) / CLIENTS ))
TOTAL=$(( PER_CLIENT * CLIENTS ))
THREADS=$(( CLIENTS < $(nproc) ? CLIENTS : $(nproc) ))

QUERY=$(mktemp)
trap 'rm -f "$QUERY"' EXIT
echo "SELECT 1;" > "$QUERY"

echo "proxy:    $HOST:$PORT"
echo "clients:  $CLIENTS"
echo "requests: $TOTAL ($PER_CLIENT per client)"
echo

PGPASSWORD="$PG_PASSWORD" pgbench \
  -h "$HOST" -p "$PORT" -U "$PG_USER" \
  -n -C \
  -c "$CLIENTS" -j "$THREADS" -t "$PER_CLIENT" \
  -f "$QUERY" \
  "$PG_DB"
