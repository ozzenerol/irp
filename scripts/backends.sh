#!/usr/bin/env bash
#
# Starts N backend instances in docker for testing irp.
#
#   ./scripts/backends.sh --service=postgres --instances=5
#   ./scripts/backends.sh --service=postgres --down
#
# Containers are named irp-<service>-<n> so they never clash with other
# containers on the machine. Running the script again recreates them.

set -euo pipefail

SERVICE=""
INSTANCES=1
DOWN=0

# The machine's 192.168.1.x LAN address, so the backends are reachable
# from other machines and the printed config works as-is.
HOST=$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | grep -m1 '^192\.168\.1\.' || true)
if [[ -z "$HOST" ]]; then
    echo "no 192.168.1.x address found on this machine" >&2
    exit 1
fi

PG_IMAGE="postgres:17-alpine"
PG_USER="irp"
PG_PASSWORD="irp"
PG_DB="irp"
PG_BASE_PORT=15432

REDIS_IMAGE="redis:7-alpine"
REDIS_BASE_PORT=16379

usage() {
    cat <<EOF
usage: $0 --service=<postgres|redis> [--instances=N] [--down]

  --service=NAME   service to run: postgres, redis
  --instances=N    number of instances to start (default 1)
  --down           stop and remove all irp-<service>-* containers
EOF
    exit "${1:-0}"
}

for arg in "$@"; do
    case "$arg" in
        --service=*)   SERVICE="${arg#*=}" ;;
        --instances=*) INSTANCES="${arg#*=}" ;;
        --down)        DOWN=1 ;;
        -h|--help)     usage 0 ;;
        *)             echo "unknown argument: $arg" >&2; usage 1 ;;
    esac
done

case "$SERVICE" in
    postgres) IMAGE="$PG_IMAGE";    BASE_PORT="$PG_BASE_PORT";    INTERNAL_PORT=5432 ;;
    redis)    IMAGE="$REDIS_IMAGE"; BASE_PORT="$REDIS_BASE_PORT"; INTERNAL_PORT=6379 ;;
    "")       echo "missing --service" >&2; usage 1 ;;
    *)        echo "unsupported service: $SERVICE" >&2; usage 1 ;;
esac

if ! [[ "$INSTANCES" =~ ^[1-9][0-9]*$ ]]; then
    echo "--instances must be a positive integer" >&2
    exit 1
fi

command -v docker >/dev/null || { echo "docker is not installed" >&2; exit 1; }

PREFIX="irp-$SERVICE"

remove_existing() {
    local ids
    ids=$(docker ps -aq --filter "name=^${PREFIX}-[0-9]+$")
    if [[ -n "$ids" ]]; then
        docker rm -f $ids >/dev/null
    fi
}

if (( DOWN )); then
    remove_existing
    echo "removed all $PREFIX-* containers"
    exit 0
fi

port_in_use() {
    ss -Htln "sport = :$1" 2>/dev/null | grep -q .
}

start_instance() {
    local name="$1" port="$2"

    case "$SERVICE" in
        postgres)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                -e POSTGRES_USER="$PG_USER" \
                -e POSTGRES_PASSWORD="$PG_PASSWORD" \
                -e POSTGRES_DB="$PG_DB" \
                "$IMAGE" -c log_connections=on -c max_connections=1000 >/dev/null
            ;;
        redis)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                "$IMAGE" >/dev/null
            ;;
    esac
}

is_ready() {
    case "$SERVICE" in
        postgres) docker exec "$1" pg_isready -q -U "$PG_USER" -d "$PG_DB" 2>/dev/null ;;
        redis)    docker exec "$1" redis-cli ping 2>/dev/null | grep -q PONG ;;
    esac
}

wait_ready() {
    local name="$1"
    for _ in $(seq 1 60); do
        is_ready "$name" && return 0
        sleep 1
    done
    return 1
}

connection_string() {
    case "$SERVICE" in
        postgres) echo "postgres://$PG_USER:$PG_PASSWORD@$HOST:$1/$PG_DB" ;;
        redis)    echo "redis://$HOST:$1" ;;
    esac
}

remove_existing
docker pull -q "$IMAGE" >/dev/null

names=()
ports=()
port="$BASE_PORT"

for i in $(seq 1 "$INSTANCES"); do
    while port_in_use "$port"; do
        port=$((port + 1))
    done

    name="$PREFIX-$i"
    echo "starting $name on $HOST:$port"
    start_instance "$name" "$port"

    names+=("$name")
    ports+=("$port")
    port=$((port + 1))
done

echo
echo "waiting for instances to be ready..."
for name in "${names[@]}"; do
    if ! wait_ready "$name"; then
        echo "$name did not become ready, check: docker logs $name" >&2
        exit 1
    fi
done

echo
printf "%-18s %-14s %-22s %s\n" "NAME" "CONTAINER" "ADDRESS" "CONNECTION"
for i in "${!names[@]}"; do
    name="${names[$i]}"
    port="${ports[$i]}"
    id=$(docker inspect -f '{{.Id}}' "$name" | cut -c1-12)
    printf "%-18s %-14s %-22s %s\n" "$name" "$id" "$HOST:$port" "$(connection_string "$port")"
done

# Ready to paste into config.json
echo
echo "backends for config.json:"
echo "    \"backends\": ["
for i in "${!names[@]}"; do
    sep=","
    (( i == ${#names[@]} - 1 )) && sep=""
    cat <<EOF
        {
            "name": "${SERVICE^^}-$((i + 1))",
            "host": "$HOST",
            "port": ${ports[$i]},
            "timeout_seconds": 30,
            "weight": 1
        }$sep
EOF
done
echo "    ]"
