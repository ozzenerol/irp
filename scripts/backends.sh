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

MYSQL_IMAGE="mysql:8.4"
MYSQL_USER="irp"
MYSQL_PASSWORD="irp"
MYSQL_DB="irp"
MYSQL_BASE_PORT=13306

MONGO_IMAGE="mongo:8.0"
MONGO_BASE_PORT=17017

RABBITMQ_IMAGE="rabbitmq:4.1-alpine"
RABBITMQ_USER="irp"
RABBITMQ_PASSWORD="irp"
RABBITMQ_BASE_PORT=17672

NATS_IMAGE="nats:2.11-alpine"
NATS_BASE_PORT=14222

KAFKA_IMAGE="apache/kafka:4.1.0"
KAFKA_BASE_PORT=19092
KAFKA_TOPIC="load"
# Kafka clients connect to the address a broker advertises, not the one they
# bootstrapped from, so every broker advertises the proxy.
KAFKA_PROXY_PORT=8317

usage() {
    cat <<EOF
usage: $0 --service=<postgres|redis|mysql|mongo|rabbitmq|nats|kafka> [--instances=N] [--down]

  --service=NAME   service to run: postgres, redis, mysql, mongo, rabbitmq,
                   nats, kafka
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
    mysql)    IMAGE="$MYSQL_IMAGE"; BASE_PORT="$MYSQL_BASE_PORT"; INTERNAL_PORT=3306 ;;
    mongo)    IMAGE="$MONGO_IMAGE"; BASE_PORT="$MONGO_BASE_PORT"; INTERNAL_PORT=27017 ;;
    rabbitmq) IMAGE="$RABBITMQ_IMAGE"; BASE_PORT="$RABBITMQ_BASE_PORT"; INTERNAL_PORT=5672 ;;
    nats)     IMAGE="$NATS_IMAGE"; BASE_PORT="$NATS_BASE_PORT"; INTERNAL_PORT=4222 ;;
    kafka)    IMAGE="$KAFKA_IMAGE"; BASE_PORT="$KAFKA_BASE_PORT"; INTERNAL_PORT=9092 ;;
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
        mysql)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                -e MYSQL_ROOT_PASSWORD="$MYSQL_PASSWORD" \
                -e MYSQL_USER="$MYSQL_USER" \
                -e MYSQL_PASSWORD="$MYSQL_PASSWORD" \
                -e MYSQL_DATABASE="$MYSQL_DB" \
                "$IMAGE" --max-connections=1000 >/dev/null
            ;;
        mongo)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                "$IMAGE" >/dev/null
            ;;
        rabbitmq)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                -e RABBITMQ_DEFAULT_USER="$RABBITMQ_USER" \
                -e RABBITMQ_DEFAULT_PASS="$RABBITMQ_PASSWORD" \
                "$IMAGE" >/dev/null
            ;;
        nats)
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                "$IMAGE" -m 8222 >/dev/null
            ;;
        kafka)
            # A single-node KRaft cluster. LOCAL is for the tools inside the
            # container, which can't reach the proxy.
            docker run -d --name "$name" \
                -p "$HOST:$port:$INTERNAL_PORT" \
                -e KAFKA_NODE_ID=1 \
                -e KAFKA_PROCESS_ROLES=broker,controller \
                -e KAFKA_LISTENERS=PLAINTEXT://:9092,LOCAL://localhost:19092,CONTROLLER://localhost:9093 \
                -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$HOST:$KAFKA_PROXY_PORT,LOCAL://localhost:19092 \
                -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,LOCAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
                -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
                -e KAFKA_INTER_BROKER_LISTENER_NAME=LOCAL \
                -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
                -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
                "$IMAGE" >/dev/null
            ;;
    esac
}

is_ready() {
    case "$SERVICE" in
        postgres) docker exec "$1" pg_isready -q -U "$PG_USER" -d "$PG_DB" 2>/dev/null ;;
        redis)    docker exec "$1" redis-cli ping 2>/dev/null | grep -q PONG ;;
        # Over TCP: the server MySQL runs while it sets up has no network.
        mysql)    docker exec "$1" mysql -h127.0.0.1 -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e 'SELECT 1' "$MYSQL_DB" >/dev/null 2>&1 ;;
        mongo)    docker exec "$1" mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' 2>/dev/null | grep -q 1 ;;
        # ping alone passes before the AMQP listener is up.
        rabbitmq) docker exec "$1" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1 ;;
        nats)     docker exec "$1" wget -qO- http://localhost:8222/healthz 2>/dev/null | grep -q ok ;;
        # Creates the topic too: every broker needs it, and through the proxy
        # there's no telling which broker a client asks to create it.
        kafka)    docker exec "$1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
                      --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 1 >/dev/null 2>&1 ;;
    esac
}

wait_ready() {
    local name="$1"
    # MySQL takes a while to set up its data directory on first start.
    for _ in $(seq 1 120); do
        is_ready "$name" && return 0
        sleep 1
    done
    return 1
}

connection_string() {
    case "$SERVICE" in
        postgres) echo "postgres://$PG_USER:$PG_PASSWORD@$HOST:$1/$PG_DB" ;;
        redis)    echo "redis://$HOST:$1" ;;
        mysql)    echo "mysql://$MYSQL_USER:$MYSQL_PASSWORD@$HOST:$1/$MYSQL_DB" ;;
        mongo)    echo "mongodb://$HOST:$1/?directConnection=true" ;;
        rabbitmq) echo "amqp://$RABBITMQ_USER:$RABBITMQ_PASSWORD@$HOST:$1/" ;;
        nats)     echo "nats://$HOST:$1" ;;
        kafka)    echo "kafka://$HOST:$1 (advertises $HOST:$KAFKA_PROXY_PORT)" ;;
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

# Ready to paste into config/<service>-config.json
echo
echo "backends for config/$SERVICE-config.json:"
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
