#!/usr/bin/env bash
# Runs a single-node Kafka broker (KRaft mode, no ZooKeeper) in a container for the lab.
#
# The container runtime is the first of `container` (Apple), `podman` and `docker` found on the
# PATH. Set KAFKA_RUNTIME to choose one.
set -euo pipefail

CONTAINER=kafka-guarantees-lab-kafka
# Keep in step with the kafka-clients version the build resolves.
IMAGE=apache/kafka:4.2.2
PORT=9092
# Three partitions, so that per-key ordering across partitions can be observed.
PARTITIONS=3
TOPICS=(language-preferences dead-letters)

usage() {
    echo "Usage: $0 {start|stop|status}"
    exit 1
}

pick_runtime() {
    if [[ -n "${KAFKA_RUNTIME:-}" ]]; then
        command -v "$KAFKA_RUNTIME" >/dev/null || {
            echo "KAFKA_RUNTIME=${KAFKA_RUNTIME} is not on the PATH." >&2
            exit 1
        }
        echo "$KAFKA_RUNTIME"
        return
    fi
    local candidate
    for candidate in container podman docker; do
        if command -v "$candidate" >/dev/null; then
            echo "$candidate"
            return
        fi
    done
    echo "No container runtime found. Install container, podman or docker." >&2
    exit 1
}

# Apple's `container` lists with `ls` and has no --filter. With --name, the name is the id.
running_names() {
    if [[ "$RUNTIME" == container ]]; then
        "$RUNTIME" ls --quiet
    else
        "$RUNTIME" ps --format '{{.Names}}'
    fi
}

is_running() {
    running_names | grep -qx "$CONTAINER"
}

remove_container() {
    "$RUNTIME" rm --force "$CONTAINER" >/dev/null 2>&1 || true
}

kafka_topics() {
    "$RUNTIME" exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${PORT}" "$@"
}

cmd="${1:-}"
[[ -z "$cmd" ]] && usage

RUNTIME=$(pick_runtime)

case "$cmd" in
  start)
    if is_running; then
        echo "Kafka is already running (${CONTAINER})"
        exit 0
    fi

    # Remove a stopped container from a previous run
    remove_container

    echo "Starting Kafka on localhost:${PORT} with ${RUNTIME} ..."
    "$RUNTIME" run --detach \
        --name "$CONTAINER" \
        --publish "${PORT}:9092" \
        --env KAFKA_NODE_ID=1 \
        --env KAFKA_PROCESS_ROLES=broker,controller \
        --env KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        --env "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:${PORT}" \
        --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        "$IMAGE" > /dev/null

    echo -n "Waiting for broker to be ready ..."
    for i in $(seq 1 30); do
        if "$RUNTIME" exec "$CONTAINER" \
               /opt/kafka/bin/kafka-broker-api-versions.sh \
               --bootstrap-server "localhost:${PORT}" &>/dev/null; then
            echo " ready."
            break
        fi
        echo -n "."
        sleep 1
        if [[ $i -eq 30 ]]; then
            echo " timed out."
            exit 1
        fi
    done

    for topic in "${TOPICS[@]}"; do
        echo "Creating topic '${topic}' ..."
        kafka_topics --create --if-not-exists \
            --topic "$topic" \
            --partitions "$PARTITIONS" \
            --replication-factor 1
    done

    echo "Done. Bootstrap server: localhost:${PORT}"
    ;;

  stop)
    if is_running; then
        echo "Stopping ${CONTAINER} ..."
        remove_container
        echo "Stopped."
    else
        echo "Kafka is not running."
    fi
    ;;

  status)
    if is_running; then
        echo "Kafka is running (${CONTAINER}, ${RUNTIME}) on localhost:${PORT}"
        echo ""
        echo "Topics:"
        kafka_topics --list
    else
        echo "Kafka is not running."
    fi
    ;;

  *)
    usage
    ;;
esac
