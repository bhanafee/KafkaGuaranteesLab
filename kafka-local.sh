#!/usr/bin/env bash
# Runs a single-node Kafka broker (KRaft mode, no ZooKeeper) for the lab, using Apple's
# `container` runtime (https://github.com/apple/container). macOS on Apple silicon only.
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

require_container() {
    if ! command -v container >/dev/null; then
        echo "Apple's 'container' is not installed. See https://github.com/apple/container" >&2
        exit 1
    fi
    if ! container system status >/dev/null 2>&1; then
        echo "The container service is not running. Start it with: container system start" >&2
        exit 1
    fi
}

# With --name, the name is the container's id, which is what `ls --quiet` prints.
is_running() {
    container ls --quiet | grep -qx "$CONTAINER"
}

remove_container() {
    container rm --force "$CONTAINER" >/dev/null 2>&1 || true
}

kafka_topics() {
    container exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${PORT}" "$@"
}

cmd="${1:-}"
[[ -z "$cmd" ]] && usage

case "$cmd" in
  start)
    require_container
    if is_running; then
        echo "Kafka is already running (${CONTAINER})"
        exit 0
    fi

    # Remove a stopped container from a previous run
    remove_container

    echo "Starting Kafka on localhost:${PORT} ..."
    container run --detach \
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
        if container exec "$CONTAINER" \
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
    require_container
    if is_running; then
        echo "Stopping ${CONTAINER} ..."
        remove_container
        echo "Stopped."
    else
        echo "Kafka is not running."
    fi
    ;;

  status)
    require_container
    if is_running; then
        echo "Kafka is running (${CONTAINER}) on localhost:${PORT}"
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
