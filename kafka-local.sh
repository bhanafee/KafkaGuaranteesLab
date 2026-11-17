#!/usr/bin/env bash
# Runs a single-node Kafka broker (KRaft mode, no ZooKeeper) for the lab, using Apple's
# `container` runtime (https://github.com/apple/container). macOS on Apple silicon only.
#
# The broker keeps its data inside the container. `stop` stops the container and keeps it, so the
# next `start` brings back the same cluster with its topics, records and committed offsets.
# `remove` deletes the container and the data with it.
set -euo pipefail

CONTAINER=kafka-guarantees-lab-kafka
# Keep in step with the kafka-clients version the build resolves.
IMAGE=apache/kafka:4.2.2
PORT=9092
# Three partitions, so that per-key ordering across partitions can be observed.
PARTITIONS=3
TOPICS=(language-preferences dead-letters)

usage() {
    echo "Usage: $0 {start|stop|remove|status}"
    echo "  start   start the broker. A stopped broker comes back with its data"
    echo "  stop    stop the broker and keep its data"
    echo "  remove  stop the broker and delete its data"
    echo "  status  show whether the broker is running, and its topics"
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
    grep -qx "$CONTAINER" <<< "$(container ls --quiet)"
}

# True for a running container and for a stopped one.
container_exists() {
    grep -qx "$CONTAINER" <<< "$(container ls --all --quiet)"
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

    if container_exists; then
        # Stopped by `stop`. The container still holds the broker's data.
        echo "Starting the stopped broker on localhost:${PORT}, with its data ..."
        container start "$CONTAINER" > /dev/null
    else
        echo "Starting a new broker on localhost:${PORT} ..."
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
    fi

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
        # Give the broker time to shut down cleanly before the runtime kills it.
        container stop --time 30 "$CONTAINER" > /dev/null
        echo "Stopped. The data is kept: '$0 start' brings the broker back, '$0 remove' deletes it."
    else
        echo "Kafka is not running."
    fi
    ;;

  remove)
    require_container
    if container_exists; then
        echo "Removing ${CONTAINER} and its data ..."
        container rm --force "$CONTAINER" > /dev/null
        echo "Removed."
    else
        echo "There is no broker to remove."
    fi
    ;;

  status)
    require_container
    if is_running; then
        echo "Kafka is running (${CONTAINER}) on localhost:${PORT}"
        echo ""
        echo "Topics:"
        kafka_topics --list
    elif container_exists; then
        echo "Kafka is stopped (${CONTAINER}). Its data is kept."
    else
        echo "Kafka is not running, and there is no stopped broker."
    fi
    ;;

  *)
    usage
    ;;
esac
