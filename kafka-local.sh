#!/usr/bin/env bash
set -euo pipefail

CONTAINER=at-least-once-kafka
IMAGE=apache/kafka:3.9.0
PORT=9092
TOPIC=language-preferences
DEAD_LETTER_TOPIC=dead-letters

usage() {
    echo "Usage: $0 {start|stop|status}"
    exit 1
}

cmd="${1:-}"
[[ -z "$cmd" ]] && usage

case "$cmd" in
  start)
    if docker ps -q --filter "name=^${CONTAINER}$" | grep -q .; then
        echo "Kafka is already running (${CONTAINER})"
        exit 0
    fi

    # Remove a stopped container from a previous run
    docker rm -f "$CONTAINER" 2>/dev/null || true

    echo "Starting Kafka on localhost:${PORT} ..."
    docker run -d \
        --name "$CONTAINER" \
        -p "${PORT}:9092" \
        -e KAFKA_NODE_ID=1 \
        -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:${PORT} \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        "$IMAGE" > /dev/null

    echo -n "Waiting for broker to be ready ..."
    for i in $(seq 1 30); do
        if docker exec "$CONTAINER" \
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

    for topic in "$TOPIC" "$DEAD_LETTER_TOPIC"; do
        echo "Creating topic '${topic}' ..."
        docker exec "$CONTAINER" \
            /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "localhost:${PORT}" \
            --create \
            --if-not-exists \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1
    done

    echo "Done. Bootstrap server: localhost:${PORT}"
    ;;

  stop)
    if docker ps -q --filter "name=^${CONTAINER}$" | grep -q .; then
        echo "Stopping ${CONTAINER} ..."
        docker rm -f "$CONTAINER" > /dev/null
        echo "Stopped."
    else
        echo "Kafka is not running."
    fi
    ;;

  status)
    if docker ps --filter "name=^${CONTAINER}$" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -q "$CONTAINER"; then
        docker ps --filter "name=^${CONTAINER}$" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        echo ""
        echo "Topics:"
        docker exec "$CONTAINER" \
            /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "localhost:${PORT}" \
            --list
    else
        echo "Kafka is not running."
    fi
    ;;

  *)
    usage
    ;;
esac
