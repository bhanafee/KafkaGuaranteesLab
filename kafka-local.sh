#!/usr/bin/env bash
# Runs a single-node Kafka broker (KRaft mode, no ZooKeeper) for the lab in a container.
#
# The script works with three container runtimes and uses the first of them that is installed and
# running, in this order:
#   container  Apple's runtime (https://github.com/apple/container), macOS on Apple silicon
#   podman
#   docker
# Set KAFKA_LOCAL_RUNTIME to one of these names to choose the runtime yourself. The script does not
# start a runtime's service or virtual machine. It says how to, and stops.
#
# The broker keeps its data inside the container. `stop` stops the container and keeps it, so the
# next `start` brings back the same cluster with its topics, records and committed offsets.
# `remove` deletes the container and the data with it.
#
# The runtimes do not share containers. A broker started with one runtime is not seen by another,
# so use the same runtime for every command.
set -euo pipefail

CONTAINER=kafka-guarantees-lab-kafka
# Keep in step with the kafka-clients version the build resolves.
IMAGE=apache/kafka:4.2.2
PORT=9092
# The application declares these topics itself and creates them at start if they are missing, so it
# does not depend on this script. The script creates them too, because the application looks only
# when it starts: a broker that is removed and started again under a running application would
# otherwise get them from the broker's auto-creation, with one partition.
# Keep PARTITIONS in step with lab.topics.partitions in application.yml.
PARTITIONS=3
TOPICS=(language-preferences dead-letters)

# In the order of preference.
RUNTIMES=(container podman docker)
# The runtime in use. Set by choose_runtime.
RUNTIME=""

usage() {
    echo "Usage: $0 {start|stop|remove|status}"
    echo "  start   start the broker. A stopped broker comes back with its data"
    echo "  stop    stop the broker and keep its data"
    echo "  remove  stop the broker and delete its data"
    echo "  status  show whether the broker is running, and its topics"
    echo ""
    echo "Uses the first of these that is installed and running: ${RUNTIMES[*]}."
    echo "Set KAFKA_LOCAL_RUNTIME to one of them to choose."
    exit 1
}

# ── What differs between the runtimes ────────────────────────────────────────

runtime_installed() {
    command -v "$1" >/dev/null 2>&1
}

# True if the runtime can run containers now. None of these commands starts anything.
runtime_ready() {
    case "$1" in
        container) container system status >/dev/null 2>&1 ;;
        # Fails when podman needs a virtual machine and it is not running.
        podman)    podman info >/dev/null 2>&1 ;;
        # Asks the daemon for its version, so it fails when the daemon cannot be reached.
        docker)    docker version --format '{{.Server.Version}}' >/dev/null 2>&1 ;;
    esac
}

how_to_start() {
    case "$1" in
        container) echo "Start it with: container system start" ;;
        podman)    echo "Start its machine with: podman machine start" ;;
        docker)    echo "Start Docker Desktop, or the docker service." ;;
    esac
}

# Prints the names of the running containers, or of all containers with --all, one per line.
container_names() {
    case "$RUNTIME" in
        # With --name, the name is the container's id, which is what `ls --quiet` prints.
        container) container ls --quiet "$@" ;;
        *)         "$RUNTIME" ps --format '{{.Names}}' "$@" ;;
    esac
}

# Podman does not assume a registry for a short image name.
image_ref() {
    case "$RUNTIME" in
        podman) echo "docker.io/${IMAGE}" ;;
        *)      echo "$IMAGE" ;;
    esac
}

# The broker has no authentication. Podman and docker publish a port on every network interface
# unless told otherwise, so they are given the loopback address.
publish_spec() {
    case "$RUNTIME" in
        container) echo "${PORT}:9092" ;;
        *)         echo "127.0.0.1:${PORT}:9092" ;;
    esac
}

# Stops the container, giving the broker 30 s to shut down cleanly before the runtime kills it.
stop_container() {
    case "$RUNTIME" in
        container) container stop --time 30 "$CONTAINER" > /dev/null ;;
        # Docker has renamed the long option. The short one is the same in docker and podman.
        *)         "$RUNTIME" stop -t 30 "$CONTAINER" > /dev/null ;;
    esac
}

# Deletes the container, running or stopped, and the broker's data with it.
remove_container() {
    case "$RUNTIME" in
        container) container rm --force "$CONTAINER" > /dev/null ;;
        # The image declares volumes, which podman and docker create outside the container and
        # leave behind unless asked to remove them.
        *)         "$RUNTIME" rm --force --volumes "$CONTAINER" > /dev/null ;;
    esac
}

# ── The same for every runtime ───────────────────────────────────────────────

# Sets RUNTIME, or explains what is missing and exits.
choose_runtime() {
    local forced="${KAFKA_LOCAL_RUNTIME:-}" candidate not_running=""

    if [[ -n "$forced" ]]; then
        case "$forced" in
            container|podman|docker) ;;
            *)
                echo "KAFKA_LOCAL_RUNTIME is '${forced}'. Use one of: ${RUNTIMES[*]}" >&2
                exit 1
                ;;
        esac
        if ! runtime_installed "$forced"; then
            echo "KAFKA_LOCAL_RUNTIME is '${forced}', which is not installed." >&2
            exit 1
        fi
        if ! runtime_ready "$forced"; then
            echo "'${forced}' is installed but not running. $(how_to_start "$forced")" >&2
            exit 1
        fi
        RUNTIME="$forced"
        return
    fi

    for candidate in "${RUNTIMES[@]}"; do
        runtime_installed "$candidate" || continue
        if runtime_ready "$candidate"; then
            RUNTIME="$candidate"
            return
        fi
        not_running+="'${candidate}' is installed but not running. $(how_to_start "$candidate")"$'\n'
    done

    if [[ -z "$not_running" ]]; then
        echo "No container runtime is installed. Install one of: ${RUNTIMES[*]}" >&2
    else
        echo -n "$not_running" >&2
        echo "No container runtime is running." >&2
    fi
    exit 1
}

is_running() {
    grep -qx "$CONTAINER" <<< "$(container_names)"
}

# True for a running container and for a stopped one.
container_exists() {
    grep -qx "$CONTAINER" <<< "$(container_names --all)"
}

kafka_topics() {
    "$RUNTIME" exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${PORT}" "$@"
}

cmd="${1:-}"
[[ -z "$cmd" ]] && usage
case "$cmd" in
    start|stop|remove|status) choose_runtime ;;
    *) usage ;;
esac

case "$cmd" in
  start)
    if is_running; then
        echo "Kafka is already running (${CONTAINER}, ${RUNTIME})"
        exit 0
    fi

    if container_exists; then
        # Stopped by `stop`. The container still holds the broker's data.
        echo "Starting the stopped broker on localhost:${PORT} with ${RUNTIME}, with its data ..."
        "$RUNTIME" start "$CONTAINER" > /dev/null
    else
        echo "Starting a new broker on localhost:${PORT} with ${RUNTIME} ..."
        "$RUNTIME" run --detach \
            --name "$CONTAINER" \
            --publish "$(publish_spec)" \
            --env KAFKA_NODE_ID=1 \
            --env KAFKA_PROCESS_ROLES=broker,controller \
            --env KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
            --env "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:${PORT}" \
            --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
            --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
            --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
            --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
            --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
            "$(image_ref)" > /dev/null
    fi

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
        stop_container
        echo "Stopped. The data is kept: '$0 start' brings the broker back, '$0 remove' deletes it."
    else
        echo "Kafka is not running."
    fi
    ;;

  remove)
    if container_exists; then
        echo "Removing ${CONTAINER} and its data ..."
        remove_container
        echo "Removed."
    else
        echo "There is no broker to remove."
    fi
    ;;

  status)
    if is_running; then
        echo "Kafka is running (${CONTAINER}, ${RUNTIME}) on localhost:${PORT}"
        echo ""
        echo "Topics:"
        kafka_topics --list
    elif container_exists; then
        echo "Kafka is stopped (${CONTAINER}, ${RUNTIME}). Its data is kept."
    else
        echo "Kafka is not running, and there is no stopped broker (${RUNTIME})."
    fi
    ;;

  *)
    usage
    ;;
esac
