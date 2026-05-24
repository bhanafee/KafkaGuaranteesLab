# AtLeastOnce

A Spring Boot application demonstrating Kafka **at-least-once delivery semantics** for
`LanguagePreference` events. It layers Kafka producer/consumer configuration with Resilience4j
circuit breakers and retries to show how the guarantee is maintained end-to-end under failure.

## Links

- [GitHub repository](https://github.com/bhanafee/AtLeastOnce)
- [Javadoc](https://bhanafee.github.io/AtLeastOnce/javadoc/)
- [Apache 2.0 License](https://bhanafee.github.io/AtLeastOnce/LICENSE)
- [Code of Conduct](https://bhanafee.github.io/AtLeastOnce/CODE_OF_CONDUCT.html)
- [Claude Code guidance](https://bhanafee.github.io/AtLeastOnce/CLAUDE.html)

## The Problem

Kafka does not guarantee at-least-once delivery out of the box. Three distinct failure modes can
cause silent message loss:

1. **Producer acknowledgement failure**: the broker does not confirm receipt and the producer does
   not retry.
2. **Offset commit before processing**: auto-commit advances the offset before `process()` finishes,
   so a crash between the commit and the work silently drops the message.
3. **Application-level failure without fallback**: an exception thrown inside the listener that is
   not caught or retried at the framework level leaves the message neither processed nor
   dead-lettered.

Each layer has a different remedy, and this demo shows all three.

## What This Demo Shows

- **`acks=all` with idempotent producer**: `enable.idempotence=true` with unlimited retries ensures
  at least one durable write while preventing duplicates from Kafka-level retries
- **Manual offset commit**: `AckMode.MANUAL_IMMEDIATE` commits the offset only after `process()`
  succeeds; a crash before `ack.acknowledge()` causes redelivery, not loss
- **Application-layer retry**: Resilience4j `@Retry` on both producer and consumer handles transient
  downstream failures (3 attempts, 1 s wait each)
- **Circuit breaker isolation**: independent Resilience4j `@CircuitBreaker` instances protect
  against cascading failure; the producer fallback routes to a dead-letter store when the circuit
  opens
- **Dead-letter topic (DLT)**: messages that exhaust `DefaultErrorHandler` retries on the consumer
  side are routed to `language-preferences.DLT` rather than silently dropped
- **Observability**: Actuator exposes `health`, `circuitbreakers`, `retries`, and `prometheus`
  endpoints; circuit breaker state is surfaced in the health check

## Requirements

- Java 17 or higher (Java 21 toolchain used for compilation)
- Kafka broker on `localhost:9092` (or use the included `kafka-local.sh`)

## Running the Demo

Start a local Kafka broker in KRaft mode (no ZooKeeper):

```bash
./kafka-local.sh start
```

Build and run:

```bash
./gradlew bootRun
```

Send a language preference event:

```bash
curl -X POST http://localhost:8080/language-preferences \
  -H 'Content-Type: application/json' \
  -d '{"customerId": "abc123", "preferredLanguage": "fr-CA"}'
# HTTP 202 Accepted
```

Check circuit breaker and retry state:

```bash
curl http://localhost:8080/actuator/circuitbreakers
curl http://localhost:8080/actuator/retries
```

Stop the broker when done:

```bash
./kafka-local.sh stop
```

## Architecture

### Delivery guarantee layers

At-least-once delivery is enforced at two independent layers. The Kafka broker protocol handles
durability on the producer side. Manual offset management handles redelivery on the consumer side.
Resilience4j adds application-level retry and circuit breaking on top of both.

```mermaid
flowchart TD
    subgraph Producer["Producer side"]
        CTRL["POST /language-preferences\nLanguagePreferenceController"]
        PROD["LanguagePreferenceProducer\n@Retry + @CircuitBreaker"]
        KT["KafkaTemplate.send()\nacks=all · idempotent · unlimited retries"]
        DLS["Dead-letter store\n(circuit breaker fallback)"]

        CTRL -->|"202 Accepted\n(fire and forget)"| PROD
        PROD --> KT
        PROD -->|"circuit open"| DLS
    end

    subgraph Kafka["Kafka"]
        TOPIC["language-preferences"]
        DLT["language-preferences.DLT"]
    end

    subgraph Consumer["Consumer side"]
        CONS["LanguagePreferenceConsumer.onMessage()\nAckMode.MANUAL_IMMEDIATE"]
        PROC["process()\n@Retry + @CircuitBreaker"]
        ACK["ack.acknowledge()\ncommit offset"]
        EH["DefaultErrorHandler\nFixedBackOff 1 s · 2 attempts"]

        CONS --> PROC
        PROC --> ACK
        CONS -->|"exception → rethrow\n(no ack)"| EH
        EH -->|"retries exhausted"| DLT
    end

    KT --> TOPIC
    TOPIC --> CONS
```

### Sequence: successful delivery

```mermaid
sequenceDiagram
    participant Client
    participant Ctrl as LanguagePreferenceController
    participant Prod as LanguagePreferenceProducer
    participant K as Kafka
    participant Cons as LanguagePreferenceConsumer
    participant Proc as process()

    Client->>Ctrl: POST /language-preferences
    Ctrl->>Prod: publish(event)
    Ctrl-->>Client: 202 Accepted
    Prod->>K: send(key=customerId, acks=all)
    K-->>Prod: broker ack
    K->>Cons: onMessage(record)
    Cons->>Proc: process(event)
    Proc-->>Cons: success
    Cons->>K: ack.acknowledge()
    Note over K: offset committed
```

### Sequence: consumer failure and DLT routing

```mermaid
sequenceDiagram
    participant K as Kafka
    participant Cons as LanguagePreferenceConsumer
    participant Proc as process()
    participant EH as DefaultErrorHandler
    participant DLT as language-preferences.DLT

    K->>Cons: onMessage(record) [attempt 1]
    Cons->>Proc: process(event)
    Proc-->>Cons: throws exception
    Cons->>EH: rethrow (no ack)
    Note over EH: FixedBackOff — wait 1 s, retry
    EH->>Cons: onMessage(record) [attempt 2]
    Cons->>Proc: process(event)
    Proc-->>Cons: throws again
    Cons->>EH: rethrow (no ack)
    Note over EH: retries exhausted
    EH->>DLT: route to dead-letter topic
```

## Configuration reference

### Kafka producer

| Setting | Value | Purpose |
|---|---|---|
| `acks` | `all` | All in-sync replicas must acknowledge before the send completes |
| `enable.idempotence` | `true` | Prevents duplicate records from broker-level retries |
| `retries` | `Integer.MAX_VALUE` | Delegates retry decisions to the application and circuit breaker |
| `max.in.flight.requests.per.connection` | `5` | Maximum allowed with idempotence enabled |

### Kafka consumer

| Setting | Value | Purpose |
|---|---|---|
| `enable.auto.commit` | `false` | Offset committed manually after successful processing only |
| `auto.offset.reset` | `earliest` | No committed offset on first start — consume from the beginning |
| `AckMode` | `MANUAL_IMMEDIATE` | Commits immediately when `ack.acknowledge()` is called |

### Resilience4j

Both the producer and consumer have independent instances configured in `application.yml`.

#### Circuit breaker defaults

| Instance | Failure threshold | Slow call threshold | Slow duration | Wait in open |
|---|---|---|---|---|
| `languagePreferenceProducer` | 50% | 80% | 2 s | 30 s |
| `languagePreferenceConsumer` | 50% | — | — | 30 s |

Both instances use a count-based sliding window of 10 calls with 3 calls permitted in half-open
state.

#### Retry defaults

Both instances: 3 attempts, 1 s fixed wait, retries on any `Exception`.

### DefaultErrorHandler

`FixedBackOff(1 s, 2 attempts)` — up to 2 delivery retries before the message is routed to the
dead-letter topic. This operates at the Kafka listener container layer, independently of the
Resilience4j retry inside `process()`.
