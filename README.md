# KafkaGuaranteesLab

A Spring Boot application demonstrating the limitations of Kafka guarantees. It layers producer/consumer
configuration with Resilience4j circuit breakers and retries to show how the guarantee is maintained end-to-end
under failure. Kafka itself is configured for its strongest guarantee — an idempotent producer with `acks=all` —
which deduplicates broker-level retries within a producer session. The demo shows
how this can greatly reduce the number of duplicate messages but cannot eliminate them entirely without
risking data loss.

## The Problem

Kafka provides strong delivery and ordering guarantees for applications that follow the API contract. Under most
conditions, every message is processed exactly once and in the same order by both producer and consumer. However,
developers need to be aware of the boundaries and plan for failures. This demo shows how to handle common failure modes.

### A Quick Peek Under the Covers

To understand where things can go awry, it's helpful to understand what happens when things go right. There are
three common delivery guarantees for messages:

* *At-most-once* guarantees that a message is delivered either once or not at all. Conceptually, the easiest way to
  ensure this is to try to send the message one time and never retry.
* *At-least-once* guarantees that a message is delivered at least once, but maybe more. Conceptually, the
  easiest way to ensure this is to send the message and retry until it is acknowledged.
* *Exactly-once* guarantees that a message is delivered once and no more. Conceptually, this is the same
  as simultaneously guaranteeing both at-least-once *and* at-most-once.

Obviously, you can't get exactly-once by using the easy approaches to at-most-once and at-least-once at the same time.
What you can do instead is use retries as needed to ensure at least once delivery, then filter duplicates. Duplicates
can happen when the sender retries while the receiver is down, or when the receiver is slow to process
messages. Kafka implements a limited exactly-once guarantee by doing just that:

* At-least-once comes from retrying until the broker acknowledges. `acks=all` makes that acknowledgement
  durable: each message is persisted by all in-sync replicas before the broker acks, so an acked message
  survives a broker failure.
* The at-most-once half, suppressing duplicates from those retries, comes from `enable.idempotence=true`,
  which has Kafka assign a unique key (producer ID + sequence number) to each message so the broker can drop a
  replayed one.

Both `acks=all` and `enable.idempotence=true` have been default Kafka settings since 3.0.0. In addition, if
idempotence is enabled, Kafka will automatically require `acks=all` for durable writes.

### Common Failure Modes

There are a few ways that exactly-once processing can fail even with idempotence enabled.

#### Timeout and Retry

Suppose a producer application has this code:

```java
public void send(Record<String,Foo> record) {
    producer.send(record);
    Thread.sleep(5);
    producer.send(record);       // Send the same record again
}
```

Kafka would send the first message, wait 5 milliseconds, then send a second message with exactly the same contents.
Kafka will view this as two separate messages. Each will be assigned a unique sequence number, and both will be delivered.

Now consider a producer application with a retry configured like this:

```java
@Retry(delay = 5)
public void send(Record<String,Foo> record) {
    producer.send(record);
}
```

If the first attempt times out (5ms is way too short), the retry will be triggered. But the first attempt might yet
succeed even after the retry. As before, Kafka would send the same message twice with different sequence numbers.
You can mitigate this failure by ensuring Kafka's internal retry loop finishes before the application retries.
Enabling idempotence requires `retries` > 0. The critical setting is `delivery.timeout.ms`, which bounds the
whole send including internal retries. Keep it *no larger* than the application's per-attempt timeout, or simply
gate the application retry on the `send()` future completing. Idempotence dedups only Kafka's own retries. A
fresh application-level `send()` gets a new sequence number and is not filtered.

#### Exception on Send

The producer might throw an exception or otherwise indicate failure on a `send()` call.

* `producer.send()` throws an exception
* On the Future returned by `send()`:
    * Any method throws an exception, including if `get(timeout)` times out
    * `isCancelled()` returns true
* On the `RecordMetadata` returned by `send().get()`:
    * `hasOffset()` or `hasTimestamp()` returns false
    * `offset()`, `timestamp()` or `partition()` returns -1
 
In most cases, the failure means the producer did not send the message successfully. However, there are a few cases
where the message did get sent despite the failure. Retries from the application tier *may* send duplicate messages
in those cases.

**Tip:** These types of failures may indicate a problem with the producer configuration. Pay particular attention to
the `acks`, `retries`, `delivery.timeout.ms` and `max.in.flight.requests.per.connection` settings.

#### Boundaries after Restart

If a producer application crashes after sending a message but before the broker acknowledges the receipt or before the
receipt is recorded by the producer, the message may be delivered again when the application restarts. This can lead to
duplicate messages. After restart, the producer will be assigned a new PID. Any message delivered before the
crash but un-acked (network loss, timeout) will be re-sent under the new PID — the broker has no way
to recognise it as a duplicate. Idempotence is per-session, not durable across process lifetimes.

#### Duplicate Producer Processes

While outside the realm of Kafka, if multiple producer instances share the same logical role then each will have a
different PID. The broker deduplicates per-PID, so each instance will send its own messages. This can lead to duplicate
messages if the same event is published by multiple instances.

## What This Demo Shows

- **`acks=all` with idempotent producer**: `enable.idempotence=true` with unlimited retries ensures
  at least one durable write while preventing duplicates from Kafka-level retries
- **Manual offset commit**: `AckMode.MANUAL_IMMEDIATE` commits the offset only after `process()`
  succeeds; a crash before `ack.acknowledge()` causes redelivery, not loss
- **Tombstones skipped**: a record with a null value carries no preference, so the consumer
  acknowledges it without processing it and without dead-lettering it
- **Application-layer retry**: Resilience4j `@Retry` on both producer and consumer handles transient
  downstream failures (3 attempts, 1 s wait each)
- **Circuit breaker isolation**: independent Resilience4j `@CircuitBreaker` instances protect
  against cascading failure; while the producer circuit is open, publishes fail fast
- **Failure surfaced to the caller**: the HTTP endpoint responds 202 only after the broker
  acknowledges the event, and 503 when the publish fails. The producer keeps no dead-letter store
  (see [Why the producer has no dead-letter store](#why-the-producer-has-no-dead-letter-store))
- **Dead-letter pointers**: a message that exhausts `DefaultErrorHandler` retries on the consumer
  side is not silently dropped. A pointer to it (topic, partition, offset) is written to the shared
  `dead-letters` topic. The message itself is not copied
  (see [Dead-letter pointers](#dead-letter-pointers))
- **Observability**: Actuator exposes `health`, `circuitbreakers`, `retries`, and `prometheus`
  endpoints; circuit breaker state is surfaced in the health check

## Requirements

- Java 17 or higher (Java 25 toolchain used for compilation)
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
# HTTP 202 Accepted once the broker acknowledges the event
# HTTP 503 Service Unavailable if the publish fails
```

Read the dead-letter pointers, if any:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic dead-letters --from-beginning
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
        CTRL["POST /language-preferences<br/>LanguagePreferenceController"]
        PROD["LanguagePreferenceProducer<br/>@Retry + @CircuitBreaker"]
        KT["KafkaTemplate.send()<br/>acks=all · idempotent · unlimited retries"]
        FAIL["503 Service Unavailable"]

        CTRL -->|"waits for the outcome"| PROD
        PROD --> KT
        PROD -->|"retries exhausted<br/>or circuit open"| FAIL
    end

    subgraph Kafka["Kafka"]
        TOPIC["language-preferences"]
        DLT["dead-letters<br/>(pointers, shared by all topics)"]
    end

    subgraph Consumer["Consumer side"]
        CONS["LanguagePreferenceConsumer.onMessage()<br/>AckMode.MANUAL_IMMEDIATE"]
        PROC["process()<br/>@Retry + @CircuitBreaker"]
        ACK["ack.acknowledge()<br/>commit offset"]
        EH["DefaultErrorHandler<br/>FixedBackOff 1 s · 2 redeliveries"]
        REC["DeadLetterPointerRecoverer"]

        CONS --> PROC
        PROC --> ACK
        CONS -->|"exception → rethrow<br/>(no ack)"| EH
        EH -->|"retries exhausted"| REC
        REC -->|"pointer acked"| ACK
    end

    REC -->|"topic · partition · offset"| DLT

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
    Prod->>K: send(key=customerId, acks=all)
    K-->>Prod: broker ack
    Prod-->>Ctrl: future completes
    Ctrl-->>Client: 202 Accepted
    K->>Cons: onMessage(record)
    Cons->>Proc: process(event)
    Proc-->>Cons: success
    Cons->>K: ack.acknowledge()
    Note over K: offset committed
```

### Sequence: consumer failure and dead-letter pointer

```mermaid
sequenceDiagram
    participant K as Kafka (language-preferences)
    participant Cons as LanguagePreferenceConsumer
    participant Proc as process()
    participant EH as DefaultErrorHandler
    participant Rec as DeadLetterPointerRecoverer
    participant DLT as Kafka (dead-letters)

    K->>Cons: onMessage(record) [delivery 1]
    Cons->>Proc: process(event)
    Proc-->>Cons: throws exception
    Cons->>EH: rethrow (no ack)
    Note over EH: FixedBackOff — wait 1 s, redeliver
    EH->>Cons: onMessage(record) [delivery 2]
    Cons->>EH: rethrow (no ack)
    Note over EH: FixedBackOff — wait 1 s, redeliver
    EH->>Cons: onMessage(record) [delivery 3]
    Cons->>EH: rethrow (no ack)
    Note over EH: retries exhausted
    EH->>Rec: accept(record, consumer, exception)
    Rec->>DLT: send(pointer: topic, partition, offset)
    DLT-->>Rec: broker ack
    Rec-->>EH: recovered
    EH->>K: commit offset + 1
    Note over K: the record stays in language-preferences
```

### Dead-letter pointers

A conventional dead-letter topic holds a full copy of each failed record, usually in one topic per
source topic. This demo keeps one shared `dead-letters` topic and writes only a pointer to the
failed record:

```json
{
  "topic": "language-preferences",
  "partition": 0,
  "offset": 42,
  "timestamp": 1794856800000,
  "consumerGroup": "kafka-guarantees-lab-group",
  "exceptionClass": "java.lang.IllegalStateException",
  "exceptionMessage": "downstream unavailable"
}
```

The pointer never reads the payload, so the same recoverer and the same topic work for any source
topic and any payload type. Customer data is not duplicated into a second topic with its own
retention and access rules.

#### Wiring

- `KafkaConfig.kafkaListenerContainerFactory()` builds the `DefaultErrorHandler` with two
  arguments: the `DeadLetterPointerRecoverer` and the `FixedBackOff`. The error handler redelivers
  a failed record until the backoff is exhausted, then calls the recoverer.
- `DeadLetterPointerRecoverer` implements Spring Kafka's `ConsumerAwareRecordRecoverer`. It gets
  the failed `ConsumerRecord`, the `Consumer` and the exception. It reads the coordinates from the
  record, the group from the consumer and the root cause from the exception.
- The pointer is sent through its own `KafkaTemplate<String, DeadLetterPointer>`, built from the
  same producer settings as the events (`acks=all`, idempotent). The key is
  `<source topic>-<source partition>`, so pointers into one source partition stay in offset order.
- The recoverer blocks on the send for up to `spring.kafka.consumer.dead-letter.send-timeout-ms`.
  If the send fails or times out, the recoverer throws. `DefaultErrorHandler` treats that as a
  failed recovery and redelivers the source record with a fresh set of retries.
- `setCommitRecovered(true)` makes the error handler commit the offset after a successful
  recovery. With a manual ack mode the container does not do this on its own.

- The consumer's value deserializer is `ErrorHandlingDeserializer` wrapping
  `JacksonJsonDeserializer`. A payload that cannot be deserialized would otherwise fail inside
  `poll()`, where the error handler has no record to recover, and the consumer would retry the
  same offset in a tight loop. With the wrapper, the failure reaches the error handler as a
  `DeserializationException`. That exception is not retried: the pointer is written at once and
  the record never reaches the listener.

The order is therefore always: pointer acknowledged, then offset committed. A crash between the
two redelivers the record, never loses it.

#### Catches

- **A pointer expires with its source.** Once retention or compaction removes the original
  record, the pointer dangles. The retention of the source topic is the deadline for dealing with
  its dead letters. On a compacted topic, a newer record with the same key removes the failed one.
- **Replay needs a seek.** A tool that reprocesses dead letters cannot simply consume
  `dead-letters`. For each pointer it must assign the source partition, seek to the offset and
  read one record. `LanguagePreferenceConsumerDeadLetterTest.readSource()` shows this.
- **Pointers can be duplicated.** A pointer send that times out may still succeed, and the record
  is then redelivered and dead-lettered again. Pointers are at-least-once, like everything else
  here. Treat (topic, partition, offset) as the identity of a dead letter.
- **A failing pointer write blocks the partition.** While `dead-letters` cannot be written, the
  failed record is redelivered indefinitely and nothing behind it in the partition is consumed.
  This is the price of never skipping a record without a pointer.
- **The exception message is copied.** Keep payload data out of exception messages, or the
  pointer carries it after all. Deserialization failures are the exception: a parser's message
  quotes the bytes it could not read, so the pointer records the fixed text
  `payload could not be deserialized` and the class of the root cause.
- **The topic must exist.** `kafka-local.sh` creates `dead-letters`. Elsewhere, create it or rely
  on broker topic auto-creation.

### Why the producer has no dead-letter store

A dead-letter topic on the producer side would sit on the same Kafka cluster as the main topic.
When publishes fail because the cluster is unreachable, a write to a dead-letter topic fails for
the same reason. It could only help with failures specific to one record or one topic, such as an
oversized record or a missing topic.

So the producer stores nothing and reports the failure. `LanguagePreferenceController` waits for
the publish outcome and responds 503 Service Unavailable when the publish fails after its retries
or the circuit is open. The caller still holds the event and decides whether to retry.

- A 503 does not prove the event was not written. If the outcome is not known before the Spring
  MVC async request timeout, the response is also 503 while the send is still in progress. A
  caller that retries can then publish a duplicate. This is the
  [Timeout and Retry](#timeout-and-retry) failure mode, one tier up.
- A system that must accept events while Kafka is down needs a durable store outside Kafka, such
  as a transactional outbox. That is outside this demo.

## Configuration reference

### Kafka producer

| Setting | Value | Purpose |
|---|---|---|
| `acks` | `all` | All in-sync replicas must acknowledge before the send completes |
| `enable.idempotence` | `true` | Prevents duplicate records from broker-level retries |
| `retries` | `Integer.MAX_VALUE` | Retries transient errors until `delivery.timeout.ms` elapses; the time bound, not the count, limits retries |
| `delivery.timeout.ms` | `120000` (default) | Upper bound on a single send, including all internal retries; not overridden in `KafkaConfig` |
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

Both instances use a count-based sliding window of 10 calls. The producer permits 3 calls in the
half-open state; the consumer leaves this at the Resilience4j default of 10.

#### Retry defaults

Both instances: 3 attempts, 1 s fixed wait, retries on any `Exception`.

### DefaultErrorHandler

`FixedBackOff(1 s, 2 attempts)` — up to 2 redeliveries (3 deliveries in total) before a pointer to
the message is written to the dead-letter topic. This operates at the Kafka listener container
layer, independently of the Resilience4j retry inside `process()`.

| Setting | Value | Purpose |
|---|---|---|
| `spring.kafka.consumer.retry.interval-ms` | `1000` | Wait between redeliveries |
| `spring.kafka.consumer.retry.max-attempts` | `2` | Redeliveries after the first delivery |
| `spring.kafka.consumer.dead-letter.topic` | `dead-letters` | Shared topic for dead-letter pointers |
| `spring.kafka.consumer.dead-letter.send-timeout-ms` | `10000` | How long the recoverer waits for the pointer to be acknowledged |
| `commitRecovered` | `true` | Commit the offset once the pointer is written |

## Layout

```mermaid
treeView-beta
    "kafka-local.sh"
    "src"
        "resources"
            "application.yml"
        "java"
            "..."
                "KafkaGuaranteesLabApplication.java"
                "config"
                    "KafkaConfig.java"
                "consumer"
                    "DeadLetterPointerRecoverer.java"
                    "LanguagePreferenceConsumer.java"
                "producer"
                    "LanguagePreferenceProducer.java"
                    "LanguagePreferenceController.java"
                "model"
                    "DeadLetterPointer.java"
                    "LanguagePreference.java"
```


## Technologies

| Component | Version |
|-----------|---------|
| Java | 25 (toolchain; runs on 17+) |
| Gradle | 9.6.1 |
| Spring Boot | 4.0.6 |
| Resilience4j | 2.4.0 |
| Spring Kafka | (via Spring Boot) |
| Micrometer/Prometheus | (via Spring Boot) |
| JaCoCo | 0.8.14 |

## Links

- [GitHub repository](https://github.com/bhanafee/KafkaGuaranteesLab)
- [Javadoc](https://bhanafee.github.io/KafkaGuaranteesLab/javadoc/)
- [Test Results](https://bhanafee.github.io/KafkaGuaranteesLab/tests/)
- [Coverage Report](https://bhanafee.github.io/KafkaGuaranteesLab/coverage/)
- [Code of Conduct](https://bhanafee.github.io/KafkaGuaranteesLab/CODE_OF_CONDUCT.html)
- [Agents guidance](https://bhanafee.github.io/KafkaGuaranteesLab/AGENTS.html)

[![Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
