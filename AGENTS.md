# Codebase Guidance

This file documents key information about the project architecture, build commands, code style, and security practices.

## Project overview

A Spring Boot lab that shows why Kafka's idempotent producer does not give exactly-once delivery, using `LanguagePreference` events (`eventId` + `customerId` + `Locale`). The producer is idempotent with `acks=all`, the consumer commits offsets manually, and Resilience4j retries and circuit breakers sit on both sides. An in-memory ledger and a fault injector make duplicates, dropped events and dead letters reproducible and visible. The README's "Where Delivery Can Go Wrong" section is the lab's argument; keep code and that section in step.

## Commands

**Build and test:**
```bash
./gradlew build              # compile, test, spotless check
./gradlew test               # run all tests
./gradlew test --tests "*ProducerTest"           # run tests by class name
./gradlew test --tests "*LabScenarioTest.lost*"  # run tests by method pattern
```

**Code quality:**
```bash
./gradlew spotlessApply           # auto-format (required before commit)
./gradlew dependencyCheckAnalyze  # OWASP vulnerability scan (slow; fails at CVSS ≥ 7)
```

**NVD key (optional):** `dependencyCheckAnalyze` reads the key from the `NVD_API_KEY` environment variable or the `nvd.apiKey` Gradle property. With a key, it updates the NVD data live. Without one, it uses the local NVD cache and logs a warning. CI runs without a key, so it also uses the cache.

**External dependencies:** Requires Kafka broker (see Local Development for local setup).

Build uses Java 25 toolchain, compiles to Java 17 bytecode (`release = "17"`). CI tests on Java 17, 21, and 25.

## Key Entry Points

- **`LanguagePreferenceController`** — HTTP endpoint (`POST /language-preferences`) for publishing events
- **`LanguagePreferenceProducer`** — sends events to Kafka with retry/circuit-breaker protection
- **`LanguagePreferenceConsumer`** — consumes events from Kafka and hands them to the processor
- **`LanguagePreferenceProcessor`** — processes an event with retry/circuit-breaker protection and a fallback
- **`DeadLetterPointerRecoverer`** — writes a pointer to a record that exhausted its retries to the `dead-letters` topic
- **`ProcessingLedger`** — records, by event id, what the consumer did with each event (`GET /lab/ledger`)
- **`FaultInjector`** — makes a send, a processing step or an offset commit fail on request (`POST /lab/faults`)

## Architecture

### Delivery guarantee layering

**Producer side** (`producer/`):
- `KafkaConfig` sets `acks=all`, idempotence enabled, unlimited retries — Kafka-level at-least-once.
- `LanguagePreferenceProducer` wraps `KafkaTemplate.send()` with Resilience4j `@Retry` + `@CircuitBreaker` (instance name `languagePreferenceProducer`). The `fallbackMethod` (`publishFallback`) is declared on `@Retry`, so it runs once per publish: after the last failed attempt, or at once when the circuit is open, because the producer retry ignores `CallNotPermittedException`. It logs a warning and returns a failed future. There is no producer dead-letter store: one on the same cluster would fail along with the main topic.
- `LanguagePreferenceController` exposes `POST /language-preferences`. It runs the publish on the task executor and waits for the outcome for at most `lab.publish.response-timeout-ms`. It returns 202 Accepted once the broker acknowledges the event, 503 "Event not published" when the publish fails, and 503 "Event not confirmed" when the outcome is not known in time. Every response carries the event id in the `Event-Id` header.

**Consumer side** (`consumer/`):
- `KafkaConfig` disables auto-commit (`enable.auto.commit=false`) and sets `AckMode.MANUAL_IMMEDIATE` — offset is committed only after `LanguagePreferenceProcessor.process()` returns or a dead-letter pointer is written.
- `LanguagePreferenceConsumer.onMessage()` calls `LanguagePreferenceProcessor.process()`, then `ack.acknowledge()`. On exception it rethrows without acking; `DefaultErrorHandler` (fixed backoff from `application.yml`) redelivers, then calls `DeadLetterPointerRecoverer`.
- `DeadLetterPointerRecoverer` writes a `DeadLetterPointer` (source topic, partition, offset, timestamp, consumer group, exception class and message) to the shared `dead-letters` topic. It does not copy the payload. It blocks until the broker acknowledges the pointer and throws if the write fails, so the record is redelivered rather than skipped. `setCommitRecovered(true)` commits the offset after the pointer is written.
- The consumer's value deserializer is `ErrorHandlingDeserializer` wrapping `JacksonJsonDeserializer`, so a record that fails deserialization is dead-lettered without retries and never reaches the listener. Its pointer records a fixed message in place of the parser's message, which quotes the payload.
- `LanguagePreferenceProcessor.process()` carries `@Retry` (with `fallbackMethod = "processFallback"`) + `@CircuitBreaker` (instance name `languagePreferenceConsumer`) for any downstream call added there. It is a separate bean injected into the consumer: Resilience4j works through the Spring proxy, and a call within one object bypasses it. Keep the fallback on `@Retry`; on `@CircuitBreaker` it would hide every failure from the retry.
- `processFallback()` follows `lab.consumer.fallback`. With `acknowledge` (the default) it logs, records the event as dropped in the ledger and returns, so the record is acknowledged unprocessed. With `rethrow` it throws, and the record is redelivered and finally dead-lettered.
- `process()` records each event in the `ProcessingLedger`. With `lab.consumer.deduplicate=true` it skips an event whose id was already processed.
- A record with a null value (a tombstone) is acknowledged and skipped. It is not processed and not dead-lettered.

**Resilience4j config** lives in `application.yml` under `resilience4j.circuitbreaker` and `resilience4j.retry`. Both producer and consumer have named instances.

### Lab instruments (`lab/`)

- The application's own settings live under `lab.*` in `application.yml`. Do not put them under `spring.kafka.*`, where they would look like Spring Boot properties.
- `ProcessingLedger` is in memory. It stands in for a downstream system and is what the scenario tests assert on.
- `FaultInjector` holds three counts: `ambiguousSends` (a send is written, then reported as failed), `processingFailures` (`process()` throws) and `lostCommits` (a processed record is not acknowledged). The producer, processor and consumer each consume a count at the step concerned.
- `LabController` exposes both under `/lab`. The endpoints are unprotected.
- The `stage1` and `stage2` profiles run the application without Resilience4j (one retry attempt, circuit breakers disabled by `CircuitBreakerSwitch`, fallback `rethrow`). `stage1` also turns idempotence off.

### Tests

- `LabScenarioTest` runs the README's scenarios through the real producer, embedded broker and listener container on three partitions. Its tests share one context and assert on the ledger by event id, never on offsets.
- The container-based consumer tests that assert on committed offsets (`...DeadLetterTest`, `...PoisonPillTest`, `...ProcessingFailureTest`) start a fresh context and broker per test method.
- Shared helpers are in `KafkaTestSupport`.
- A new scenario in the README's summary table should get a test in `LabScenarioTest`, or an entry under "What the lab cannot reproduce".

**Observability**: Actuator exposes `health`, `info`, `prometheus`, `circuitbreakers`, and `retries`.

## Shared build sections

`build.gradle` and `settings.gradle` wrap the build logic this project shares with the other Java/Gradle projects in `// shared:begin NAME` and `// shared:end NAME` markers. The canonical copies live in `claude-skills/init-java-project/shared/`. The `shared-build` CI job checks out that directory and fails if a wrapped section differs from its canonical copy. Indentation, blank lines and comment lines are ignored. To change a shared section, change the canonical copy and the same section in every project in one change set. Run the check locally with `bash <claude-skills>/init-java-project/shared/check-shared-sections.sh . <claude-skills>/init-java-project/shared`.

## Code style

Spotless enforces Google Java Format. Run `./gradlew spotlessApply` before committing. `module-info.java` is excluded from formatting.

## Security patches

For CVE patch management, see the `gradle-security-patch` skill. Use `/gradle-security-patch` to pin a CVE fix in the version catalog.

## Local Development

**Running the application:**
```bash
./kafka-local.sh start       # start local Kafka (KRaft mode, no ZooKeeper); needs Apple's container and `container system start`
./gradlew bootRun            # start the application (expects Kafka on localhost:9092)
./kafka-local.sh stop        # stop local Kafka
```

The application publishes `LanguagePreference` events to Kafka and consumes them for processing.
