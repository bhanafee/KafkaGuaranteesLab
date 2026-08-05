# Codebase Guidance

This file documents key information about the project architecture, build commands, code style, and security practices.

## Project overview

A Spring Boot application demonstrating Kafka **at-least-once delivery semantics** for `LanguagePreference` events (`customerId` + `Locale`). It pairs Kafka producer/consumer configuration (idempotent producer, manual offset commit) with Resilience4j circuit breakers and retries to show how the guarantee is maintained end-to-end under failure.

## Commands

```bash
./gradlew build                   # compile, test, spotless check
./gradlew test                    # run tests
./gradlew test --tests "..."      # run a single test class
./gradlew spotlessApply           # auto-format (required before commit)
./gradlew dependencyCheckAnalyze  # OWASP vulnerability scan (slow; fails at CVSS ≥ 7)
```

Build uses Java 25 toolchain, compiles to Java 17 bytecode (`release = "17"`). CI tests on Java 17, 21, and 25.

## Architecture

### Delivery guarantee layering

**Producer side** (`producer/`):
- `KafkaConfig` sets `acks=all`, idempotence enabled, unlimited retries — Kafka-level at-least-once.
- `LanguagePreferenceProducer` wraps `KafkaTemplate.send()` with Resilience4j `@Retry` + `@CircuitBreaker` (instance name `languagePreferenceProducer`). The circuit breaker's `fallbackMethod` logs and drops to a dead-letter store when open.
- `LanguagePreferenceController` exposes `POST /language-preferences` and returns 202 Accepted immediately (fire-and-forget to the producer).

**Consumer side** (`consumer/`):
- `KafkaConfig` disables auto-commit (`enable.auto.commit=false`) and sets `AckMode.MANUAL_IMMEDIATE` — offset is committed only after `process()` succeeds.
- `LanguagePreferenceConsumer.onMessage()` calls `process()`, then `ack.acknowledge()`. On exception it rethrows without acking; `DefaultErrorHandler` (fixed backoff from `application.yml`) retries, then routes to the dead-letter topic (DLT).
- `process()` carries its own `@Retry` + `@CircuitBreaker` (instance name `languagePreferenceConsumer`) for any downstream call added there.

**Resilience4j config** lives in `application.yml` under `resilience4j.circuitbreaker` and `resilience4j.retry`. Both producer and consumer have named instances.

**Observability**: Actuator exposes `health`, `info`, `prometheus`, `circuitbreakers`, and `retries`.

## Code style

Spotless enforces Google Java Format. Run `./gradlew spotlessApply` before committing. `module-info.java` is excluded from formatting.

## Security patches

For CVE patch management, see the `gradle-security-patch` skill. Use `/gradle-security-patch` to pin a CVE fix in the version catalog.

## Local Development

**Running the application:**
```bash
./kafka-local.sh start       # start local Kafka (KRaft mode, no ZooKeeper)
./gradlew bootRun            # start the application (expects Kafka on localhost:9092)
./kafka-local.sh stop        # stop local Kafka
```

The application publishes `LanguagePreference` events to Kafka and consumes them for processing.
