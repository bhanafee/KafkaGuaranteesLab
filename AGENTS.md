# Codebase Guidance

This file documents key information about the project architecture, build commands, code style, and security practices.

## Project overview

A Spring Boot application demonstrating Kafka **at-least-once delivery semantics** for `LanguagePreference` events (`customerId` + `Locale`). It pairs Kafka producer/consumer configuration (idempotent producer, manual offset commit) with Resilience4j circuit breakers and retries to show how the guarantee is maintained end-to-end under failure.

## Commands

**Build and test:**
```bash
./gradlew build              # compile, test, spotless check
./gradlew test               # run all tests
./gradlew test --tests "*ProducerTest"           # run tests by class name
./gradlew test --tests "*ConsumerTest.test*"     # run tests by method pattern
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
- **`LanguagePreferenceConsumer`** — consumes events from Kafka and processes them

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

## Shared build sections

`build.gradle` and `settings.gradle` wrap the build logic this project shares with the other Java/Gradle projects in `// shared:begin NAME` and `// shared:end NAME` markers. The canonical copies live in `claude-skills/init-java-project/shared/`. The `shared-build` CI job checks out that directory and fails if a wrapped section differs from its canonical copy. Indentation, blank lines and comment lines are ignored. To change a shared section, change the canonical copy and the same section in every project in one change set. Run the check locally with `bash <claude-skills>/init-java-project/shared/check-shared-sections.sh . <claude-skills>/init-java-project/shared`.

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
