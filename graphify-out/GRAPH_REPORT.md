# Graph Report - /Users/brian/Projects/kafkaguaranteeslab  (2026-08-01)

## Corpus Check
- Corpus is ~6,075 words - fits in a single context window. You may not need a graph.

## Summary
- 126 nodes · 172 edges · 27 communities (12 shown, 15 thin omitted)
- Extraction: 66% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.8)
- Token cost: 8,500 input · 4,200 output

## Community Hubs (Navigation)
- Delivery Patterns & Architecture
- Spring Kafka Configuration
- Kafka Consumer Implementation
- Producer & Resilience
- Test Infrastructure
- REST API Endpoints
- Documentation Deployment
- CI/CD Pipeline
- Build Scripts
- Application Setup
- Document Processing Utilities
- Local Kafka Script
- Failure Mode Boundaries
- Timeout & Retry Mechanisms
- Send Failure Handling
- Package Publishing Workflow
- Build Dependencies & Versioning
- At-Most-Once Guarantee
- Duplicate Process Failure Mode
- Gradle Version Management
- Kafka Consumer Settings
- Prometheus Metrics
- Resilience4j Dependency
- Spring Boot Version

## God Nodes (most connected - your core abstractions)
1. `LanguagePreference` - 18 edges
2. `LanguagePreferenceProducer` - 12 edges
3. `Spring Boot application` - 9 edges
4. `LanguagePreferenceConsumer` - 7 edges
5. `KafkaConfig` - 6 edges
6. `LanguagePreferenceController` - 6 edges
7. `LanguagePreferenceProducerTest` - 6 edges
8. `LanguagePreferenceProducer` - 6 edges
9. `LanguagePreferenceConsumer` - 5 edges
10. `At-least-once delivery guarantee` - 4 edges

## Surprising Connections (you probably didn't know these)
- `LanguagePreferenceProducer` ----> `Resilience4j circuit breaker (languagePreferenceProducer)`  [0.9]
  CLAUDE.md → application.yml
- `LanguagePreferenceProducer` ----> `Resilience4j retry (languagePreferenceProducer)`  [0.9]
  CLAUDE.md → application.yml
- `LanguagePreferenceConsumer` ----> `Resilience4j circuit breaker (languagePreferenceConsumer)`  [0.9]
  CLAUDE.md → application.yml
- `LanguagePreferenceConsumer` ----> `Resilience4j retry (languagePreferenceConsumer)`  [0.9]
  CLAUDE.md → application.yml
- `Spring Kafka consumer group` ----> `LanguagePreferenceConsumer`  [0.95]
  application.yml → CLAUDE.md

## Import Cycles
- None detected.

## Communities (27 total, 15 thin omitted)

### Community 0 - "Delivery Patterns & Architecture"
Cohesion: 0.09
Nodes (31): Spring Actuator endpoints, At-least-once delivery guarantee, Circuit breaker pattern, Code of Conduct, Dead-letter topic (DLT), DefaultErrorHandler, Delivery guarantee layering, EmbeddedKafka testing (+23 more)

### Community 1 - "Spring Kafka Configuration"
Cohesion: 0.31
Nodes (8): Bean, ConcurrentKafkaListenerContainerFactory, Configuration, ConsumerFactory, ProducerFactory, KafkaTemplate, KafkaConfig, LanguagePreference

### Community 2 - "Kafka Consumer Implementation"
Cohesion: 0.33
Nodes (8): Acknowledgment, ConsumerRecord, KafkaListener, CircuitBreaker, Logger, Retry, Service, LanguagePreferenceConsumer

### Community 3 - "Producer & Resilience"
Cohesion: 0.33
Nodes (7): SendResult, CircuitBreaker, KafkaTemplate, Logger, Retry, Service, LanguagePreferenceProducer

### Community 4 - "Test Infrastructure"
Cohesion: 0.39
Nodes (5): DirtiesContext, EmbeddedKafka, SpringBootTest, LanguagePreferenceProducerTest, Test

### Community 5 - "REST API Endpoints"
Cohesion: 0.39
Nodes (5): PostMapping, RequestMapping, ResponseEntity, RestController, LanguagePreferenceController

### Community 6 - "Documentation Deployment"
Cohesion: 0.50
Nodes (5): GitHub Pages deployment, Deploy Site to GitHub Pages workflow, JaCoCo coverage report, Mermaid diagram initialization, Pandoc markdown to HTML conversion

### Community 7 - "CI/CD Pipeline"
Cohesion: 0.50
Nodes (4): CI test matrix (Java versions), Gradle CI workflow, Build Javadoc workflow, Java 25 toolchain

### Community 8 - "Build Scripts"
Cohesion: 0.83
Nodes (3): gradlew script, die(), warn()

## Knowledge Gaps
- **30 isolated node(s):** `At-most-once delivery guarantee`, `Kafka delivery.timeout.ms`, `Producer idempotence boundary (per-session)`, `Kafka producer settings reference`, `Kafka consumer settings reference` (+25 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **15 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `LanguagePreference` connect `Spring Kafka Configuration` to `Kafka Consumer Implementation`, `Producer & Resilience`, `Test Infrastructure`, `REST API Endpoints`?**
  _High betweenness centrality (0.111) - this node is a cross-community bridge._
- **Why does `LanguagePreferenceProducer` connect `Producer & Resilience` to `Spring Kafka Configuration`, `Test Infrastructure`, `REST API Endpoints`?**
  _High betweenness centrality (0.025) - this node is a cross-community bridge._
- **What connects `At-most-once delivery guarantee`, `Kafka delivery.timeout.ms`, `Producer idempotence boundary (per-session)` to the rest of the system?**
  _30 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Delivery Patterns & Architecture` be split into smaller, more focused modules?**
  _Cohesion score 0.09032258064516129 - nodes in this community are weakly interconnected._