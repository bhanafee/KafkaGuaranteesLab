package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Stage 1 of the lab, started with the {@code stage1} profile: no idempotence and no Resilience4j.
 * The application runs, publishes and consumes, and a processing failure goes straight to
 * redelivery and the dead-letter topic.
 */
@SpringBootTest(properties = "lab.consumer.redelivery.interval-ms=100")
@ActiveProfiles("stage1")
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LabStageOneTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private ProducerFactory<String, LanguagePreference> producerFactory;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;
  @Autowired private EmbeddedKafkaBroker broker;
  @Autowired private ProcessingLedger ledger;
  @Autowired private FaultInjector faults;

  @Value("${lab.dead-letter.topic}")
  private String deadLetterTopic;

  @Test
  void idempotenceAndCircuitBreakersAreOff() {
    assertThat(producerFactory.getConfigurationProperties())
        .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)
        .containsEntry(ProducerConfig.ACKS_CONFIG, "all");
    assertThat(circuitBreakerRegistry.circuitBreaker("languagePreferenceProducer").getState())
        .isEqualTo(CircuitBreaker.State.DISABLED);
    assertThat(circuitBreakerRegistry.circuitBreaker("languagePreferenceConsumer").getState())
        .isEqualTo(CircuitBreaker.State.DISABLED);
  }

  @Test
  void eventIsPublishedAndProcessed_andAFailingOneIsDeadLettered() throws Exception {
    var processed = new LanguagePreference("cust-1", Locale.FRENCH);
    producer.publish(processed).get(5, TimeUnit.SECONDS);
    awaitUntil(() -> ledger.processedCount(processed.eventId()) == 1, Duration.ofSeconds(15));

    // One attempt per delivery, and one delivery plus two redeliveries: three failures in all.
    faults.set(Faults.none().withProcessingFailures(3));
    var failing = new LanguagePreference("cust-2", Locale.GERMAN);
    producer.publish(failing).get(5, TimeUnit.SECONDS);

    DeadLetterPointer pointer = KafkaTestSupport.readDeadLetter(broker, deadLetterTopic).value();
    assertThat(pointer.topic()).isEqualTo(LanguagePreferenceProducer.TOPIC);
    assertThat(pointer.exceptionMessage()).isEqualTo("Injected fault: downstream unavailable");
    assertThat(faults.pending().processingFailures()).isZero();
    assertThat(ledger.processedCount(failing.eventId())).isZero();
    assertThat(ledger.wasDropped(failing.eventId())).isFalse();
  }
}
