package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

/**
 * The contrast to {@link ConsumerOpenCircuitTest}: with {@code lab.consumer.fallback=rethrow} the
 * same open circuit dead-letters the events it rejects and drops none. The pointer of such an event
 * names the rejected call and not the fault that opened the circuit.
 *
 * <p>The topic has one partition, so each pointer is matched to its event by offset. The retry and
 * redelivery waits are shortened and the open state is lengthened, as in {@code
 * ConsumerOpenCircuitTest}.
 */
@SpringBootTest(
    properties = {
      "lab.consumer.fallback=rethrow",
      "lab.consumer.redelivery.interval-ms=100",
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms",
      "resilience4j.circuitbreaker.instances.languagePreferenceConsumer.wait-duration-in-open-state=1h"
    })
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class ConsumerOpenCircuitRethrowTest {

  private static final String INJECTED = "Injected fault: downstream unavailable";

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private ProcessingLedger ledger;
  @Autowired private FaultInjector faults;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;
  @Autowired private EmbeddedKafkaBroker broker;

  @Value("${lab.dead-letter.topic}")
  private String deadLetterTopic;

  @Test
  void openCircuit_deadLettersHealthyEvents_andThePointerNamesTheRejectedCall() throws Exception {
    // The first event fails all 9 calls of its 3 deliveries. The tenth failure is the first call
    // for the second event and opens the circuit. Every later call is rejected.
    faults.set(Faults.none().withProcessingFailures(10));
    long failedOffset = publish("cust-failing");
    long openingOffset = publish("cust-opening");
    long healthyOffset = publish("cust-healthy");

    Map<Long, DeadLetterPointer> pointers = readDeadLetters(3);

    assertThat(pointers.get(failedOffset).exceptionClass())
        .isEqualTo(IllegalStateException.class.getName());
    assertThat(pointers.get(failedOffset).exceptionMessage()).isEqualTo(INJECTED);
    // This event met one injected fault, and its pointer does not say so.
    assertThat(pointers.get(openingOffset).exceptionClass())
        .isEqualTo(CallNotPermittedException.class.getName());
    // This event met no fault at all.
    assertThat(pointers.get(healthyOffset).exceptionClass())
        .isEqualTo(CallNotPermittedException.class.getName());

    assertThat(faults.pending()).isEqualTo(Faults.none());
    assertThat(circuitBreakerRegistry.circuitBreaker("languagePreferenceConsumer").getState())
        .isEqualTo(CircuitBreaker.State.OPEN);
    // Nothing was processed and nothing was dropped: every event has a pointer instead.
    assertThat(ledger.summary().processings()).isZero();
    assertThat(ledger.summary().dropped()).isEmpty();
  }

  /** Publishes an event and returns the offset of its record in the topic's one partition. */
  private long publish(String customerId) throws Exception {
    var event = new LanguagePreference(customerId, Locale.FRENCH);
    return producer.publish(event).get(10, TimeUnit.SECONDS).getRecordMetadata().offset();
  }

  /** Reads dead-letter pointers until there is one for each of the expected source offsets. */
  private Map<Long, DeadLetterPointer> readDeadLetters(int expected) {
    var props = KafkaTestUtils.consumerProps(broker, "open-circuit-test", true);
    Map<Long, DeadLetterPointer> bySourceOffset = new HashMap<>();
    try (var deadLetters =
        new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JacksonJsonDeserializer<>(DeadLetterPointer.class, false))) {
      broker.consumeFromAnEmbeddedTopic(deadLetters, deadLetterTopic);
      long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (bySourceOffset.size() < expected && System.nanoTime() < deadline) {
        for (var record : deadLetters.poll(Duration.ofMillis(500))) {
          bySourceOffset.put(record.value().offset(), record.value());
        }
      }
    }
    assertThat(bySourceOffset).hasSize(expected);
    return bySourceOffset;
  }
}
