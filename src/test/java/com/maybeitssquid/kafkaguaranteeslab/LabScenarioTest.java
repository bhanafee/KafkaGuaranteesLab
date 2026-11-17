package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

/**
 * The scenarios the lab is about, each run through the real producer, broker and listener
 * container, with the idempotent producer on. The ledger shows what the consumer did with each
 * event.
 *
 * <p>The topic has three partitions, as it does when created by kafka-local.sh. The tests share one
 * context and one broker. Each uses its own events and reads the ledger by event id, so they do not
 * depend on each other or on offsets.
 *
 * <p>The retry and redelivery intervals are shortened. The attempt counts come from
 * application.yml.
 */
@SpringBootTest(
    properties = {
      "lab.consumer.redelivery.interval-ms=100",
      "resilience4j.retry.instances.languagePreferenceProducer.wait-duration=10ms",
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms"
    })
@DirtiesContext
@EmbeddedKafka(
    partitions = 3,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LabScenarioTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(15);

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private ProducerFactory<String, LanguagePreference> producerFactory;
  @Autowired private EmbeddedKafkaBroker broker;
  @Autowired private ProcessingLedger ledger;
  @Autowired private FaultInjector faults;

  @BeforeEach
  void clearFaults() {
    faults.set(new Faults(0, 0, 0));
  }

  @Test
  void idempotenceIsOn() {
    assertThat(producerFactory.getConfigurationProperties())
        .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
  }

  @Test
  void eventSentOnce_isProcessedOnce() throws Exception {
    var event = new LanguagePreference("cust-once", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.processedCount(event.eventId()) == 1, TIMEOUT);
    assertSettledAt(event, 1);
  }

  @Test
  void applicationRetry_afterAnAmbiguousSend_processesTheEventTwice() throws Exception {
    // The first send is written to the topic but reported as failed. The Resilience4j retry sends
    // the event again. To the broker that is a new record with a new sequence number.
    faults.set(new Faults(1, 0, 0));
    var event = new LanguagePreference("cust-retry", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.processedCount(event.eventId()) == 2, TIMEOUT);
    assertThat(ledger.summary().duplicates()).containsEntry(event.eventId(), 2);
  }

  @Test
  void callerSendingTheSameEventAgain_processesItTwice() throws Exception {
    // A caller that got a 503, or lost its connection, sends the same event again.
    var event = new LanguagePreference("cust-caller", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);
    producer.publish(event).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.processedCount(event.eventId()) == 2, TIMEOUT);
  }

  @Test
  void secondProducerInstance_isNotDeduplicatedByTheBroker() throws Exception {
    // A second producer, as after a restart or in a second application instance. It is idempotent
    // too, but it has its own producer id, and the broker deduplicates per producer id.
    var event = new LanguagePreference("cust-second", Locale.FRENCH);
    producer.publish(event).get(5, TimeUnit.SECONDS);

    var props = KafkaTestUtils.producerProps(broker);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    try (var second =
        new KafkaProducer<>(
            props, new StringSerializer(), new JacksonJsonSerializer<LanguagePreference>())) {
      second
          .send(new ProducerRecord<>(LanguagePreferenceProducer.TOPIC, event.customerId(), event))
          .get(5, TimeUnit.SECONDS);
    }

    awaitUntil(() -> ledger.processedCount(event.eventId()) == 2, TIMEOUT);
  }

  @Test
  void lostCommit_redeliversTheRecord_andProcessesItTwice() throws Exception {
    // The event is processed, but its offset is not committed. The container delivers it again.
    faults.set(new Faults(0, 0, 1));
    var event = new LanguagePreference("cust-commit", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.processedCount(event.eventId()) == 2, TIMEOUT);
  }

  @Test
  void processingFailure_thatOutlastsTheRetries_dropsTheEvent() throws Exception {
    // All 3 attempts fail. The default fallback acknowledges the record unprocessed.
    faults.set(new Faults(0, 3, 0));
    var event = new LanguagePreference("cust-drop", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.wasDropped(event.eventId()), TIMEOUT);
    assertSettledAt(event, 0);
  }

  @Test
  void eventsOfOneCustomer_areProcessedInOrder_acrossThreePartitions() throws Exception {
    // Events of several customers are interleaved. Each customer's events share a key and so a
    // partition, where their order is kept. Order between customers is not guaranteed.
    List<String> customers =
        List.of("cust-order-a", "cust-order-b", "cust-order-c", "cust-order-d");
    List<LanguagePreference> published = new ArrayList<>();
    for (int round = 0; round < 5; round++) {
      for (String customer : customers) {
        var event = new LanguagePreference(customer, Locale.FRENCH);
        published.add(event);
        producer.publish(event).get(5, TimeUnit.SECONDS);
      }
    }

    for (String customer : customers) {
      List<String> expected =
          published.stream()
              .filter(event -> event.customerId().equals(customer))
              .map(LanguagePreference::eventId)
              .toList();
      awaitUntil(() -> ledger.eventIdsFor(customer).size() == expected.size(), TIMEOUT);
      assertThat(ledger.eventIdsFor(customer)).containsExactlyElementsOf(expected);
    }
  }

  /** Waits long enough for a redelivery to arrive, then checks the count has not moved. */
  private void assertSettledAt(LanguagePreference event, int expected) throws Exception {
    Thread.sleep(1_000);
    assertThat(ledger.processedCount(event.eventId())).isEqualTo(expected);
  }
}
