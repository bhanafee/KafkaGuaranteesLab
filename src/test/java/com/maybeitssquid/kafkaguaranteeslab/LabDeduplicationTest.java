package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * What closes the duplicate gap: with {@code lab.consumer.deduplicate=true} the consumer recognises
 * an event by its own id, so a duplicate that Kafka's idempotent producer let through is processed
 * once. It does not close the ordering gap: an event that is written late is still processed late.
 *
 * <p>The producer's retry waits 1 s, long enough for a second publish to finish in between.
 */
@SpringBootTest(
    properties = {
      "lab.consumer.deduplicate=true",
      "resilience4j.retry.instances.languagePreferenceProducer.wait-duration=1s"
    })
@DirtiesContext
@EmbeddedKafka(
    partitions = 3,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LabDeduplicationTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private ProcessingLedger ledger;
  @Autowired private FaultInjector faults;

  @Test
  void applicationRetry_afterAnAmbiguousSend_isProcessedOnce() throws Exception {
    faults.set(Faults.none().withAmbiguousSends(1));
    var event = new LanguagePreference("cust-1", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    // Both records reach the consumer. The second is recognised and skipped.
    awaitUntil(() -> ledger.summary().skippedDuplicates() == 1, Duration.ofSeconds(15));
    assertThat(ledger.processedCount(event.eventId())).isEqualTo(1);
    assertThat(ledger.summary().duplicates()).isEmpty();
  }

  @Test
  void applicationRetry_thatLandsBehindANewerEvent_overwritesIt_despiteDeduplication()
      throws Exception {
    // A first publish makes the producer fetch its metadata, so the ones below are quick.
    producer
        .publish(new LanguagePreference("cust-warm-up", Locale.ITALIAN))
        .get(5, TimeUnit.SECONDS);

    // The first attempt of the older event fails with nothing written. While the retry waits, a
    // newer event of the same customer is published. The retry then writes the older one behind
    // it. Each event is processed once, so there is no repeat for the consumer to recognise.
    faults.set(Faults.none().withFailedSends(1));
    var older = new LanguagePreference("cust-reorder", Locale.FRENCH);
    var newer = new LanguagePreference("cust-reorder", Locale.GERMAN);

    var retried = producer.publish(older);
    producer.publish(newer).get(5, TimeUnit.SECONDS);
    retried.get(5, TimeUnit.SECONDS);

    awaitUntil(() -> ledger.eventIdsFor("cust-reorder").size() == 2, Duration.ofSeconds(15));
    assertThat(ledger.eventIdsFor("cust-reorder"))
        .containsExactly(newer.eventId(), older.eventId());
    assertThat(ledger.preferenceOf("cust-reorder")).isEqualTo("fr");
  }
}
