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
 * once.
 */
@SpringBootTest(
    properties = {
      "lab.consumer.deduplicate=true",
      "resilience4j.retry.instances.languagePreferenceProducer.wait-duration=10ms"
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
    faults.set(new Faults(1, 0, 0));
    var event = new LanguagePreference("cust-1", Locale.FRENCH);

    producer.publish(event).get(5, TimeUnit.SECONDS);

    // Both records reach the consumer. The second is recognised and skipped.
    awaitUntil(() -> ledger.summary().skippedDuplicates() == 1, Duration.ofSeconds(15));
    assertThat(ledger.processedCount(event.eventId())).isEqualTo(1);
    assertThat(ledger.summary().duplicates()).isEmpty();
  }
}
