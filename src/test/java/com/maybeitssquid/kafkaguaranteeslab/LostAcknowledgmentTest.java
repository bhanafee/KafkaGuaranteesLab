package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.startApp;
import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.startBrokerBehind;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * The one difference between stage 1 and stage 2 of the lab: what happens when the broker writes a
 * batch and its acknowledgment is lost. Kafka's producer sends the batch again. Without idempotence
 * the broker appends it a second time. With idempotence the broker recognises the sequence numbers
 * and drops the repeat.
 *
 * <p>The broker advertises the port of a {@link LostAcknowledgmentProxy}, so every client
 * connection passes through it. Both stages run without Resilience4j, so the only retry is Kafka's
 * own, inside one call to {@code send()}.
 */
class LostAcknowledgmentTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static LostAcknowledgmentProxy proxy;
  private static EmbeddedKafkaBroker broker;
  private static String bootstrapServers;

  @BeforeAll
  static void startBrokerBehindProxy() throws Exception {
    proxy = new LostAcknowledgmentProxy();
    broker = startBrokerBehind(proxy);
    bootstrapServers = "localhost:" + proxy.port();
  }

  @AfterAll
  static void stopBrokerAndProxy() throws Exception {
    broker.destroy();
    proxy.close();
  }

  @Test
  void withoutIdempotence_aBatchWhoseAcknowledgmentIsLost_isWrittenTwice() throws Exception {
    assertThat(processingsAfterALostAcknowledgment("stage1")).isEqualTo(2);
  }

  @Test
  void withIdempotence_aBatchWhoseAcknowledgmentIsLost_isWrittenOnce() throws Exception {
    assertThat(processingsAfterALostAcknowledgment("stage2")).isEqualTo(1);
  }

  /**
   * Publishes one event while the proxy loses the acknowledgment of its batch, and returns how many
   * times the event was processed.
   */
  private static int processingsAfterALostAcknowledgment(String profile) throws Exception {
    try (var app = startApp(bootstrapServers, "--spring.profiles.active=" + profile)) {
      var producer = app.getBean(LanguagePreferenceProducer.class);
      var ledger = app.getBean(ProcessingLedger.class);

      // A first event connects the producer and is processed to the end, ledger entry included,
      // so the next produce request through the proxy is the one under test.
      var first = new LanguagePreference("cust-warm-up-" + profile, Locale.ITALIAN);
      producer.publish(first).get(30, TimeUnit.SECONDS);
      awaitUntil(() -> ledger.processedCount(first.eventId()) == 1, TIMEOUT);

      int droppedBefore = proxy.dropped();
      var event = new LanguagePreference("cust-lost-ack-" + profile, Locale.FRENCH);
      proxy.dropNextProduceAcknowledgment();

      // The call succeeds: Kafka's own retry gets an acknowledgment for the batch it sent again.
      producer.publish(event).get(30, TimeUnit.SECONDS);
      assertThat(proxy.dropped()).isEqualTo(droppedBefore + 1);

      awaitUntil(() -> ledger.processedCount(event.eventId()) >= 1, TIMEOUT);
      // Leave time for a second record to be consumed, if there is one.
      Thread.sleep(2_000);
      return ledger.processedCount(event.eventId());
    }
  }
}
