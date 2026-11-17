package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.maybeitssquid.kafkaguaranteeslab.lab.Crasher;
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * The scenarios that need the application to stop, or to run twice. One broker outlives the
 * application instances that the tests start and stop, so the topics, the committed offsets and the
 * ledger's journal carry over from one instance to the next.
 *
 * <p>A test cannot halt its own JVM. In place of the halt, the injected crash throws an {@link
 * Error} on the thread concerned. On the listener thread that stops the container with the record
 * unacknowledged, which is the state a real crash leaves behind.
 */
class LabRestartTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private static EmbeddedKafkaBroker broker;

  @BeforeAll
  static void startBroker() {
    broker = new EmbeddedKafkaKraftBroker(1, 3, LanguagePreferenceProducer.TOPIC, "dead-letters");
    broker.afterPropertiesSet();
  }

  @AfterAll
  static void stopBroker() {
    broker.destroy();
  }

  /** Thrown in place of halting the JVM. */
  private static final class SimulatedCrash extends Error {
    SimulatedCrash(String reason) {
      super(reason);
    }
  }

  private static ConfigurableApplicationContext startApp(String... extraArgs) {
    List<String> args = new ArrayList<>();
    args.add("--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString());
    args.add("--server.port=0");
    args.add("--lab.consumer.redelivery.interval-ms=100");
    args.addAll(List.of(extraArgs));
    return new SpringApplicationBuilder(KafkaGuaranteesLabApplication.class)
        .run(args.toArray(String[]::new));
  }

  /** Makes an injected crash end the calling thread, and reports whether one has happened. */
  private static AtomicBoolean simulateCrashes(ConfigurableApplicationContext app) {
    var crashed = new AtomicBoolean();
    app.getBean(Crasher.class)
        .onCrash(reason -> {
          crashed.set(true);
          throw new SimulatedCrash(reason);
        });
    return crashed;
  }

  private static boolean hasPartitions(ConfigurableApplicationContext app) {
    return app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers().stream()
        .allMatch(container -> {
          var assigned = container.getAssignedPartitions();
          return assigned != null && !assigned.isEmpty();
        });
  }

  private static ProcessingLedger ledgerOf(ConfigurableApplicationContext app) {
    return app.getBean(ProcessingLedger.class);
  }

  private static void publish(ConfigurableApplicationContext app, LanguagePreference event)
      throws Exception {
    app.getBean(LanguagePreferenceProducer.class).publish(event).get(10, TimeUnit.SECONDS);
  }

  /**
   * Runs an instance that processes the event and then crashes before committing its offset.
   *
   * @return the ledger's count of skipped repeats when the instance stopped
   */
  private static int processAndCrashBeforeCommit(LanguagePreference event) throws Exception {
    try (var app = startApp()) {
      var crashed = simulateCrashes(app);
      app.getBean(FaultInjector.class).set(Faults.none().withCrashBeforeCommit(1));

      publish(app, event);

      awaitUntil(() -> crashed.get(), TIMEOUT);
      assertThat(ledgerOf(app).processedCount(event.eventId())).isEqualTo(1);
      return ledgerOf(app).summary().skippedDuplicates();
    }
  }

  @Test
  void consumerThatStopsBetweenProcessingAndCommit_processesTheEventAgainAfterRestart()
      throws Exception {
    var event = new LanguagePreference("cust-crash", Locale.FRENCH);
    processAndCrashBeforeCommit(event);

    try (var restarted = startApp()) {
      // The ledger was replayed from its topic, so the first processing is still on record. The
      // offset was never committed, so the record is delivered and processed again.
      awaitUntil(() -> ledgerOf(restarted).processedCount(event.eventId()) == 2, TIMEOUT);
      assertThat(ledgerOf(restarted).summary().duplicates()).containsEntry(event.eventId(), 2);
    }
  }

  @Test
  void deduplication_recognisesAnEventProcessedBeforeTheRestart() throws Exception {
    var event = new LanguagePreference("cust-crash-dedup", Locale.FRENCH);
    int skippedBefore = processAndCrashBeforeCommit(event);

    try (var restarted = startApp("--lab.consumer.deduplicate=true")) {
      var ledger = ledgerOf(restarted);
      // The record is delivered again. Its id is in the replayed ledger, so it is skipped.
      awaitUntil(() -> ledger.summary().skippedDuplicates() == skippedBefore + 1, TIMEOUT);
      assertThat(ledger.processedCount(event.eventId())).isEqualTo(1);
    }
  }

  @Test
  void twoInstances_shareOneLedger_andTheBrokerKeepsTheSameEventFromBoth() throws Exception {
    var event = new LanguagePreference("cust-two", Locale.FRENCH);

    try (var first = startApp();
        var second = startApp()) {
      // Wait for the group to settle with partitions on both instances. Publishing earlier would
      // work too, but an instance that is closed while it is still joining the group takes most
      // of a minute to stop.
      awaitUntil(() -> hasPartitions(first) && hasPartitions(second), TIMEOUT);

      // Each instance has its own producer id, so the broker keeps both records. Whichever
      // instance consumes them, both ledgers show the event processed twice.
      publish(first, event);
      publish(second, event);

      awaitUntil(() -> ledgerOf(first).processedCount(event.eventId()) == 2, TIMEOUT);
      awaitUntil(() -> ledgerOf(second).processedCount(event.eventId()) == 2, TIMEOUT);
    }
  }

  @Test
  void applicationThatStopsMidPublish_sendsNothing() throws Exception {
    var event = new LanguagePreference("cust-unsent", Locale.FRENCH);

    try (var app = startApp()) {
      var crashed = simulateCrashes(app);
      app.getBean(FaultInjector.class).set(Faults.none().withCrashBeforeSend(1));

      Throwable failure = catchThrowable(() -> publish(app, event));

      assertThat(failure).isNotNull();
      assertThat(crashed).isTrue();
    }
    try (var restarted = startApp()) {
      // Nothing was written, so there is nothing to consume after the restart either.
      Thread.sleep(1_000);
      assertThat(ledgerOf(restarted).processedCount(event.eventId())).isZero();
    }
  }
}
