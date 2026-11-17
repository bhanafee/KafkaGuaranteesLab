package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * The cost of the consumer's circuit breaker with the default fallback: once injected failures have
 * opened the circuit, the consumer acknowledges every event it reads without processing it,
 * including events that nothing is wrong with.
 *
 * <p>This is the experiment "An open circuit drops healthy events" of the README, with the same
 * fault count. The circuit breaker's window and threshold come from application.yml. The retry wait
 * is shortened. The open state is lengthened so that the circuit cannot leave it by itself while
 * the test runs, and the test moves it to half-open directly.
 *
 * <p>The class has a context of its own, because an open circuit would fail the events of any other
 * test.
 */
@SpringBootTest(
    properties = {
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms",
      "resilience4j.circuitbreaker.instances.languagePreferenceConsumer.wait-duration-in-open-state=1h"
    })
@DirtiesContext
@EmbeddedKafka(
    partitions = 3,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class ConsumerOpenCircuitTest {

  private static final String INSTANCE = "languagePreferenceConsumer";
  private static final Duration WAIT = Duration.ofSeconds(60);

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private ProcessingLedger ledger;
  @Autowired private FaultInjector faults;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;
  @Autowired private RetryRegistry retryRegistry;

  @Test
  void openCircuit_dropsHealthyEvents_untilItLetsCallsThroughAgain() throws Exception {
    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(INSTANCE);

    // Ten failing calls fill the window of 10. The first three events use 3 attempts each and the
    // fourth uses the last one, which opens the circuit. Its second attempt is rejected.
    faults.set(Faults.none().withProcessingFailures(10));
    List<LanguagePreference> failing = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      failing.add(publish("cust-failing-" + i));
    }
    for (LanguagePreference event : failing) {
      awaitUntil(() -> ledger.wasDropped(event.eventId()), WAIT);
    }
    assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    assertThat(faults.pending()).isEqualTo(Faults.none());

    // No fault is pending, so each of these events would be processed if process() were called.
    var retryMetrics = retryRegistry.retry(INSTANCE).getMetrics();
    long notRetriedBefore = retryMetrics.getNumberOfFailedCallsWithoutRetryAttempt();
    long rejectedBefore = circuitBreaker.getMetrics().getNumberOfNotPermittedCalls();
    List<LanguagePreference> healthy = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      healthy.add(publish("cust-healthy-" + i));
    }
    for (LanguagePreference event : healthy) {
      awaitUntil(() -> ledger.wasDropped(event.eventId()), WAIT);
      assertThat(ledger.processedCount(event.eventId())).isZero();
    }
    assertThat(ledger.summary().processings()).isZero();
    assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    // The retry ignores the rejection: each event is rejected once and goes to the fallback.
    assertThat(circuitBreaker.getMetrics().getNumberOfNotPermittedCalls() - rejectedBefore)
        .isEqualTo(3);
    assertThat(retryMetrics.getNumberOfFailedCallsWithoutRetryAttempt() - notRetriedBefore)
        .isEqualTo(3);

    // Once the circuit lets calls through again, the same kind of event is processed. Moving to
    // half-open directly avoids waiting on the open-state timer.
    circuitBreaker.transitionToHalfOpenState();
    var afterwards = publish("cust-afterwards");
    awaitUntil(() -> ledger.processedCount(afterwards.eventId()) == 1, WAIT);
    assertThat(ledger.wasDropped(afterwards.eventId())).isFalse();
    // The dropped events stay dropped. Nothing delivers them again.
    assertThat(ledger.summary().processings()).isEqualTo(1);
  }

  private LanguagePreference publish(String customerId) throws Exception {
    var event = new LanguagePreference(customerId, Locale.FRENCH);
    producer.publish(event).get(10, TimeUnit.SECONDS);
    return event;
  }
}
