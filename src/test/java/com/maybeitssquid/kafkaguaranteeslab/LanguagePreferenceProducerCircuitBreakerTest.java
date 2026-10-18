package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Circuit breaker guarantee: when sends fail, the producer's circuit opens, and later publishes
 * fail fast through the fallback without reaching {@link KafkaTemplate}.
 *
 * <p>The retry wait is shortened. The attempt count and circuit breaker thresholds still come from
 * application.yml.
 */
@SpringBootTest(
    properties = "resilience4j.retry.instances.languagePreferenceProducer.wait-duration=10ms")
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC})
class LanguagePreferenceProducerCircuitBreakerTest {

  private static final String CIRCUIT_BREAKER = "languagePreferenceProducer";
  private static final int MAX_PUBLISHES = 100;

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;

  @MockitoBean private KafkaTemplate<String, LanguagePreference> kafkaTemplate;

  private CircuitBreaker circuitBreaker;

  @BeforeEach
  void resetCircuitBreaker() {
    // The circuit breaker is a singleton shared by every test in this context.
    circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER);
    circuitBreaker.reset();
  }

  @Test
  void sendFailures_openCircuit_andLaterPublishesFailFastWithoutSending() throws Exception {
    when(kafkaTemplate.send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any()))
        .thenThrow(new IllegalStateException("broker unavailable"));

    var event = new LanguagePreference("cust-1", Locale.FRENCH);
    int publishes = 0;
    while (circuitBreaker.getState() != CircuitBreaker.State.OPEN && publishes < MAX_PUBLISHES) {
      producer.publish(event);
      publishes++;
    }
    assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

    clearInvocations(kafkaTemplate);
    var future = producer.publish(event);

    assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(CallNotPermittedException.class);
    verify(kafkaTemplate, never()).send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any());
  }

  @Test
  void asyncSendFailures_areRetried_andReachTheCallerAfterRetriesAreExhausted() throws Exception {
    // Resilience4j decorates CompletionStage return values, so a failed future is retried like a
    // thrown exception. The caller still receives the broker failure once retries are exhausted.
    when(kafkaTemplate.send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any()))
        .thenReturn(CompletableFuture.failedFuture(new KafkaException("async broker failure")));

    var future = producer.publish(new LanguagePreference("cust-1", Locale.FRENCH));

    assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(KafkaException.class);
    // Resilience4j max-attempts counts total calls, so 3 means 3 sends with no extra retries.
    verify(kafkaTemplate, times(3)).send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any());
  }
}
