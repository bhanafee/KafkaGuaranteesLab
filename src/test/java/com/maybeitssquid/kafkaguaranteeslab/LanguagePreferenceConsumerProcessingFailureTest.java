package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceConsumer;
import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceProcessor;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * What happens when {@code LanguagePreferenceProcessor.process()} fails, in three stages: the
 * Resilience4j retry repeats the call within one delivery, the fallback absorbs a failure that
 * outlasts the retries, and only a failing fallback makes the listener container redeliver.
 *
 * <p>The retry intervals are shortened. The attempt counts still come from application.yml.
 */
@SpringBootTest(
    properties = {
      "lab.topics.partitions=1",
      "lab.consumer.redelivery.interval-ms=100",
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LanguagePreferenceConsumerProcessingFailureTest {

  private static final String CIRCUIT_BREAKER = "languagePreferenceConsumer";

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private EmbeddedKafkaBroker broker;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @MockitoSpyBean private LanguagePreferenceConsumer consumer;
  @MockitoSpyBean private LanguagePreferenceProcessor processor;

  @Test
  void transientFailure_isRetriedWithinOneDelivery() throws Exception {
    failProcessTimes(1);
    // Stubbing the spy goes through the proxy and counts as a call, so measure from here.
    var circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER);
    int failedBefore = circuitBreaker.getMetrics().getNumberOfFailedCalls();
    int successfulBefore = circuitBreaker.getMetrics().getNumberOfSuccessfulCalls();

    producer.publish(new LanguagePreference("cust-1", Locale.FRENCH)).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> committedOffset() == 1, Duration.ofSeconds(10));
    verify(processor, times(2)).process(any());
    verify(processor, never()).processFallback(any(), any());
    verify(consumer, times(1)).onMessage(any(), any());
    // The circuit breaker saw both attempts, so the annotations on process() are in effect.
    var metrics = circuitBreaker.getMetrics();
    assertThat(metrics.getNumberOfFailedCalls() - failedBefore).isEqualTo(1);
    assertThat(metrics.getNumberOfSuccessfulCalls() - successfulBefore).isEqualTo(1);
  }

  @Test
  void exhaustedRetries_runFallback_andRecordIsAcknowledgedUnprocessed() throws Exception {
    failProcessTimes(Integer.MAX_VALUE);

    producer.publish(new LanguagePreference("cust-1", Locale.FRENCH)).get(5, TimeUnit.SECONDS);

    // The fallback does not throw, so onMessage acknowledges the record and it is not redelivered.
    awaitUntil(() -> committedOffset() == 1, Duration.ofSeconds(10));
    verify(processor, times(3)).process(any());
    verify(processor, times(1)).processFallback(any(), any());
    verify(consumer, times(1)).onMessage(any(), any());
  }

  @Test
  void failingFallback_redeliversRecord_andCommitsOffsetAfterSuccess() throws Exception {
    // The first delivery fails all 3 attempts and its fallback throws. The second succeeds.
    failProcessTimes(3);
    doThrow(new IllegalStateException("fallback failed"))
        .when(processor)
        .processFallback(any(), any());

    var published = new LanguagePreference("cust-1", Locale.FRENCH);
    producer.publish(published).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> committedOffset() == 1, Duration.ofSeconds(10));
    verify(consumer, times(2)).onMessage(any(), any());
    var received = ArgumentCaptor.forClass(LanguagePreference.class);
    verify(processor, times(4)).process(received.capture());
    assertThat(received.getAllValues()).containsOnly(published);
  }

  private void failProcessTimes(int failures) {
    var calls = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (calls.incrementAndGet() <= failures) {
                throw new IllegalStateException("downstream unavailable");
              }
              return invocation.callRealMethod();
            })
        .when(processor)
        .process(any());
  }

  private long committedOffset() throws Exception {
    return KafkaTestSupport.committedOffset(broker, groupId, LanguagePreferenceProducer.TOPIC);
  }
}
