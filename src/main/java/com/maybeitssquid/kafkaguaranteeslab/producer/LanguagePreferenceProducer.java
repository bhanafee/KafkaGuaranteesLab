package com.maybeitssquid.kafkaguaranteeslab.producer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class LanguagePreferenceProducer {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceProducer.class);
  public static final String TOPIC = "language-preferences";

  private final KafkaTemplate<String, LanguagePreference> kafkaTemplate;

  public LanguagePreferenceProducer(KafkaTemplate<String, LanguagePreference> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  // The fallback belongs to @Retry, the outer of the two, so it runs once per publish and not once
  // per attempt.
  @Retry(name = "languagePreferenceProducer", fallbackMethod = "publishFallback")
  @CircuitBreaker(name = "languagePreferenceProducer")
  public CompletableFuture<SendResult<String, LanguagePreference>> publish(
      LanguagePreference event) {
    log.info("Publishing event: customerId={}", event.customerId());
    return kafkaTemplate
        .send(TOPIC, event.customerId(), event)
        .whenComplete((result, ex) -> {
          if (ex != null) {
            log.error("Failed to publish event: customerId={}", event.customerId(), ex);
          } else {
            log.info(
                "Published event: customerId={} partition={} offset={}",
                event.customerId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
          }
        });
  }

  // Called by Resilience4j once a publish has finally failed: after the last retry attempt, or at
  // once when the circuit is open, because the retry ignores CallNotPermittedException. The event
  // is not stored anywhere: the failed future tells the caller it was not sent.
  public CompletableFuture<SendResult<String, LanguagePreference>> publishFallback(
      LanguagePreference event, Throwable t) {
    log.warn(
        "Publish failed — event not sent: customerId={} reason={}",
        event.customerId(),
        t.getMessage());
    return CompletableFuture.failedFuture(t);
  }
}
