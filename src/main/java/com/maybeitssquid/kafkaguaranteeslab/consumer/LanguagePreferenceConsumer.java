package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class LanguagePreferenceConsumer {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceConsumer.class);

  /**
   * Consumes language preference events with manual acknowledgment. The offset is committed only
   * after process() succeeds, guaranteeing at-least-once delivery. Resilience4j retries wrap the
   * downstream call.
   */
  @KafkaListener(
      topics = "language-preferences",
      groupId = "${spring.kafka.consumer.group-id}",
      containerFactory = "kafkaListenerContainerFactory")
  public void onMessage(ConsumerRecord<String, LanguagePreference> record, Acknowledgment ack) {
    LanguagePreference event = record.value();
    log.info(
        "Received event: customerId={} partition={} offset={}",
        event.customerId(),
        record.partition(),
        record.offset());
    try {
      process(event);
      ack.acknowledge(); // commit offset only on success
    } catch (Exception ex) {
      log.error(
          "Processing failed — will be retried by error handler: customerId={}",
          event.customerId(),
          ex);
      // Do NOT ack; DefaultErrorHandler in KafkaConfig will retry then route to DLT
      throw ex;
    }
  }

  @Retry(name = "languagePreferenceConsumer")
  @CircuitBreaker(name = "languagePreferenceConsumer")
  public void process(LanguagePreference event) {
    // TODO: replace with real downstream call (DB write, HTTP call, etc.)
    log.info(
        "Processing language preference: customerId={} locale={}",
        event.customerId(),
        event.preferredLanguage());
  }
}
