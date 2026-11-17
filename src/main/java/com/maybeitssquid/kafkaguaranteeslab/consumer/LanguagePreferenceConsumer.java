package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class LanguagePreferenceConsumer {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceConsumer.class);

  private final LanguagePreferenceProcessor processor;

  public LanguagePreferenceConsumer(LanguagePreferenceProcessor processor) {
    this.processor = processor;
  }

  /**
   * Consumes language preference events with manual acknowledgment. The offset is committed only
   * after {@link LanguagePreferenceProcessor#process} returns, or after a pointer to the failed
   * record is written to the dead-letter topic.
   *
   * <p>Processing is delegated to a separate bean so that its Resilience4j retry and circuit
   * breaker apply. It runs on this thread, in this call stack. The processor's fallback absorbs a
   * failure that outlasts the retries, so an exception reaches this handler only if the fallback
   * throws too.
   *
   * <p>A record with a null value (a tombstone) is acknowledged without being processed.
   */
  @KafkaListener(
      topics = "language-preferences",
      groupId = "${spring.kafka.consumer.group-id}",
      containerFactory = "kafkaListenerContainerFactory")
  public void onMessage(ConsumerRecord<String, LanguagePreference> record, Acknowledgment ack) {
    LanguagePreference event = record.value();
    if (event == null) {
      // A record with a null value is a tombstone. It carries no preference to process, so
      // acknowledge it and move on rather than fail and dead-letter it.
      log.info(
          "Skipping tombstone: key={} partition={} offset={}",
          record.key(),
          record.partition(),
          record.offset());
      ack.acknowledge();
      return;
    }
    log.info(
        "Received event: customerId={} partition={} offset={}",
        event.customerId(),
        record.partition(),
        record.offset());
    try {
      processor.process(event);
      ack.acknowledge(); // commit offset only once processing has returned
    } catch (Exception ex) {
      log.error(
          "Processing failed — will be retried by error handler: customerId={}",
          event.customerId(),
          ex);
      // Do NOT ack. DefaultErrorHandler in KafkaConfig redelivers the record. Once its retries are
      // exhausted, DeadLetterPointerRecoverer writes a pointer to the record (not a copy) to the
      // shared dead-letter topic, and only then is the offset committed.
      throw ex;
    }
  }
}
