package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.lab.Crasher;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
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
  private final FaultInjector faults;
  private final Crasher crasher;

  /**
   * Creates the consumer.
   *
   * @param processor processes each event
   * @param faults supplies injected lost commits and crashes
   * @param crasher stops the application for an injected crash
   */
  public LanguagePreferenceConsumer(
      LanguagePreferenceProcessor processor, FaultInjector faults, Crasher crasher) {
    this.processor = processor;
    this.faults = faults;
    this.crasher = crasher;
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
        "Received event: eventId={} customerId={} partition={} offset={}",
        event.eventId(),
        event.customerId(),
        record.partition(),
        record.offset());
    try {
      processor.process(event);
      if (faults.takeCrashBeforeCommit()) {
        // The event has been processed and the application stops before the commit. The record
        // is delivered again after a restart, or to another instance of the group.
        crasher.crash("event " + event.eventId() + " processed, offset not committed");
      }
      if (faults.takeLostCommit()) {
        // The event has been processed, but the offset is not committed. The record is delivered
        // again, as it would be after a consumer that stopped at this point was restarted.
        throw new IllegalStateException("Injected fault: offset commit lost");
      }
      ack.acknowledge(); // commit offset only once processing has returned
    } catch (Exception ex) {
      log.error(
          "Delivery failed, record not acknowledged — error handler takes over: customerId={}",
          event.customerId(),
          ex);
      // Do NOT ack. DefaultErrorHandler in KafkaConfig redelivers the record. Once its retries are
      // exhausted, DeadLetterPointerRecoverer writes a pointer to the record (not a copy) to the
      // shared dead-letter topic, and only then is the offset committed.
      throw ex;
    }
  }
}
