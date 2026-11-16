package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;

/**
 * Recovers a record that exhausted its delivery retries by writing a {@link DeadLetterPointer} to
 * the shared dead-letter topic. The pointer names the record's topic, partition and offset. The
 * payload is not copied.
 *
 * <p>The recoverer reads only the record's coordinates, so one instance and one dead-letter topic
 * serve every source topic and payload type.
 *
 * <p>The send blocks until the broker acknowledges the pointer. If it fails or times out, this
 * recoverer throws, the error handler treats the recovery as failed, and the source record is
 * delivered again. The source offset is therefore never committed without a pointer. A send that
 * times out here may still succeed later, so the same record can get more than one pointer.
 */
public class DeadLetterPointerRecoverer implements ConsumerAwareRecordRecoverer {

  private static final Logger log = LoggerFactory.getLogger(DeadLetterPointerRecoverer.class);

  private final KafkaOperations<String, DeadLetterPointer> template;
  private final String deadLetterTopic;
  private final Duration sendTimeout;

  public DeadLetterPointerRecoverer(
      KafkaOperations<String, DeadLetterPointer> template,
      String deadLetterTopic,
      Duration sendTimeout) {
    this.template = template;
    this.deadLetterTopic = deadLetterTopic;
    this.sendTimeout = sendTimeout;
  }

  @Override
  public void accept(
      ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, Exception exception) {
    Throwable cause = NestedExceptionUtils.getMostSpecificCause(exception);
    var pointer =
        new DeadLetterPointer(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            consumer == null ? null : consumer.groupMetadata().groupId(),
            cause.getClass().getName(),
            cause.getMessage());
    // Keyed by source partition, so pointers into one partition stay in offset order.
    String key = record.topic() + "-" + record.partition();
    try {
      template
          .send(deadLetterTopic, key, pointer)
          .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new KafkaException("Interrupted writing dead-letter pointer " + pointer, ex);
    } catch (ExecutionException | TimeoutException ex) {
      throw new KafkaException("Failed to write dead-letter pointer " + pointer, ex);
    }
    log.warn("Dead-lettered record: {}", pointer);
  }
}
