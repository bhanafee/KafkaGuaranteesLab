package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceConsumer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.util.Locale;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

/**
 * At-least-once duplicates: when the offset commit is lost after {@code process()} succeeds, the
 * record is delivered again and processed a second time.
 *
 * <p>Calls {@code onMessage} directly rather than through the listener container. Embedded Kafka
 * gives no way to fail the container's commit, so this shows the consumer's behaviour but not the
 * container's.
 */
class LanguagePreferenceConsumerLostAckTest {

  @Test
  void lostAck_redeliversRecord_andProcessesItAgain() {
    var consumer = spy(new LanguagePreferenceConsumer());
    var event = new LanguagePreference("cust-1", Locale.FRENCH);
    var record = new ConsumerRecord<>(LanguagePreferenceProducer.TOPIC, 0, 0L, "cust-1", event);
    Acknowledgment lostAck =
        () -> {
          throw new IllegalStateException("offset commit lost");
        };
    Acknowledgment ack = () -> {};

    // process() succeeds, but the commit fails, so the exception propagates and the error handler
    // redelivers the record.
    assertThatThrownBy(() -> consumer.onMessage(record, lostAck))
        .isInstanceOf(IllegalStateException.class);
    consumer.onMessage(record, ack);

    verify(consumer, times(2)).process(event);
  }
}
