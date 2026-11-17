package com.maybeitssquid.kafkaguaranteeslab;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceConsumer;
import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceProcessor;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Tombstones: a record with a null value is acknowledged without being processed, so it neither
 * fails nor ends up in the dead-letter topic.
 *
 * <p>Calls {@code onMessage} directly rather than through the listener container.
 */
class LanguagePreferenceConsumerTombstoneTest {

  @Test
  void tombstone_isAcknowledged_withoutBeingProcessed() {
    var processor = mock(LanguagePreferenceProcessor.class);
    var consumer = new LanguagePreferenceConsumer(processor, new FaultInjector());
    var tombstone =
        new ConsumerRecord<String, LanguagePreference>(
            LanguagePreferenceProducer.TOPIC, 0, 0L, "cust-1", null);
    var ack = mock(Acknowledgment.class);

    consumer.onMessage(tombstone, ack);

    verify(processor, never()).process(any());
    verify(ack).acknowledge();
  }
}
