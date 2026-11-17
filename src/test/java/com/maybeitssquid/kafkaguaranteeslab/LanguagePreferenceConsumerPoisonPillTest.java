package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.DeadLetterPointerRecoverer;
import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceProcessor;
import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.core.exc.StreamReadException;

/**
 * Poison pill guarantee: a record whose payload cannot be deserialized gets a dead-letter pointer
 * and does not block the records behind it. The pointer does not quote the payload.
 */
@SpringBootTest(properties = "lab.consumer.redelivery.interval-ms=100")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LanguagePreferenceConsumerPoisonPillTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private EmbeddedKafkaBroker broker;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${lab.dead-letter.topic}")
  private String deadLetterTopic;

  @MockitoSpyBean private LanguagePreferenceProcessor processor;

  @Test
  void unreadablePayload_getsPointer_andDoesNotBlockLaterRecords() throws Exception {
    var producerProps = KafkaTestUtils.producerProps(broker);
    try (var raw =
        new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer())) {
      var poison = new ProducerRecord<>(LanguagePreferenceProducer.TOPIC, "cust-0", "not json {");
      poison
          .headers()
          .add("__TypeId__", LanguagePreference.class.getName().getBytes(StandardCharsets.UTF_8));
      raw.send(poison).get(5, TimeUnit.SECONDS);
    }
    var valid = new LanguagePreference("cust-1", Locale.FRENCH);
    producer.publish(valid).get(5, TimeUnit.SECONDS);

    DeadLetterPointer pointer = KafkaTestSupport.readDeadLetter(broker, deadLetterTopic).value();
    assertThat(pointer.topic()).isEqualTo(LanguagePreferenceProducer.TOPIC);
    assertThat(pointer.offset()).isZero();
    assertThat(pointer.consumerGroup()).isEqualTo(groupId);
    // The parser's own message quotes the unreadable payload, so a fixed text replaces it.
    assertThat(pointer.exceptionClass()).isEqualTo(StreamReadException.class.getName());
    assertThat(pointer.exceptionMessage()).isEqualTo(DeadLetterPointerRecoverer.UNREADABLE_PAYLOAD);

    awaitUntil(() -> committedOffset() == 2, Duration.ofSeconds(10));
    // The unreadable record never reaches the listener. The record behind it is processed.
    verify(processor, times(1)).process(any());
    verify(processor).process(valid);
  }

  private long committedOffset() throws Exception {
    return KafkaTestSupport.committedOffset(broker, groupId, LanguagePreferenceProducer.TOPIC);
  }
}
