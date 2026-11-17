package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceProcessor;
import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * Dead-letter guarantee: a record that fails every delivery is not dropped. A pointer to it is
 * written to the shared dead-letter topic, the offset is then committed, and the pointer leads back
 * to the original record in the source topic.
 *
 * <p>A delivery fails only when an exception reaches {@code onMessage}. With the default fallback a
 * processing failure never does, so this test runs with {@code lab.consumer.fallback=rethrow}.
 *
 * <p>The retry intervals are shortened. The attempt counts still come from application.yml. The
 * circuit breaker's window is widened so that the 9 failing calls cannot open the circuit: this
 * test is about dead-lettering, and an open circuit would change how many calls reach {@code
 * process()}.
 */
@SpringBootTest(
    properties = {
      "lab.topics.partitions=1",
      "lab.consumer.fallback=rethrow",
      "lab.consumer.redelivery.interval-ms=100",
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms",
      "resilience4j.circuitbreaker.instances.languagePreferenceConsumer.sliding-window-size=100",
      "resilience4j.circuitbreaker.instances.languagePreferenceConsumer.minimum-number-of-calls=100"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LanguagePreferenceConsumerDeadLetterTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private EmbeddedKafkaBroker broker;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${lab.dead-letter.topic}")
  private String deadLetterTopic;

  @MockitoSpyBean private LanguagePreferenceProcessor processor;

  @Test
  void exhaustedRetries_writePointerToDeadLetterTopic_andCommitOffset() throws Exception {
    doThrow(new IllegalStateException("downstream unavailable")).when(processor).process(any());

    var published = new LanguagePreference("cust-1", Locale.FRENCH);
    producer.publish(published).get(5, TimeUnit.SECONDS);

    var deadLetter = KafkaTestSupport.readDeadLetter(broker, deadLetterTopic);
    assertThat(deadLetter.key()).isEqualTo(LanguagePreferenceProducer.TOPIC + "-0");
    DeadLetterPointer pointer = deadLetter.value();
    assertThat(pointer.topic()).isEqualTo(LanguagePreferenceProducer.TOPIC);
    assertThat(pointer.partition()).isZero();
    assertThat(pointer.offset()).isZero();
    assertThat(pointer.timestamp()).isPositive();
    assertThat(pointer.consumerGroup()).isEqualTo(groupId);
    assertThat(pointer.exceptionClass()).isEqualTo(IllegalStateException.class.getName());
    assertThat(pointer.exceptionMessage()).isEqualTo("downstream unavailable");

    // One delivery plus max-attempts (2) redeliveries, each with 3 Resilience4j attempts.
    verify(processor, times(9)).process(any());
    verify(processor, times(3)).processFallback(any(), any());
    // The failed record is skipped only after its pointer is written.
    awaitUntil(() -> committedOffset() == 1, Duration.ofSeconds(10));

    // The pointer holds no payload. The record is still in the source topic.
    assertThat(readSource(pointer)).isEqualTo(published);
  }

  /** Follows a pointer the way a replay tool would: assign the partition and seek to the offset. */
  private LanguagePreference readSource(DeadLetterPointer pointer) {
    var props = KafkaTestUtils.consumerProps(broker, "replay-test", false);
    var partition = new TopicPartition(pointer.topic(), pointer.partition());
    try (var source =
        new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JacksonJsonDeserializer<>(LanguagePreference.class, false))) {
      source.assign(List.of(partition));
      source.seek(partition, pointer.offset());
      var records = source.poll(Duration.ofSeconds(10)).records(partition);
      assertThat(records).isNotEmpty();
      assertThat(records.get(0).offset()).isEqualTo(pointer.offset());
      return records.get(0).value();
    }
  }

  private long committedOffset() throws Exception {
    return KafkaTestSupport.committedOffset(broker, groupId, LanguagePreferenceProducer.TOPIC);
  }
}
