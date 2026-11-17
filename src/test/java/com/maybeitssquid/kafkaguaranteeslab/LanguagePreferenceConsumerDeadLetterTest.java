package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * <p>A delivery fails only when an exception reaches {@code onMessage}. The processor's fallback
 * normally absorbs a processing failure, so here the fallback is made to throw as well.
 *
 * <p>The retry intervals are shortened. The attempt counts still come from application.yml.
 */
@SpringBootTest(
    properties = {
      "spring.kafka.consumer.retry.interval-ms=100",
      "resilience4j.retry.instances.languagePreferenceConsumer.wait-duration=10ms"
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

  @Value("${spring.kafka.consumer.dead-letter.topic}")
  private String deadLetterTopic;

  @MockitoSpyBean private LanguagePreferenceProcessor processor;

  @Test
  void exhaustedRetries_writePointerToDeadLetterTopic_andCommitOffset() throws Exception {
    doThrow(new IllegalStateException("downstream unavailable")).when(processor).process(any());
    doThrow(new IllegalStateException("downstream unavailable"))
        .when(processor)
        .processFallback(any(), any());

    var published = new LanguagePreference("cust-1", Locale.FRENCH);
    producer.publish(published).get(5, TimeUnit.SECONDS);

    DeadLetterPointer pointer = readPointer();
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

  private DeadLetterPointer readPointer() {
    var props = KafkaTestUtils.consumerProps(broker, "dead-letter-test", true);
    try (var deadLetters =
        new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JacksonJsonDeserializer<>(DeadLetterPointer.class, false))) {
      broker.consumeFromAnEmbeddedTopic(deadLetters, deadLetterTopic);
      ConsumerRecord<String, DeadLetterPointer> record =
          KafkaTestUtils.getSingleRecord(deadLetters, deadLetterTopic, Duration.ofSeconds(15));
      assertThat(record.key()).isEqualTo(LanguagePreferenceProducer.TOPIC + "-0");
      return record.value();
    }
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
    var config =
        Map.<String, Object>of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    try (var admin = AdminClient.create(config)) {
      var offsets =
          admin
              .listConsumerGroupOffsets(groupId)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS);
      var partition = new TopicPartition(LanguagePreferenceProducer.TOPIC, 0);
      return offsets.containsKey(partition) ? offsets.get(partition).offset() : -1;
    }
  }

  private static void awaitUntil(Callable<Boolean> condition, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.call()) {
      if (System.nanoTime() > deadline) {
        fail("condition not met within " + timeout);
      }
      Thread.sleep(50);
    }
  }
}
