package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import tools.jackson.core.exc.StreamReadException;

/**
 * Poison pill guarantee: a record whose payload cannot be deserialized gets a dead-letter pointer
 * and does not block the records behind it. The pointer does not quote the payload.
 */
@SpringBootTest(properties = "spring.kafka.consumer.retry.interval-ms=100")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LanguagePreferenceConsumerPoisonPillTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private EmbeddedKafkaBroker broker;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${spring.kafka.consumer.dead-letter.topic}")
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

    DeadLetterPointer pointer = readPointer();
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
      return record.value();
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
