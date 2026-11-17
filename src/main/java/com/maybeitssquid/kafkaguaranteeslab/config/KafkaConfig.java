package com.maybeitssquid.kafkaguaranteeslab.config;

import com.maybeitssquid.kafkaguaranteeslab.consumer.DeadLetterPointerRecoverer;
import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${lab.consumer.redelivery.interval-ms}")
  private long retryIntervalMs;

  @Value("${lab.consumer.redelivery.max-attempts}")
  private long retryMaxAttempts;

  @Value("${lab.dead-letter.topic}")
  private String deadLetterTopic;

  @Value("${lab.dead-letter.send-timeout-ms}")
  private long deadLetterSendTimeoutMs;

  @Value("${lab.producer.idempotence}")
  private boolean idempotence;

  // ── Producer ────────────────────────────────────────────────────────────

  private Map<String, Object> producerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
    // At-least-once delivery: wait for all in-sync replicas to ack
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    // On in the lab's normal configuration. The stage1 profile turns it off to show what it adds.
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    return props;
  }

  @Bean
  public ProducerFactory<String, LanguagePreference> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerProps());
  }

  @Bean
  public KafkaTemplate<String, LanguagePreference> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  // ── Dead letters ────────────────────────────────────────────────────────

  // Dead-letter pointers get the same durability settings as the events themselves.
  @Bean
  public ProducerFactory<String, DeadLetterPointer> deadLetterProducerFactory() {
    return new DefaultKafkaProducerFactory<>(producerProps());
  }

  @Bean
  public KafkaTemplate<String, DeadLetterPointer> deadLetterKafkaTemplate() {
    return new KafkaTemplate<>(deadLetterProducerFactory());
  }

  @Bean
  public DeadLetterPointerRecoverer deadLetterPointerRecoverer() {
    return new DeadLetterPointerRecoverer(
        deadLetterKafkaTemplate(), deadLetterTopic, Duration.ofMillis(deadLetterSendTimeoutMs));
  }

  // ── Consumer ────────────────────────────────────────────────────────────

  /**
   * Builds the consumer factory with manual commit and an error-handling value deserializer.
   *
   * <p>The key deserializer is not wrapped, because {@link StringDeserializer} never fails on the
   * record's bytes. It decodes with {@link String#String(byte[], java.nio.charset.Charset)}, which
   * replaces every malformed or unmappable byte sequence with the replacement character U+FFFD. A
   * corrupt key therefore arrives as a garbled string and is not dead-lettered. The decoding is
   * also lossy: two different corrupt keys can decode to the same string. The broker still treats
   * them as different keys, because it partitions and compacts on the raw bytes.
   *
   * <p>This is harmless here, where the key is only logged. A consumer that uses the key for a
   * lookup or a write needs a key deserializer that rejects malformed input, wrapped in {@link
   * ErrorHandlingDeserializer} like the value.
   */
  @Bean
  public ConsumerFactory<String, LanguagePreference> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // A payload that cannot be deserialized must not fail inside poll(), where no record is
    // available to the error handler. ErrorHandlingDeserializer catches the failure and hands the
    // record on, so it gets a dead-letter pointer like any other failed record.
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
    // Disable auto-commit for manual acknowledgment (at-least-once)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(
        JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.maybeitssquid.kafkaguaranteeslab.model");
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, LanguagePreference>
      kafkaListenerContainerFactory() {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, LanguagePreference>();
    factory.setConsumerFactory(consumerFactory());
    // Manual acknowledgment — commit only after successful processing
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    // Redeliver a failed record with a fixed backoff. When the retries are exhausted, write a
    // pointer to the record to the dead-letter topic. If that write fails, the recoverer throws and
    // the record is redelivered, so the record is never skipped without a pointer.
    var errorHandler =
        new DefaultErrorHandler(
            deadLetterPointerRecoverer(), new FixedBackOff(retryIntervalMs, retryMaxAttempts));
    // With a manual ack mode the container does not commit a recovered record on its own. Without
    // this, a dead-lettered record at the end of a partition is redelivered after a restart.
    errorHandler.setCommitRecovered(true);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
  }
}
