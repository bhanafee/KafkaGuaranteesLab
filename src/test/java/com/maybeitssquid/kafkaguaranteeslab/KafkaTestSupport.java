package com.maybeitssquid.kafkaguaranteeslab;

import static org.junit.jupiter.api.Assertions.fail;

import com.maybeitssquid.kafkaguaranteeslab.model.DeadLetterPointer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/** Helpers shared by the tests that run against the embedded broker. */
final class KafkaTestSupport {

  private KafkaTestSupport() {}

  /** Polls a condition until it holds, and fails the test if it does not hold in time. */
  static void awaitUntil(Callable<Boolean> condition, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.call()) {
      if (System.nanoTime() > deadline) {
        fail("condition not met within " + timeout);
      }
      Thread.sleep(50);
    }
  }

  /**
   * Starts an instance of the application against a broker that outlives it, and waits until its
   * consumer has partitions. An instance that is closed while its consumer is still joining stays
   * in the group as a pending member, and the next rebalance waits most of a minute for it.
   */
  static ConfigurableApplicationContext startApp(String bootstrapServers, String... extraArgs)
      throws Exception {
    List<String> args = new ArrayList<>();
    args.add("--spring.kafka.bootstrap-servers=" + bootstrapServers);
    args.add("--server.port=0");
    args.add("--lab.consumer.redelivery.interval-ms=100");
    args.addAll(List.of(extraArgs));
    var app =
        new SpringApplicationBuilder(KafkaGuaranteesLabApplication.class)
            .run(args.toArray(String[]::new));
    awaitUntil(() -> hasPartitions(app), Duration.ofSeconds(60));
    return app;
  }

  /** Returns whether the instance's listener container has been assigned partitions. */
  static boolean hasPartitions(ConfigurableApplicationContext app) {
    return app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers().stream()
        .allMatch(container -> {
          var assigned = container.getAssignedPartitions();
          return assigned != null && !assigned.isEmpty();
        });
  }

  /**
   * Returns the sum of a group's committed offsets over all partitions of a topic, or -1 if the
   * group has committed nothing for it. With one partition this is that partition's offset.
   */
  static long committedOffset(EmbeddedKafkaBroker broker, String groupId, String topic)
      throws Exception {
    var config =
        Map.<String, Object>of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    try (var admin = AdminClient.create(config)) {
      var offsets =
          admin
              .listConsumerGroupOffsets(groupId)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS);
      var committed =
          offsets.entrySet().stream()
              .filter(entry -> entry.getKey().topic().equals(topic) && entry.getValue() != null)
              .mapToLong(entry -> entry.getValue().offset())
              .toArray();
      return committed.length == 0 ? -1 : java.util.Arrays.stream(committed).sum();
    }
  }

  /** Reads the single record expected in the dead-letter topic. */
  static ConsumerRecord<String, DeadLetterPointer> readDeadLetter(
      EmbeddedKafkaBroker broker, String deadLetterTopic) {
    var props = KafkaTestUtils.consumerProps(broker, "dead-letter-test", true);
    try (var deadLetters =
        new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JacksonJsonDeserializer<>(DeadLetterPointer.class, false))) {
      broker.consumeFromAnEmbeddedTopic(deadLetters, deadLetterTopic);
      return KafkaTestUtils.getSingleRecord(deadLetters, deadLetterTopic, Duration.ofSeconds(15));
    }
  }
}
