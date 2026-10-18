package com.maybeitssquid.kafkaguaranteeslab;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.maybeitssquid.kafkaguaranteeslab.consumer.LanguagePreferenceConsumer;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * Redelivery guarantee: when {@code process()} fails, the record is delivered again, and the offset
 * is committed only after a successful attempt.
 *
 * <p>The retry interval is shortened. The attempt count (max-attempts 2) still comes from
 * application.yml.
 */
@SpringBootTest(properties = "spring.kafka.consumer.retry.interval-ms=100")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC})
class LanguagePreferenceConsumerRedeliveryTest {

  @Autowired private LanguagePreferenceProducer producer;
  @Autowired private EmbeddedKafkaBroker broker;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @MockitoSpyBean private LanguagePreferenceConsumer consumer;

  @Test
  void processFailure_redeliversRecord_andCommitsOffsetAfterSuccess() throws Exception {
    var calls = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("downstream unavailable");
              }
              return invocation.callRealMethod();
            })
        .when(consumer)
        .process(any());

    producer.publish(new LanguagePreference("cust-1", Locale.FRENCH)).get(5, TimeUnit.SECONDS);

    awaitUntil(() -> committedOffset() == 1, Duration.ofSeconds(10));
    verify(consumer, times(2)).process(any());
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
