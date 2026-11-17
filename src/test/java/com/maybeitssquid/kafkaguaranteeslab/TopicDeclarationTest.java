package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.startApp;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * The application creates its own topics, so it runs on a broker that was not prepared for it.
 *
 * <p>Each test starts a broker without topics. The broker does not create topics on first use, so a
 * topic that exists was created by the application's declaration or by the test.
 */
class TopicDeclarationTest {

  private static final String EVENTS = LanguagePreferenceProducer.TOPIC;
  private static final String DEAD_LETTERS = "dead-letters";
  private static final String LEDGER = "lab-ledger";

  private EmbeddedKafkaBroker broker;

  @BeforeEach
  void startBrokerWithoutTopics() {
    broker =
        new EmbeddedKafkaKraftBroker(1, 1).brokerProperty("auto.create.topics.enable", "false");
    broker.afterPropertiesSet();
  }

  @AfterEach
  void stopBroker() {
    broker.destroy();
  }

  @Test
  void onABrokerWithoutTopics_theApplicationCreatesThem() throws Exception {
    assertThat(existingTopics()).doesNotContain(EVENTS, DEAD_LETTERS, LEDGER);

    try (var app = startApp(broker.getBrokersAsString())) {
      assertThat(partitions(EVENTS)).isEqualTo(3);
      assertThat(partitions(DEAD_LETTERS)).isEqualTo(3);
      assertThat(partitions(LEDGER)).isEqualTo(1);
    }
  }

  /**
   * The tests that assert on one partition's offset create their topics with one partition and set
   * {@code lab.topics.partitions=1}. This shows that the setting keeps such a topic as it is, and
   * that without it the application adds partitions.
   */
  @Test
  void aTopicWithFewerPartitions_isGrownToTheSetting() throws Exception {
    broker.addTopics(new NewTopic(EVENTS, 1, (short) 1), new NewTopic(DEAD_LETTERS, 1, (short) 1));

    try (var app = startApp(broker.getBrokersAsString(), "--lab.topics.partitions=1")) {
      assertThat(partitions(EVENTS)).isEqualTo(1);
      assertThat(partitions(DEAD_LETTERS)).isEqualTo(1);
    }

    try (var app = startApp(broker.getBrokersAsString())) {
      assertThat(partitions(EVENTS)).isEqualTo(3);
      assertThat(partitions(DEAD_LETTERS)).isEqualTo(3);
    }
  }

  private Set<String> existingTopics() throws Exception {
    try (var admin = admin()) {
      return admin.listTopics().names().get(30, TimeUnit.SECONDS);
    }
  }

  private int partitions(String topic) throws Exception {
    try (var admin = admin()) {
      return admin
          .describeTopics(List.of(topic))
          .allTopicNames()
          .get(30, TimeUnit.SECONDS)
          .get(topic)
          .partitions()
          .size();
    }
  }

  private AdminClient admin() {
    return AdminClient.create(
        Map.<String, Object>of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
  }
}
