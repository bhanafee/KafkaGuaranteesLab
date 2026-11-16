package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * The controller responds once the publish outcome is known: 202 Accepted when the broker
 * acknowledges the record, and 503 Service Unavailable when the publish fails.
 *
 * <p>Calls the endpoint over HTTP on a random port, so no web-test module is needed.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "resilience4j.retry.instances.languagePreferenceProducer.wait-duration=10ms")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC})
class LanguagePreferenceControllerTest {

  @LocalServerPort private int port;

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @MockitoBean private KafkaTemplate<String, LanguagePreference> kafkaTemplate;

  @Test
  void publish_returnsAccepted_whenTheBrokerAcknowledgesTheSend() throws Exception {
    var event = new LanguagePreference("cust-1", Locale.FRENCH);
    var record = new ProducerRecord<>(LanguagePreferenceProducer.TOPIC, event.customerId(), event);
    var metadata =
        new RecordMetadata(
            new TopicPartition(LanguagePreferenceProducer.TOPIC, 0), 0L, 0, 0L, 0, 0);
    when(kafkaTemplate.send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any()))
        .thenReturn(CompletableFuture.completedFuture(new SendResult<>(record, metadata)));

    HttpResponse<String> response = post();

    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).isEqualTo("Event queued: cust-1");
  }

  @Test
  void publish_returnsServiceUnavailable_whenTheBrokerRejectsEverySend() throws Exception {
    when(kafkaTemplate.send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any()))
        .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker unavailable")));

    HttpResponse<String> response = post();

    assertThat(response.statusCode()).isEqualTo(503);
    assertThat(response.body()).isEqualTo("Event not published: cust-1");
    // The response waits for the retries, so all three sends have happened by now.
    verify(kafkaTemplate, times(3)).send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any());
  }

  private HttpResponse<String> post() throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/language-preferences"))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "{\"customerId\":\"cust-1\",\"preferredLanguage\":\"fr\"}"))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
