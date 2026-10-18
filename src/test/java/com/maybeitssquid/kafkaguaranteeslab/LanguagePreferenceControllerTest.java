package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * The controller hands the event to the producer and returns before the send completes. A broker
 * failure therefore never reaches the HTTP caller: the response is 202 Accepted regardless.
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
  void publish_returnsAccepted_evenWhenTheBrokerRejectsEverySend() throws Exception {
    when(kafkaTemplate.send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any()))
        .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker unavailable")));

    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/language-preferences"))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "{\"customerId\":\"cust-1\",\"preferredLanguage\":\"fr\"}"))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).isEqualTo("Event queued: cust-1");
    // The send fails after the response has been returned. Retries run in the background.
    verify(kafkaTemplate, timeout(5_000).times(3))
        .send(eq(LanguagePreferenceProducer.TOPIC), anyString(), any());
  }
}
