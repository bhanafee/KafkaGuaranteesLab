package com.maybeitssquid.kafkaguaranteeslab;

import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.awaitUntil;
import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.startApp;
import static com.maybeitssquid.kafkaguaranteeslab.KafkaTestSupport.startBrokerBehind;
import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceController;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * The scenario "the outcome is unknown", to its end: an event whose caller was answered 503 {@code
 * Event not confirmed} is written and processed afterwards.
 *
 * <p>A test cannot stop and restart the embedded broker with its data kept. In place of that, a
 * {@link LostAcknowledgmentProxy} in front of the broker holds the produce request, which is what
 * the producer sees of a broker that has stopped answering, and releases it after the response.
 */
class OutcomeUnknownTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static LostAcknowledgmentProxy proxy;
  private static EmbeddedKafkaBroker broker;

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @BeforeAll
  static void startBrokerBehindProxy() throws Exception {
    proxy = new LostAcknowledgmentProxy();
    broker = startBrokerBehind(proxy);
  }

  @AfterAll
  static void stopBrokerAndProxy() throws Exception {
    broker.destroy();
    proxy.close();
  }

  @Test
  void eventAnsweredNotConfirmed_isProcessedOnceTheBrokerAnswersAgain() throws Exception {
    try (var app =
        startApp("localhost:" + proxy.port(), "--lab.publish.response-timeout-ms=1000")) {
      var ledger = app.getBean(ProcessingLedger.class);
      String url =
          "http://localhost:"
              + app.getEnvironment().getProperty("local.server.port")
              + "/language-preferences";

      // A first event connects the producer and is processed to the end, ledger entry included,
      // so the only produce request the proxy holds afterwards is the one under test.
      HttpResponse<String> first = post(url, "cust-warm-up");
      assertThat(first.statusCode()).isEqualTo(202);
      awaitUntil(() -> ledger.processedCount(eventId(first)) == 1, TIMEOUT);

      int heldBefore = proxy.held();
      proxy.holdProduceRequests();
      try {
        HttpResponse<String> response = post(url, "cust-unknown");

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).isEqualTo("Event not confirmed: cust-unknown");
        // The request left the producer and has not reached the broker, so nothing is processed.
        assertThat(proxy.held()).isGreaterThan(heldBefore);
        assertThat(ledger.processedCount(eventId(response))).isZero();

        proxy.releaseProduceRequests();

        // The caller has its 503. The send was never cancelled, and now it completes.
        awaitUntil(() -> ledger.processedCount(eventId(response)) == 1, TIMEOUT);
      } finally {
        proxy.releaseProduceRequests();
      }
    }
  }

  private static String eventId(HttpResponse<String> response) {
    return response
        .headers()
        .firstValue(LanguagePreferenceController.EVENT_ID_HEADER)
        .orElseThrow();
  }

  private HttpResponse<String> post(String url, String customerId) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "{\"customerId\":\"" + customerId + "\",\"preferredLanguage\":\"fr\"}"))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
