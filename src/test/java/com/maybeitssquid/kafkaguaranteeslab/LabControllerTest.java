package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector.Faults;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/** The lab endpoints, called over HTTP the way the README shows. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC, "dead-letters"})
class LabControllerTest {

  @LocalServerPort private int port;
  @Autowired private FaultInjector faults;

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @Test
  void setFaults_acceptsARequestThatNamesOnlyOneFault() throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/lab/faults"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"ambiguousSends\": 1}"))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(faults.pending()).isEqualTo(new Faults(1, 0, 0));
  }

  @Test
  void ledger_reportsTotals() throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/lab/ledger")).build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body())
        .contains("\"events\"", "\"duplicates\"", "\"dropped\"", "\"skippedDuplicates\"");
  }
}
