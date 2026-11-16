package com.maybeitssquid.kafkaguaranteeslab.producer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/language-preferences")
public class LanguagePreferenceController {

  private final LanguagePreferenceProducer producer;

  public LanguagePreferenceController(LanguagePreferenceProducer producer) {
    this.producer = producer;
  }

  /**
   * Publishes the event and responds once the outcome is known. The response is 202 Accepted after
   * the broker acknowledges the record, and 503 Service Unavailable when the publish fails after
   * its retries or the circuit is open.
   *
   * <p>The request is handled asynchronously. If the outcome is not known before the async request
   * timeout, Spring MVC also responds 503. In that case the send is still in progress and may yet
   * succeed, so a caller that retries on 503 can publish a duplicate.
   */
  @PostMapping
  public CompletableFuture<ResponseEntity<String>> publish(@RequestBody LanguagePreference event) {
    return producer
        .publish(event)
        .thenApply(result -> ResponseEntity.accepted().body("Event queued: " + event.customerId()))
        .exceptionally(ex ->
            ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Event not published: " + event.customerId()));
  }
}
