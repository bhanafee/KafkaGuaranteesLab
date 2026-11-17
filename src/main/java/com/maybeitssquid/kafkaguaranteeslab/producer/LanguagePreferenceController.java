package com.maybeitssquid.kafkaguaranteeslab.producer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/language-preferences")
public class LanguagePreferenceController {

  /** Response header carrying the event's id, for a caller that wants to send the event again. */
  public static final String EVENT_ID_HEADER = "Event-Id";

  private final LanguagePreferenceProducer producer;
  private final long responseTimeoutMs;

  /**
   * Creates the controller.
   *
   * @param producer publishes the events
   * @param responseTimeoutMs how long to wait for the publish outcome before answering
   */
  public LanguagePreferenceController(
      LanguagePreferenceProducer producer,
      @Value("${lab.publish.response-timeout-ms}") long responseTimeoutMs) {
    this.producer = producer;
    this.responseTimeoutMs = responseTimeoutMs;
  }

  /**
   * Publishes the event and responds once the outcome is known, or once {@code
   * lab.publish.response-timeout-ms} has passed.
   *
   * <ul>
   *   <li><b>202 Accepted:</b> the broker acknowledged the record.
   *   <li><b>503, "Event not published":</b> the publish failed after its retries, or the circuit
   *       is open.
   *   <li><b>503, "Event not confirmed":</b> the outcome was not known in time. The send is still
   *       in progress and may yet succeed.
   * </ul>
   *
   * <p>Neither 503 proves that the event is absent from the topic, so a caller that sends it again
   * can publish a duplicate. Every response carries the event's id in the {@value #EVENT_ID_HEADER}
   * header. A caller that repeats that id in the {@code eventId} field lets the consumer recognise
   * the repeat.
   *
   * @param event the event to publish. An event without an {@code eventId} is given a random one
   * @return the response, once the outcome is known or the wait is over
   */
  @PostMapping
  public CompletableFuture<ResponseEntity<String>> publish(@RequestBody LanguagePreference event) {
    return producer
        .publish(event)
        .orTimeout(responseTimeoutMs, TimeUnit.MILLISECONDS)
        .thenApply(result -> response(HttpStatus.ACCEPTED, "Event queued: ", event))
        .exceptionally(ex ->
            response(
                HttpStatus.SERVICE_UNAVAILABLE,
                isTimeout(ex) ? "Event not confirmed: " : "Event not published: ",
                event));
  }

  private static ResponseEntity<String> response(
      HttpStatus status, String message, LanguagePreference event) {
    return ResponseEntity.status(status)
        .header(EVENT_ID_HEADER, event.eventId())
        .body(message + event.customerId());
  }

  private static boolean isTimeout(Throwable ex) {
    Throwable cause =
        ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
    return cause instanceof TimeoutException;
  }
}
