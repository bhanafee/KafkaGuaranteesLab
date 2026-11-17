package com.maybeitssquid.kafkaguaranteeslab.producer;

import com.maybeitssquid.kafkaguaranteeslab.lab.Crasher;
import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * Publishes language preference events to Kafka, under the {@code languagePreferenceProducer}
 * Resilience4j retry and circuit breaker configured in {@code application.yml}.
 *
 * <p><b>How the two annotations nest.</b> The retry is the outer one and the circuit breaker the
 * inner one. Each attempt passes through the circuit breaker separately, so a publish that fails
 * all of its attempts adds three failures to the circuit breaker's window, not one.
 *
 * <ul>
 *   <li><b>Retry:</b> up to 3 attempts, with a fixed 1 s wait between them, on any {@code
 *       Exception} except {@code CallNotPermittedException}. An open circuit rejects every attempt,
 *       so retrying the rejection would only delay the failure.
 *   <li><b>Circuit breaker:</b> looks at the last 10 calls. When at least half of them failed, or
 *       at least 80% took longer than 2 s, the circuit opens for 30 s and rejects every call. It
 *       then lets 3 trial calls through to decide whether to close again.
 * </ul>
 *
 * <p><b>The fallback belongs to the retry.</b> A fallback on the outer annotation runs once per
 * publish, after the last attempt. On the circuit breaker it would run once per attempt.
 *
 * <p><b>This configuration slows publishing down.</b>
 *
 * <ul>
 *   <li>{@link #publish} returns a future, so Resilience4j does not block the caller between
 *       attempts. It runs the retries on its own scheduler thread. The future still completes only
 *       after the last attempt, so whoever waits on it waits for all of them.
 *   <li>One attempt can itself take a long time. Kafka keeps retrying inside a single {@code
 *       send()} until {@code delivery.timeout.ms} (2 minutes by default). Three attempts of that
 *       length, plus the waits, exceed 6 minutes before the caller learns of a failure.
 *   <li>The first attempt runs on the caller's thread, and {@code send()} can block there while it
 *       waits for topic metadata, up to {@code max.block.ms}. The HTTP controller calls this method
 *       from a task executor for that reason.
 *   <li>Slowness alone opens the circuit. Sends that succeed but take longer than 2 s count against
 *       the slow-call threshold, and an open circuit fails publishes that would have succeeded.
 * </ul>
 */
@Service
public class LanguagePreferenceProducer {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceProducer.class);
  public static final String TOPIC = "language-preferences";

  private final KafkaTemplate<String, LanguagePreference> kafkaTemplate;
  private final FaultInjector faults;
  private final Crasher crasher;

  /**
   * Creates the producer.
   *
   * @param kafkaTemplate sends the records
   * @param faults supplies injected failed and ambiguous sends, and crashes
   * @param crasher stops the application for an injected crash
   */
  public LanguagePreferenceProducer(
      KafkaTemplate<String, LanguagePreference> kafkaTemplate,
      FaultInjector faults,
      Crasher crasher) {
    this.kafkaTemplate = kafkaTemplate;
    this.faults = faults;
    this.crasher = crasher;
  }

  /**
   * Sends the event to Kafka, keyed by customer id, and returns a future for the outcome.
   *
   * <p><b>A failed send is ambiguous.</b> When {@code send()} fails, the caller has no way of
   * knowing whether the message was already transmitted. A send can time out after the request left
   * the producer, or lose the broker's acknowledgment on the way back. The broker may have written
   * the record even though the future failed. A failure here means "not confirmed", never "not
   * sent".
   *
   * <p><b>A retry is a new message to Kafka.</b> Within one {@code send()}, the idempotent producer
   * gives the record a producer id and sequence number, and the broker uses them to drop a repeat
   * caused by Kafka's own retries. That is the record's idempotency key. A Resilience4j retry calls
   * {@code send()} again, which produces a <em>new</em> idempotency key for the same event. The
   * broker cannot tell that the two records are the same event, so if the earlier attempt did reach
   * the log, both are delivered.
   *
   * <p>Retrying at this level therefore exchanges a possible lost event for a possible duplicate.
   * That is at-least-once delivery, and it is why consumers of this topic must tolerate the same
   * event twice.
   *
   * <p><b>Where failures land.</b> In the returned future, not in a thrown exception. That holds
   * for an exception thrown by {@code send()} itself, for a future that fails later, and for a call
   * rejected by an open circuit. Once the last attempt has failed, {@link #publishFallback}
   * supplies the failed future.
   *
   * @param event the event to publish
   * @return a future that completes with the send result once the broker acknowledges the record,
   *     or fails once the publish has finally failed
   */
  @Retry(name = "languagePreferenceProducer", fallbackMethod = "publishFallback")
  @CircuitBreaker(name = "languagePreferenceProducer")
  public CompletableFuture<SendResult<String, LanguagePreference>> publish(
      LanguagePreference event) {
    log.info("Publishing event: eventId={} customerId={}", event.eventId(), event.customerId());
    if (faults.takeCrashBeforeSend()) {
      // The application stops with the event accepted and nothing sent. Only the caller has it.
      crasher.crash("event " + event.eventId() + " accepted, not sent");
    }
    if (faults.takeFailedSend()) {
      // Nothing is sent. This is what a send looks like when the broker stays unreachable until
      // the delivery timeout runs out: the attempt fails and the topic does not hold the record.
      log.error(
          "Failed to publish event: eventId={} customerId={} (injected, nothing written)",
          event.eventId(),
          event.customerId());
      return CompletableFuture.failedFuture(
          new TimeoutException("Injected fault: record not written"));
    }
    return kafkaTemplate
        .send(TOPIC, event.customerId(), event)
        .thenApply(result -> {
          if (faults.takeAmbiguousSend()) {
            // The record is in the topic, but the caller is told the send failed. This is what a
            // send looks like when its acknowledgment is lost and the delivery timeout runs out.
            throw new TimeoutException("Injected fault: record written, acknowledgment lost");
          }
          return result;
        })
        .whenComplete((result, ex) -> {
          if (ex != null) {
            log.error(
                "Failed to publish event: eventId={} customerId={}",
                event.eventId(),
                event.customerId(),
                ex);
          } else {
            log.info(
                "Published event: eventId={} customerId={} partition={} offset={}",
                event.eventId(),
                event.customerId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
          }
        });
  }

  /**
   * Called by the retry once a publish has finally failed: after the last attempt, or at once when
   * the circuit is open. It logs a warning and returns a failed future carrying the failure.
   *
   * <p>The event is not stored anywhere. A dead-letter topic on the same cluster would fail along
   * with the main topic, so the failed future is how the caller learns that the event is not
   * confirmed. The ambiguity described on {@link #publish} still applies: an earlier attempt may
   * have reached the log.
   *
   * @param event the event that could not be published
   * @param t the failure of the last attempt, or the rejection by the open circuit
   * @return a future that has already failed with {@code t}
   */
  public CompletableFuture<SendResult<String, LanguagePreference>> publishFallback(
      LanguagePreference event, Throwable t) {
    log.warn(
        "Publish failed — event not confirmed: eventId={} customerId={} reason={}",
        event.eventId(),
        event.customerId(),
        t.getMessage());
    return CompletableFuture.failedFuture(t);
  }
}
