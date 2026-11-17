package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.lab.FaultInjector;
import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Processes language preference events for {@link LanguagePreferenceConsumer}, under the {@code
 * languagePreferenceConsumer} Resilience4j retry and circuit breaker configured in {@code
 * application.yml}.
 *
 * <p><b>How the two annotations nest.</b> The retry is the outer one and the circuit breaker the
 * inner one. Each attempt passes through the circuit breaker separately, so a record that fails all
 * of its retry attempts adds three failures to the circuit breaker's window.
 *
 * <ul>
 *   <li><b>Retry:</b> up to 3 attempts, with a fixed 1 s wait between them, on any {@code
 *       Exception}.
 *   <li><b>Circuit breaker:</b> looks at the last 10 calls. When at least half of them failed, the
 *       circuit opens for 30 s and rejects every call with {@code CallNotPermittedException}. It
 *       then lets trial calls through to decide whether to close again.
 * </ul>
 *
 * <p><b>The fallback belongs to the retry.</b> A fallback on the outer annotation runs once, after
 * the last attempt. On the circuit breaker it would turn every failed attempt into a normal return,
 * and the retry would never see a failure to retry.
 *
 * <p><b>This configuration slows the consumer down.</b> The Resilience4j retry waits by sleeping on
 * the thread that called {@link #process}, which is the listener thread. While this application, a
 * downstream system or the connection to a downstream system is failing, the consumer falls behind
 * and its lag grows. The records it has not yet read wait in Kafka, not in this application's
 * memory.
 *
 * <ul>
 *   <li>A record that keeps failing holds its partition for the two 1 s waits plus the time of the
 *       three attempts. Nothing behind it is read meanwhile.
 *   <li>An open circuit does not remove the waits. This retry also retries the rejection, so each
 *       record still costs about 2 s while the circuit is open.
 *   <li>The time adds up across the records of one poll. If a batch takes longer than {@code
 *       max.poll.interval.ms}, the broker removes the consumer from its group, and the records it
 *       had not yet committed are delivered again.
 *   <li>If the failure reaches the listener container, each redelivery repeats all three attempts,
 *       so the delays multiply.
 * </ul>
 */
@Service
public class LanguagePreferenceProcessor {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceProcessor.class);

  /** What {@link #processFallback} does with an event that could not be processed. */
  public enum FallbackMode {
    /** Log the failure and return, so the record is acknowledged unprocessed. */
    ACKNOWLEDGE,
    /** Throw the failure, so the record is redelivered and finally dead-lettered. */
    RETHROW
  }

  private final ProcessingLedger ledger;
  private final FaultInjector faults;
  private final FallbackMode fallbackMode;
  private final boolean deduplicate;

  /**
   * Creates the processor.
   *
   * @param ledger records what happened to each event
   * @param faults supplies injected processing failures and partial failures
   * @param fallbackMode what the fallback does with an event that could not be processed
   * @param deduplicate whether to skip an event whose id was already processed
   */
  public LanguagePreferenceProcessor(
      ProcessingLedger ledger,
      FaultInjector faults,
      @Value("${lab.consumer.fallback}") FallbackMode fallbackMode,
      @Value("${lab.consumer.deduplicate}") boolean deduplicate) {
    this.ledger = ledger;
    this.faults = faults;
    this.fallbackMode = fallbackMode;
    this.deduplicate = deduplicate;
  }

  /**
   * Processes one event. This lab records it in the {@link ProcessingLedger}, which stands in for a
   * downstream system. A real implementation would make its downstream calls here, such as a
   * database write or an HTTP request.
   *
   * <p><b>Duplicates.</b> The same event can arrive more than once. With {@code
   * lab.consumer.deduplicate} off, each arrival is processed and the ledger shows the repeat. With
   * it on, an event whose id was already processed is skipped. Recognising an event by its own id
   * is what Kafka's idempotent producer cannot do. In the lab the ledger entry is both the change
   * and the record of the id. A real consumer must record the id in the same atomic step as the
   * change it guards, in a store that outlives the consumer.
   *
   * <p>Downstream processing can fail. A downstream client usually brings its own retry and circuit
   * breaker logic, and it may throw exceptions. The logic in this implementation assumes that
   * downstream processing is safe to retry (e.g., it is idempotent).
   *
   * <p><b>Thread.</b> All of that processing runs on the thread and in the call stack of the {@link
   * LanguagePreferenceConsumer#onMessage} call that read the message. There is no hand-off to
   * another thread: {@code onMessage} does not return, and its record is not acknowledged, until
   * this method has returned or thrown. If it were passed to a different thread then this
   * application could accumulate and potentially lose unprocessed messages while continuing to
   * acknowledge them to Kafka.
   *
   * <p><b>Where exceptions land.</b> An exception thrown here travels up that same call stack. It
   * reaches the circuit breaker first, which records the failure, and then the retry, which waits
   * and calls this method again. If the last retry attempt fails, the retry logic in Resilience4j
   * calls {@link #processFallback} with the exception. {@code onMessage} sees an exception only if
   * the fallback throws one.
   *
   * @param event the event to process
   */
  @Retry(name = "languagePreferenceConsumer", fallbackMethod = "processFallback")
  @CircuitBreaker(name = "languagePreferenceConsumer")
  public void process(LanguagePreference event) {
    if (faults.takeProcessingFailure()) {
      throw new IllegalStateException("Injected fault: downstream unavailable");
    }
    if (deduplicate && ledger.processedCount(event.eventId()) > 0) {
      log.info("Skipping duplicate: eventId={} customerId={}", event.eventId(), event.customerId());
      ledger.skippedDuplicate(event);
      return;
    }
    log.info(
        "Processing language preference: eventId={} customerId={} locale={}",
        event.eventId(),
        event.customerId(),
        event.preferredLanguage());
    ledger.processed(event);
    if (faults.takePartialFailure()) {
      // The change has been made and the call fails all the same. Whatever tries again, the retry
      // or a redelivery, repeats the change unless the event is recognised by its id.
      throw new IllegalStateException("Injected fault: downstream failed after the change");
    }
  }

  /**
   * Called by the retry when {@link #process} has failed on every attempt, including attempts
   * rejected because the circuit is open. What it does next is set by {@code
   * lab.consumer.fallback}.
   *
   * <p><b>Thread.</b> This runs on the same thread and in the same call stack as {@code process}:
   * the one of the {@code onMessage} call that read the message.
   *
   * <p><b>{@code acknowledge}, the default.</b> The failure stops here. This method logs the error
   * and returns normally, so {@code onMessage} sees a successful call and acknowledges the record.
   * The event is then neither processed nor dead-lettered. The log line and the ledger's list of
   * dropped events are the only traces of it. This keeps the consumer moving at the price of the
   * delivery guarantee.
   *
   * <p><b>{@code rethrow}.</b> This method throws the failure, and the exception lands back in the
   * {@code onMessage} handler. The handler does not acknowledge the unprocessed message, so it is
   * retried: {@code DefaultErrorHandler} redelivers it and, once the redeliveries are exhausted,
   * writes a dead-letter pointer to it. This keeps the guarantee at the price of a partition that
   * waits while the record is retried.
   *
   * @param event the event that could not be processed
   * @param t the failure of the last attempt
   */
  public void processFallback(LanguagePreference event, Throwable t) {
    if (fallbackMode == FallbackMode.RETHROW) {
      log.error(
          "Processing failed after retries — rethrowing: eventId={} customerId={}",
          event.eventId(),
          event.customerId());
      if (t instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (t instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException(t);
    }
    log.error(
        "Processing failed after retries — event not processed: eventId={} customerId={}",
        event.eventId(),
        event.customerId(),
        t);
    ledger.dropped(event);
  }
}
