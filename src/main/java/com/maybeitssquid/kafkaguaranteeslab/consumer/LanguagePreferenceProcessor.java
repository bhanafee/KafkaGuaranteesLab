package com.maybeitssquid.kafkaguaranteeslab.consumer;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Processes a language preference event on behalf of {@link LanguagePreferenceConsumer}.
 *
 * <p>This is a separate bean so that the consumer's call goes through the Spring proxy.
 * Resilience4j applies {@code @Retry} and {@code @CircuitBreaker} in that proxy. A call from one
 * method to another on the same object bypasses it, and the annotations then do nothing.
 */
@Service
public class LanguagePreferenceProcessor {

  private static final Logger log = LoggerFactory.getLogger(LanguagePreferenceProcessor.class);

  /**
   * Processes one event. This demo only logs it. A real implementation would make its downstream
   * calls here, such as a database write or an HTTP request.
   *
   * <p>Downstream processing can fail. Each downstream client would usually bring its own retry and
   * circuit breaker logic, and it may also throw exceptions. The {@code languagePreferenceConsumer}
   * retry and circuit breaker on this method wrap all of it: a failed attempt is retried, and once
   * the circuit is open, attempts are rejected without reaching the downstream system.
   *
   * <p>All of that processing, and every exception it throws, happens on the same thread and in the
   * same call stack as the {@link LanguagePreferenceConsumer#onMessage} handler that read the
   * message. The waits between retries therefore block the consumer thread, and no other record
   * from the same partition is read until this method returns or throws.
   *
   * <p>When every attempt has failed, {@link #processFallback} runs in place of an exception.
   *
   * @param event the event to process
   */
  // The fallback belongs to @Retry, the outer of the two. On @CircuitBreaker it would turn every
  // failed attempt into a normal return, and the retry would never see a failure to retry.
  @Retry(name = "languagePreferenceConsumer", fallbackMethod = "processFallback")
  @CircuitBreaker(name = "languagePreferenceConsumer")
  public void process(LanguagePreference event) {
    log.info(
        "Processing language preference: customerId={} locale={}",
        event.customerId(),
        event.preferredLanguage());
  }

  /**
   * Called by Resilience4j when {@link #process} has failed on every retry attempt, including
   * attempts rejected because the circuit is open. It logs the error and does not throw.
   *
   * <p>Because it returns normally, {@code onMessage} sees a successful call and acknowledges the
   * record. The event is then neither processed nor dead-lettered: this log line is the only trace
   * of it.
   *
   * <p>If this fallback also threw an exception, the exception would land back in the {@code
   * onMessage} handler, on the same thread and call stack. The handler would not acknowledge the
   * unprocessed message, so it would be retried: {@code DefaultErrorHandler} would redeliver it,
   * and once those redeliveries were exhausted, write a dead-letter pointer to it.
   *
   * @param event the event that could not be processed
   * @param t the failure of the last attempt
   */
  public void processFallback(LanguagePreference event, Throwable t) {
    log.error(
        "Processing failed after retries — event not processed: customerId={}",
        event.customerId(),
        t);
  }
}
