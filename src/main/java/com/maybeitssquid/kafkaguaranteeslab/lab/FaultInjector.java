package com.maybeitssquid.kafkaguaranteeslab.lab;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Makes the application fail on request, so that a delivery scenario can be produced at will
 * instead of waiting for a broker or a network to misbehave.
 *
 * <p>Each fault is a count of how many more times it should happen. The producer, the consumer and
 * the processor each ask before the step concerned, and a positive count is used up one at a time.
 */
@Component
public class FaultInjector {

  /**
   * The faults still to be injected.
   *
   * @param ambiguousSends sends that will be written to the topic and then reported as failed
   * @param processingFailures calls to {@code process()} that will throw
   * @param lostCommits records that will be processed and then not acknowledged
   */
  public record Faults(int ambiguousSends, int processingFailures, int lostCommits) {}

  private final AtomicInteger ambiguousSends = new AtomicInteger();
  private final AtomicInteger processingFailures = new AtomicInteger();
  private final AtomicInteger lostCommits = new AtomicInteger();

  /** Creates an injector with no faults pending. */
  public FaultInjector() {}

  /**
   * Sets the faults to inject, replacing any still pending.
   *
   * @param faults the number of times each fault should happen
   */
  public void set(Faults faults) {
    ambiguousSends.set(Math.max(0, faults.ambiguousSends()));
    processingFailures.set(Math.max(0, faults.processingFailures()));
    lostCommits.set(Math.max(0, faults.lostCommits()));
  }

  /**
   * Returns the faults still pending.
   *
   * @return the remaining count of each fault
   */
  public Faults pending() {
    return new Faults(ambiguousSends.get(), processingFailures.get(), lostCommits.get());
  }

  /**
   * Asks whether the send that just succeeded should be reported as failed. This imitates a send
   * whose record reached the topic but whose acknowledgment never reached the producer.
   *
   * @return true if the caller should report failure
   */
  public boolean takeAmbiguousSend() {
    return take(ambiguousSends);
  }

  /**
   * Asks whether this call to {@code process()} should fail. This imitates a downstream system that
   * is unavailable.
   *
   * @return true if the caller should throw
   */
  public boolean takeProcessingFailure() {
    return take(processingFailures);
  }

  /**
   * Asks whether the record that was just processed should go unacknowledged. This imitates an
   * offset commit that is lost, or a consumer that stops between processing and commit.
   *
   * @return true if the caller should fail instead of acknowledging
   */
  public boolean takeLostCommit() {
    return take(lostCommits);
  }

  private static boolean take(AtomicInteger remaining) {
    return remaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
  }
}
