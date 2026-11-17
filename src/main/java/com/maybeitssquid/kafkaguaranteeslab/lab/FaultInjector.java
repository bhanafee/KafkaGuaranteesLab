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
   * @param failedSends sends that will fail without writing anything to the topic
   * @param ambiguousSends sends that will be written to the topic and then reported as failed
   * @param processingFailures calls to {@code process()} that will throw
   * @param lostCommits records that will be processed and then not acknowledged
   */
  public record Faults(
      int failedSends, int ambiguousSends, int processingFailures, int lostCommits) {

    /**
     * Returns a set with no faults, as a starting point for the {@code with} methods.
     *
     * @return a set in which every count is 0
     */
    public static Faults none() {
      return new Faults(0, 0, 0, 0);
    }

    /**
     * Returns a copy with a different count of failed sends.
     *
     * @param count sends that will fail without writing anything
     * @return the copy
     */
    public Faults withFailedSends(int count) {
      return new Faults(count, ambiguousSends, processingFailures, lostCommits);
    }

    /**
     * Returns a copy with a different count of ambiguous sends.
     *
     * @param count sends that will be written and then reported as failed
     * @return the copy
     */
    public Faults withAmbiguousSends(int count) {
      return new Faults(failedSends, count, processingFailures, lostCommits);
    }

    /**
     * Returns a copy with a different count of processing failures.
     *
     * @param count calls to {@code process()} that will throw
     * @return the copy
     */
    public Faults withProcessingFailures(int count) {
      return new Faults(failedSends, ambiguousSends, count, lostCommits);
    }

    /**
     * Returns a copy with a different count of lost commits.
     *
     * @param count records that will be processed and then not acknowledged
     * @return the copy
     */
    public Faults withLostCommits(int count) {
      return new Faults(failedSends, ambiguousSends, processingFailures, count);
    }
  }

  private final AtomicInteger failedSends = new AtomicInteger();
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
    failedSends.set(Math.max(0, faults.failedSends()));
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
    return new Faults(
        failedSends.get(), ambiguousSends.get(), processingFailures.get(), lostCommits.get());
  }

  /**
   * Asks whether the next send should fail without being attempted. This imitates a send that Kafka
   * gave up on with nothing written, as when the broker stays unreachable for the whole delivery
   * timeout.
   *
   * @return true if the caller should report failure and send nothing
   */
  public boolean takeFailedSend() {
    return take(failedSends);
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
   * Asks whether the record that was just processed should go unacknowledged. The caller then fails
   * the delivery, and the error handler redelivers the record after its backoff. This imitates a
   * consumer that stops between processing and commit and is restarted. A commit that merely fails
   * redelivers nothing until such a restart or a rebalance.
   *
   * <p>Each lost commit uses up one delivery of the error handler's redelivery budget. Enough of
   * them in a row dead-letter a record that was processed every time.
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
