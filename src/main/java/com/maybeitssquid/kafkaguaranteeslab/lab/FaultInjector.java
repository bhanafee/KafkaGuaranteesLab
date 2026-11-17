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
   * @param processingFailures calls to {@code process()} that will throw before changing anything
   * @param partialFailures calls to {@code process()} that will make their change and then throw
   * @param lostCommits records that will be processed and then not acknowledged
   * @param crashBeforeSend publishes that will stop the application before anything is sent
   * @param crashBeforeCommit records that will be processed, after which the application stops
   *     before the offset is committed
   */
  public record Faults(
      int failedSends,
      int ambiguousSends,
      int processingFailures,
      int partialFailures,
      int lostCommits,
      int crashBeforeSend,
      int crashBeforeCommit) {

    /**
     * Returns a set with no faults, as a starting point for the {@code with} methods.
     *
     * @return a set in which every count is 0
     */
    public static Faults none() {
      return new Faults(0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * Returns a copy with a different count of failed sends.
     *
     * @param count sends that will fail without writing anything
     * @return the copy
     */
    public Faults withFailedSends(int count) {
      return new Faults(
          count,
          ambiguousSends,
          processingFailures,
          partialFailures,
          lostCommits,
          crashBeforeSend,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of ambiguous sends.
     *
     * @param count sends that will be written and then reported as failed
     * @return the copy
     */
    public Faults withAmbiguousSends(int count) {
      return new Faults(
          failedSends,
          count,
          processingFailures,
          partialFailures,
          lostCommits,
          crashBeforeSend,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of processing failures.
     *
     * @param count calls to {@code process()} that will throw
     * @return the copy
     */
    public Faults withProcessingFailures(int count) {
      return new Faults(
          failedSends,
          ambiguousSends,
          count,
          partialFailures,
          lostCommits,
          crashBeforeSend,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of partial failures.
     *
     * @param count calls to {@code process()} that will make their change and then throw
     * @return the copy
     */
    public Faults withPartialFailures(int count) {
      return new Faults(
          failedSends,
          ambiguousSends,
          processingFailures,
          count,
          lostCommits,
          crashBeforeSend,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of lost commits.
     *
     * @param count records that will be processed and then not acknowledged
     * @return the copy
     */
    public Faults withLostCommits(int count) {
      return new Faults(
          failedSends,
          ambiguousSends,
          processingFailures,
          partialFailures,
          count,
          crashBeforeSend,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of crashes before a send.
     *
     * @param count publishes that will stop the application before anything is sent
     * @return the copy
     */
    public Faults withCrashBeforeSend(int count) {
      return new Faults(
          failedSends,
          ambiguousSends,
          processingFailures,
          partialFailures,
          lostCommits,
          count,
          crashBeforeCommit);
    }

    /**
     * Returns a copy with a different count of crashes before a commit.
     *
     * @param count records that will be processed, after which the application stops
     * @return the copy
     */
    public Faults withCrashBeforeCommit(int count) {
      return new Faults(
          failedSends,
          ambiguousSends,
          processingFailures,
          partialFailures,
          lostCommits,
          crashBeforeSend,
          count);
    }
  }

  private final AtomicInteger failedSends = new AtomicInteger();
  private final AtomicInteger ambiguousSends = new AtomicInteger();
  private final AtomicInteger processingFailures = new AtomicInteger();
  private final AtomicInteger partialFailures = new AtomicInteger();
  private final AtomicInteger lostCommits = new AtomicInteger();
  private final AtomicInteger crashBeforeSend = new AtomicInteger();
  private final AtomicInteger crashBeforeCommit = new AtomicInteger();

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
    partialFailures.set(Math.max(0, faults.partialFailures()));
    lostCommits.set(Math.max(0, faults.lostCommits()));
    crashBeforeSend.set(Math.max(0, faults.crashBeforeSend()));
    crashBeforeCommit.set(Math.max(0, faults.crashBeforeCommit()));
  }

  /**
   * Returns the faults still pending.
   *
   * @return the remaining count of each fault
   */
  public Faults pending() {
    return new Faults(
        failedSends.get(),
        ambiguousSends.get(),
        processingFailures.get(),
        partialFailures.get(),
        lostCommits.get(),
        crashBeforeSend.get(),
        crashBeforeCommit.get());
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
   * Asks whether this call to {@code process()} should fail before it changes anything. This
   * imitates a downstream system that is unavailable.
   *
   * @return true if the caller should throw
   */
  public boolean takeProcessingFailure() {
    return take(processingFailures);
  }

  /**
   * Asks whether this call to {@code process()} should fail after it has made its change. This
   * imitates a downstream call that took effect and then reported an error, or the first of two
   * downstream changes succeeding and the second failing.
   *
   * @return true if the caller should throw although the change was made
   */
  public boolean takePartialFailure() {
    return take(partialFailures);
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

  /**
   * Asks whether the application should stop now, before this publish sends anything. This is the
   * application stopping mid-publish: the caller's connection breaks and the event is in no topic.
   *
   * @return true if the caller should stop the application
   */
  public boolean takeCrashBeforeSend() {
    return take(crashBeforeSend);
  }

  /**
   * Asks whether the application should stop now, after the record was processed and before its
   * offset is committed. After a restart the record is delivered again.
   *
   * @return true if the caller should stop the application
   */
  public boolean takeCrashBeforeCommit() {
    return take(crashBeforeCommit);
  }

  private static boolean take(AtomicInteger remaining) {
    return remaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
  }
}
