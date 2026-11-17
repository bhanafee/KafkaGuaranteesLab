package com.maybeitssquid.kafkaguaranteeslab.lab;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP access to the lab's instruments: the ledger that shows what happened to each event, and the
 * fault injector that makes things go wrong. These endpoints are unprotected and belong in a lab
 * only.
 */
@RestController
@RequestMapping("/lab")
public class LabController {

  /**
   * The body of a request to set faults. A count that is left out is null and means 0, so a request
   * can name only the fault it wants.
   *
   * @param failedSends sends that should fail without writing anything to the topic
   * @param ambiguousSends sends to write to the topic and then report as failed
   * @param processingFailures calls to {@code process()} that should throw before changing anything
   * @param partialFailures calls to {@code process()} that should make their change and then throw
   * @param lostCommits records to process and then not acknowledge
   */
  public record FaultRequest(
      Integer failedSends,
      Integer ambiguousSends,
      Integer processingFailures,
      Integer partialFailures,
      Integer lostCommits) {

    FaultInjector.Faults toFaults() {
      return new FaultInjector.Faults(
          orZero(failedSends),
          orZero(ambiguousSends),
          orZero(processingFailures),
          orZero(partialFailures),
          orZero(lostCommits));
    }

    private static int orZero(Integer count) {
      return count == null ? 0 : count;
    }
  }

  private final ProcessingLedger ledger;
  private final FaultInjector faults;

  /**
   * Creates the controller.
   *
   * @param ledger the ledger to report on
   * @param faults the injector to control
   */
  public LabController(ProcessingLedger ledger, FaultInjector faults) {
    this.ledger = ledger;
    this.faults = faults;
  }

  /**
   * Reports what the consumer has done so far.
   *
   * @return the ledger's totals, including duplicates and dropped events
   */
  @GetMapping("/ledger")
  public ProcessingLedger.Summary ledger() {
    return ledger.summary();
  }

  /** Empties the ledger. */
  @DeleteMapping("/ledger")
  public void resetLedger() {
    ledger.reset();
  }

  /**
   * Reports the faults still pending.
   *
   * @return the remaining count of each fault
   */
  @GetMapping("/faults")
  public FaultInjector.Faults faults() {
    return faults.pending();
  }

  /**
   * Sets the faults to inject, replacing any still pending. A count left out of the request is 0.
   *
   * @param requested the number of times each fault should happen
   * @return the faults now pending
   */
  @PostMapping("/faults")
  public FaultInjector.Faults setFaults(@RequestBody FaultRequest requested) {
    faults.set(requested.toFaults());
    return faults.pending();
  }
}
