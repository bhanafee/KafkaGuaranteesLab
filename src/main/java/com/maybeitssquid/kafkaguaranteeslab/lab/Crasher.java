package com.maybeitssquid.kafkaguaranteeslab.lab;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stops the application the way a crash does: at once, with no shutdown. Nothing is flushed, no
 * offset is committed and the consumer does not leave its group. The {@link FaultInjector} decides
 * when.
 */
@Component
public class Crasher {

  private static final Logger log = LoggerFactory.getLogger(Crasher.class);

  private volatile Consumer<String> action = reason -> Runtime.getRuntime().halt(1);

  /** Creates a crasher that halts the JVM. */
  public Crasher() {}

  /**
   * Stops the application. This method does not return.
   *
   * @param reason what the application was doing, for the log
   */
  public void crash(String reason) {
    log.error("Injected crash: {}. Halting the JVM without a shutdown.", reason);
    action.accept(reason);
  }

  /**
   * Replaces what {@link #crash} does. A test cannot halt its own JVM, so it puts an action here
   * that ends the calling thread instead.
   *
   * @param action called with the reason in place of halting the JVM
   */
  public void onCrash(Consumer<String> action) {
    this.action = action;
  }
}
