package com.maybeitssquid.kafkaguaranteeslab.lab;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Records what the consumer did with each event, by event id. It is how the lab makes a duplicate
 * or an unprocessed event visible: without it, processing an event twice looks the same as
 * processing it once.
 *
 * <p>The ledger lives in memory. It is empty after a restart and it grows with every event, so it
 * suits a lab and nothing else. It stands in for the downstream system a real consumer would write
 * to.
 */
@Component
public class ProcessingLedger {

  /**
   * One processing of one event.
   *
   * @param eventId the event's id
   * @param customerId the event's customer
   */
  public record Entry(String eventId, String customerId) {}

  /**
   * Totals across all events.
   *
   * @param events number of distinct events processed at least once
   * @param processings number of times {@code process()} completed, counting repeats
   * @param duplicates events processed more than once, with the number of times each was processed
   * @param dropped events acknowledged without being processed
   * @param skippedDuplicates repeats that the consumer recognised by event id and did not process
   * @param preferences each customer's current preference as a language tag: the one in the event
   *     processed last for that customer, which is not always the one sent last
   */
  public record Summary(
      int events,
      int processings,
      Map<String, Integer> duplicates,
      Set<String> dropped,
      int skippedDuplicates,
      Map<String, String> preferences) {}

  private final Map<String, Integer> processed = new LinkedHashMap<>();
  private final List<Entry> history = new ArrayList<>();
  private final Set<String> dropped = new LinkedHashSet<>();
  private final Map<String, String> preferences = new LinkedHashMap<>();
  private int skippedDuplicates;

  /** Creates an empty ledger. */
  public ProcessingLedger() {}

  /**
   * Records that an event was processed, and makes its preference the customer's current one. The
   * last event processed wins, as in a downstream system that overwrites the stored preference.
   *
   * @param event the processed event
   */
  public synchronized void processed(LanguagePreference event) {
    processed.merge(event.eventId(), 1, Integer::sum);
    history.add(new Entry(event.eventId(), event.customerId()));
    if (event.customerId() != null && event.preferredLanguage() != null) {
      preferences.put(event.customerId(), event.preferredLanguage().toLanguageTag());
    }
  }

  /**
   * Records that an event was acknowledged without being processed.
   *
   * @param event the dropped event
   */
  public synchronized void dropped(LanguagePreference event) {
    dropped.add(event.eventId());
  }

  /**
   * Records that a repeat of an already processed event was recognised and not processed again.
   *
   * @param event the repeated event
   */
  public synchronized void skippedDuplicate(LanguagePreference event) {
    skippedDuplicates++;
  }

  /**
   * Returns how many times an event was processed.
   *
   * @param eventId the event's id
   * @return the number of times, or 0 if it was never processed
   */
  public synchronized int processedCount(String eventId) {
    return processed.getOrDefault(eventId, 0);
  }

  /**
   * Returns whether an event was acknowledged without being processed.
   *
   * @param eventId the event's id
   * @return true if the event was dropped
   */
  public synchronized boolean wasDropped(String eventId) {
    return dropped.contains(eventId);
  }

  /**
   * Returns a customer's current preference: the one in the event processed last for that customer.
   *
   * @param customerId the customer
   * @return the preference as a language tag, or null if no event of the customer was processed
   */
  public synchronized String preferenceOf(String customerId) {
    return preferences.get(customerId);
  }

  /**
   * Returns the ids of a customer's events in the order they were processed, repeats included.
   *
   * @param customerId the customer
   * @return the event ids in processing order
   */
  public synchronized List<String> eventIdsFor(String customerId) {
    return history.stream()
        .filter(entry -> customerId.equals(entry.customerId()))
        .map(Entry::eventId)
        .toList();
  }

  /**
   * Returns the totals across all events.
   *
   * @return a snapshot of the totals
   */
  public synchronized Summary summary() {
    Map<String, Integer> duplicates = new LinkedHashMap<>();
    processed.forEach((eventId, count) -> {
      if (count > 1) {
        duplicates.put(eventId, count);
      }
    });
    return new Summary(
        processed.size(),
        history.size(),
        duplicates,
        new LinkedHashSet<>(dropped),
        skippedDuplicates,
        new LinkedHashMap<>(preferences));
  }

  /** Forgets everything recorded so far. */
  public synchronized void reset() {
    processed.clear();
    history.clear();
    dropped.clear();
    preferences.clear();
    skippedDuplicates = 0;
  }
}
