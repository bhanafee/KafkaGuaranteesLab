package com.maybeitssquid.kafkaguaranteeslab.lab;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;

/**
 * One line of the ledger's journal: something the consumer did with an event, or a request to
 * forget everything before it.
 *
 * @param type what happened
 * @param eventId the event's id, or null for a reset
 * @param customerId the event's customer, or null for a reset
 * @param language the event's preference as a language tag, or null if it had none
 */
public record LedgerEntry(Type type, String eventId, String customerId, String language) {

  /** What a journal entry records. */
  public enum Type {
    /** The event was processed. */
    PROCESSED,
    /** The event was acknowledged without being processed. */
    DROPPED,
    /** A repeat of the event was recognised by its id and not processed again. */
    SKIPPED,
    /** Everything recorded before this entry is to be forgotten. */
    RESET
  }

  static LedgerEntry of(Type type, LanguagePreference event) {
    return new LedgerEntry(
        type,
        event.eventId(),
        event.customerId(),
        event.preferredLanguage() == null ? null : event.preferredLanguage().toLanguageTag());
  }

  static LedgerEntry reset() {
    return new LedgerEntry(Type.RESET, null, null, null);
  }
}
