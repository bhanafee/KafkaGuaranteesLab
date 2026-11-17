package com.maybeitssquid.kafkaguaranteeslab.model;

import java.util.Locale;
import java.util.UUID;

/**
 * A customer's language preference.
 *
 * <p>The event id identifies the event itself, however many times it is sent. Kafka's idempotent
 * producer cannot do that: it numbers each call to {@code send()}, so the same event sent twice is
 * two records to the broker. The event id is what lets a consumer recognise the second one.
 *
 * @param eventId identifies this event. A random id is assigned when it is null, so a caller that
 *     wants a retry to be recognised must supply its own id and repeat it
 * @param customerId the customer, used as the Kafka record key
 * @param preferredLanguage the customer's preferred language
 */
public record LanguagePreference(String eventId, String customerId, Locale preferredLanguage) {

  /** Assigns a random event id when none was supplied. */
  public LanguagePreference {
    if (eventId == null || eventId.isBlank()) {
      eventId = UUID.randomUUID().toString();
    }
  }

  /**
   * Creates an event with a random event id.
   *
   * @param customerId the customer
   * @param preferredLanguage the customer's preferred language
   */
  public LanguagePreference(String customerId, Locale preferredLanguage) {
    this(null, customerId, preferredLanguage);
  }
}
