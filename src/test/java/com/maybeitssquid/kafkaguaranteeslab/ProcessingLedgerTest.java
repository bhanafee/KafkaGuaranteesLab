package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.lab.ProcessingLedger;
import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class ProcessingLedgerTest {

  @Test
  void summary_reportsDuplicatesAndDroppedEvents() {
    var ledger = new ProcessingLedger();
    var once = new LanguagePreference("cust-1", Locale.FRENCH);
    var twice = new LanguagePreference("cust-1", Locale.GERMAN);
    var dropped = new LanguagePreference("cust-2", Locale.ITALIAN);

    ledger.processed(once);
    ledger.processed(twice);
    ledger.processed(twice);
    ledger.dropped(dropped);
    ledger.skippedDuplicate(once);

    var summary = ledger.summary();
    assertThat(summary.events()).isEqualTo(2);
    assertThat(summary.processings()).isEqualTo(3);
    assertThat(summary.duplicates()).containsOnlyKeys(twice.eventId()).containsValue(2);
    assertThat(summary.dropped()).containsExactly(dropped.eventId());
    assertThat(summary.skippedDuplicates()).isEqualTo(1);
    assertThat(ledger.eventIdsFor("cust-1"))
        .containsExactly(once.eventId(), twice.eventId(), twice.eventId());
  }

  @Test
  void preference_isTheOneProcessedLast() {
    var ledger = new ProcessingLedger();
    var older = new LanguagePreference("cust-1", Locale.FRENCH);
    var newer = new LanguagePreference("cust-1", Locale.GERMAN);

    ledger.processed(older);
    ledger.processed(newer);
    assertThat(ledger.preferenceOf("cust-1")).isEqualTo("de");

    ledger.processed(older);
    assertThat(ledger.preferenceOf("cust-1")).isEqualTo("fr");
    assertThat(ledger.summary().preferences()).containsEntry("cust-1", "fr");
    assertThat(ledger.preferenceOf("cust-2")).isNull();
  }

  @Test
  void eventsWithoutAnId_getDifferentIds() {
    var first = new LanguagePreference("cust-1", Locale.FRENCH);
    var second = new LanguagePreference("cust-1", Locale.FRENCH);

    assertThat(first.eventId()).isNotBlank().isNotEqualTo(second.eventId());
  }
}
