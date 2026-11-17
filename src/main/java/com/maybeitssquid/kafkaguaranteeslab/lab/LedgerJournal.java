package com.maybeitssquid.kafkaguaranteeslab.lab;

/**
 * Where the {@link ProcessingLedger} writes its entries. A journal stores an entry and then hands
 * it back to {@link ProcessingLedger#apply}, which is the only place the ledger's state changes.
 */
@FunctionalInterface
public interface LedgerJournal {

  /**
   * Stores an entry and returns once the ledger has applied it.
   *
   * @param entry the entry to record
   * @throws IllegalStateException if the entry could not be stored and applied in time. The entry
   *     may have been stored all the same
   */
  void append(LedgerEntry entry);
}
