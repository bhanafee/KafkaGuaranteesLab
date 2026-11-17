package com.maybeitssquid.kafkaguaranteeslab.lab;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.stereotype.Component;

/**
 * Keeps the ledger in a Kafka topic, so that it survives a restart of the application and is the
 * same in every instance. Without this, a scenario that stops the application leaves nothing to
 * look at afterwards.
 *
 * <p><b>One path into the ledger.</b> {@link #append} writes an entry to the topic and waits for
 * the broker to acknowledge it. A reader thread reads the topic from its beginning and hands every
 * entry to {@link ProcessingLedger#apply}, whichever instance wrote it. {@code append} returns once
 * the reader has applied the entry it wrote. The ledger therefore holds the topic's entries in the
 * topic's order, in every instance.
 *
 * <p><b>Start.</b> This starts before the listener container and replays the topic first, so the
 * consumer finds the ids of the events processed before a restart. If the broker cannot be reached
 * in time, the application starts with an incomplete ledger and the reader catches up later.
 *
 * <p><b>The topic has one partition</b>, which gives the entries one order. Entries are written to
 * partition 0 whatever the topic was created with.
 *
 * <p>The journal is itself a downstream system that can fail. An entry that is written but not
 * confirmed in time makes {@code append} throw, and the event is then retried although its entry
 * exists.
 */
@Component
public class KafkaLedgerJournal implements LedgerJournal, SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(KafkaLedgerJournal.class);

  private final ProcessingLedger ledger;
  private final String bootstrapServers;
  private final TopicPartition partition;
  private final Duration timeout;

  private final Object lock = new Object();
  // Offset of the next entry to apply. Guarded by lock.
  private long applied;

  private volatile boolean running;
  private volatile KafkaConsumer<String, LedgerEntry> consumer;
  private volatile CountDownLatch replayed = new CountDownLatch(1);
  private Producer<String, LedgerEntry> producer;
  private Thread reader;

  /**
   * Creates the journal and makes it the ledger's journal.
   *
   * @param ledger the ledger to feed
   * @param bootstrapServers the Kafka cluster holding the topic
   * @param topic the journal's topic
   * @param timeoutMs how long to wait for the replay at start, and for each entry to be written and
   *     read back
   */
  public KafkaLedgerJournal(
      ProcessingLedger ledger,
      @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
      @Value("${lab.ledger.topic}") String topic,
      @Value("${lab.ledger.timeout-ms}") long timeoutMs) {
    this.ledger = ledger;
    this.bootstrapServers = bootstrapServers;
    this.partition = new TopicPartition(topic, 0);
    this.timeout = Duration.ofMillis(timeoutMs);
    ledger.journalTo(this);
  }

  @Override
  public void append(LedgerEntry entry) {
    long offset;
    try {
      offset =
          producer
              .send(new ProducerRecord<>(partition.topic(), partition.partition(), null, entry))
              .get(timeout.toMillis(), TimeUnit.MILLISECONDS)
              .offset();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted writing ledger entry " + entry, ex);
    } catch (ExecutionException | TimeoutException | RuntimeException ex) {
      throw new IllegalStateException("Ledger entry not confirmed: " + entry, ex);
    }
    awaitApplied(offset, entry);
  }

  private void awaitApplied(long offset, LedgerEntry entry) {
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (lock) {
      while (applied <= offset) {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remainingMs <= 0) {
          throw new IllegalStateException("Ledger entry written but not read back: " + entry);
        }
        try {
          lock.wait(remainingMs);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted reading back ledger entry " + entry, ex);
        }
      }
    }
  }

  private void advanceTo(long next) {
    synchronized (lock) {
      applied = next;
      lock.notifyAll();
    }
  }

  private long applied() {
    synchronized (lock) {
      return applied;
    }
  }

  /** Reads the topic for as long as the journal runs, reconnecting after a failure. */
  private void read() {
    while (running) {
      try (var client =
          new KafkaConsumer<>(
              consumerProps(),
              new StringDeserializer(),
              new JacksonJsonDeserializer<>(LedgerEntry.class, false))) {
        consumer = client;
        client.assign(List.of(partition));
        client.seek(partition, applied());
        long end = client.endOffsets(List.of(partition), timeout).get(partition);
        while (running) {
          if (applied() >= end) {
            replayed.countDown();
          }
          try {
            for (ConsumerRecord<String, LedgerEntry> record : client.poll(Duration.ofMillis(200))) {
              if (record.value() != null) {
                ledger.apply(record.value());
              }
              advanceTo(record.offset() + 1);
            }
          } catch (RecordDeserializationException ex) {
            log.warn("Skipping unreadable ledger entry at offset {}", ex.offset());
            client.seek(partition, ex.offset() + 1);
            advanceTo(ex.offset() + 1);
          }
        }
      } catch (WakeupException ex) {
        // stop() woke the consumer. The loop condition ends the thread.
      } catch (RuntimeException ex) {
        if (running) {
          log.warn("Ledger reader failed, reconnecting: {}", ex.toString());
          pause();
        }
      }
    }
  }

  private void pause() {
    try {
      Thread.sleep(1_000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  private Map<String, Object> producerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    // send() must not block for longer than append() is prepared to wait.
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timeout.toMillis());
    return props;
  }

  private Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // No group: every instance reads the whole topic and commits nothing.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  @Override
  public void start() {
    producer =
        new KafkaProducer<>(
            producerProps(), new StringSerializer(), new JacksonJsonSerializer<LedgerEntry>());
    replayed = new CountDownLatch(1);
    running = true;
    reader = new Thread(this::read, "ledger-journal");
    reader.setDaemon(true);
    reader.start();
    try {
      if (!replayed.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        log.warn(
            "Ledger not replayed within {}. It is incomplete until the broker can be reached.",
            timeout);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void stop() {
    running = false;
    KafkaConsumer<String, LedgerEntry> client = consumer;
    if (client != null) {
      client.wakeup();
    }
    try {
      reader.join(2_000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    producer.close(Duration.ofSeconds(2));
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /** Starts before the listener container, so the ledger is replayed before any record arrives. */
  @Override
  public int getPhase() {
    return 0;
  }
}
