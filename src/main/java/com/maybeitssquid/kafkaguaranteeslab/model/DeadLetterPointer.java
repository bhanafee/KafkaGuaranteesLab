package com.maybeitssquid.kafkaguaranteeslab.model;

/**
 * Points at a record that a consumer gave up on. The record itself stays in its source topic. It is
 * found again by seeking to {@code offset} in {@code partition} of {@code topic}.
 *
 * @param topic source topic of the failed record
 * @param partition source partition of the failed record
 * @param offset offset of the failed record within the source partition
 * @param timestamp timestamp of the failed record, in epoch milliseconds
 * @param consumerGroup consumer group that failed to process the record, or null if unknown
 * @param exceptionClass class name of the root cause of the failure
 * @param exceptionMessage message of the root cause of the failure, which may be null. A fixed text
 *     when the payload could not be deserialized, because the parser's message quotes it
 */
public record DeadLetterPointer(
    String topic,
    int partition,
    long offset,
    long timestamp,
    String consumerGroup,
    String exceptionClass,
    String exceptionMessage) {}
