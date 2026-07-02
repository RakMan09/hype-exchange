package com.hypeexchange.streams.serde;

import com.hypeexchange.common.model.EventTimed;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Uses each record's embedded event-time ({@code ts}) rather than the broker/ingest
 * time, so windowing is by event time and late/out-of-order events are placed in the
 * window they belong to. Falls back to the record timestamp for anything unexpected.
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof EventTimed timed && timed.ts() > 0) {
            return timed.ts();
        }
        long recordTs = record.timestamp();
        return recordTs >= 0 ? recordTs : Math.max(0, partitionTime);
    }
}
